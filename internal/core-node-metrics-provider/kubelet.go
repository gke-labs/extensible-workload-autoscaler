package core_node_metrics_provider

import (
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	corev1 "k8s.io/api/core/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1"
)

type KubeletPodMetrics struct {
	Containers map[string]*KubeletContainerMetrics
}

type KubeletContainerMetrics struct {
	CPUUsageSeconds *float64
	MemoryBytes     *float64
	Timestamp       *int64 // Milliseconds
}

func (a *CoreNodeMetricsProvider) scrapeKubelet(port, path string) map[string]*KubeletPodMetrics {
	if a.hostIP == "" {
		return nil
	}

	if port == "" {
		port = "10250"
	}
	if path == "" {
		path = "/metrics/resource"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	url := fmt.Sprintf("https://%s:%s%s", a.hostIP, port, path)

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+a.token)

	resp, err := a.kubeletClient.Do(req)
	if err != nil {
		slog.Error("Failed to scrape kubelet", "error", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		slog.Warn("Kubelet returned error", "statusCode", resp.StatusCode)
		return nil
	}

	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		slog.Error("Failed to parse kubelet metrics", "error", err)
		return nil
	}

	result := make(map[string]*KubeletPodMetrics)

	if fam, ok := families["container_cpu_usage_seconds_total"]; ok {
		for _, m := range fam.Metric {
			ns := getLabel(m, "namespace")
			pod := getLabel(m, "pod")
			container := getLabel(m, "container")
			if ns == "" || pod == "" || container == "" {
				continue
			}

			key := ns + "/" + pod
			if result[key] == nil {
				result[key] = &KubeletPodMetrics{Containers: make(map[string]*KubeletContainerMetrics)}
			}
			if result[key].Containers[container] == nil {
				result[key].Containers[container] = &KubeletContainerMetrics{}
			}

			val := m.Counter.GetValue()
			result[key].Containers[container].CPUUsageSeconds = &val
			if m.TimestampMs != nil {
				ts := *m.TimestampMs
				result[key].Containers[container].Timestamp = &ts
			}
		}
	}

	if fam, ok := families["container_memory_working_set_bytes"]; ok {
		for _, m := range fam.Metric {
			ns := getLabel(m, "namespace")
			pod := getLabel(m, "pod")
			container := getLabel(m, "container")
			if ns == "" || pod == "" || container == "" {
				continue
			}

			key := ns + "/" + pod
			if result[key] == nil {
				result[key] = &KubeletPodMetrics{Containers: make(map[string]*KubeletContainerMetrics)}
			}
			if result[key].Containers[container] == nil {
				result[key].Containers[container] = &KubeletContainerMetrics{}
			}

			val := m.Gauge.GetValue()
			result[key].Containers[container].MemoryBytes = &val
			if m.TimestampMs != nil {
				ts := *m.TimestampMs
				result[key].Containers[container].Timestamp = &ts
			}
		}
	}

	return result
}

func getLabel(m *dto.Metric, name string) string {
	for _, pair := range m.Label {
		if pair.GetName() == name {
			return pair.GetValue()
		}
	}
	return ""
}

func (a *CoreNodeMetricsProvider) processKubeletMetric(pod corev1.Pod, m *pb.MetricDefinition, kubeletMetrics map[string]map[string]*KubeletPodMetrics, seenKeys map[string]bool, class *xasv1.MetricProviderClass) []*pb.MetricBatch {
	getParam := func(key string) string {
		if val, ok := m.Params[key]; ok {
			return val
		}
		if class != nil {
			if val, ok := class.Spec.Config[key]; ok {
				return val
			}
		}
		return ""
	}

	port := getParam("port")
	path := getParam("path")
	cfgKey := fmt.Sprintf("%s|%s", port, path)

	podStatsMap, ok := kubeletMetrics[cfgKey]
	if !ok {
		return nil
	}

	key := pod.Namespace + "/" + pod.Name
	podStats, ok := podStatsMap[key]
	if !ok {
		return nil
	}

	metricType := getParam("type")
	metricMode := getParam("mode")

	// Everything is computed and emitted per container: one batch per
	// container, each carrying that container's own sample. The Control Plane
	// keeps the per-container breakdown in
	// ControlMetrics.pod_metrics[pod].container_metrics and derives the
	// pod-level value from the container samples.
	batches := make([]*pb.MetricBatch, 0, len(podStats.Containers))

	for cName, cStats := range podStats.Containers {
		val, ts, ok := a.containerUsage(pod, cName, cStats, metricType, m.Name, seenKeys)
		if !ok {
			continue
		}

		if metricMode == "utilization" {
			// Utilization is a per-container ratio: the container usage over
			// the request of that very same container. A container without a
			// request for this resource has no meaningful ratio, so it is
			// skipped.
			req, ok := getContainerRequest(&pod, cName, metricType)
			if !ok || req <= 0 {
				continue
			}
			val /= req
		}

		batches = append(batches, &pb.MetricBatch{
			PodName:       pod.Name,
			ContainerName: cName,
			Samples: []*pb.MetricSample{
				{Name: m.Name, Value: val, Timestamp: ts},
			},
		})
	}

	if len(batches) == 0 {
		return nil
	}

	// Map iteration order is random; sort for deterministic output.
	sort.Slice(batches, func(i, j int) bool { return batches[i].ContainerName < batches[j].ContainerName })
	return batches
}

// containerUsage returns the current usage of a single container for the given
// metric type, along with the timestamp of the sample (in seconds).
//
// For "cpu" the raw kubelet counter is converted into a rate (cores) using the
// previously scraped sample, so no value is reported on the very first scrape.
// For "memory" the working set gauge is reported as is (bytes).
func (a *CoreNodeMetricsProvider) containerUsage(pod corev1.Pod, containerName string, cStats *KubeletContainerMetrics, metricType, metricName string, seenKeys map[string]bool) (float64, int64, bool) {
	timestamp := func() int64 {
		if cStats.Timestamp != nil {
			return *cStats.Timestamp / 1000
		}
		return time.Now().Unix()
	}

	switch {
	case metricType == "cpu" && cStats.CPUUsageSeconds != nil:
		rawVal := *cStats.CPUUsageSeconds
		ts := timestamp()

		stateKey := fmt.Sprintf("%s/%s/%s/cpu/%s", pod.Namespace, pod.Name, containerName, metricName)
		seenKeys[stateKey] = true

		last, ok := a.lastSamples[stateKey]
		a.lastSamples[stateKey] = SampleState{Timestamp: ts, Value: rawVal}

		if !ok || ts <= last.Timestamp {
			return 0, 0, false
		}

		diff := rawVal - last.Value
		if diff < 0 {
			// Counter reset (container restart): treat the current value as
			// the delta.
			diff = rawVal
		}
		return diff / float64(ts-last.Timestamp), ts, true

	case metricType == "memory" && cStats.MemoryBytes != nil:
		return *cStats.MemoryBytes, timestamp(), true
	}

	return 0, 0, false
}

func getContainerRequest(pod *corev1.Pod, containerName, metricType string) (float64, bool) {
	for _, c := range pod.Spec.Containers {
		if c.Name == containerName {
			if metricType == "cpu" {
				if q, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
					return float64(q.MilliValue()) / 1000.0, true
				}
			} else if metricType == "memory" {
				if q, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
					return float64(q.Value()), true
				}
			}
			return 0, false
		}
	}
	return 0, false
}
