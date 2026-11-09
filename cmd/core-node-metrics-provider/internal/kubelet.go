package internal

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
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
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
		if val, ok := m.GetParams()[key]; ok {
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
	// container, each carrying that container's own sample. The Server
	// keeps the per-container breakdown in
	// ControlMetrics.pod_container_metrics[pod].container_metrics and derives
	// the pod-level value from the container samples.
	batches := make([]*pb.MetricBatch, 0, len(podStats.Containers))

	for cName, cStats := range podStats.Containers {
		sample, ok := a.containerSample(pod, cName, cStats, metricType, metricMode, m.GetName(), seenKeys)
		if !ok {
			continue
		}
		sample.SetName(m.GetName())
		sample.SetRecommenderName(m.GetRecommenderName())
		sample.SetResourceName(metricType)

		batches = append(batches, pb.MetricBatch_builder{
			PodName:       new(pod.Name),
			ContainerName: new(cName),
			Samples:       []*pb.MetricSample{sample},
		}.Build())
	}

	if len(batches) == 0 {
		return nil
	}

	// Map iteration order is random; sort for deterministic output.
	sort.Slice(batches, func(i, j int) bool { return batches[i].GetContainerName() < batches[j].GetContainerName() })
	return batches
}

// containerSample returns the sample of a single container for the given
// metric type and mode, reporting false when there is nothing to report.
//
// CPU usage is reported as a counter: the raw kubelet counter, i.e. the
// cumulative CPU time of the container in seconds, timestamped as measured by
// the kubelet. The Server derives the usage rate (cores) from consecutive
// samples, accounting for the resets of the counter (container restarts).
//
// Every other sample is a gauge of the current usage of the container (see
// containerUsage). A utilization is a per-container ratio: the container
// usage over the request of that very same container. A container without a
// request for the resource has no meaningful ratio, so it is skipped.
func (a *CoreNodeMetricsProvider) containerSample(pod corev1.Pod, containerName string, cStats *KubeletContainerMetrics, metricType, metricMode, metricName string, seenKeys map[string]bool) (*pb.MetricSample, bool) {
	if metricType == "cpu" && metricMode != "utilization" {
		if cStats.CPUUsageSeconds == nil {
			return nil, false
		}
		ts := time.Now()
		if cStats.Timestamp != nil {
			ts = time.UnixMilli(*cStats.Timestamp)
		}
		return pb.MetricSample_builder{
			Counter:   cStats.CPUUsageSeconds,
			Timestamp: timestamppb.New(ts),
		}.Build(), true
	}

	val, ts, ok := a.containerUsage(pod, containerName, cStats, metricType, metricName, seenKeys)
	if !ok {
		return nil, false
	}
	if metricMode == "utilization" {
		req, ok := getContainerRequest(&pod, containerName, metricType)
		if !ok || req <= 0 {
			return nil, false
		}
		val /= req
	}
	return pb.MetricSample_builder{
		Gauge:     new(val),
		Timestamp: timestamppb.New(time.Unix(ts, 0)),
	}.Build(), true
}

// containerUsage returns the current usage of a single container for the given
// metric type as a gauge, along with the timestamp of the sample (in seconds).
//
// For "cpu" the raw kubelet counter is converted into a rate (cores) using the
// previously scraped sample, so no value is reported on the very first scrape.
// This only serves the CPU utilization: the CPU usage itself is reported as a
// counter, see containerSample.
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
