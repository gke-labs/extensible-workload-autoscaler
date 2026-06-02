package core_node_metrics_provider

import (
	"fmt"
	"log/slog"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	corev1 "k8s.io/api/core/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1"
)

func (a *CoreNodeMetricsProvider) scrapePod(ip string, port string, path string, whitelist []string) ([]*pb.MetricSample, error) {
	if port == "" {
		port = "8080"
	}
	if path == "" {
		path = "/metrics"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	url := fmt.Sprintf("http://%s:%s%s", ip, port, path)

	resp, err := a.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, err
	}

	var samples []*pb.MetricSample

	for name, fam := range families {
		wantBase := false
		wantSum := false
		wantCount := false

		for _, w := range whitelist {
			if w == name {
				wantBase = true
			}
			if w == name+"_sum" {
				wantSum = true
			}
			if w == name+"_count" {
				wantCount = true
			}
			if w == name+"_bucket" {
				wantBase = true
			}
		}

		if !wantBase && !wantSum && !wantCount {
			continue
		}

		for _, m := range fam.Metric {
			labels := make(map[string]string)
			for _, pair := range m.Label {
				labels[pair.GetName()] = pair.GetValue()
			}

			switch fam.GetType() {
			case dto.MetricType_GAUGE:
				if m.Gauge != nil && wantBase {
					samples = append(samples, &pb.MetricSample{
						Name:   name,
						Labels: labels,
						Value:  m.Gauge.GetValue(),
					})
				}
			case dto.MetricType_COUNTER:
				if m.Counter != nil && wantBase {
					samples = append(samples, &pb.MetricSample{
						Name:   name,
						Labels: labels,
						Value:  m.Counter.GetValue(),
					})
				}
			case dto.MetricType_HISTOGRAM:
				if m.Histogram != nil {
					if wantBase {
						buckets := make(map[string]uint64)
						for _, b := range m.Histogram.Bucket {
							le := fmt.Sprintf("%g", b.GetUpperBound())
							buckets[le] = b.GetCumulativeCount()
						}

						sum := m.Histogram.GetSampleSum()
						count := m.Histogram.GetSampleCount()

						samples = append(samples, &pb.MetricSample{
							Name:             name,
							Labels:           labels,
							HistogramBuckets: buckets,
							HistogramSum:     sum,
							HistogramCount:   count,
						})
					}

					if wantSum {
						samples = append(samples, &pb.MetricSample{
							Name:   name + "_sum",
							Labels: labels,
							Value:  m.Histogram.GetSampleSum(),
						})
					}

					if wantCount {
						samples = append(samples, &pb.MetricSample{
							Name:   name + "_count",
							Labels: labels,
							Value:  float64(m.Histogram.GetSampleCount()),
						})
					}
				}
			}
		}
	}
	return samples, nil
}

func (a *CoreNodeMetricsProvider) processPrometheusMetric(pod corev1.Pod, m *pb.MetricDefinition, class *xasv1.MetricProviderClass) []*pb.MetricBatch {
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

	metricName := getParam("metric")
	if metricName == "" {
		return nil
	}
	port := getParam("port")
	path := getParam("path")

	ip := pod.Status.PodIP
	samples, err := a.scrapePod(ip, port, path, []string{metricName})
	if err != nil {
		slog.Error("Failed to scrape prometheus metric from pod", "pod", pod.Name, "ip", ip, "port", port, "path", path, "metric", metricName, "error", err)
		return nil
	}

	for i := range samples {
		samples[i].Name = m.Name
	}

	if len(samples) > 0 {
		return []*pb.MetricBatch{{
			EntityKey: pod.Name,
			Samples:   samples,
		}}
	}
	return nil
}
