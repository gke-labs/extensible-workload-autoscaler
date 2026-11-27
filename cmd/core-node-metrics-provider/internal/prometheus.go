package internal

import (
	"fmt"
	"log/slog"
	"strings"

	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
)

// familySamples returns the samples of the whitelisted metric names found in
// the metric families.
func familySamples(families map[string]*dto.MetricFamily, whitelist []string) []*pb.MetricSample {
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
					samples = append(samples, pb.MetricSample_builder{
						Name:   new(name),
						Labels: labels,
						Gauge:  new(m.Gauge.GetValue()),
					}.Build())
				}
			case dto.MetricType_COUNTER:
				if m.Counter != nil && wantBase {
					samples = append(samples, pb.MetricSample_builder{
						Name:    new(name),
						Labels:  labels,
						Counter: new(m.Counter.GetValue()),
					}.Build())
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

						samples = append(samples, pb.MetricSample_builder{
							Name:   new(name),
							Labels: labels,
							Histogram: pb.Histogram_builder{
								Buckets: buckets,
								Sum:     new(sum),
								Count:   new(count),
							}.Build(),
						}.Build())
					}

					// The sum and the count of the observations of a
					// histogram are cumulative, like counters: they only
					// increase (the sum, as long as no observation is
					// negative), except when the source resets them.
					if wantSum {
						samples = append(samples, pb.MetricSample_builder{
							Name:    new(name + "_sum"),
							Labels:  labels,
							Counter: new(m.Histogram.GetSampleSum()),
						}.Build())
					}

					if wantCount {
						samples = append(samples, pb.MetricSample_builder{
							Name:    new(name + "_count"),
							Labels:  labels,
							Counter: new(float64(m.Histogram.GetSampleCount())),
						}.Build())
					}
				}
			}
		}
	}
	return samples
}

func (a *CoreNodeMetricsProvider) processPrometheusMetric(pod corev1.Pod, m *pb.MetricDefinition, class *xasv1.MetricProviderClass) []*pb.MetricBatch {
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

	metricName := getParam("metric")
	if metricName == "" {
		return nil
	}
	port := getParam("port")
	path := getParam("path")

	if port == "" {
		port = "8080"
	}
	if path == "" {
		path = "/metrics"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	ip := pod.Status.PodIP
	url := fmt.Sprintf("http://%s:%s%s", ip, port, path)
	families, err := scrapeEndpoint(a.httpClient, url, "", false)
	if err != nil {
		slog.Error("Failed to scrape prometheus metric from pod", "pod", pod.Name, "ip", ip, "port", port, "path", path, "metric", metricName, "error", err)
		return nil
	}
	return prometheusExporter{}.Export(families, pod, m, class)
}

// prometheusExporter exports the metrics scraped from the Prometheus endpoint
// of a pod.
type prometheusExporter struct{}

func (prometheusExporter) Export(families map[string]*dto.MetricFamily, pod corev1.Pod, m *pb.MetricDefinition, class *xasv1.MetricProviderClass) []*pb.MetricBatch {
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

	samples := familySamples(families, []string{getParam("metric")})
	for i := range samples {
		samples[i].SetName(m.GetName())
		samples[i].SetRecommenderName(m.GetRecommenderName())
	}

	if len(samples) > 0 {
		return []*pb.MetricBatch{pb.MetricBatch_builder{
			PodName: new(pod.Name),
			Samples: samples,
		}.Build()}
	}
	return nil
}
