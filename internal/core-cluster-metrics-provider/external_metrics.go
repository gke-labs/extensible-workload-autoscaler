package core_cluster_metrics_provider

import (
	"log/slog"
	"time"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func (p *CoreClusterMetricsProvider) processExternalMetric(namespace string, m *pb.MetricDefinition, class *xasv1.MetricProviderClass) []*pb.MetricBatch {
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
		slog.Warn("Missing 'metric' parameter for external metric", "policyMetric", m.Name)
		return nil
	}

	selectorStr := getParam("selector")
	var selector labels.Selector
	var err error
	if selectorStr != "" {
		selector, err = labels.Parse(selectorStr)
		if err != nil {
			slog.Error("Failed to parse selector for external metric", "metric", m.Name, "selector", selectorStr, "error", err)
			return nil
		}
	} else {
		selector = labels.Everything()
	}

	if p.externalMetricsClient == nil {
		slog.Error("External metrics client not initialized")
		return nil
	}

	metricList, err := p.externalMetricsClient.NamespacedMetrics(namespace).List(metricName, selector)
	if err != nil {
		slog.Error("Failed to query external metrics API", "namespace", namespace, "metric", metricName, "error", err)
		return nil
	}

	var samples []*pb.MetricSample
	for _, item := range metricList.Items {
		val := item.Value.AsApproximateFloat64()
		ts := item.Timestamp.Unix()
		if ts == 0 {
			ts = time.Now().Unix()
		}

		samples = append(samples, &pb.MetricSample{
			Name:      m.Name,
			Labels:    item.MetricLabels,
			Value:     val,
			Timestamp: ts,
		})
	}

	if len(samples) > 0 {
		return []*pb.MetricBatch{{
			PodName: "", // Global / External metric scope
			Samples: samples,
		}}
	}

	return nil
}
