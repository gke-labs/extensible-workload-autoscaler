package core_cluster_metrics_provider

import (
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/labels"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
)

func (p *CoreClusterMetricsProvider) processExternalMetric(namespace string, m *pb.MetricDefinition, class *xasv1.MetricProviderClass) []*pb.MetricBatch {
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
		slog.Warn("Missing 'metric' parameter for external metric", "policyMetric", m.GetName())
		return nil
	}

	selectorStr := getParam("selector")
	var selector labels.Selector
	var err error
	if selectorStr != "" {
		selector, err = labels.Parse(selectorStr)
		if err != nil {
			slog.Error("Failed to parse selector for external metric", "metric", m.GetName(), "selector", selectorStr, "error", err)
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

		samples = append(samples, pb.MetricSample_builder{
			Name:            proto.String(m.GetName()),
			RecommenderName: proto.String(m.GetRecommenderName()),
			Labels:          item.MetricLabels,
			Value:           proto.Float64(val),
			Timestamp:       timestamppb.New(time.Unix(ts, 0)),
		}.Build())
	}

	if len(samples) > 0 {
		return []*pb.MetricBatch{pb.MetricBatch_builder{
			PodName: proto.String(""), // Global / External metric scope
			Samples: samples,
		}.Build()}
	}

	return nil
}
