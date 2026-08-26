package core_cluster_metrics_provider

import (
	"log/slog"
	"time"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func (p *CoreClusterMetricsProvider) processCustomMetric(
	namespace string,
	pol *pb.Policy,
	m *pb.MetricDefinition,
	class *xasv1.MetricProviderClass,
) []*pb.MetricBatch {
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
		slog.Warn("Missing 'metric' parameter for custom metric", "policyMetric", m.Name)
		return nil
	}

	if p.customMetricsClient == nil {
		slog.Error("Custom metrics client not initialized")
		return nil
	}

	explicitKind := getParam("targetKind")
	targetKind := explicitKind
	if targetKind == "" && pol.Workload != nil {
		targetKind = pol.Workload.Kind
	}
	if targetKind == "" {
		targetKind = "Pod"
	}

	targetName := getParam("targetName")
	if targetName == "" && pol.Workload != nil {
		targetName = pol.Workload.Name
	}

	targetGroup := getParam("targetGroup")
	if targetGroup == "" && explicitKind == "" && pol.Workload != nil {
		targetGroup = pol.Workload.Group
	}

	var metricSelector labels.Selector
	if selectorStr := getParam("selector"); selectorStr != "" {
		var err error
		metricSelector, err = labels.Parse(selectorStr)
		if err != nil {
			slog.Error("Failed to parse metric selector for custom metric", "metric", m.Name, "selector", selectorStr, "error", err)
			return nil
		}
	} else {
		metricSelector = labels.Everything()
	}

	// Handle Pod Metrics (scraped across individual pods matching workload label selector)
	if targetKind == "Pod" {
		selectorStr := getParam("podSelector")
		if selectorStr == "" {
			selectorStr = pol.Selector
		}

		var podSelector labels.Selector
		var err error
		if selectorStr != "" {
			podSelector, err = labels.Parse(selectorStr)
			if err != nil {
				slog.Error("Failed to parse pod label selector for custom pod metric", "metric", m.Name, "selector", selectorStr, "error", err)
				return nil
			}
		} else {
			podSelector = labels.Everything()
		}

		metricList, err := p.customMetricsClient.NamespacedMetrics(namespace).GetForObjects(
			schema.GroupKind{Group: "", Kind: "Pod"},
			podSelector,
			metricName,
			metricSelector,
		)
		if err != nil {
			slog.Error("Failed to query custom pod metrics API", "namespace", namespace, "metric", metricName, "error", err)
			return nil
		}

		var batches []*pb.MetricBatch
		for _, item := range metricList.Items {
			val := item.Value.AsApproximateFloat64()
			ts := item.Timestamp.Unix()
			if ts == 0 {
				ts = time.Now().Unix()
			}

			podName := item.DescribedObject.Name
			batches = append(batches, &pb.MetricBatch{
				EntityKey: podName,
				Samples: []*pb.MetricSample{
					{
						Name:      m.Name,
						Value:     val,
						Timestamp: ts,
					},
				},
			})
		}
		return batches
	}

	// Handle Object Metrics (attached to a single Kubernetes object like Service, Ingress, Deployment, Namespace)
	groupKind := schema.GroupKind{Group: targetGroup, Kind: targetKind}
	item, err := p.customMetricsClient.NamespacedMetrics(namespace).GetForObject(
		groupKind,
		targetName,
		metricName,
		metricSelector,
	)
	if err != nil {
		slog.Error("Failed to query custom object metric API", "namespace", namespace, "kind", targetKind, "name", targetName, "metric", metricName, "error", err)
		return nil
	}

	val := item.Value.AsApproximateFloat64()
	ts := item.Timestamp.Unix()
	if ts == 0 {
		ts = time.Now().Unix()
	}

	return []*pb.MetricBatch{
		{
			EntityKey: "",
			Samples: []*pb.MetricSample{
				{
					Name:      m.Name,
					Value:     val,
					Timestamp: ts,
				},
			},
		},
	}
}
