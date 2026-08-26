package core_cluster_metrics_provider

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	customv1beta2 "k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
	customclient "k8s.io/metrics/pkg/client/custom_metrics"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1"
)

type fakeCustomClient struct {
	objectMetric  *customv1beta2.MetricValue
	objectMetrics *customv1beta2.MetricValueList
	err           error
}

func (f *fakeCustomClient) RootScopedMetrics() customclient.MetricsInterface {
	return f
}

func (f *fakeCustomClient) NamespacedMetrics(namespace string) customclient.MetricsInterface {
	return f
}

func (f *fakeCustomClient) GetForObject(groupKind schema.GroupKind, name string, metricName string, metricSelector labels.Selector) (*customv1beta2.MetricValue, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.objectMetric, nil
}

func (f *fakeCustomClient) GetForObjects(groupKind schema.GroupKind, selector labels.Selector, metricName string, metricSelector labels.Selector) (*customv1beta2.MetricValueList, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.objectMetrics, nil
}

func TestProcessCustomMetric(t *testing.T) {
	now := time.Unix(1720000000, 0)

	class := &xasv1.MetricProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: "k8s-custom-metrics"},
		Spec: xasv1.MetricProviderClassSpec{
			Type: "CustomMetrics",
		},
	}

	policy := &pb.Policy{
		Id: &pb.PolicyId{
			ClusterName: "default",
			Namespace:   "sample-custom-metrics",
			Name:        "custom-test-policy",
		},
		Workload: &pb.WorkloadRef{
			Group:     "apps",
			Version:   "v1",
			Kind:      "Deployment",
			Name:      "custom-metrics-consumer",
			Namespace: "sample-custom-metrics",
		},
		Selector: "app=custom-metrics-consumer",
	}

	t.Run("Pod Custom Metric Query (GetForObjects)", func(t *testing.T) {
		def := &pb.MetricDefinition{
			Name:     "pod_http_requests",
			Provider: "k8s-custom-metrics",
			Params: map[string]string{
				"targetKind": "Pod",
				"metric":     "http_requests_per_second",
			},
		}

		client := &fakeCustomClient{
			objectMetrics: &customv1beta2.MetricValueList{
				Items: []customv1beta2.MetricValue{
					{
						DescribedObject: corev1.ObjectReference{
							Kind: "Pod",
							Name: "custom-metrics-consumer-pod-1",
						},
						Timestamp: metav1.Time{Time: now},
						Value:     *resource.NewQuantity(85, resource.DecimalSI),
					},
				},
			},
		}

		provider := &CoreClusterMetricsProvider{
			customMetricsClient: client,
		}

		want := []*pb.MetricBatch{
			{
				EntityKey: "custom-metrics-consumer-pod-1",
				Samples: []*pb.MetricSample{
					{
						Name:      "pod_http_requests",
						Value:     85,
						Timestamp: 1720000000,
					},
				},
			},
		}

		got := provider.processCustomMetric(policy.Workload.Namespace, policy, def, class)

		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Errorf("processCustomMetric mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("Object Custom Metric Query (GetForObject)", func(t *testing.T) {
		def := &pb.MetricDefinition{
			Name:     "service_http_requests",
			Provider: "k8s-custom-metrics",
			Params: map[string]string{
				"targetKind": "Service",
				"targetName": "frontend-service",
				"metric":     "http_requests_per_second",
			},
		}

		client := &fakeCustomClient{
			objectMetric: &customv1beta2.MetricValue{
				DescribedObject: corev1.ObjectReference{
					Kind: "Service",
					Name: "frontend-service",
				},
				Timestamp: metav1.Time{Time: now},
				Value:     *resource.NewQuantity(450, resource.DecimalSI),
			},
		}

		provider := &CoreClusterMetricsProvider{
			customMetricsClient: client,
		}

		want := []*pb.MetricBatch{
			{
				EntityKey: "",
				Samples: []*pb.MetricSample{
					{
						Name:      "service_http_requests",
						Value:     450,
						Timestamp: 1720000000,
					},
				},
			},
		}

		got := provider.processCustomMetric(policy.Workload.Namespace, policy, def, class)

		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Errorf("processCustomMetric mismatch (-want +got):\n%s", diff)
		}
	})
}
