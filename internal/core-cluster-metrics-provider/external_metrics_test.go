package core_cluster_metrics_provider

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	externalv1beta1 "k8s.io/metrics/pkg/apis/external_metrics/v1beta1"
	externalclient "k8s.io/metrics/pkg/client/external_metrics"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1"
)

type fakeExternalClient struct {
	items []externalv1beta1.ExternalMetricValue
	err   error
}

func (f *fakeExternalClient) NamespacedMetrics(namespace string) externalclient.MetricsInterface {
	return f
}

func (f *fakeExternalClient) List(metricName string, metricSelector labels.Selector) (*externalv1beta1.ExternalMetricValueList, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &externalv1beta1.ExternalMetricValueList{Items: f.items}, nil
}

func TestProcessExternalMetric(t *testing.T) {
	now := time.Unix(1720000000, 0)

	class := &xasv1.MetricProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: "k8s-external-metrics"},
		Spec: xasv1.MetricProviderClassSpec{
			Type: "ExternalMetrics",
		},
	}

	tests := []struct {
		name      string
		namespace string
		def       *pb.MetricDefinition
		items     []externalv1beta1.ExternalMetricValue
		want      []*pb.MetricBatch
	}{
		{
			name:      "Valid External Metric Query",
			namespace: "default",
			def: &pb.MetricDefinition{
				Name:     "queue_depth",
				Provider: "k8s-external-metrics",
				Params: map[string]string{
					"metric":   "pubsub_messages",
					"selector": "sub=task-sub",
				},
			},
			items: []externalv1beta1.ExternalMetricValue{
				{
					MetricName:   "pubsub_messages",
					MetricLabels: map[string]string{"sub": "task-sub"},
					Timestamp:    metav1.Time{Time: now},
					Value:        *resource.NewQuantity(150, resource.DecimalSI),
				},
			},
			want: []*pb.MetricBatch{
				{
					PodName: "",
					Samples: []*pb.MetricSample{
						{
							Name:      "queue_depth",
							Labels:    map[string]string{"sub": "task-sub"},
							Value:     150,
							Timestamp: 1720000000,
						},
					},
				},
			},
		},
		{
			name:      "Missing Metric Param",
			namespace: "default",
			def: &pb.MetricDefinition{
				Name:     "queue_depth",
				Provider: "k8s-external-metrics",
				Params:   map[string]string{},
			},
			items: nil,
			want:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeExternalClient{items: tc.items}

			provider := &CoreClusterMetricsProvider{
				externalMetricsClient: client,
			}

			got := provider.processExternalMetric(tc.namespace, tc.def, class)

			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("processExternalMetric mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
