package core_node_metrics_provider

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1"
)

func TestProcessKubeletMetric(t *testing.T) {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "main",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"), // 0.1 cores
							corev1.ResourceMemory: resource.MustParse("100Mi"),
						},
					},
				},
				{
					Name: "sidecar",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("50m"), // 0.05 cores
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name     string
		def      *pb.MetricDefinition
		class    *xasv1.MetricProviderClass
		metrics1 *KubeletPodMetrics
		metrics2 *KubeletPodMetrics // T+10s
		want     []*pb.MetricBatch
	}{
		{
			name: "CPU Rate (Cores) - Aggregated",
			def: &pb.MetricDefinition{
				Name:     "cpu_cores",
				Provider: "kubelet",
				Params:   map[string]string{"type": "cpu"},
			},
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main":    {CPUUsageSeconds: floatPtr(10.0), Timestamp: intPtr(1000000000)},
				"sidecar": {CPUUsageSeconds: floatPtr(5.0), Timestamp: intPtr(1000000000)},
			}},
			metrics2: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				// Main: +1.0s in 10s -> 0.1 cores.
				// Sidecar: +0.5s in 10s -> 0.05 cores.
				// Total: 0.15 cores.
				"main":    {CPUUsageSeconds: floatPtr(11.0), Timestamp: intPtr(1000010000)},
				"sidecar": {CPUUsageSeconds: floatPtr(5.5), Timestamp: intPtr(1000010000)},
			}},
			want: []*pb.MetricBatch{{
				EntityKey: "test-pod",
				Samples:   []*pb.MetricSample{{Name: "cpu_cores", Value: 0.15, Timestamp: 1000010}},
			}},
		},
		{
			name: "CPU Utilization (%) - From Class",
			def: &pb.MetricDefinition{
				Name:     "cpu_util",
				Provider: "kubelet",
				Params:   map[string]string{"type": "cpu"},
			},
			class: &xasv1.MetricProviderClass{
				Spec: xasv1.MetricProviderClassSpec{
					Config: map[string]string{"mode": "utilization"},
				},
			},
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main":    {CPUUsageSeconds: floatPtr(10.0), Timestamp: intPtr(1000000000)},
				"sidecar": {CPUUsageSeconds: floatPtr(5.0), Timestamp: intPtr(1000000000)},
			}},
			metrics2: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				// Total Used: 0.15. Total Req: 0.15. -> 100%.
				"main":    {CPUUsageSeconds: floatPtr(11.0), Timestamp: intPtr(1000010000)},
				"sidecar": {CPUUsageSeconds: floatPtr(5.5), Timestamp: intPtr(1000010000)},
			}},
			want: []*pb.MetricBatch{{
				EntityKey: "test-pod",
				Samples:   []*pb.MetricSample{{Name: "cpu_util", Value: 1.0, Timestamp: 1000010}},
			}},
		},
		{
			name: "Memory Bytes",
			def: &pb.MetricDefinition{
				Name:     "mem_bytes",
				Provider: "kubelet",
				Params:   map[string]string{"type": "memory"},
			},
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main": {MemoryBytes: floatPtr(1024 * 1024), Timestamp: intPtr(1000000000)}, // 1Mi
			}},
			want: []*pb.MetricBatch{{
				EntityKey: "test-pod",
				Samples:   []*pb.MetricSample{{Name: "mem_bytes", Value: 1048576, Timestamp: 1000000}},
			}},
		},
		{
			name: "Memory Utilization (%)",
			def: &pb.MetricDefinition{
				Name:     "mem_util",
				Provider: "kubelet",
				Params:   map[string]string{"type": "memory", "mode": "utilization"},
			},
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				// Request 100Mi. Usage 50Mi. Util = 50%.
				"main": {MemoryBytes: floatPtr(50 * 1024 * 1024), Timestamp: intPtr(1000000000)},
			}},
			want: []*pb.MetricBatch{{
				EntityKey: "test-pod",
				Samples:   []*pb.MetricSample{{Name: "mem_util", Value: 0.5, Timestamp: 1000000}},
			}},
		},
		{
			name: "Specific Container Filter",
			def: &pb.MetricDefinition{
				Name:     "sidecar_cpu",
				Provider: "kubelet",
				Params:   map[string]string{"type": "cpu", "container": "sidecar"},
			},
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main":    {CPUUsageSeconds: floatPtr(10.0), Timestamp: intPtr(1000000000)},
				"sidecar": {CPUUsageSeconds: floatPtr(5.0), Timestamp: intPtr(1000000000)},
			}},
			metrics2: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main":    {CPUUsageSeconds: floatPtr(20.0), Timestamp: intPtr(1000010000)},
				"sidecar": {CPUUsageSeconds: floatPtr(5.1), Timestamp: intPtr(1000010000)},
			}},
			want: []*pb.MetricBatch{{
				EntityKey: "test-pod",
				Samples:   []*pb.MetricSample{{Name: "sidecar_cpu", Value: 0.01, Timestamp: 1000010}},
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := &CoreNodeMetricsProvider{
				lastSamples: make(map[string]SampleState),
			}
			seen := make(map[string]bool)

			getParam := func(m *pb.MetricDefinition, class *xasv1.MetricProviderClass, key string) string {
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

			port := getParam(tc.def, tc.class, "port")
			path := getParam(tc.def, tc.class, "path")
			cfgKey := fmt.Sprintf("%s|%s", port, path)

			// T0 (Initialization for Rate)
			if tc.metrics2 != nil {
				metricsMap := map[string]map[string]*KubeletPodMetrics{
					cfgKey: {"default/test-pod": tc.metrics1},
				}
				provider.processKubeletMetric(pod, tc.def, metricsMap, seen, tc.class)
			}

			// T1 (Actual Check)
			finalMetrics := tc.metrics2
			if finalMetrics == nil {
				finalMetrics = tc.metrics1
			}

			metricsMap := map[string]map[string]*KubeletPodMetrics{
				cfgKey: {"default/test-pod": finalMetrics},
			}

			got := provider.processKubeletMetric(pod, tc.def, metricsMap, seen, tc.class)

			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
				t.Errorf("Mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func floatPtr(v float64) *float64 { return &v }
func intPtr(v int64) *int64       { return &v }
