package internal

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
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
			// CPU usage is the raw kubelet counter (cumulative CPU seconds),
			// reported as is: the Server derives the usage rate (cores) from
			// consecutive samples. Nothing is computed from the previous
			// scrape: its samples were reported on their own.
			name: "CPU Usage (Counter) - Per Container",
			def: pb.MetricDefinition_builder{
				Name:     new("cpu_usage"),
				Provider: new("kubelet"),
				Params:   map[string]string{"type": "cpu", "mode": "usage"},
			}.Build(),
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main":    {CPUUsageSeconds: new(10.0), Timestamp: new(int64(1000000000))},
				"sidecar": {CPUUsageSeconds: new(5.0), Timestamp: new(int64(1000000000))},
			}},
			metrics2: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main":    {CPUUsageSeconds: new(11.0), Timestamp: new(int64(1000010000))},
				"sidecar": {CPUUsageSeconds: new(5.5), Timestamp: new(int64(1000010000))},
			}},
			want: []*pb.MetricBatch{
				pb.MetricBatch_builder{
					PodName:       new("test-pod"),
					ContainerName: new("main"),
					Samples:       []*pb.MetricSample{pb.MetricSample_builder{Name: new("cpu_usage"), ResourceName: new("cpu"), Counter: new(11.0), Timestamp: timestamppb.New(time.UnixMilli(1000010000))}.Build()},
				}.Build(),
				pb.MetricBatch_builder{
					PodName:       new("test-pod"),
					ContainerName: new("sidecar"),
					Samples:       []*pb.MetricSample{pb.MetricSample_builder{Name: new("cpu_usage"), ResourceName: new("cpu"), Counter: new(5.5), Timestamp: timestamppb.New(time.UnixMilli(1000010000))}.Build()},
				}.Build(),
			},
		},
		{
			// Unlike a rate, a counter needs no previous sample: it is
			// reported from the very first scrape. The kubelet timestamp is
			// kept to the millisecond, as the rate the Server derives is only
			// as accurate as the time elapsed between two samples.
			name: "CPU Usage (Counter) - First Scrape",
			def: pb.MetricDefinition_builder{
				Name:     new("cpu_usage"),
				Provider: new("kubelet"),
				Params:   map[string]string{"type": "cpu"}, // Usage is the default mode.
			}.Build(),
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main": {CPUUsageSeconds: new(10.25), Timestamp: new(int64(1000000250))},
			}},
			want: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName:       new("test-pod"),
				ContainerName: new("main"),
				Samples:       []*pb.MetricSample{pb.MetricSample_builder{Name: new("cpu_usage"), ResourceName: new("cpu"), Counter: new(10.25), Timestamp: timestamppb.New(time.UnixMilli(1000000250))}.Build()},
			}.Build()},
		},
		{
			// A counter reset (container restart) is reported as is too: the
			// Server accounts for it when it derives the rate.
			name: "CPU Usage (Counter) - Reset",
			def: pb.MetricDefinition_builder{
				Name:     new("cpu_usage"),
				Provider: new("kubelet"),
				Params:   map[string]string{"type": "cpu", "mode": "usage"},
			}.Build(),
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main": {CPUUsageSeconds: new(10.0), Timestamp: new(int64(1000000000))},
			}},
			metrics2: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main": {CPUUsageSeconds: new(0.5), Timestamp: new(int64(1000010000))},
			}},
			want: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName:       new("test-pod"),
				ContainerName: new("main"),
				Samples:       []*pb.MetricSample{pb.MetricSample_builder{Name: new("cpu_usage"), ResourceName: new("cpu"), Counter: new(0.5), Timestamp: timestamppb.New(time.UnixMilli(1000010000))}.Build()},
			}.Build()},
		},
		{
			// The CPU utilization is unchanged: a gauge, the usage rate since
			// the previous scrape over the container request.
			name: "CPU Utilization (%) - First Scrape",
			def: pb.MetricDefinition_builder{
				Name:     new("cpu_util"),
				Provider: new("kubelet"),
				Params:   map[string]string{"type": "cpu", "mode": "utilization"},
			}.Build(),
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main": {CPUUsageSeconds: new(10.0), Timestamp: new(int64(1000000000))},
			}},
			want: nil, // A rate needs a previous sample.
		},
		{
			name: "CPU Utilization (%) - From Class",
			def: pb.MetricDefinition_builder{
				Name:     new("cpu_util"),
				Provider: new("kubelet"),
				Params:   map[string]string{"type": "cpu"},
			}.Build(),
			class: &xasv1.MetricProviderClass{
				Spec: xasv1.MetricProviderClassSpec{
					Config: map[string]string{"mode": "utilization"},
				},
			},
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main":    {CPUUsageSeconds: new(10.0), Timestamp: new(int64(1000000000))},
				"sidecar": {CPUUsageSeconds: new(5.0), Timestamp: new(int64(1000000000))},
			}},
			metrics2: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				// Each container is rated against its own request:
				// main    0.1 cores / 0.1 request  -> 100%.
				// sidecar 0.05 cores / 0.05 request -> 100%.
				"main":    {CPUUsageSeconds: new(11.0), Timestamp: new(int64(1000010000))},
				"sidecar": {CPUUsageSeconds: new(5.5), Timestamp: new(int64(1000010000))},
			}},
			want: []*pb.MetricBatch{
				pb.MetricBatch_builder{
					PodName:       new("test-pod"),
					ContainerName: new("main"),
					Samples:       []*pb.MetricSample{pb.MetricSample_builder{Name: new("cpu_util"), ResourceName: new("cpu"), Gauge: new(1.0), Timestamp: timestamppb.New(time.Unix(1000010, 0))}.Build()},
				}.Build(),
				pb.MetricBatch_builder{
					PodName:       new("test-pod"),
					ContainerName: new("sidecar"),
					Samples:       []*pb.MetricSample{pb.MetricSample_builder{Name: new("cpu_util"), ResourceName: new("cpu"), Gauge: new(1.0), Timestamp: timestamppb.New(time.Unix(1000010, 0))}.Build()},
				}.Build(),
			},
		},
		{
			name: "Memory Bytes",
			def: pb.MetricDefinition_builder{
				Name:     new("mem_bytes"),
				Provider: new("kubelet"),
				Params:   map[string]string{"type": "memory"},
			}.Build(),
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main": {MemoryBytes: new(float64(1024 * 1024)), Timestamp: new(int64(1000000000))}, // 1Mi
			}},
			want: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName:       new("test-pod"),
				ContainerName: new("main"),
				Samples:       []*pb.MetricSample{pb.MetricSample_builder{Name: new("mem_bytes"), ResourceName: new("memory"), Gauge: new(float64(1048576)), Timestamp: timestamppb.New(time.Unix(1000000, 0))}.Build()},
			}.Build()},
		},
		{
			name: "Memory Utilization (%)",
			def: pb.MetricDefinition_builder{
				Name:     new("mem_util"),
				Provider: new("kubelet"),
				Params:   map[string]string{"type": "memory", "mode": "utilization"},
			}.Build(),
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				// Request 100Mi. Usage 50Mi. Util = 50%.
				"main": {MemoryBytes: new(float64(50 * 1024 * 1024)), Timestamp: new(int64(1000000000))},
			}},
			want: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName:       new("test-pod"),
				ContainerName: new("main"),
				Samples:       []*pb.MetricSample{pb.MetricSample_builder{Name: new("mem_util"), ResourceName: new("memory"), Gauge: new(0.5), Timestamp: timestamppb.New(time.Unix(1000000, 0))}.Build()},
			}.Build()},
		},
		{
			// The utilization rate is computed per container, each against its
			// own request.
			name: "Uneven Per-Container Utilization",
			def: pb.MetricDefinition_builder{
				Name:     new("cpu_util_per_container"),
				Provider: new("kubelet"),
				Params:   map[string]string{"type": "cpu", "mode": "utilization"},
			}.Build(),
			metrics1: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				"main":    {CPUUsageSeconds: new(10.0), Timestamp: new(int64(1000000000))},
				"sidecar": {CPUUsageSeconds: new(5.0), Timestamp: new(int64(1000000000))},
			}},
			metrics2: &KubeletPodMetrics{Containers: map[string]*KubeletContainerMetrics{
				// Main: +10s in 10s -> 1 core / 0.1 request -> 1000%.
				// Sidecar: +0.1s in 10s -> 0.01 cores / 0.05 request -> 20%.
				"main":    {CPUUsageSeconds: new(20.0), Timestamp: new(int64(1000010000))},
				"sidecar": {CPUUsageSeconds: new(5.1), Timestamp: new(int64(1000010000))},
			}},
			want: []*pb.MetricBatch{
				pb.MetricBatch_builder{
					PodName:       new("test-pod"),
					ContainerName: new("main"),
					Samples:       []*pb.MetricSample{pb.MetricSample_builder{Name: new("cpu_util_per_container"), ResourceName: new("cpu"), Gauge: new(10.0), Timestamp: timestamppb.New(time.Unix(1000010, 0))}.Build()},
				}.Build(),
				pb.MetricBatch_builder{
					PodName:       new("test-pod"),
					ContainerName: new("sidecar"),
					Samples:       []*pb.MetricSample{pb.MetricSample_builder{Name: new("cpu_util_per_container"), ResourceName: new("cpu"), Gauge: new(0.2), Timestamp: timestamppb.New(time.Unix(1000010, 0))}.Build()},
				}.Build(),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := &CoreNodeMetricsProvider{
				lastSamples: make(map[string]SampleState),
			}
			seen := make(map[string]bool)

			getParam := func(m *pb.MetricDefinition, class *xasv1.MetricProviderClass, key string) string {
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

			port := getParam(tc.def, tc.class, "port")
			path := getParam(tc.def, tc.class, "path")
			cfgKey := fmt.Sprintf("%s|%s", port, path)

			// T0: a previous scrape, which a rate (utilization) needs. The
			// samples it reports are not checked.
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

			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
				t.Errorf("Mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
