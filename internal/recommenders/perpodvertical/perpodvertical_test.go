package perpodvertical

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

// podContainers builds a PodContainerMetrics entry: container name -> metric
// name -> value.
func podContainers(byContainer map[string]map[string]float64) *pb.ContainerMetrics {
	cm := &pb.ContainerMetrics{ContainerMetrics: make(map[string]*pb.MetricValues)}
	for name, values := range byContainer {
		cm.ContainerMetrics[name] = &pb.MetricValues{Values: values}
	}
	return cm
}

func TestPerPodVerticalRecommender(t *testing.T) {
	rec := &PerPodVerticalRecommender{}

	tests := []struct {
		name     string
		def      *pb.RecommenderDefinition
		state    *pb.ControlMetrics
		wantVote *pb.Recommendation
	}{
		{
			name: "Basic CPU recommendation with safety margin",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"metric":       "pod_cpu",
					"target":       "0.8",
					"safetyMargin": "1.2",
					"container":    "main",
				},
			},
			state: &pb.ControlMetrics{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod1": podContainers(map[string]map[string]float64{"main": {"pod_cpu": 0.4}}), // 0.4 * (1.2 / 0.8) = 0.6 -> 600m
					"pod2": podContainers(map[string]map[string]float64{"main": {"pod_cpu": 0.8}}), // 0.8 * (1.2 / 0.8) = 1.2 -> 1200m
				},
			},
			wantVote: &pb.Recommendation{
				IsActive: true,
				PodContainerResources: []*pb.PodContainerResource{
					{
						PodName: "pod1",
						ContainerResources: &pb.ContainerResource{
							ContainerName: "main",
							Requests:      map[string]string{"cpu": "600m"},
							Limits:        map[string]string{},
						},
					},
					{
						PodName: "pod2",
						ContainerResources: &pb.ContainerResource{
							ContainerName: "main",
							Requests:      map[string]string{"cpu": "1200m"},
							Limits:        map[string]string{},
						},
					},
				},
			},
		},
		{
			name: "Only the target container's usage is used, not its neighbors'",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"metric":    "pod_cpu",
					"target":    "1.0",
					"container": "main",
				},
			},
			state: &pb.ControlMetrics{
				// The pod-level value mixes main and sidecar and must be ignored.
				PodMetrics: map[string]*pb.MetricValues{
					"pod1": {Values: map[string]float64{"pod_cpu": 0.75}},
				},
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod1": podContainers(map[string]map[string]float64{
						"main":    {"pod_cpu": 1.0},
						"sidecar": {"pod_cpu": 0.5},
					}),
				},
			},
			wantVote: &pb.Recommendation{
				IsActive: true,
				PodContainerResources: []*pb.PodContainerResource{
					{
						PodName: "pod1",
						ContainerResources: &pb.ContainerResource{
							ContainerName: "main",
							Requests:      map[string]string{"cpu": "1000m"},
							Limits:        map[string]string{},
						},
					},
				},
			},
		},
		{
			name: "Pods not reporting the target container are skipped",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"metric":    "pod_cpu",
					"target":    "1.0",
					"container": "main",
				},
			},
			state: &pb.ControlMetrics{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod1": podContainers(map[string]map[string]float64{"main": {"pod_cpu": 0.3}}),
					"pod2": podContainers(map[string]map[string]float64{"sidecar": {"pod_cpu": 0.9}}),
				},
			},
			wantVote: &pb.Recommendation{
				IsActive: true,
				PodContainerResources: []*pb.PodContainerResource{
					{
						PodName: "pod1",
						ContainerResources: &pb.ContainerResource{
							ContainerName: "main",
							Requests:      map[string]string{"cpu": "300m"},
							Limits:        map[string]string{},
						},
					},
				},
			},
		},
		{
			name: "CPU bounds minCpu and maxCpu",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"metric":    "pod_cpu",
					"target":    "0.5",
					"container": "main",
					"minCpu":    "200m",
					"maxCpu":    "1000m",
				},
			},
			state: &pb.ControlMetrics{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod-low":  podContainers(map[string]map[string]float64{"main": {"pod_cpu": 0.05}}), // 0.05 / 0.5 = 0.1 (100m) -> minCpu 200m
					"pod-high": podContainers(map[string]map[string]float64{"main": {"pod_cpu": 1.5}}),  // 1.5 / 0.5 = 3.0 (3000m) -> maxCpu 1000m
				},
			},
			wantVote: &pb.Recommendation{
				IsActive: true,
				PodContainerResources: []*pb.PodContainerResource{
					{
						PodName: "pod-high",
						ContainerResources: &pb.ContainerResource{
							ContainerName: "main",
							Requests:      map[string]string{"cpu": "1000m"},
							Limits:        map[string]string{},
						},
					},
					{
						PodName: "pod-low",
						ContainerResources: &pb.ContainerResource{
							ContainerName: "main",
							Requests:      map[string]string{"cpu": "200m"},
							Limits:        map[string]string{},
						},
					},
				},
			},
		},
		{
			name: "Memory recommendation",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"metric":       "pod_memory",
					"target":       "0.8",
					"safetyMargin": "1.2", // 262144000 * (1.2 / 0.8) = 393216000 bytes
					"container":    "main",
					"minMemory":    "128Mi",
				},
			},
			state: &pb.ControlMetrics{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod1": podContainers(map[string]map[string]float64{"main": {"pod_memory": 262144000}}), // 250Mi bytes
				},
			},
			wantVote: &pb.Recommendation{
				IsActive: true,
				PodContainerResources: []*pb.PodContainerResource{
					{
						PodName: "pod1",
						ContainerResources: &pb.ContainerResource{
							ContainerName: "main",
							Requests:      map[string]string{"memory": "393216000"},
							Limits:        map[string]string{},
						},
					},
				},
			},
		},
		{
			name: "Pod-scoped metric is rejected with an actionable message",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"metric":    "pod_cpu",
					"target":    "1.0",
					"container": "main",
				},
			},
			state: &pb.ControlMetrics{
				PodMetrics: map[string]*pb.MetricValues{
					"pod1": {Values: map[string]float64{"pod_cpu": 0.5}},
				},
			},
			wantVote: &pb.Recommendation{
				Message: `metric "pod_cpu" has no per-container data: define it with scope "PodContainer"`,
			},
		},
		{
			name: "No metrics at all",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"metric":    "pod_cpu",
					"target":    "1.0",
					"container": "main",
				},
			},
			state: &pb.ControlMetrics{},
			wantVote: &pb.Recommendation{
				Message: "no pod container metrics available",
			},
		},
		{
			name: "Missing container param",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"metric": "pod_cpu",
					"target": "1.0",
				},
			},
			state: &pb.ControlMetrics{},
			wantVote: &pb.Recommendation{
				Message: "missing container param",
			},
		},
	}

	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.SortRepeated(func(a, b *pb.PodContainerResource) bool {
			return a.PodName < b.PodName
		}),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := rec.Recommend(tc.def, tc.state, nil)
			if diff := cmp.Diff(tc.wantVote, got, opts...); diff != "" {
				t.Errorf("Recommend() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
