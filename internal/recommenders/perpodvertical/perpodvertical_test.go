package perpodvertical

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

// podContainers builds a PodContainerMetrics entry: container name -> metric
// name -> value.
func podContainers(byContainer map[string]map[string]float64) *pb.ContainerMetrics {
	cm := pb.ContainerMetrics_builder{ContainerMetrics: make(map[string]*pb.MetricValues)}.Build()
	for name, values := range byContainer {
		cm.GetContainerMetrics()[name] = pb.MetricValues_builder{Values: values}.Build()
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
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"metric":       "pod_cpu",
					"target":       "0.8",
					"safetyMargin": "1.2",
					"container":    "main",
				},
			}.Build(),
			state: pb.ControlMetrics_builder{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod1": podContainers(map[string]map[string]float64{"main": {"pod_cpu": 0.4}}), // 0.4 * (1.2 / 0.8) = 0.6 -> 600m
					"pod2": podContainers(map[string]map[string]float64{"main": {"pod_cpu": 0.8}}), // 0.8 * (1.2 / 0.8) = 1.2 -> 1200m
				},
			}.Build(),
			wantVote: pb.Recommendation_builder{
				IsActive: proto.Bool(true),
				PodContainerResources: []*pb.PodContainerResource{
					pb.PodContainerResource_builder{
						PodName: proto.String("pod1"),
						ContainerResources: pb.ContainerResource_builder{
							ContainerName: proto.String("main"),
							Requests:      map[string]string{"cpu": "600m"},
							Limits:        map[string]string{},
						}.Build(),
					}.Build(),
					pb.PodContainerResource_builder{
						PodName: proto.String("pod2"),
						ContainerResources: pb.ContainerResource_builder{
							ContainerName: proto.String("main"),
							Requests:      map[string]string{"cpu": "1200m"},
							Limits:        map[string]string{},
						}.Build(),
					}.Build(),
				},
			}.Build(),
		},
		{
			name: "Only the target container's usage is used, not its neighbors'",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"metric":    "pod_cpu",
					"target":    "1.0",
					"container": "main",
				},
			}.Build(),
			state: pb.ControlMetrics_builder{
				// The pod-level value mixes main and sidecar and must be ignored.
				PodMetrics: map[string]*pb.MetricValues{
					"pod1": pb.MetricValues_builder{Values: map[string]float64{"pod_cpu": 0.75}}.Build(),
				},
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod1": podContainers(map[string]map[string]float64{
						"main":    {"pod_cpu": 1.0},
						"sidecar": {"pod_cpu": 0.5},
					}),
				},
			}.Build(),
			wantVote: pb.Recommendation_builder{
				IsActive: proto.Bool(true),
				PodContainerResources: []*pb.PodContainerResource{
					pb.PodContainerResource_builder{
						PodName: proto.String("pod1"),
						ContainerResources: pb.ContainerResource_builder{
							ContainerName: proto.String("main"),
							Requests:      map[string]string{"cpu": "1000m"},
							Limits:        map[string]string{},
						}.Build(),
					}.Build(),
				},
			}.Build(),
		},
		{
			name: "Pods not reporting the target container are skipped",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"metric":    "pod_cpu",
					"target":    "1.0",
					"container": "main",
				},
			}.Build(),
			state: pb.ControlMetrics_builder{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod1": podContainers(map[string]map[string]float64{"main": {"pod_cpu": 0.3}}),
					"pod2": podContainers(map[string]map[string]float64{"sidecar": {"pod_cpu": 0.9}}),
				},
			}.Build(),
			wantVote: pb.Recommendation_builder{
				IsActive: proto.Bool(true),
				PodContainerResources: []*pb.PodContainerResource{
					pb.PodContainerResource_builder{
						PodName: proto.String("pod1"),
						ContainerResources: pb.ContainerResource_builder{
							ContainerName: proto.String("main"),
							Requests:      map[string]string{"cpu": "300m"},
							Limits:        map[string]string{},
						}.Build(),
					}.Build(),
				},
			}.Build(),
		},
		{
			name: "CPU bounds minCpu and maxCpu",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"metric":    "pod_cpu",
					"target":    "0.5",
					"container": "main",
					"minCpu":    "200m",
					"maxCpu":    "1000m",
				},
			}.Build(),
			state: pb.ControlMetrics_builder{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod-low":  podContainers(map[string]map[string]float64{"main": {"pod_cpu": 0.05}}), // 0.05 / 0.5 = 0.1 (100m) -> minCpu 200m
					"pod-high": podContainers(map[string]map[string]float64{"main": {"pod_cpu": 1.5}}),  // 1.5 / 0.5 = 3.0 (3000m) -> maxCpu 1000m
				},
			}.Build(),
			wantVote: pb.Recommendation_builder{
				IsActive: proto.Bool(true),
				PodContainerResources: []*pb.PodContainerResource{
					pb.PodContainerResource_builder{
						PodName: proto.String("pod-high"),
						ContainerResources: pb.ContainerResource_builder{
							ContainerName: proto.String("main"),
							Requests:      map[string]string{"cpu": "1000m"},
							Limits:        map[string]string{},
						}.Build(),
					}.Build(),
					pb.PodContainerResource_builder{
						PodName: proto.String("pod-low"),
						ContainerResources: pb.ContainerResource_builder{
							ContainerName: proto.String("main"),
							Requests:      map[string]string{"cpu": "200m"},
							Limits:        map[string]string{},
						}.Build(),
					}.Build(),
				},
			}.Build(),
		},
		{
			name: "Memory recommendation",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"metric":       "pod_memory",
					"target":       "0.8",
					"safetyMargin": "1.2", // 262144000 * (1.2 / 0.8) = 393216000 bytes
					"container":    "main",
					"minMemory":    "128Mi",
				},
			}.Build(),
			state: pb.ControlMetrics_builder{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod1": podContainers(map[string]map[string]float64{"main": {"pod_memory": 262144000}}), // 250Mi bytes
				},
			}.Build(),
			wantVote: pb.Recommendation_builder{
				IsActive: proto.Bool(true),
				PodContainerResources: []*pb.PodContainerResource{
					pb.PodContainerResource_builder{
						PodName: proto.String("pod1"),
						ContainerResources: pb.ContainerResource_builder{
							ContainerName: proto.String("main"),
							Requests:      map[string]string{"memory": "393216000"},
							Limits:        map[string]string{},
						}.Build(),
					}.Build(),
				},
			}.Build(),
		},
		{
			name: "Pod-scoped metric is rejected with an actionable message",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"metric":    "pod_cpu",
					"target":    "1.0",
					"container": "main",
				},
			}.Build(),
			state: pb.ControlMetrics_builder{
				PodMetrics: map[string]*pb.MetricValues{
					"pod1": pb.MetricValues_builder{Values: map[string]float64{"pod_cpu": 0.5}}.Build(),
				},
			}.Build(),
			wantVote: pb.Recommendation_builder{
				Message: proto.String(`metric "pod_cpu" has no per-container data: define it with scope "PodContainer"`),
			}.Build(),
		},
		{
			name: "No metrics at all",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"metric":    "pod_cpu",
					"target":    "1.0",
					"container": "main",
				},
			}.Build(),
			state: &pb.ControlMetrics{},
			wantVote: pb.Recommendation_builder{
				Message: proto.String("no pod container metrics available"),
			}.Build(),
		},
		{
			name: "Missing container param",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"metric": "pod_cpu",
					"target": "1.0",
				},
			}.Build(),
			state: &pb.ControlMetrics{},
			wantVote: pb.Recommendation_builder{
				Message: proto.String("missing container param"),
			}.Build(),
		},
		{
			name: "Container level with limitRatio",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"metric":     "pod_cpu",
					"target":     "1.0",
					"container":  "main",
					"limitRatio": "1.5",
				},
			}.Build(),
			state: pb.ControlMetrics_builder{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod1": podContainers(map[string]map[string]float64{"main": {"pod_cpu": 0.4}}),
				},
			}.Build(),
			wantVote: pb.Recommendation_builder{
				IsActive: proto.Bool(true),
				PodContainerResources: []*pb.PodContainerResource{
					pb.PodContainerResource_builder{
						PodName: proto.String("pod1"),
						ContainerResources: pb.ContainerResource_builder{
							ContainerName: proto.String("main"),
							Requests:      map[string]string{"cpu": "400m"},
							Limits:        map[string]string{"cpu": "600m"},
						}.Build(),
					}.Build(),
				},
			}.Build(),
		},
		{
			name: "Pod level sums all containers and targets pod-level resources",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"resourceLevel": "Pod",
					"metric":        "pod_cpu",
					"target":        "0.8",
					"safetyMargin":  "1.2",
					"limitRatio":    "2",
				},
			}.Build(),
			state: pb.ControlMetrics_builder{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					// (1.0 + 0.6) * 1.5 = 2.4 -> 2400m, limit 4800m
					"pod1": podContainers(map[string]map[string]float64{
						"main":    {"pod_cpu": 1.0},
						"sidecar": {"pod_cpu": 0.6},
					}),
					// A pod missing a container sums what it has: 0.2 * 1.5 = 0.3 -> 300m, limit 600m
					"pod2": podContainers(map[string]map[string]float64{
						"main": {"pod_cpu": 0.2},
					}),
				},
			}.Build(),
			wantVote: pb.Recommendation_builder{
				IsActive: proto.Bool(true),
				PodContainerResources: []*pb.PodContainerResource{
					pb.PodContainerResource_builder{
						PodName: proto.String("pod1"),
						ContainerResources: pb.ContainerResource_builder{
							Requests: map[string]string{"cpu": "2400m"},
							Limits:   map[string]string{"cpu": "4800m"},
						}.Build(),
					}.Build(),
					pb.PodContainerResource_builder{
						PodName: proto.String("pod2"),
						ContainerResources: pb.ContainerResource_builder{
							Requests: map[string]string{"cpu": "300m"},
							Limits:   map[string]string{"cpu": "600m"},
						}.Build(),
					}.Build(),
				},
			}.Build(),
		},
		{
			name: "Pod level bounds apply to the pod total",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"resourceLevel": "Pod",
					"metric":        "pod_cpu",
					"target":        "1.0",
					"maxCpu":        "1",
				},
			}.Build(),
			state: pb.ControlMetrics_builder{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					// 0.7 + 0.7 = 1.4 -> capped at 1000m, although each container is below the cap.
					"pod1": podContainers(map[string]map[string]float64{
						"a": {"pod_cpu": 0.7},
						"b": {"pod_cpu": 0.7},
					}),
				},
			}.Build(),
			wantVote: pb.Recommendation_builder{
				IsActive: proto.Bool(true),
				PodContainerResources: []*pb.PodContainerResource{
					pb.PodContainerResource_builder{
						PodName: proto.String("pod1"),
						ContainerResources: pb.ContainerResource_builder{
							Requests: map[string]string{"cpu": "1000m"},
							Limits:   map[string]string{},
						}.Build(),
					}.Build(),
				},
			}.Build(),
		},
		{
			name: "Pod level rejects container param",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"resourceLevel": "Pod",
					"metric":        "pod_cpu",
					"target":        "1.0",
					"container":     "main",
				},
			}.Build(),
			state: &pb.ControlMetrics{},
			wantVote: pb.Recommendation_builder{
				Message: proto.String(`container param is not allowed with resourceLevel "Pod"`),
			}.Build(),
		},
		{
			name: "Invalid resourceLevel",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"resourceLevel": "Node",
					"metric":        "pod_cpu",
					"target":        "1.0",
				},
			}.Build(),
			state: &pb.ControlMetrics{},
			wantVote: pb.Recommendation_builder{
				Message: proto.String(`invalid resourceLevel "Node": must be "Container" or "Pod"`),
			}.Build(),
		},
		{
			name: "limitRatio below 1 is rejected",
			def: pb.RecommenderDefinition_builder{
				Params: map[string]string{
					"metric":     "pod_cpu",
					"target":     "1.0",
					"container":  "main",
					"limitRatio": "0.5",
				},
			}.Build(),
			state: &pb.ControlMetrics{},
			wantVote: pb.Recommendation_builder{
				Message: proto.String("limitRatio must be >= 1"),
			}.Build(),
		},
	}

	opts := []cmp.Option{
		protocmp.Transform(), protocmp.IgnoreDefaultScalars(),
		protocmp.SortRepeated(func(a, b *pb.PodContainerResource) bool {
			return a.GetPodName() < b.GetPodName()
		}),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := rec.Recommend(tc.def, tc.state, nil, 0, nil)
			if diff := cmp.Diff(tc.wantVote, got, opts...); diff != "" {
				t.Errorf("Recommend() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
