package perpodvertical

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

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
				PodMetrics: map[string]*pb.MetricValues{
					"pod1": {Values: map[string]float64{"pod_cpu": 0.4}}, // 0.4 * (1.2 / 0.8) = 0.6 -> 600m
					"pod2": {Values: map[string]float64{"pod_cpu": 0.8}}, // 0.8 * (1.2 / 0.8) = 1.2 -> 1200m
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
			name: "CPU bounds minCpu and maxCpu",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"metric": "pod_cpu",
					"target": "0.5",
					"minCpu": "200m",
					"maxCpu": "1000m",
				},
			},
			state: &pb.ControlMetrics{
				PodMetrics: map[string]*pb.MetricValues{
					"pod-low":  {Values: map[string]float64{"pod_cpu": 0.05}}, // 0.05 / 0.5 = 0.1 (100m) -> minCpu 200m
					"pod-high": {Values: map[string]float64{"pod_cpu": 1.5}},  // 1.5 / 0.5 = 3.0 (3000m) -> maxCpu 1000m
				},
			},
			wantVote: &pb.Recommendation{
				IsActive: true,
				PodContainerResources: []*pb.PodContainerResource{
					{
						PodName: "pod-high",
						ContainerResources: &pb.ContainerResource{
							Requests: map[string]string{"cpu": "1000m"},
							Limits:   map[string]string{},
						},
					},
					{
						PodName: "pod-low",
						ContainerResources: &pb.ContainerResource{
							Requests: map[string]string{"cpu": "200m"},
							Limits:   map[string]string{},
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
					"minMemory":    "128Mi",
				},
			},
			state: &pb.ControlMetrics{
				PodMetrics: map[string]*pb.MetricValues{
					"pod1": {Values: map[string]float64{"pod_memory": 262144000}}, // 250Mi bytes
				},
			},
			wantVote: &pb.Recommendation{
				IsActive: true,
				PodContainerResources: []*pb.PodContainerResource{
					{
						PodName: "pod1",
						ContainerResources: &pb.ContainerResource{
							Requests: map[string]string{"memory": "393216000"},
							Limits:   map[string]string{},
						},
					},
				},
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
