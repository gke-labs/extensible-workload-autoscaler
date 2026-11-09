package linear_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/cmd/core-recommenders/internal/linear"
)

func TestLinearRecommend(t *testing.T) {
	tests := []struct {
		name           string
		def            *pb.RecommenderDefinition
		controlMetrics map[string]float64
		readyReplicas  int32
		want           *pb.Recommendation
	}{
		{
			name: "Scale Up",
			def: pb.RecommenderDefinition_builder{
				Name:        new("cpu"),
				Recommender: new("Linear"),
				Params:      map[string]string{"metric": "cpu", "target": "0.5"},
			}.Build(),
			controlMetrics: map[string]float64{"cpu": 0.8},
			readyReplicas:  2,
			// Desired = 2 * (0.8 / 0.5) = 2 * 1.6 = 3.2 -> ceil(3.2) = 4
			want: pb.Recommendation_builder{
				Replicas: new(int32(4)),
				IsActive: new(true),
			}.Build(),
		},
		{
			name: "Scale Down",
			def: pb.RecommenderDefinition_builder{
				Name:        new("cpu"),
				Recommender: new("Linear"),
				Params:      map[string]string{"metric": "cpu", "target": "0.5"},
			}.Build(),
			controlMetrics: map[string]float64{"cpu": 0.2},
			readyReplicas:  4,
			// Desired = 4 * (0.2 / 0.5) = 4 * 0.4 = 1.6 -> ceil(1.6) = 2
			want: pb.Recommendation_builder{
				Replicas: new(int32(2)),
				IsActive: new(true),
			}.Build(),
		},
		{
			name: "Missing Metric",
			def: pb.RecommenderDefinition_builder{
				Name:        new("cpu"),
				Recommender: new("Linear"),
				Params:      map[string]string{"metric": "unknown", "target": "0.5"},
			}.Build(),
			controlMetrics: map[string]float64{"cpu": 0.8},
			readyReplicas:  2,
			want: pb.Recommendation_builder{
				Message: new("metric 'unknown' not found"),
			}.Build(),
		},
		{
			name: "Missing Param",
			def: pb.RecommenderDefinition_builder{
				Name:        new("cpu"),
				Recommender: new("Linear"),
				Params:      map[string]string{"metric": "cpu"},
			}.Build(),
			controlMetrics: map[string]float64{"cpu": 0.8},
			readyReplicas:  2,
			want: pb.Recommendation_builder{
				Message: new("missing target param"),
			}.Build(),
		},
	}

	rec := &linear.LinearRecommender{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := pb.ControlMetrics_builder{Values: tc.controlMetrics}.Build()
			got := rec.Recommend(tc.def, state, nil, tc.readyReplicas, nil)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
				t.Errorf("Recommend() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
