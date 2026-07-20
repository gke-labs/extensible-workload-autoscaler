package linear_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/recommenders/linear"
)

func TestLinearRecommend(t *testing.T) {
	tests := []struct {
		name           string
		def            *pb.RecommenderDefinition
		controlMetrics map[string]float64
		readyReplicas  int32
		want           *pb.RecommenderVote
	}{
		{
			name: "Scale Up",
			def: &pb.RecommenderDefinition{
				Name:        "cpu",
				Recommender: "Linear",
				Params:      map[string]string{"metric": "cpu", "target": "0.5"},
			},
			controlMetrics: map[string]float64{"cpu": 0.8},
			readyReplicas:  2,
			// Desired = 2 * (0.8 / 0.5) = 2 * 1.6 = 3.2 -> ceil(3.2) = 4
			want: &pb.RecommenderVote{
				DesiredReplicas: 4,
				IsActive:        true,
			},
		},
		{
			name: "Scale Down",
			def: &pb.RecommenderDefinition{
				Name:        "cpu",
				Recommender: "Linear",
				Params:      map[string]string{"metric": "cpu", "target": "0.5"},
			},
			controlMetrics: map[string]float64{"cpu": 0.2},
			readyReplicas:  4,
			// Desired = 4 * (0.2 / 0.5) = 4 * 0.4 = 1.6 -> ceil(1.6) = 2
			want: &pb.RecommenderVote{
				DesiredReplicas: 2,
				IsActive:        true,
			},
		},
		{
			name: "Missing Metric",
			def: &pb.RecommenderDefinition{
				Name:        "cpu",
				Recommender: "Linear",
				Params:      map[string]string{"metric": "unknown", "target": "0.5"},
			},
			controlMetrics: map[string]float64{"cpu": 0.8},
			readyReplicas:  2,
			want: &pb.RecommenderVote{
				Message: "metric 'unknown' not found",
			},
		},
		{
			name: "Missing Param",
			def: &pb.RecommenderDefinition{
				Name:        "cpu",
				Recommender: "Linear",
				Params:      map[string]string{"metric": "cpu"},
			},
			controlMetrics: map[string]float64{"cpu": 0.8},
			readyReplicas:  2,
			want: &pb.RecommenderVote{
				Message: "missing target param",
			},
		},
	}

	rec := &linear.LinearRecommender{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := &pb.ControlMetrics{
				Values:        tc.controlMetrics,
				ReadyReplicas: tc.readyReplicas,
			}
			got := rec.Recommend(tc.def, state)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("Recommend() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
