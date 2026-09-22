package policy_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
)

func TestMetrics(t *testing.T) {
	tests := []struct {
		name   string
		policy *pb.Policy
		want   []*pb.MetricDefinition
	}{
		{
			name: "Policy-wide metrics only",
			policy: &pb.Policy{
				Metrics: []*pb.MetricDefinition{{Name: "cpu"}, {Name: "rps"}},
			},
			want: []*pb.MetricDefinition{{Name: "cpu"}, {Name: "rps"}},
		},
		{
			name: "Policy-wide and recommender-owned metrics",
			policy: &pb.Policy{
				Metrics: []*pb.MetricDefinition{{Name: "rps"}},
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": {Definitions: []*pb.MetricDefinition{
						{Name: "cpu", RecommenderName: "vpa"},
						{Name: "memory", RecommenderName: "vpa"},
					}},
					"hpa": {Definitions: []*pb.MetricDefinition{
						{Name: "cpu", RecommenderName: "hpa"},
					}},
				},
			},
			want: []*pb.MetricDefinition{
				{Name: "rps"},
				{Name: "cpu", RecommenderName: "hpa"},
				{Name: "cpu", RecommenderName: "vpa"},
				{Name: "memory", RecommenderName: "vpa"},
			},
		},
	}

	// Metrics are collected independently of each other, so their order does
	// not matter. Identity is the <name, recommender> pair.
	sortMetrics := cmpopts.SortSlices(func(a, b *pb.MetricDefinition) bool {
		if a.GetRecommenderName() != b.GetRecommenderName() {
			return a.GetRecommenderName() < b.GetRecommenderName()
		}
		return a.GetName() < b.GetName()
	})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := policy.MetricDefinitions(tc.policy)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), sortMetrics); diff != "" {
				t.Errorf("Metrics() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
