package policy_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
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
			policy: pb.Policy_builder{
				Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("cpu")}.Build(), pb.MetricDefinition_builder{Name: proto.String("rps")}.Build()},
			}.Build(),
			want: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("cpu")}.Build(), pb.MetricDefinition_builder{Name: proto.String("rps")}.Build()},
		},
		{
			name: "Policy-wide and recommender-owned metrics",
			policy: pb.Policy_builder{
				Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("rps")}.Build()},
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
						pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("vpa")}.Build(),
						pb.MetricDefinition_builder{Name: proto.String("memory"), RecommenderName: proto.String("vpa")}.Build(),
					}}.Build(),
					"hpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
						pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("hpa")}.Build(),
					}}.Build(),
				},
			}.Build(),
			want: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("rps")}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("hpa")}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("vpa")}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("memory"), RecommenderName: proto.String("vpa")}.Build(),
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
			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), sortMetrics); diff != "" {
				t.Errorf("Metrics() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
