package cron_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/cmd/core-recommenders/internal/cron"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
)

func TestCronRecommend(t *testing.T) {
	// Mon Jan 2 15:04:05 MST 2006
	// Let's set time to a Monday, 10:00 AM
	// 2024-01-01 was a Monday.
	refTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	clk := &clock.FakeClock{CurrentTime: refTime}

	tests := []struct {
		name string
		def  *pb.RecommenderDefinition
		want *pb.Recommendation
	}{
		{
			name: "Active Window (9am-5pm)",
			def: pb.RecommenderDefinition_builder{
				Name:        proto.String("work_hours"),
				Recommender: proto.String("Cron"),
				Params: map[string]string{
					"start":    "0 9 * * *",
					"end":      "0 17 * * *",
					"timezone": "UTC",
					"replicas": "5",
				},
			}.Build(),
			want: pb.Recommendation_builder{
				IsActive: proto.Bool(true),
				Replicas: proto.Int32(5),
			}.Build(),
		},
		{
			name: "Inactive Window (11pm-7am)",
			def: pb.RecommenderDefinition_builder{
				Name:        proto.String("night_shift"),
				Recommender: proto.String("Cron"),
				Params: map[string]string{
					"start":    "0 23 * * *",
					"end":      "0 7 * * *",
					"timezone": "UTC",
					"replicas": "10",
				},
			}.Build(),
			want: pb.Recommendation_builder{
				IsActive: proto.Bool(false),
				Replicas: proto.Int32(0),
			}.Build(),
		},
		{
			name: "Activation only (no replicas param)",
			def: pb.RecommenderDefinition_builder{
				Name:        proto.String("activation_only"),
				Recommender: proto.String("Cron"),
				Params: map[string]string{
					"start":    "0 9 * * *",
					"end":      "0 17 * * *",
					"timezone": "UTC",
				},
			}.Build(),
			want: pb.Recommendation_builder{
				IsActive: proto.Bool(true),
			}.Build(),
		},
		{
			name: "Invalid Cron",
			def: pb.RecommenderDefinition_builder{
				Name:        proto.String("bad"),
				Recommender: proto.String("Cron"),
				Params: map[string]string{
					"start": "invalid",
					"end":   "* * * * *",
				},
			}.Build(),
			want: pb.Recommendation_builder{
				Message: proto.String("expected exactly 5 fields, found 1: [invalid]"),
			}.Build(),
		},
	}

	rec := &cron.Recommender{Clock: clk}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := rec.Recommend(tc.def, nil, nil, 0, nil)
			// Message varies slightly by library version, check prefix or simple diff
			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
				t.Errorf("Recommend() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
