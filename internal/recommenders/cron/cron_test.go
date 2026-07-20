package cron_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/recommenders/cron"
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
		want *pb.RecommenderVote
	}{
		{
			name: "Active Window (9am-5pm)",
			def: &pb.RecommenderDefinition{
				Name:        "work_hours",
				Recommender: "Cron",
				Params: map[string]string{
					"start":    "0 9 * * *",
					"end":      "0 17 * * *",
					"timezone": "UTC",
					"replicas": "5",
				},
			},
			want: &pb.RecommenderVote{
				IsActive:        true,
				DesiredReplicas: 5,
			},
		},
		{
			name: "Inactive Window (11pm-7am)",
			def: &pb.RecommenderDefinition{
				Name:        "night_shift",
				Recommender: "Cron",
				Params: map[string]string{
					"start":    "0 23 * * *",
					"end":      "0 7 * * *",
					"timezone": "UTC",
					"replicas": "10",
				},
			},
			want: &pb.RecommenderVote{
				IsActive:        false,
				DesiredReplicas: 0,
			},
		},
		{
			name: "Invalid Cron",
			def: &pb.RecommenderDefinition{
				Name:        "bad",
				Recommender: "Cron",
				Params: map[string]string{
					"start": "invalid",
					"end":   "* * * * *",
				},
			},
			want: &pb.RecommenderVote{
				Message: "expected exactly 5 fields, found 1: [invalid]",
			},
		},
	}

	rec := &cron.Recommender{Clock: clk}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := rec.Recommend(tc.def, nil)
			// Message varies slightly by library version, check prefix or simple diff
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("Recommend() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
