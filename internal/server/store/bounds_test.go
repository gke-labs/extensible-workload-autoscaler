package store_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store"
)

func TestBoundsArbitration(t *testing.T) {
	type res = pb.ContainerResource
	m := func(kv ...string) map[string]string {
		out := map[string]string{}
		for i := 0; i+1 < len(kv); i += 2 {
			out[kv[i]] = kv[i+1]
		}
		return out
	}

	tests := []struct {
		name string
		// Resources recommended by vpa-1, vpa-2 (both Active) and dry-1
		// (DryRun), for container "main". Nil means no recommendation.
		vpa1, vpa2, dry *res
		want            *res
	}{
		{
			name: "widest band: min of lower bounds, max of upper bounds",
			vpa1: &res{Requests: m("cpu", "500m", "memory", "200Mi"), LowerBound: m("cpu", "400m", "memory", "150Mi"), UpperBound: m("cpu", "700m", "memory", "300Mi")},
			vpa2: &res{Requests: m("cpu", "600m", "memory", "180Mi"), LowerBound: m("cpu", "300m", "memory", "170Mi"), UpperBound: m("cpu", "900m", "memory", "250Mi")},
			want: &res{Requests: m("cpu", "600m", "memory", "200Mi"), LowerBound: m("cpu", "300m", "memory", "150Mi"), UpperBound: m("cpu", "900m", "memory", "300Mi")},
		},
		{
			name: "a recommender without bounds is skipped for that side",
			vpa1: &res{Requests: m("cpu", "500m"), LowerBound: m("cpu", "400m")},
			vpa2: &res{Requests: m("cpu", "450m"), UpperBound: m("cpu", "800m")},
			want: &res{Requests: m("cpu", "500m"), LowerBound: m("cpu", "400m"), UpperBound: m("cpu", "800m")},
		},
		{
			name: "no recommender with bounds stays unbounded",
			vpa1: &res{Requests: m("cpu", "500m")},
			vpa2: &res{Requests: m("cpu", "600m")},
			want: &res{Requests: m("cpu", "600m")},
		},
		{
			name: "bounds are clamped around the winning request",
			// vpa-2 wins with 1 CPU, above vpa-1's upper bound. The lower
			// bound of vpa-2 is above vpa-1's request but the min is kept.
			vpa1: &res{Requests: m("cpu", "500m"), LowerBound: m("cpu", "1200m"), UpperBound: m("cpu", "600m")},
			vpa2: &res{Requests: m("cpu", "1")},
			want: &res{Requests: m("cpu", "1"), LowerBound: m("cpu", "1"), UpperBound: m("cpu", "1")},
		},
		{
			name: "dry-run bounds are ignored",
			vpa1: &res{Requests: m("cpu", "500m"), LowerBound: m("cpu", "400m"), UpperBound: m("cpu", "600m")},
			dry:  &res{Requests: m("cpu", "500m"), LowerBound: m("cpu", "100m"), UpperBound: m("cpu", "5")},
			want: &res{Requests: m("cpu", "500m"), LowerBound: m("cpu", "400m"), UpperBound: m("cpu", "600m")},
		},
	}

	for _, tt := range tests {
		for _, level := range []string{"workload", "pod"} {
			t.Run(tt.name+"/"+level, func(t *testing.T) {
				clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
				s := store.NewMemoryStoreWithClock(clk)
				id := &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: "pol"}
				s.UpdatePolicy("default", &pb.Policy{
					Id: id,
					Scaling: []*pb.RecommenderDefinition{
						{Name: "vpa-1", Recommender: "VPA", Mode: "Active"},
						{Name: "vpa-2", Recommender: "VPA", Mode: "Active"},
						{Name: "dry-1", Recommender: "VPA", Mode: "DryRun"},
					},
					Workload: &pb.WorkloadRef{Name: "app"},
				})
				s.UpdateWorkload(&pb.UpdateWorkloadRequest{
					Id:       id,
					Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}},
				})

				for name, r := range map[string]*res{"vpa-1": tt.vpa1, "vpa-2": tt.vpa2, "dry-1": tt.dry} {
					if r == nil {
						continue
					}
					cr := &res{ContainerName: "main", Requests: r.Requests, LowerBound: r.LowerBound, UpperBound: r.UpperBound}
					rec := &pb.Recommendation{IsActive: true}
					if level == "workload" {
						rec.WorkloadResources = []*pb.ContainerResource{cr}
					} else {
						rec.PodContainerResources = []*pb.PodContainerResource{{PodName: "p1", ContainerResources: cr}}
					}
					s.UpdateRecommenderState(&pb.UpdateRecommenderStateRequest{Id: id, RecommenderName: name, Recommendation: rec})
				}
				s.CalculateAll()

				resp, ok := s.GetRecommendation(id)
				if !ok || resp.Recommendation == nil {
					t.Fatalf("Expected recommendation, got nil")
				}
				var got *pb.ContainerResource
				if level == "workload" {
					if n := len(resp.Recommendation.WorkloadResources); n != 1 {
						t.Fatalf("Want 1 arbitrated workload resource, got %d", n)
					}
					got = resp.Recommendation.WorkloadResources[0]
				} else {
					if n := len(resp.Recommendation.PodContainerResources); n != 1 {
						t.Fatalf("Want 1 arbitrated pod resource, got %d", n)
					}
					got = resp.Recommendation.PodContainerResources[0].ContainerResources
				}

				want := &res{
					ContainerName: "main",
					Requests:      tt.want.Requests,
					Limits:        map[string]string{},
					LowerBound:    tt.want.LowerBound,
					UpperBound:    tt.want.UpperBound,
				}
				if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
					t.Errorf("Arbitrated resources mismatch (-want +got):\n%s", diff)
				}
			})
		}
	}
}
