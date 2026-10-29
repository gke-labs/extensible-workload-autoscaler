package grpc_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
)

// ignoreEtag drops the server-computed etag from policy comparisons, for tests
// that compare against the policy they sent.
var ignoreEtag = protocmp.IgnoreFields(&pb.Policy{}, "etag")

// mustUpdatePolicy sends p and fails the test on error. It returns the stored
// policy, whose etag must be echoed back on the next update.
func mustUpdatePolicy(t *testing.T, client pb.XASServerClient, p *pb.Policy) *pb.Policy {
	t.Helper()
	stored, err := client.UpdatePolicy(context.Background(), pb.UpdatePolicyRequest_builder{Policy: p, AllowMissing: proto.Bool(true)}.Build())
	if err != nil {
		t.Fatalf("UpdatePolicy(%s) failed: %v", p.GetId().GetName(), err)
	}
	return stored
}

// getPolicy reads a policy through GetPolicy, as a client would before a
// read-modify-write.
func getPolicy(t *testing.T, client pb.XASServerClient, id *pb.PolicyId) *pb.Policy {
	t.Helper()
	p, err := client.GetPolicy(context.Background(), pb.GetPolicyRequest_builder{Id: id}.Build())
	if err != nil {
		t.Fatalf("GetPolicy(%v) failed: %v", id, err)
	}
	return p
}

// TestUpdatePolicyEtagCodesGRPC checks the gRPC code returned for each ETag
// case of UpdatePolicy.
func TestUpdatePolicyEtagCodesGRPC(t *testing.T) {
	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()

	tests := []struct {
		name         string
		existing     bool
		allowMissing bool
		etag         func(stored string) string
		wantCode     codes.Code
	}{
		{"missing without allow_missing", false, false, func(string) string { return "" }, codes.NotFound},
		{"create with empty etag and allow_missing", false, true, func(string) string { return "" }, codes.OK},
		{"create with an etag and allow_missing", false, true, func(string) string { return "some-etag" }, codes.NotFound},
		{"update with empty etag (overwrite)", true, false, func(string) string { return "" }, codes.OK},
		{"update with matching etag", true, false, func(s string) string { return s }, codes.OK},
		{"update with stale etag", true, false, func(s string) string { return s + "-stale" }, codes.Aborted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
			defer cleanup()

			var stored *pb.Policy
			if tt.existing {
				stored = mustUpdatePolicy(t, client, pb.Policy_builder{Id: id, MaxReplicas: proto.Int32(10)}.Build())
			}

			resp, err := client.UpdatePolicy(context.Background(), pb.UpdatePolicyRequest_builder{
				Policy:       pb.Policy_builder{Id: id, MaxReplicas: proto.Int32(20), Etag: proto.String(tt.etag(stored.GetEtag()))}.Build(),
				AllowMissing: proto.Bool(tt.allowMissing),
			}.Build())
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("UpdatePolicy() code = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if tt.wantCode != codes.OK {
				return
			}
			if resp.GetEtag() == "" {
				t.Error("UpdatePolicy() response has no etag")
			}
			if resp.GetMaxReplicas() != 20 {
				t.Errorf("UpdatePolicy() MaxReplicas = %d, want 20", resp.GetMaxReplicas())
			}
		})
	}
}

// TestUpdatePolicyEtagLifecycleGRPC checks that each write issues a new ETag,
// that ListPolicies returns it, and that the previous one becomes stale.
func TestUpdatePolicyEtagLifecycleGRPC(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()

	v1 := mustUpdatePolicy(t, client, pb.Policy_builder{Id: id, MaxReplicas: proto.Int32(10)}.Build())
	if got := getPolicy(t, client, id).GetEtag(); got != v1.GetEtag() {
		t.Fatalf("ListPolicies() etag = %q, want %q", got, v1.GetEtag())
	}

	v2 := mustUpdatePolicy(t, client, pb.Policy_builder{Id: id, MaxReplicas: proto.Int32(20), Etag: proto.String(v1.GetEtag())}.Build())
	if v2.GetEtag() == v1.GetEtag() {
		t.Fatalf("etag did not change after a content change: %q", v2.GetEtag())
	}
	if got := getPolicy(t, client, id).GetEtag(); got != v2.GetEtag() {
		t.Errorf("ListPolicies() etag = %q, want %q", got, v2.GetEtag())
	}

	// The first etag is now stale.
	_, err := client.UpdatePolicy(context.Background(), pb.UpdatePolicyRequest_builder{
		Policy: pb.Policy_builder{Id: id, MaxReplicas: proto.Int32(30), Etag: proto.String(v1.GetEtag())}.Build(),
	}.Build())
	if status.Code(err) != codes.Aborted {
		t.Errorf("UpdatePolicy(old etag) code = %v, want Aborted", status.Code(err))
	}
	if got := getPolicy(t, client, id).GetMaxReplicas(); got != 20 {
		t.Errorf("rejected write changed MaxReplicas to %d, want 20", got)
	}
}

// TestUpdatePolicyReadModifyWriteGRPC simulates the Controller and a
// recommender both editing the same policy from the same read. The second
// writer is rejected, re-reads, and retries; neither change is lost.
func TestUpdatePolicyReadModifyWriteGRPC(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	mustUpdatePolicy(t, client, pb.Policy_builder{
		Id: id, MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		Scaling: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("vpa"), Recommender: proto.String("vpa")}.Build()},
	}.Build())

	// Both writers read the same version.
	controllerCopy := getPolicy(t, client, id)
	recommenderCopy := getPolicy(t, client, id)

	// The Controller changes the fields it owns and wins.
	controllerCopy.SetMaxReplicas(50)
	mustUpdatePolicy(t, client, controllerCopy)

	// The recommender changes its own field from the stale copy: rejected.
	recommenderCopy.SetRecommenderMetrics(map[string]*pb.MetricDefinitionList{
		"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("vpa"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()}}.Build(),
	})
	_, err := client.UpdatePolicy(context.Background(), pb.UpdatePolicyRequest_builder{Policy: recommenderCopy}.Build())
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale write code = %v, want Aborted", status.Code(err))
	}

	// It re-reads, re-applies its change, and retries.
	fresh := getPolicy(t, client, id)
	fresh.SetRecommenderMetrics(recommenderCopy.GetRecommenderMetrics())
	final := mustUpdatePolicy(t, client, fresh)

	if final.GetMaxReplicas() != 50 {
		t.Errorf("Controller change lost: MaxReplicas = %d, want 50", final.GetMaxReplicas())
	}
	if _, ok := final.GetRecommenderMetrics()["vpa"]; !ok {
		t.Error("recommender change lost: recommender_metrics[vpa] missing")
	}
}
