package grpc_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
)

// ignoreEtag drops the server-computed etag from policy comparisons, for tests
// that compare against the policy they sent.
var ignoreEtag = protocmp.IgnoreFields(&pb.Policy{}, "etag")

// mustUpdatePolicy sends p and fails the test on error. It returns the stored
// policy, whose etag must be echoed back on the next update.
func mustUpdatePolicy(t *testing.T, client pb.XASServerClient, p *pb.Policy) *pb.Policy {
	t.Helper()
	stored, err := client.UpdatePolicy(context.Background(), &pb.UpdatePolicyRequest{Policy: p})
	if err != nil {
		t.Fatalf("UpdatePolicy(%s) failed: %v", p.GetId().GetName(), err)
	}
	return stored
}

// getPolicy reads a policy through ListPolicies, as a client would before a
// read-modify-write.
func getPolicy(t *testing.T, client pb.XASServerClient, id *pb.PolicyId) *pb.Policy {
	t.Helper()
	resp, err := client.ListPolicies(context.Background(), &pb.ListPoliciesRequest{ClusterName: id.GetClusterName()})
	if err != nil {
		t.Fatalf("ListPolicies() failed: %v", err)
	}
	for _, p := range resp.GetPolicies() {
		if proto.Equal(p.GetId(), id) {
			return p
		}
	}
	t.Fatalf("policy %v not found", id)
	return nil
}

// TestUpdatePolicyEtagCodesGRPC checks the gRPC code returned for each ETag
// case of UpdatePolicy.
func TestUpdatePolicyEtagCodesGRPC(t *testing.T) {
	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}

	tests := []struct {
		name     string
		existing bool
		etag     func(stored string) string
		wantCode codes.Code
	}{
		{"create with empty etag", false, func(string) string { return "" }, codes.OK},
		{"create with an etag", false, func(string) string { return "some-etag" }, codes.NotFound},
		{"update with empty etag (overwrite)", true, func(string) string { return "" }, codes.OK},
		{"update with matching etag", true, func(s string) string { return s }, codes.OK},
		{"update with stale etag", true, func(s string) string { return s + "-stale" }, codes.Aborted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
			defer cleanup()

			var stored *pb.Policy
			if tt.existing {
				stored = mustUpdatePolicy(t, client, &pb.Policy{Id: id, MaxReplicas: 10})
			}

			resp, err := client.UpdatePolicy(context.Background(), &pb.UpdatePolicyRequest{
				Policy: &pb.Policy{Id: id, MaxReplicas: 20, Etag: tt.etag(stored.GetEtag())},
			})
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
	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}

	v1 := mustUpdatePolicy(t, client, &pb.Policy{Id: id, MaxReplicas: 10})
	if got := getPolicy(t, client, id).GetEtag(); got != v1.GetEtag() {
		t.Fatalf("ListPolicies() etag = %q, want %q", got, v1.GetEtag())
	}

	v2 := mustUpdatePolicy(t, client, &pb.Policy{Id: id, MaxReplicas: 20, Etag: v1.GetEtag()})
	if v2.GetEtag() == v1.GetEtag() {
		t.Fatalf("etag did not change after a content change: %q", v2.GetEtag())
	}
	if got := getPolicy(t, client, id).GetEtag(); got != v2.GetEtag() {
		t.Errorf("ListPolicies() etag = %q, want %q", got, v2.GetEtag())
	}

	// The first etag is now stale.
	_, err := client.UpdatePolicy(context.Background(), &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{Id: id, MaxReplicas: 30, Etag: v1.GetEtag()},
	})
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
	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	mustUpdatePolicy(t, client, &pb.Policy{Id: id, MinReplicas: 1, MaxReplicas: 10})

	// Both writers read the same version.
	controllerCopy := getPolicy(t, client, id)
	recommenderCopy := getPolicy(t, client, id)

	// The Controller changes the fields it owns and wins.
	controllerCopy.MaxReplicas = 50
	mustUpdatePolicy(t, client, controllerCopy)

	// The recommender changes its own field from the stale copy: rejected.
	recommenderCopy.RecommenderMetrics = map[string]*pb.MetricDefinitionList{
		"vpa": {Definitions: []*pb.MetricDefinition{{Name: "cpu", RecommenderName: "vpa", Gauge: &pb.Gauge{Aggregation: "Avg"}}}},
	}
	_, err := client.UpdatePolicy(context.Background(), &pb.UpdatePolicyRequest{Policy: recommenderCopy})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale write code = %v, want Aborted", status.Code(err))
	}

	// It re-reads, re-applies its change, and retries.
	fresh := getPolicy(t, client, id)
	fresh.RecommenderMetrics = recommenderCopy.RecommenderMetrics
	final := mustUpdatePolicy(t, client, fresh)

	if final.GetMaxReplicas() != 50 {
		t.Errorf("Controller change lost: MaxReplicas = %d, want 50", final.GetMaxReplicas())
	}
	if _, ok := final.GetRecommenderMetrics()["vpa"]; !ok {
		t.Error("recommender change lost: recommender_metrics[vpa] missing")
	}
}
