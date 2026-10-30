package policy

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

// fakeLister serves pages[i] for page token "i" ("" is page 0), and fails on
// page failAt if failAt >= 0.
type fakeLister struct {
	pages    [][]*pb.Policy
	failAt   int
	requests []*pb.ListPoliciesRequest
}

func (f *fakeLister) ListPolicies(_ context.Context, req *pb.ListPoliciesRequest, _ ...grpc.CallOption) (*pb.ListPoliciesResponse, error) {
	f.requests = append(f.requests, req)
	i := len(f.requests) - 1
	if i == f.failAt {
		return nil, errors.New("boom")
	}
	resp := pb.ListPoliciesResponse_builder{Policies: f.pages[i]}
	if i+1 < len(f.pages) {
		resp.NextPageToken = proto.String(string(rune('0' + i + 1)))
	}
	return resp.Build(), nil
}

func newPolicy(name string) *pb.Policy {
	return pb.Policy_builder{
		Id: pb.PolicyId_builder{
			ClusterName: proto.String("c1"),
			Namespace:   proto.String("default"),
			Name:        proto.String(name),
		}.Build(),
	}.Build()
}

func TestListAll_MultiplePages(t *testing.T) {
	a, b, c := newPolicy("a"), newPolicy("b"), newPolicy("c")
	f := &fakeLister{pages: [][]*pb.Policy{{a, b}, {c}}, failAt: -1}

	got, err := ListAll(context.Background(), f, "c1")
	if err != nil {
		t.Fatalf("ListAll() failed: %v", err)
	}
	if diff := cmp.Diff([]*pb.Policy{a, b, c}, got, protocmp.Transform()); diff != "" {
		t.Errorf("ListAll() mismatch (-want +got):\n%s", diff)
	}
	// Each request must carry the cluster and the token of the previous page.
	var tokens []string
	for _, req := range f.requests {
		if req.GetClusterName() != "c1" {
			t.Errorf("request cluster_name = %q, want %q", req.GetClusterName(), "c1")
		}
		tokens = append(tokens, req.GetPageToken())
	}
	if diff := cmp.Diff([]string{"", "1"}, tokens); diff != "" {
		t.Errorf("page tokens mismatch (-want +got):\n%s", diff)
	}
}

func TestListAll_SinglePage(t *testing.T) {
	f := &fakeLister{pages: [][]*pb.Policy{{newPolicy("a")}}, failAt: -1}
	got, err := ListAll(context.Background(), f, "c1")
	if err != nil || len(got) != 1 || len(f.requests) != 1 {
		t.Errorf("ListAll() = %d policies, %v after %d requests; want 1, nil after 1", len(got), err, len(f.requests))
	}
}

func TestListAll_ErrorDiscardsPartialResults(t *testing.T) {
	f := &fakeLister{pages: [][]*pb.Policy{{newPolicy("a")}, {newPolicy("b")}}, failAt: 1}
	got, err := ListAll(context.Background(), f, "c1")
	if err == nil || got != nil {
		t.Errorf("ListAll() = %v, %v; want nil, error", got, err)
	}
}
