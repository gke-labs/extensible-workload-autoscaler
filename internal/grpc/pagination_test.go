package grpc_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasgrpc "github.com/gke-labs/extensible-workload-autoscaler/internal/grpc"
)

type recordedCall struct {
	clusterName string
	pageSize    int32
	pageToken   string
	opts        []grpc.CallOption
}

type fakePager struct {
	pages  [][]*pb.Policy
	failAt int
	err    error
	calls  []recordedCall
}

func (f *fakePager) listPolicies(_ context.Context, req *pb.ListPoliciesRequest, opts ...grpc.CallOption) (*pb.ListPoliciesResponse, error) {
	// Snapshot request fields at call time because ListAllPages mutates req.SetPageToken in place.
	f.calls = append(f.calls, recordedCall{
		clusterName: req.GetClusterName(),
		pageSize:    req.GetPageSize(),
		pageToken:   req.GetPageToken(),
		opts:        append([]grpc.CallOption(nil), opts...),
	})
	i := len(f.calls) - 1
	if i == f.failAt {
		return nil, f.err
	}
	resp := pb.ListPoliciesResponse_builder{Policies: f.pages[i]}
	if i+1 < len(f.pages) {
		resp.NextPageToken = new(strconv.Itoa(i + 1))
	}
	return resp.Build(), nil
}

type testCallOption struct {
	grpc.EmptyCallOption
	ID string
}

func newPolicy(name string) *pb.Policy {
	return pb.Policy_builder{
		Id: pb.PolicyId_builder{
			ClusterName: new("c1"),
			Namespace:   new("default"),
			Name:        new(name),
		}.Build(),
	}.Build()
}

func TestListAllPages_MultiplePages(t *testing.T) {
	a, b, c, d, e := newPolicy("a"), newPolicy("b"), newPolicy("c"), newPolicy("d"), newPolicy("e")
	f := &fakePager{
		pages:  [][]*pb.Policy{{a, b}, {c}, {d, e}},
		failAt: -1,
	}
	req := pb.ListPoliciesRequest_builder{
		ClusterName: new("c1"),
		PageSize:    new(int32(2)),
		PageToken:   new("stale-initial"),
	}.Build()

	got, err := xasgrpc.ListAllPages(context.Background(), req, f.listPolicies, (*pb.ListPoliciesResponse).GetPolicies)
	if err != nil {
		t.Fatalf("ListAllPages() failed: %v", err)
	}
	if diff := cmp.Diff([]*pb.Policy{a, b, c, d, e}, got, protocmp.Transform()); diff != "" {
		t.Errorf("ListAllPages() mismatch (-want +got):\n%s", diff)
	}

	var tokens []string
	for i, call := range f.calls {
		if call.clusterName != "c1" {
			t.Errorf("call %d cluster_name = %q, want %q", i, call.clusterName, "c1")
		}
		if call.pageSize != 2 {
			t.Errorf("call %d page_size = %d, want 2", i, call.pageSize)
		}
		tokens = append(tokens, call.pageToken)
	}
	if diff := cmp.Diff([]string{"", "1", "2"}, tokens); diff != "" {
		t.Errorf("page tokens mismatch (-want +got):\n%s", diff)
	}
}

func TestListAllPages_SinglePage(t *testing.T) {
	a := newPolicy("a")
	f := &fakePager{
		pages:  [][]*pb.Policy{{a}},
		failAt: -1,
	}
	req := pb.ListPoliciesRequest_builder{ClusterName: new("c1")}.Build()

	got, err := xasgrpc.ListAllPages(context.Background(), req, f.listPolicies, (*pb.ListPoliciesResponse).GetPolicies)
	if err != nil {
		t.Fatalf("ListAllPages() failed: %v", err)
	}
	if diff := cmp.Diff([]*pb.Policy{a}, got, protocmp.Transform()); diff != "" {
		t.Errorf("ListAllPages() mismatch (-want +got):\n%s", diff)
	}
	if len(f.calls) != 1 || f.calls[0].pageToken != "" {
		t.Errorf("calls = %+v, want 1 call with empty page_token", f.calls)
	}
}

func TestListAllPages_EmptyResponse(t *testing.T) {
	f := &fakePager{
		pages:  [][]*pb.Policy{{}},
		failAt: -1,
	}
	req := pb.ListPoliciesRequest_builder{ClusterName: new("c1")}.Build()

	got, err := xasgrpc.ListAllPages(context.Background(), req, f.listPolicies, (*pb.ListPoliciesResponse).GetPolicies)
	if err != nil {
		t.Fatalf("ListAllPages() failed: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("ListAllPages() = %v, want empty slice", got)
	}
	if len(f.calls) != 1 {
		t.Errorf("len(calls) = %d, want 1", len(f.calls))
	}
}

func TestListAllPages_ErrorDiscardsPartialResults(t *testing.T) {
	wantErr := errors.New("boom")
	f := &fakePager{
		pages:  [][]*pb.Policy{{newPolicy("a")}, {newPolicy("b")}},
		failAt: 1,
		err:    wantErr,
	}
	req := pb.ListPoliciesRequest_builder{ClusterName: new("c1")}.Build()

	got, err := xasgrpc.ListAllPages(context.Background(), req, f.listPolicies, (*pb.ListPoliciesResponse).GetPolicies)
	if !errors.Is(err, wantErr) || got != nil {
		t.Errorf("ListAllPages() = %v, %v; want nil, %v", got, err, wantErr)
	}
}

func TestListAllPages_ForwardsCallOptions(t *testing.T) {
	f := &fakePager{
		pages:  [][]*pb.Policy{{newPolicy("a")}, {newPolicy("b")}},
		failAt: -1,
	}
	opt1 := testCallOption{ID: "first"}
	opt2 := testCallOption{ID: "second"}
	req := pb.ListPoliciesRequest_builder{ClusterName: new("c1")}.Build()

	_, err := xasgrpc.ListAllPages(context.Background(), req, f.listPolicies, (*pb.ListPoliciesResponse).GetPolicies, opt1, opt2)
	if err != nil {
		t.Fatalf("ListAllPages() failed: %v", err)
	}
	wantOpts := []grpc.CallOption{opt1, opt2}
	for i, call := range f.calls {
		if diff := cmp.Diff(wantOpts, call.opts); diff != "" {
			t.Errorf("call %d options mismatch (-want +got):\n%s", i, diff)
		}
	}
}

// Compile-time check that pb.XASServerClient.ListPolicies works directly with ListAllPages.
func TestListAllPages_XASServerClientSignature(t *testing.T) {
	var client pb.XASServerClient
	if client != nil {
		req := pb.ListPoliciesRequest_builder{ClusterName: new("c1")}.Build()
		_, _ = xasgrpc.ListAllPages(context.Background(), req, client.ListPolicies, (*pb.ListPoliciesResponse).GetPolicies)
	}
}
