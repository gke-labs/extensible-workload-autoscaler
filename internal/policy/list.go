package policy

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

// Lister is the subset of pb.XASServerClient used by ListAll.
type Lister interface {
	ListPolicies(ctx context.Context, in *pb.ListPoliciesRequest, opts ...grpc.CallOption) (*pb.ListPoliciesResponse, error)
}

// ListAll returns every policy of a cluster, following next_page_token until
// the last page. If any page fails, it returns the error and no policies, so
// that callers never mistake a partial list for the full one.
//
// The pages are not a consistent snapshot: a policy created while listing is
// only returned if it sorts after the current page. ctx bounds the whole
// listing, not each page.
func ListAll(ctx context.Context, client Lister, clusterName string) ([]*pb.Policy, error) {
	var policies []*pb.Policy
	token := ""
	for {
		resp, err := client.ListPolicies(ctx, pb.ListPoliciesRequest_builder{
			ClusterName: proto.String(clusterName),
			PageToken:   proto.String(token),
		}.Build())
		if err != nil {
			return nil, err
		}
		policies = append(policies, resp.GetPolicies()...)
		token = resp.GetNextPageToken()
		if token == "" {
			return policies, nil
		}
	}
}
