package grpc

import (
	"cmp"
	"encoding/base64"
	"encoding/json"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

const (
	defaultPageSize = 100
	maxPageSize     = 1000
)

func effectivePageSize(requested int32) int {
	switch {
	case requested == 0:
		return defaultPageSize
	case requested > maxPageSize:
		return maxPageSize
	default:
		return int(requested)
	}
}

// startIndex returns the index of the first policy sorted after cursor in
// policies, which must be sorted by cluster, namespace and name. A nil cursor
// (first page) starts at 0. The cursor's policy need not exist anymore: if it
// was deleted, the search lands on its would-be position, which already is
// the next policy.
func startIndex(policies []*pb.Policy, cursor *pageCursor) int {
	if cursor == nil {
		return 0
	}
	i, found := slices.BinarySearchFunc(policies, cursor, func(p *pb.Policy, c *pageCursor) int {
		id := p.GetId()
		return cmp.Or(
			cmp.Compare(id.GetClusterName(), c.Cluster),
			cmp.Compare(id.GetNamespace(), c.Namespace),
			cmp.Compare(id.GetName(), c.Name),
		)
	})
	if found {
		return i + 1
	}
	return i
}

type pageCursor struct {
	Cluster   string `json:"c"`
	Namespace string `json:"ns"`
	Name      string `json:"n"`
}

func (c pageCursor) encode() (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

func decodePageToken(token, clusterName string) (*pageCursor, error) {
	if token == "" {
		return nil, nil
	}
	b, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid page_token")
	}
	var c pageCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid page_token")
	}
	if c.Cluster != clusterName {
		return nil, status.Errorf(codes.InvalidArgument, "page_token was issued for a different cluster_name")
	}
	return &c, nil
}
