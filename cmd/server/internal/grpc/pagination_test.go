package grpc

import (
	"encoding/base64"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

func TestPageToken_RoundTrip(t *testing.T) {
	want := pageCursor{Cluster: "c1", Namespace: "default", Name: "web"}
	token, err := want.encode()
	if err != nil {
		t.Fatalf("encode() failed: %v", err)
	}
	got, err := decodePageToken(token, "c1")
	if err != nil {
		t.Fatalf("decodePageToken() failed: %v", err)
	}
	if diff := cmp.Diff(&want, got); diff != "" {
		t.Errorf("decodePageToken() mismatch (-want +got):\n%s", diff)
	}
}

func TestDecodePageToken_Empty(t *testing.T) {
	got, err := decodePageToken("", "c1")
	if err != nil || got != nil {
		t.Errorf("decodePageToken(\"\") = %v, %v; want nil, nil", got, err)
	}
}

func TestDecodePageToken_Invalid(t *testing.T) {
	otherCluster, err := pageCursor{Cluster: "c2", Namespace: "default", Name: "web"}.encode()
	if err != nil {
		t.Fatalf("encode() failed: %v", err)
	}
	tests := []struct {
		name  string
		token string
	}{
		{name: "not base64", token: "%%%"},
		{name: "not JSON", token: base64.URLEncoding.EncodeToString([]byte("not json"))},
		{name: "other cluster", token: otherCluster},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodePageToken(tc.token, "c1")
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Errorf("decodePageToken() code = %v, want %v (err: %v)", got, codes.InvalidArgument, err)
			}
		})
	}
}

func TestEffectivePageSize(t *testing.T) {
	tests := []struct {
		requested int32
		want      int
	}{
		{requested: 0, want: defaultPageSize},
		{requested: 1, want: 1},
		{requested: maxPageSize, want: maxPageSize},
		{requested: maxPageSize + 1, want: maxPageSize},
	}
	for _, tc := range tests {
		if got := effectivePageSize(tc.requested); got != tc.want {
			t.Errorf("effectivePageSize(%d) = %d, want %d", tc.requested, got, tc.want)
		}
	}
}

func TestStartIndex(t *testing.T) {
	var policies []*pb.Policy
	for _, name := range []string{"a", "c", "d"} { // "b" was deleted
		policies = append(policies, pb.Policy_builder{
			Id: pb.PolicyId_builder{
				ClusterName: proto.String("c1"),
				Namespace:   proto.String("default"),
				Name:        proto.String(name),
			}.Build(),
		}.Build())
	}
	cursor := func(name string) *pageCursor {
		return &pageCursor{Cluster: "c1", Namespace: "default", Name: name}
	}
	tests := []struct {
		name   string
		cursor *pageCursor
		want   int
	}{
		{name: "first page", cursor: nil, want: 0},
		{name: "cursor exists", cursor: cursor("a"), want: 1},
		{name: "cursor deleted", cursor: cursor("b"), want: 1},
		{name: "cursor is last", cursor: cursor("d"), want: 3},
		{name: "cursor past end", cursor: cursor("z"), want: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := startIndex(policies, tc.cursor); got != tc.want {
				t.Errorf("startIndex() = %d, want %d", got, tc.want)
			}
		})
	}
}
