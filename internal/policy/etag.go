package policy

import (
	"fmt"
	"hash/fnv"
	"strconv"

	"google.golang.org/protobuf/proto"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

func CreateEtag(policy *pb.Policy) (string, error) {
	// The etag is not part of its own hash.
	p := proto.CloneOf(policy)
	p.ClearEtag()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("unable to marshal policy to bytes: %w", err)
	}
	h := fnv.New64a()
	h.Write(b)
	etag := strconv.FormatUint(h.Sum64(), 16)
	return etag, nil
}
