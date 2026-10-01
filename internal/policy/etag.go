package policy

import (
	"fmt"
	"hash/fnv"
	"strconv"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"google.golang.org/protobuf/proto"
)

func CreateEtag(policy *pb.Policy) (string, error) {
	// Shallow copy of every public field.
	p := &pb.Policy{
		Id:                 policy.Id,
		Workload:           policy.Workload,
		MinReplicas:        policy.MinReplicas,
		MaxReplicas:        policy.MaxReplicas,
		Metrics:            policy.Metrics,
		Activation:         policy.Activation,
		Scaling:            policy.Scaling,
		Selector:           policy.Selector,
		RecommenderMetrics: policy.RecommenderMetrics,
		Etag:               "", // left empty. the etag is not part of its own hash.
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("unable to marshal policy to bytes: %w", err)
	}
	h := fnv.New64a()
	h.Write(b)
	etag := strconv.FormatUint(h.Sum64(), 16)
	return etag, nil
}
