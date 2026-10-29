package engine

import (
	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

// PolicyID uniquely identifies a policy (in a given cluster).
type PolicyID struct {
	name      string
	namespace string
}

// PolicyIDFromProto creates a PolicyID from a proto PolicyId and returns the cluster name as well.
func PolicyIDFromProto(id *pb.PolicyId) (PolicyID, string) {
	return PolicyID{
		name:      id.GetName(),
		namespace: id.GetNamespace(),
	}, id.GetClusterName()
}

// PolicyState holds a policy's state, i.e. the policy definition itself and
// the state of the workload it refers to.
type PolicyState struct {
	policy   *pb.Policy
	workload *pb.Workload
}

// State holds the whole engine state, i.e. policies and workloads.
type State map[PolicyID]*PolicyState
