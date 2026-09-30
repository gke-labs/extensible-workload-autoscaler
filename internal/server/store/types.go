package store

import (
	"fmt"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

// policyID identifies a policy across clusters.
type policyID struct {
	// cluster is the name of the cluster the policy belongs to.
	cluster string
	// ns is the namespace of the policy.
	ns string
	// name is the name of the policy.
	name string
}

// newPolicyID returns the policyID of the policy identified by id.
func newPolicyID(id *pb.PolicyId) policyID {
	return policyID{cluster: id.GetClusterName(), ns: id.GetNamespace(), name: id.GetName()}
}

// MarshalText implements encoding.TextMarshaler for JSON encoding.
func (id policyID) MarshalText() ([]byte, error) {
	return fmt.Appendf(nil, "%s/%s/%s", id.cluster, id.ns, id.name), nil
}

// metricID identifies the metric whose state is tracked. A metric is
// identified by the <name, recommenderName> pair: its name is only unique
// within its owner. Policy-wide metrics have no owner.
type metricID struct {
	// name is the name of the metric.
	name string
	// recommenderName is the name of the recommender owning the metric, or
	// empty for policy-wide metrics.
	recommenderName string
}

// newMetricID returns the ID of the metric defined by def.
func newMetricID(def *pb.MetricDefinition) metricID {
	return metricID{name: def.GetName(), recommenderName: def.GetRecommenderName()}
}

// String returns "<recommenderName>/<name>" for owned metrics and the bare
// name for policy-wide metrics.
func (id metricID) String() string {
	if id.recommenderName == "" {
		return id.name
	}
	return id.recommenderName + "/" + id.name
}

// MarshalText implements encoding.TextMarshaler so that maps keyed by metricID
// can be serialized to JSON (e.g. by the state dump).
func (id metricID) MarshalText() ([]byte, error) {
	return []byte(id.String()), nil
}
