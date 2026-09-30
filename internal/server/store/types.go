package store

import (
	"fmt"
	"hash/fnv"
	"sort"

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

// seriesID identifies a timeseries within a metric. It's composed of the pod,
// container and labels of the series.
type seriesID struct {
	// pod is the name of the pod reporting the series, or empty for series
	// not attributed to a pod.
	pod string
	// container is the name of the container reporting the series, or empty
	// for series not attributed to a container.
	container string
	// labelHash is the hash of the series labels.
	labelHash uint64
}

// newSeriesID returns the ID of the series reported by the given container of
// the given pod with the given labels.
func newSeriesID(pod, container string, labels map[string]string) seriesID {
	return seriesID{pod: pod, container: container, labelHash: hashLabels(labels)}
}

// MarshalText implements encoding.TextMarshaler for JSON encoding.
func (id seriesID) MarshalText() ([]byte, error) {
	return fmt.Appendf(nil, "%s|%s|%016x", id.pod, id.container, id.labelHash), nil
}

// hashLabels returns the 64-bit FNV-1a hash of labels. The hash does not depend
// on the iteration order of the map. Empty label sets hash to 0.
func hashLabels(labels map[string]string) uint64 {
	if len(labels) == 0 {
		return 0
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, k := range keys {
		// Separate keys and values with a byte that cannot appear in label
		// names, so that e.g. {"a": "bc"} and {"ab": "c"} hash differently.
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(labels[k]))
		h.Write([]byte{0})
	}
	return h.Sum64()
}
