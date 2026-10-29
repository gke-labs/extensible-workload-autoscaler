package aggregation

import (
	"strconv"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/cmd/server/internal/store/model"
	"k8s.io/apimachinery/pkg/api/resource"
)

// SeriesTransform modifies an aggregate in-place.
type SeriesTransform func(workload map[string]*pb.PodState, agg *SeriesAggregate, resourceName string)

// ContainerResourceWeightedTransform weights a container's sample by its share
// of the pod requests for resourceName. Aggregates must be weightable.
func ContainerResourceWeightedTransform(workload map[string]*pb.PodState, agg *SeriesAggregate, resourceName string) {
	weight := containerWeight(workload[agg.podName], agg.containerName, resourceName)
	agg.sample.Value *= weight
}

// weightable reports whether an aggregate can take part in a request-weighted
// rollup: either it already describes the pod as a whole, or its container
// declares a request for resourceName.
func weightable(workload map[string]*pb.PodState, agg *SeriesAggregate, resourceName string) bool {
	return agg.containerName == "" || containerWeight(workload[agg.podName], agg.containerName, resourceName) > 0
}

// ReplicaAverageTransform reports a policy-wide aggregate (one not bound to a
// pod, e.g. an external metric such as a queue length) per ready replica, like
// the average of per-pod aggregates. Aggregates bound to a pod are left untouched.
func ReplicaAverageTransform(workload map[string]*pb.PodState, agg *SeriesAggregate, _ string) {
	agg.sample.Value /= float64(max(1, readyReplicas(workload)))
}

// readyReplicas returns the number of ready pods of the workload.
func readyReplicas(workload map[string]*pb.PodState) int {
	n := 0
	for _, p := range workload {
		if p.GetIsReady() {
			n++
		}
	}
	return n
}

// containerWeight returns the share of the pod's total request for
// resourceName that is requested by the container called containerName, as a
// weight in the [0, 1] range.
//
// Weighting a per-container resource value (e.g. a usage/request ratio) by
// this share makes the sum of the weighted values of the pod's containers
// equal to the pod-level value.
//
// Only containers declaring a valid (parseable, positive) request for the
// resource count towards the pod's total.
//
// An empty containerName denotes a sample that already describes the pod as a
// whole, which keeps the full weight of 1.
func containerWeight(pod *pb.PodState, containerName, resourceName string) float64 {
	if containerName == "" {
		return 1
	}
	var request, total float64
	for _, c := range pod.GetContainers() {
		v, ok := containerRequest(c, resourceName)
		if !ok {
			continue
		}
		total += v
		if c.GetName() == containerName {
			request = v
		}
	}
	if total <= 0 {
		return 0
	}
	return request / total
}

// containerRequest returns the amount of resourceName requested by the
// container. It reports false if the container does not declare a valid
// (parseable, positive) request for the resource.
func containerRequest(c *pb.ContainerState, resourceName string) (float64, bool) {
	raw, ok := c.GetRequests()[resourceName]
	if !ok {
		return 0, false
	}
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return 0, false
	}
	v := q.AsApproximateFloat64()
	if v <= 0 {
		return 0, false
	}
	return v, true
}

// CreatePercentileTransform returns a transform replacing the distribution
// carried by an aggregate with the value of its p-th percentile, p being in the
// [0, 1] range. Aggregates without a distribution are left untouched.
func CreatePercentileTransform(p float64) SeriesTransform {
	return func(_ map[string]*pb.PodState, agg *SeriesAggregate, _ string) {
		switch s := agg.sample; {
		case s.Distribution != nil:
			agg.sample = model.Sample{Value: s.Distribution.Percentile(p)}
		case s.DecayingHistogram != nil:
			// A percentile is not affected by the decay, so the histogram does
			// not need to be decayed to the current time first.
			agg.sample = model.Sample{Value: s.DecayingHistogram.Percentile(p, s.DecayingHistogram.ReferenceTime)}
		}
	}
}

// parsePercentile parses a percentile such as "p95" or "95" into the [0, 1]
// range, defaulting to 0.95.
func parsePercentile(s string) float64 {
	if len(s) > 0 {
		cleanStr := s
		if len(s) > 1 && (s[0] == 'p' || s[0] == 'P') {
			cleanStr = s[1:]
		}
		if val, err := strconv.ParseFloat(cleanStr, 64); err == nil {
			return val / 100.0
		}
	}
	return 0.95
}
