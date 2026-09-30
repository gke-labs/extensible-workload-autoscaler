package aggregation

import (
	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/podutil"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store/model"
)

// SeriesFilter checks if an aggregate should be filtered out, e.g. because it
// represents a non-ready pod.
type SeriesFilter func(workload map[string]*pb.PodState, agg *SeriesAggregate) bool

// NonEligiblePodSeriesFilter reports whether an aggregate belongs to a pod that's not
// ready and should be ignored.
func NonEligiblePodSeriesFilter(workload map[string]*pb.PodState, agg *SeriesAggregate) bool {
	if agg.podName == "" {
		return false
	}
	p, ok := workload[agg.podName]
	if !ok {
		return true
	}
	return !podutil.IsEligible(podutil.FromPodState(p))
}

// CreateLabelSeriesFilter returns a filter reporting whether an aggregate does not
// carry every label in filter and should be ignored. An empty filter matches
// every aggregate.
//
// The filter must run before the labels dimension is aggregated away.
func CreateLabelSeriesFilter(filter map[string]string) SeriesFilter {
	return func(_ map[string]*pb.PodState, agg *SeriesAggregate) bool {
		return !model.MatchFilter(agg.labels, filter)
	}
}
