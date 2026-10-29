package aggregation

import (
	"time"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/podutil"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store/model"
)

// SeriesFilter checks if an aggregate should be filtered out as of time now,
// e.g. because it represents a non-eligible pod.
type SeriesFilter func(workload map[string]*pb.PodState, agg *SeriesAggregate, now time.Time) bool

// CreateNonEligiblePodSeriesFilter returns a filter reporting whether an
// aggregate belongs to a pod that's not eligible for a metric with the given
// eligibility, and should be ignored. See podutil.IsPodEligible.
func CreateNonEligiblePodSeriesFilter(eligibility *pb.MetricDefinition_Eligibility) SeriesFilter {
	return func(workload map[string]*pb.PodState, agg *SeriesAggregate, now time.Time) bool {
		return agg.podName != "" && !podutil.IsPodEligible(workload, agg.podName, eligibility, now)
	}
}

// CreateLabelSeriesFilter returns a filter reporting whether an aggregate does not
// carry every label in filter and should be ignored. An empty filter matches
// every aggregate.
//
// The filter must run before the labels dimension is aggregated away.
func CreateLabelSeriesFilter(filter map[string]string) SeriesFilter {
	return func(_ map[string]*pb.PodState, agg *SeriesAggregate, _ time.Time) bool {
		return !model.MatchFilter(agg.labels, filter)
	}
}
