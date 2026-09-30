package aggregation

import (
	"iter"
	"math"
	"time"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store/model"
)

// SeriesReducer combines a set of samples into a single sample.
type SeriesReducer func(samples iter.Seq[model.Sample]) model.Sample

// NewSeriesReducer returns the reducer named op ("Avg", "Sum", "Max"
// or "Min"), defaulting to the average.
func NewSeriesReducer(op string) SeriesReducer {
	switch op {
	case "Sum":
		return SumSeriesReducer
	case "Max":
		return MaxSeriesReducer
	case "Min":
		return MinSeriesReducer
	default:
		return AverageSeriesReducer
	}
}

// SumSeriesReducer sums the samples.
func SumSeriesReducer(samples iter.Seq[model.Sample]) model.Sample {
	var sum float64
	for s := range samples {
		sum += s.Value
	}
	return model.Sample{Value: sum}
}

// AverageSeriesReducer averages the samples.
func AverageSeriesReducer(samples iter.Seq[model.Sample]) model.Sample {
	var sum float64
	var count int
	for s := range samples {
		sum += s.Value
		count++
	}
	if count == 0 {
		return model.Sample{Value: 0}
	}
	return model.Sample{Value: sum / float64(count)}
}

// MaxSeriesReducer returns the largest sample.
func MaxSeriesReducer(samples iter.Seq[model.Sample]) model.Sample {
	maxValue, found := math.Inf(-1), false
	for s := range samples {
		maxValue, found = max(maxValue, s.Value), true
	}
	if !found {
		return model.Sample{}
	}
	return model.Sample{Value: maxValue}
}

// MinSeriesReducer returns the smallest sample.
func MinSeriesReducer(samples iter.Seq[model.Sample]) model.Sample {
	minValue, found := math.Inf(1), false
	for s := range samples {
		minValue, found = min(minValue, s.Value), true
	}
	if !found {
		return model.Sample{}
	}
	return model.Sample{Value: minValue}
}

// MergeDistributionSeriesReducer sums the buckets of the distributions
// carried by the samples into a single one, i.e. the resulting distribution
// holds the observations of every input distribution. The input distributions
// are left untouched.
func MergeDistributionSeriesReducer(samples iter.Seq[model.Sample]) model.Sample {
	merged := model.NewDistribution()
	for s := range samples {
		if s.Distribution != nil {
			merged.Merge(s.Distribution)
		}
	}
	return model.Sample{Distribution: merged}
}

// CreateMergeDecayingDistributionSeriesReducer returns a reducer
// merging the decaying histograms carried by the samples into a single one,
// i.e. the resulting distribution holds the samples of every input
// distribution. The input histograms are left untouched.
func CreateMergeDecayingDistributionSeriesReducer(def *pb.DecayingDistribution) SeriesReducer {
	halfLife := def.HalfLife.AsDuration()
	return func(samples iter.Seq[model.Sample]) model.Sample {
		// The reference time is set by the first merged histogram.
		merged, err := model.NewDecayingHistogram(time.Time{}, halfLife, def.BucketSize)
		if err != nil {
			return model.Sample{}
		}
		for s := range samples {
			if s.DecayingHistogram != nil {
				merged.Merge(s.DecayingHistogram)
			}
		}
		return model.Sample{DecayingHistogram: merged}
	}
}
