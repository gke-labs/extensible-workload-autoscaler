package model

import (
	"cmp"
	"math"
	"slices"
	"strconv"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

// Distribution is a cumulative histogram with floating point bucket counts,
// such as the per-second rate of each bucket of a Prometheus histogram.
// Buckets are keyed by their upper bound ("le"), "+Inf" being the last one.
type Distribution struct {
	Buckets map[string]float64
}

// NewDistribution returns an empty distribution.
func NewDistribution() *Distribution {
	return &Distribution{Buckets: make(map[string]float64)}
}

// NewDistributionFromHistogram returns the distribution holding the buckets
// of h, or nil if h is nil. Counts are exact up to 2^53.
func NewDistributionFromHistogram(h *pb.Histogram) *Distribution {
	if h == nil {
		return nil
	}
	d := &Distribution{Buckets: make(map[string]float64, len(h.GetBuckets()))}
	for le, count := range h.GetBuckets() {
		d.Buckets[le] = float64(count)
	}
	return d
}

// RateDistribution returns the per-second rate of each bucket of a cumulative
// distribution, from the counts observed in previous to the ones observed in
// current, deltaSeconds later. A bucket whose count decreased (e.g. after a
// counter reset) counts from zero, like a bucket missing from previous.
func RateDistribution(previous, current *Distribution, deltaSeconds float64) *Distribution {
	d := NewDistribution()
	for le, count := range current.Buckets {
		diff := count - previous.Buckets[le]
		if diff < 0 {
			diff = count
		}
		d.Buckets[le] = diff / deltaSeconds
	}
	return d
}

// Merge adds the buckets of other into d.
func (d *Distribution) Merge(other *Distribution) {
	for le, count := range other.Buckets {
		d.Buckets[le] += count
	}
}

// Percentile returns the approximate value under which the fraction p (in the
// [0, 1] range) of the observations fall, by interpolating linearly within the
// bucket holding the p-th observation, like Prometheus' histogram_quantile.
// The first bucket is assumed to start at 0.
//
// It returns the lower bound of the "+Inf" bucket when the p-th observation
// falls into it, and 0 when the distribution is empty.
func (d *Distribution) Percentile(p float64) float64 {
	type bucket struct{ le, count float64 }
	buckets := make([]bucket, 0, len(d.Buckets))
	for leStr, count := range d.Buckets {
		// ParseFloat accepts "+Inf".
		le, err := strconv.ParseFloat(leStr, 64)
		if err != nil {
			continue
		}
		buckets = append(buckets, bucket{le: le, count: count})
	}
	if len(buckets) == 0 {
		return 0
	}
	slices.SortFunc(buckets, func(a, b bucket) int { return cmp.Compare(a.le, b.le) })

	total := buckets[len(buckets)-1].count
	if total <= 0 {
		return 0
	}
	rank := total * p
	var prevLe, prevCount float64
	for _, b := range buckets {
		if b.count < rank {
			prevLe, prevCount = b.le, b.count
			continue
		}
		if math.IsInf(b.le, 1) {
			return prevLe
		}
		countDiff := b.count - prevCount
		if countDiff <= 0 {
			return b.le
		}
		return prevLe + (b.le-prevLe)*(rank-prevCount)/countDiff
	}
	return prevLe
}
