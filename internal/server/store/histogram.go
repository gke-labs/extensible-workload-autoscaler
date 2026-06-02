package store

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
)

// DecayingHistogram implements a time-decaying histogram.
// Samples are weighted based on their age relative to a half-life.
type DecayingHistogram struct {
	HalfLife   time.Duration
	BucketSize float64

	// buckets maps bucket_index -> weighted_count
	Buckets map[int]float64
	// Reference timestamp for decay calculation
	ReferenceTime time.Time
	// Total weighted count
	TotalWeight float64
}

// NewDecayingHistogram creates a new decaying histogram.
func NewDecayingHistogram(now time.Time, halfLife time.Duration, bucketSizeStr string) (*DecayingHistogram, error) {
	// Parse bucket size (supports "0.1", "100Mi", etc.)
	// For now, simple float parsing. TODO: Add resource quantity parsing.
	bucketSize, err := strconv.ParseFloat(bucketSizeStr, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid bucket size: %v", err)
	}

	return &DecayingHistogram{
		HalfLife:      halfLife,
		BucketSize:    bucketSize,
		Buckets:       make(map[int]float64),
		ReferenceTime: now,
		TotalWeight:   0,
	}, nil
}

// Add sample to the histogram.
func (h *DecayingHistogram) Add(value float64, timestamp time.Time) {
	if timestamp.Before(h.ReferenceTime) {
		// Sample is from the past. Add it with a reduced weight
		// equivalent to what it would be if it had been added at its own time
		// and then decayed to the current ReferenceTime.
		elapsed := h.ReferenceTime.Sub(timestamp)
		weight := math.Pow(0.5, float64(elapsed)/float64(h.HalfLife))

		bucketIdx := int(math.Floor(value / h.BucketSize))
		h.Buckets[bucketIdx] += weight
		h.TotalWeight += weight
		return
	}

	// Decay existing data to the new timestamp
	h.decay(timestamp)

	// Add new sample with weight 1.0 (since it's "now")
	bucketIdx := int(math.Floor(value / h.BucketSize))
	h.Buckets[bucketIdx] += 1.0
	h.TotalWeight += 1.0
}

// decay updates the weights of all buckets based on the elapsed time.
func (h *DecayingHistogram) decay(now time.Time) {
	if now.Before(h.ReferenceTime) || now.Equal(h.ReferenceTime) {
		return
	}

	elapsed := now.Sub(h.ReferenceTime)
	decayFactor := math.Pow(0.5, float64(elapsed)/float64(h.HalfLife))

	// If decay is too small, just reset (optimization)
	if decayFactor < 1e-6 {
		h.Buckets = make(map[int]float64)
		h.TotalWeight = 0
		h.ReferenceTime = now
		return
	}

	// Apply decay
	h.TotalWeight *= decayFactor
	for k := range h.Buckets {
		h.Buckets[k] *= decayFactor
		// Cleanup empty buckets
		if h.Buckets[k] < 1e-6 {
			delete(h.Buckets, k)
		}
	}

	h.ReferenceTime = now
}

// Percentile calculates the approximate percentile value.
func (h *DecayingHistogram) Percentile(p float64, now time.Time) float64 {
	h.decay(now)

	if h.TotalWeight == 0 {
		return 0
	}

	targetWeight := h.TotalWeight * p
	currentWeight := 0.0

	// Find the bucket containing the percentile
	// Optimization: Iterate only over existing buckets in sorted order
	indices := make([]int, 0, len(h.Buckets))
	for k := range h.Buckets {
		indices = append(indices, k)
	}
	sort.Ints(indices)

	for _, i := range indices {
		w := h.Buckets[i]
		currentWeight += w
		if currentWeight >= targetWeight {
			return float64(i+1) * h.BucketSize
		}
	}

	if len(indices) > 0 {
		return float64(indices[len(indices)-1]+1) * h.BucketSize
	}
	return 0
}
