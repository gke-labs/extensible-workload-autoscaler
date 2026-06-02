package store

import (
	"fmt"
	"math"
	"time"
)

// SlidingWindow implements a simple sliding window aggregation.
// It keeps a fixed duration of samples.
type SlidingWindow struct {
	Duration    time.Duration
	Aggregation string // "Avg", "Max", "Min", "Sum"

	// Samples sorted by time (insertion order)
	Samples []WindowSample
}

// WindowSample represents a timestamped value.
type WindowSample struct {
	Value     float64
	Timestamp time.Time
}

// NewSlidingWindow creates a new sliding window.
func NewSlidingWindow(duration time.Duration, aggregation string) *SlidingWindow {
	return &SlidingWindow{
		Duration:    duration,
		Aggregation: aggregation,
		Samples:     make([]WindowSample, 0),
	}
}

// Add sample to the window.
func (w *SlidingWindow) Add(value float64, timestamp time.Time) {
	// Prune old samples
	cutoff := timestamp.Add(-w.Duration)
	startIdx := len(w.Samples)
	for i, s := range w.Samples {
		if s.Timestamp.After(cutoff) {
			startIdx = i
			break
		}
	}
	if startIdx > 0 {
		w.Samples = w.Samples[startIdx:]
	}

	// Add new sample
	w.Samples = append(w.Samples, WindowSample{Value: value, Timestamp: timestamp})
}

// Value calculates the aggregated value of the current window.
func (w *SlidingWindow) Value(now time.Time) (float64, error) {
	// Prune old samples
	cutoff := now.Add(-w.Duration)
	startIdx := len(w.Samples)
	for i, s := range w.Samples {
		if s.Timestamp.After(cutoff) {
			startIdx = i
			break
		}
	}
	if startIdx > 0 {
		w.Samples = w.Samples[startIdx:]
	}

	if len(w.Samples) == 0 {
		return 0, fmt.Errorf("no samples in window")
	}

	switch w.Aggregation {
	case "Avg":
		sum := 0.0
		for _, s := range w.Samples {
			sum += s.Value
		}
		return sum / float64(len(w.Samples)), nil
	case "Max":
		max := math.Inf(-1)
		for _, s := range w.Samples {
			if s.Value > max {
				max = s.Value
			}
		}
		return max, nil
	case "Min":
		min := math.Inf(1)
		for _, s := range w.Samples {
			if s.Value < min {
				min = s.Value
			}
		}
		return min, nil
	case "Sum":
		sum := 0.0
		for _, s := range w.Samples {
			sum += s.Value
		}
		return sum, nil
	default:
		return 0, fmt.Errorf("unknown aggregation: %s", w.Aggregation)
	}
}
