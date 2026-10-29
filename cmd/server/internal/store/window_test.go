package store

import (
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestNewSlidingWindow(t *testing.T) {
	duration := 5 * time.Minute
	agg := "Avg"
	w := NewSlidingWindow(duration, agg)

	if w.Duration != duration {
		t.Errorf("Duration = %v, want %v", w.Duration, duration)
	}
	if w.Aggregation != agg {
		t.Errorf("Aggregation = %v, want %v", w.Aggregation, agg)
	}
	if len(w.Samples) != 0 {
		t.Errorf("Initial samples count = %d, want 0", len(w.Samples))
	}
}

func TestSlidingWindow_AddAndPrune(t *testing.T) {
	duration := 10 * time.Second
	w := NewSlidingWindow(duration, "Avg")
	start := time.Unix(1000, 0)

	// 1. Add samples within the window
	w.Add(1.0, start)
	w.Add(2.0, start.Add(2*time.Second))
	w.Add(3.0, start.Add(5*time.Second))

	if len(w.Samples) != 3 {
		t.Errorf("Samples count = %d, want 3", len(w.Samples))
	}

	// 2. Add sample that causes pruning
	// New sample at 1012. Window is [1002, 1012].
	// s[0] at 1000: cutoff = 1002. 1000 <= 1002, so pruned.
	// s[1] at 1002: cutoff = 1002. 1002 <= 1002, so pruned.
	// s[2] at 1005: cutoff = 1002. 1005 > 1002, so kept.
	w.Add(4.0, start.Add(12*time.Second))

	if len(w.Samples) != 2 { // s[2] and new sample at 1012
		t.Errorf("Samples count after pruning = %d, want 2", len(w.Samples))
	}

	wantSamples := []float64{3.0, 4.0}
	var gotSamples []float64
	for _, s := range w.Samples {
		gotSamples = append(gotSamples, s.Value)
	}

	if diff := cmp.Diff(wantSamples, gotSamples); diff != "" {
		t.Errorf("Samples values mismatch (-want +got):\n%s", diff)
	}
}

func TestSlidingWindow_Value(t *testing.T) {
	tests := []struct {
		name        string
		aggregation string
		values      []float64
		want        float64
		wantErr     bool
	}{
		{
			name:        "Average",
			aggregation: "Avg",
			values:      []float64{10, 20, 30},
			want:        20.0,
		},
		{
			name:        "Max",
			aggregation: "Max",
			values:      []float64{10, 50, 30},
			want:        50.0,
		},
		{
			name:        "Min",
			aggregation: "Min",
			values:      []float64{10, 50, 5},
			want:        5.0,
		},
		{
			name:        "Sum",
			aggregation: "Sum",
			values:      []float64{1, 2, 3, 4},
			want:        10.0,
		},
		{
			name:        "Empty window",
			aggregation: "Avg",
			values:      []float64{},
			wantErr:     true,
		},
		{
			name:        "Unknown aggregation",
			aggregation: "Unknown",
			values:      []float64{1, 2, 3},
			wantErr:     true,
		},
	}

	start := time.Unix(1000, 0)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := NewSlidingWindow(1*time.Hour, tc.aggregation)
			for i, v := range tc.values {
				w.Add(v, start.Add(time.Duration(i)*time.Second))
			}

			got, err := w.Value(start.Add(time.Duration(len(tc.values)) * time.Second))
			if (err != nil) != tc.wantErr {
				t.Fatalf("Value() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}

			if diff := cmp.Diff(tc.want, got, cmpopts.EquateApprox(0, 0.0001)); diff != "" {
				t.Errorf("Value() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSlidingWindow_LargePruning(t *testing.T) {
	// Test pruning when all samples should be removed
	duration := 1 * time.Second
	w := NewSlidingWindow(duration, "Sum")
	start := time.Unix(1000, 0)

	w.Add(10.0, start)
	w.Add(20.0, start.Add(100*time.Millisecond))

	// Add sample 10 seconds later
	w.Add(30.0, start.Add(10*time.Second))

	if len(w.Samples) != 1 {
		t.Errorf("Expected 1 sample after large time jump, got %d", len(w.Samples))
	}

	val, _ := w.Value(start.Add(10 * time.Second))
	if val != 30.0 {
		t.Errorf("Value = %f, want 30.0", val)
	}
}

func TestSlidingWindow_ExtremeValues(t *testing.T) {
	w := NewSlidingWindow(1*time.Hour, "Max")
	start := time.Now()

	w.Add(math.MaxFloat64, start)
	w.Add(0, start.Add(time.Second))

	val, _ := w.Value(start.Add(time.Second))
	if val != math.MaxFloat64 {
		t.Errorf("Max should handle MaxFloat64, got %f", val)
	}

	wMin := NewSlidingWindow(1*time.Hour, "Min")
	wMin.Add(-math.MaxFloat64, start)
	wMin.Add(0, start.Add(time.Second))

	valMin, _ := wMin.Value(start.Add(time.Second))
	if valMin != -math.MaxFloat64 {
		t.Errorf("Min should handle -MaxFloat64, got %f", valMin)
	}
}
