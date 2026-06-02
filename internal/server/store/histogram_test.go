package store

import (
	"math"
	"testing"
	"time"
)

func TestNewDecayingHistogram(t *testing.T) {
	now := time.Now()
	halfLife := 10 * time.Minute

	tests := []struct {
		name         string
		bucketSize   string
		wantError    bool
		wantSize     float64
		wantRefTime  time.Time
		wantHalfLife time.Duration
	}{
		{
			name:         "Valid initialization",
			bucketSize:   "0.1",
			wantError:    false,
			wantSize:     0.1,
			wantRefTime:  now,
			wantHalfLife: halfLife,
		},
		{
			name:       "Invalid bucket size (text)",
			bucketSize: "invalid",
			wantError:  true,
		},
		{
			name:         "Valid integer string",
			bucketSize:   "10",
			wantError:    false,
			wantSize:     10.0,
			wantRefTime:  now,
			wantHalfLife: halfLife,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, err := NewDecayingHistogram(now, halfLife, tc.bucketSize)
			if (err != nil) != tc.wantError {
				t.Fatalf("NewDecayingHistogram() error = %v, wantError %v", err, tc.wantError)
			}
			if tc.wantError {
				return
			}

			if h.BucketSize != tc.wantSize {
				t.Errorf("BucketSize = %v, want %v", h.BucketSize, tc.wantSize)
			}
			if !h.ReferenceTime.Equal(tc.wantRefTime) {
				t.Errorf("ReferenceTime = %v, want %v", h.ReferenceTime, tc.wantRefTime)
			}
			if h.HalfLife != tc.wantHalfLife {
				t.Errorf("HalfLife = %v, want %v", h.HalfLife, tc.wantHalfLife)
			}
			if len(h.Buckets) != 0 {
				t.Error("New histogram should have empty buckets")
			}
			if h.TotalWeight != 0 {
				t.Error("New histogram should have 0 TotalWeight")
			}
		})
	}
}

func TestDecayingHistogram_Add(t *testing.T) {
	start := time.Unix(1000, 0)
	halfLife := 10 * time.Second
	// Bucket size 1.0 means value 1.5 goes to bucket 1 [1.0, 2.0)
	h, err := NewDecayingHistogram(start, halfLife, "1.0")
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// 1. Add sample at T0
	h.Add(1.5, start)

	if h.TotalWeight != 1.0 {
		t.Errorf("After first add, TotalWeight = %f, want 1.0", h.TotalWeight)
	}
	if h.Buckets[1] != 1.0 {
		t.Errorf("Bucket[1] = %f, want 1.0", h.Buckets[1])
	}

	// 2. Add sample at T0 + HalfLife (10s later)
	// Existing weight (1.0) should decay to 0.5.
	// New sample adds 1.0.
	// Total should be 1.5.
	t1 := start.Add(halfLife)
	h.Add(2.5, t1) // Bucket 2

	if math.Abs(h.TotalWeight-1.5) > 1e-6 {
		t.Errorf("At T1, TotalWeight = %f, want 1.5", h.TotalWeight)
	}
	if math.Abs(h.Buckets[1]-0.5) > 1e-6 {
		t.Errorf("At T1, Bucket[1] (decayed) = %f, want 0.5", h.Buckets[1])
	}
	if math.Abs(h.Buckets[2]-1.0) > 1e-6 {
		t.Errorf("At T1, Bucket[2] (new) = %f, want 1.0", h.Buckets[2])
	}
	if !h.ReferenceTime.Equal(t1) {
		t.Errorf("ReferenceTime not updated to %v, got %v", t1, h.ReferenceTime)
	}

	// 3. Add sample in the PAST (T0)
	// Current RefTime is T1 (1010). T0 (1000) is in past (1 HL ago).
	// It should be added with weight 0.5.
	h.Add(3.5, start) // Bucket 3

	// Total was 1.5. Add 0.5 -> 2.0.
	if math.Abs(h.TotalWeight-2.0) > 1e-6 {
		t.Errorf("After past add, TotalWeight = %f, want 2.0", h.TotalWeight)
	}
	if math.Abs(h.Buckets[3]-0.5) > 1e-6 {
		t.Errorf("After past add, Bucket[3] = %f, want 0.5", h.Buckets[3])
	}
	// RefTime should NOT change (it was T1)
	if !h.ReferenceTime.Equal(t1) {
		t.Errorf("ReferenceTime changed on past add to %v, should stay %v", h.ReferenceTime, t1)
	}
}

func TestDecayingHistogram_OutOfOrder(t *testing.T) {
	start := time.Unix(1000, 0)
	halfLife := 10 * time.Second
	h, _ := NewDecayingHistogram(start, halfLife, "1.0")

	// 1. Add high value (100) at T0
	h.Add(100.0, start) // Bucket 100, Weight 1.0

	// 2. Advance to T1 (10s later, 1 HalfLife)
	// We don't call anything yet. ReferenceTime is still T0.

	// 3. Add low value (10) at T1
	t1 := start.Add(10 * time.Second)
	h.Add(10.0, t1)
	// Histogram decays T0->T1: Weight(100) = 0.5
	// New sample at T1: Weight(10) = 1.0
	// ReferenceTime = T1

	if math.Abs(h.TotalWeight-1.5) > 1e-6 {
		t.Fatalf("TotalWeight mismatch: got %f, want 1.5", h.TotalWeight)
	}

	// 4. ADD SAMPLE FROM PAST (T0)
	// ReferenceTime is T1 (1010). We add at T0 (1000).
	// It should be added with weight 0.5 (since it's 1 HL old relative to T1).
	h.Add(10.0, start)

	// TotalWeight: 1.5 + 0.5 = 2.0
	if math.Abs(h.TotalWeight-2.0) > 1e-6 {
		t.Errorf("TotalWeight after out-of-order: got %f, want 2.0", h.TotalWeight)
	}
	// Weight in Bucket 10: 1.0 (from step 3) + 0.5 (from step 4) = 1.5
	if math.Abs(h.Buckets[10]-1.5) > 1e-6 {
		t.Errorf("Bucket 10 weight: got %f, want 1.5", h.Buckets[10])
	}

	// 5. Verify Percentile (P90)
	// Weight(100) = 0.5. Weight(10) = 1.5. Total = 2.0.
	// Target p90 = 2.0 * 0.9 = 1.8.
	// Bucket 10 (weight 1.5) is NOT enough.
	// We need Bucket 100.
	p90 := h.Percentile(0.9, t1)
	if p90 != 101.0 {
		t.Errorf("P90 should still be high: got %f, want 101.0", p90)
	}

	// 6. Add more low values at T1 to push P90 down
	h.Add(10.0, t1) // Weight(10) becomes 1.5 + 1.0 = 2.5. Total = 3.0.
	// Target p90 = 3.0 * 0.9 = 2.7.
	// Bucket 10 (2.5) is still < 2.7. Still Bucket 100.

	h.Add(10.0, t1) // Weight(10) = 3.5. Total = 4.0.
	// Target p90 = 4.0 * 0.9 = 3.6.
	// Bucket 10 (3.5) is still < 3.6. Still Bucket 100.

	h.Add(10.0, t1) // Weight(10) = 4.5. Total = 5.0.
	// Target p90 = 5.0 * 0.9 = 4.5.
	// Bucket 10 (4.5) >= 4.5. SUCCESS! P90 should drop.
	p90 = h.Percentile(0.9, t1)
	if p90 != 11.0 {
		t.Errorf("P90 should have dropped: got %f, want 11.0", p90)
	}
}

func TestDecayingHistogram_Decay(t *testing.T) {
	start := time.Unix(1000, 0)
	halfLife := 10 * time.Second
	h, _ := NewDecayingHistogram(start, halfLife, "1.0")

	// Add 10.0 at T0
	h.Add(5.0, start) // Bucket 5, Weight 1.0

	// 1. Decay by HalfLife
	t1 := start.Add(halfLife)
	h.Percentile(0.5, t1) // Percentile calls decay(t1)

	if math.Abs(h.TotalWeight-0.5) > 1e-6 {
		t.Errorf("After 1 HL, weight = %f, want 0.5", h.TotalWeight)
	}

	// 2. Decay by another HalfLife (Total 2 HL) -> 0.25
	t2 := t1.Add(halfLife)
	h.Percentile(0.5, t2)

	if math.Abs(h.TotalWeight-0.25) > 1e-6 {
		t.Errorf("After 2 HL, weight = %f, want 0.25", h.TotalWeight)
	}

	// 3. Decay significantly (cleanup threshold is 1e-6)
	// 2^-20 is approx 1e-6.
	tFar := t2.Add(20 * halfLife)
	h.Percentile(0.5, tFar)

	if len(h.Buckets) != 0 {
		t.Errorf("Buckets should be empty after deep decay, got size %d", len(h.Buckets))
	}
	if h.TotalWeight != 0 {
		t.Errorf("TotalWeight should be 0 after reset, got %f", h.TotalWeight)
	}
}

func TestDecayingHistogram_Percentile(t *testing.T) {
	start := time.Unix(1000, 0)
	halfLife := 1 * time.Hour // Long half life to ignore decay for first part
	h, _ := NewDecayingHistogram(start, halfLife, "1.0")

	// Empty
	if got := h.Percentile(0.9, start); got != 0 {
		t.Errorf("Empty percentile = %f, want 0", got)
	}

	// Single Value: 5.5 -> Bucket 5 [5.0, 6.0). Upper bound 6.0.
	h.Add(5.5, start)
	if got := h.Percentile(1.0, start); got != 6.0 {
		t.Errorf("P100 single value = %f, want 6.0", got)
	}
	if got := h.Percentile(0.0, start); got != 6.0 {
		t.Errorf("P0 single value = %f, want 6.0", got)
	}

	// Distribution:
	// Bucket 5: 1.0 (from above)
	// Add 1.0 (Bucket 1): 1 count
	h.Add(1.5, start)
	// Add 9.0 (Bucket 9): 1 count
	h.Add(9.5, start)
	// Weights: B1:1, B5:1, B9:1. Total: 3.

	// P0 (first bucket upper bound) -> Bucket 1 -> 2.0
	if got := h.Percentile(0.0, start); got != 2.0 {
		t.Errorf("P0 = %f, want 2.0", got)
	}
	// P33 (target 1.0) -> Covers B1. -> 2.0
	if got := h.Percentile(0.33, start); got != 2.0 {
		t.Errorf("P33 = %f, want 2.0", got)
	}
	// P50 (target 1.5) -> B1(1)+B5(1)=2 > 1.5. In B5 -> 6.0
	if got := h.Percentile(0.5, start); got != 6.0 {
		t.Errorf("P50 = %f, want 6.0", got)
	}
	// P100 (target 3.0) -> B9 -> 10.0
	if got := h.Percentile(1.0, start); got != 10.0 {
		t.Errorf("P100 = %f, want 10.0", got)
	}

	// Verify Decay Impact
	// Advance time significantly so existing weights become negligible
	// Then add a new value. Percentile should be dominated by new value.
	future := start.Add(100 * halfLife)
	h.Add(2.5, future) // Bucket 2. Old weights ~0.

	// P100 should now be Bucket 2 -> 3.0
	// (Old max was Bucket 9 -> 10.0, but it decayed)
	if got := h.Percentile(1.0, future); got != 3.0 {
		t.Errorf("P100 after decay = %f, want 3.0", got)
	}
}

func TestDecayingHistogram_Percentile_ReadDecay(t *testing.T) {
	// Ensure calling Percentile with a future timestamp decays the weights properly
	// before calculating.
	start := time.Unix(1000, 0)
	halfLife := 10 * time.Second
	h, _ := NewDecayingHistogram(start, halfLife, "1.0")

	h.Add(10.5, start) // Bucket 10. Weight 1.0.

	// Advance 1 HalfLife via Percentile call.
	// TotalWeight should be 0.5 effectively.
	// But return value logic:
	// target = 0.5 * 1.0 = 0.5.
	// Bucket 10 has 0.5. >= target.
	// Returns Bucket 10 upper bound -> 11.0.
	// Value doesn't change, but internal weight state should.

	t1 := start.Add(halfLife)
	val := h.Percentile(1.0, t1)
	if val != 11.0 {
		t.Errorf("P100 = %f, want 11.0", val)
	}

	if math.Abs(h.TotalWeight-0.5) > 1e-6 {
		t.Errorf("TotalWeight after Percentile read = %f, want 0.5", h.TotalWeight)
	}
}

func TestDecayingHistogram_EdgeCases(t *testing.T) {
	start := time.Unix(1000, 0)
	halfLife := 10 * time.Second

	t.Run("Zero Bucket Size", func(t *testing.T) {
		_, err := NewDecayingHistogram(start, halfLife, "0.0")
		// Division by zero would happen on Add.
		// We expect New to allow it (it just parses float), but Add behavior is interesting.
		// Actually, math.Floor(x / 0) -> +Inf. int(+Inf) is undefined/max int.
		// Let's see if we should protect against this in New.
		// The current implementation allows it.
		// Let's create one and see what happens (safely).
		// Actually, let's skip crashing the test runner if it panics, but Go shouldn't panic on div by zero float.
		if err != nil {
			t.Log("New rejected 0.0 bucket size")
		}
	})

	t.Run("Negative Values", func(t *testing.T) {
		h, _ := NewDecayingHistogram(start, halfLife, "1.0")
		h.Add(-5.5, start)
		// -5.5 / 1.0 = -5.5. Floor(-5.5) = -6.
		// Bucket -6. Upper bound should be (-6+1)*1.0 = -5.0.

		p := h.Percentile(1.0, start)
		if p != -5.0 {
			t.Errorf("P100 of -5.5 = %f, want -5.0", p)
		}
	})

	t.Run("Tiny HalfLife", func(t *testing.T) {
		h, _ := NewDecayingHistogram(start, 1*time.Nanosecond, "1.0")
		h.Add(5.0, start)

		// Immediate decay
		t1 := start.Add(1 * time.Second)
		h.Percentile(1.0, t1)

		if h.TotalWeight != 0 {
			t.Errorf("Expected 0 weight after massive decay, got %v", h.TotalWeight)
		}
	})
}
