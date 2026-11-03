package aggregation

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/cmd/server/internal/store/model"
)

// t0 is the reference time of the tests.
var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// at returns the time d after t0.
func at(d time.Duration) time.Time {
	return t0.Add(d)
}

// add feeds a scalar sample measured at ts to w.
func add(w model.TimeWindow, ts time.Time, value float64) {
	w.AddSample(model.Sample{Timestamp: ts, Value: value})
}

// applyValue returns the scalar aggregate of w, failing if there is none.
func applyValue(t *testing.T, w model.TimeWindow) float64 {
	t.Helper()
	s, ok := w.Apply()
	if !ok {
		t.Fatalf("Apply() reported no value")
	}
	return s.Value
}

func TestRateAggregation_InstantaneousRate(t *testing.T) {
	// A zero window computes the rate between the last two samples.
	r := NewRateAggregation(0, model.SampleTypeGauge)
	if _, ok := r.Apply(); ok {
		t.Errorf("Apply() reported a value for an empty window")
	}

	add(r, t0, 0)
	if _, ok := r.Apply(); ok {
		t.Errorf("Apply() reported a value for a single sample")
	}

	add(r, at(10*time.Second), 100)
	if got, want := applyValue(t, r), 10.0; got != want {
		t.Errorf("rate = %v, want %v", got, want)
	}

	// (500 - 100) / 20s, rather than (500 - 0) / 30s.
	add(r, at(30*time.Second), 500)
	if got, want := applyValue(t, r), 20.0; got != want {
		t.Errorf("rate = %v, want %v", got, want)
	}
	if got, want := r.Len(), 2; got != want {
		t.Errorf("Len() = %v, want %v", got, want)
	}
}

func TestRateAggregation_InstantaneousRateCounterReset(t *testing.T) {
	r := NewRateAggregation(0, model.SampleTypeCounter)
	add(r, t0, 100)
	add(r, at(10*time.Second), 150)
	// The counter resets: it grew by 20 from zero over the last 10s.
	add(r, at(20*time.Second), 20)
	if got, want := applyValue(t, r), 2.0; got != want {
		t.Errorf("rate = %v, want %v", got, want)
	}
}

func TestRateAggregation_WindowSpansItsSamples(t *testing.T) {
	// A positive window computes the rate between its oldest and its newest
	// samples.
	r := NewRateAggregation(time.Minute, model.SampleTypeGauge)
	add(r, t0, 0)
	add(r, at(10*time.Second), 100)
	add(r, at(30*time.Second), 500)
	if got, want := applyValue(t, r), 500.0/30; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 1e-9)) {
		t.Errorf("rate = %v, want %v", got, want)
	}

	// The first sample falls out of the window.
	add(r, at(70*time.Second), 900)
	if got, want := applyValue(t, r), 800.0/60; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 1e-9)) {
		t.Errorf("rate = %v, want %v", got, want)
	}
	if got, want := r.Len(), 3; got != want {
		t.Errorf("Len() = %v, want %v", got, want)
	}
}

func TestDecayingDistributionAggregation_InstantaneousRate(t *testing.T) {
	// A zero rate window feeds the histogram with the instantaneous rates.
	def := pb.DecayingDistribution_builder{
		HalfLife:   durationpb.New(time.Hour),
		BucketSize: proto.String("1"),
		Rate:       durationpb.New(0),
	}.Build()
	d := NewDecayingDistributionAggregation(def, model.SampleTypeCounter)
	if d.rate == nil {
		t.Fatalf("no rate window for a zero rate")
	}

	add(d, t0, 0)
	if _, ok := d.Apply(); ok {
		t.Errorf("Apply() reported a value before any rate could be computed")
	}
	// +100 in 10s: a rate of 10/s is observed.
	add(d, at(10*time.Second), 100)
	// +500 in 20s: a rate of 25/s is observed, rather than 600 / 30s = 20/s.
	add(d, at(30*time.Second), 600)

	s, ok := d.Apply()
	if !ok {
		t.Fatalf("Apply() reported no value")
	}
	hist := s.DecayingHistogram
	if got, want := hist.TotalWeight, 2.0; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 0.01)) {
		t.Errorf("TotalWeight = %v, want ~%v", got, want)
	}
	// The p100 falls in the bucket of the highest observation, [25, 26).
	if got, want := hist.Percentile(1, hist.ReferenceTime), 26.0; got != want {
		t.Errorf("p100 = %v, want %v", got, want)
	}
}

func TestNewDecayingDistributionAggregation_NoRate(t *testing.T) {
	def := pb.DecayingDistribution_builder{
		HalfLife:   durationpb.New(time.Hour),
		BucketSize: proto.String("1"),
	}.Build()
	if d := NewDecayingDistributionAggregation(def, model.SampleTypeGauge); d.rate != nil {
		t.Errorf("got a rate window without a rate configured")
	}
}
