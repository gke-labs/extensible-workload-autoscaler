package store

import (
	"testing"
	"time"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/cmd/server/internal/store/model"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// t0 is the reference time of the tests.
var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// noTimestamp leaves a sample without a timestamp of its own, so that it
// defaults to the request (ingestion) timestamp.
var noTimestamp time.Time

// at returns the time d after t0.
func at(d time.Duration) time.Time {
	return t0.Add(d)
}

// readyPods returns a workload of ready pods with the given names.
func readyPods(names ...string) map[string]*pb.PodState {
	workload := make(map[string]*pb.PodState, len(names))
	for _, name := range names {
		workload[name] = pb.PodState_builder{Name: proto.String(name), IsReady: proto.Bool(true)}.Build()
	}
	return workload
}

// newSample returns a scalar sample measured at ts.
func newSample(name string, ts time.Time, val float64) *pb.MetricSample {
	b := pb.MetricSample_builder{Name: proto.String(name), Value: proto.Float64(val)}
	if !ts.IsZero() {
		b.Timestamp = timestamppb.New(ts)
	}
	return b.Build()
}

// ingestSample ingests m, as reported by pod-a at ingestTime.
func ingestSample(t *testing.T, ms *MetricStore, policy *pb.Policy, m *pb.MetricSample, ingestTime time.Time) {
	t.Helper()
	batches := []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod-a"), Samples: []*pb.MetricSample{m}}.Build()}
	if err := ms.IngestMetrics(policy, readyPods("pod-a"), batches, ingestTime); err != nil {
		t.Fatalf("IngestMetrics() = %v", err)
	}
}

// onlySeries returns the single series recorded for metric name.
func onlySeries(t *testing.T, ms *MetricStore, name string) *model.Series {
	t.Helper()
	metric := ms.Series[metricID{name: name}]
	if metric == nil || len(metric.Series) != 1 {
		t.Fatalf("got %v for %q, want a single series", metric, name)
	}
	for _, ser := range metric.Series {
		return ser
	}
	return nil
}

// apply returns the temporal aggregate of ser, failing if there is none.
func apply(t *testing.T, ser *model.Series) model.Sample {
	t.Helper()
	s, ok := ser.TimeWindow.Apply()
	if !ok {
		t.Fatalf("TimeWindow.Apply() reported no value")
	}
	return s
}

func newRatePolicy(name string) *pb.Policy {
	return pb.Policy_builder{Metrics: []*pb.MetricDefinition{
		pb.MetricDefinition_builder{Name: proto.String(name), Rate: pb.Rate_builder{Window: durationpb.New(time.Minute)}.Build()}.Build(),
	}}.Build()
}

func newGaugePolicy(name string) *pb.Policy {
	return pb.Policy_builder{Metrics: []*pb.MetricDefinition{
		pb.MetricDefinition_builder{Name: proto.String(name), Gauge: pb.Gauge_builder{}.Build()}.Build(),
	}}.Build()
}

func newDecayingDistributionPolicy(name string) *pb.Policy {
	return pb.Policy_builder{Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{
		Name: proto.String(name),
		DecayingDistribution: pb.DecayingDistribution_builder{
			HalfLife:   durationpb.New(time.Hour),
			BucketSize: proto.String("0.1"),
		}.Build(),
	}.Build()}}.Build()
}

func TestSampleTimestamp(t *testing.T) {
	tests := []struct {
		name   string
		ts     time.Time
		want   time.Time
		wantOK bool
	}{
		{name: "unset defaults to ingest time", ts: noTimestamp, want: t0, wantOK: true},
		{name: "past is kept", ts: at(-30 * time.Second), want: at(-30 * time.Second), wantOK: true},
		{name: "equal is kept", ts: t0, want: t0, wantOK: true},
		{name: "slightly ahead is kept", ts: at(maxSampleLead), want: at(maxSampleLead), wantOK: true},
		{name: "far ahead is invalid", ts: at(maxSampleLead + time.Second), wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := sampleTimestamp(newSample("m", tc.ts, 0), t0)
			if ok != tc.wantOK {
				t.Fatalf("sampleTimestamp() ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && !got.Equal(tc.want) {
				t.Errorf("sampleTimestamp() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIngest_CounterDefaultsToIngestTime(t *testing.T) {
	policy := newRatePolicy("reqs")
	ms := NewMetricStore()

	ingestSample(t, ms, policy, newSample("reqs", noTimestamp, 0), t0)
	ingestSample(t, ms, policy, newSample("reqs", noTimestamp, 100), at(10*time.Second))

	ser := onlySeries(t, ms, "reqs")
	if got, want := apply(t, ser).Value, 10.0; got != want {
		t.Errorf("rate = %v, want %v", got, want)
	}
	if got, want := ser.LastRaw.Timestamp, at(10*time.Second); !got.Equal(want) {
		t.Errorf("LastRaw.Timestamp = %v, want %v", got, want)
	}
}

func TestIngest_HistogramDefaultsToIngestTime(t *testing.T) {
	policy := pb.Policy_builder{Metrics: []*pb.MetricDefinition{
		pb.MetricDefinition_builder{Name: proto.String("latency"), Distribution: pb.Distribution_builder{Percentile: proto.String("p90")}.Build()}.Build(),
	}}.Build()
	ms := NewMetricStore()

	hist := func(count uint64) *pb.MetricSample {
		return pb.MetricSample_builder{Name: proto.String("latency"), Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"1": count}}.Build()}.Build()
	}
	ingestSample(t, ms, policy, hist(0), t0)
	ingestSample(t, ms, policy, hist(100), at(10*time.Second))

	ser := onlySeries(t, ms, "latency")
	if diff := cmp.Diff(map[string]float64{"1": 10}, apply(t, ser).Distribution.Buckets); diff != "" {
		t.Errorf("bucket rates mismatch (-want +got):\n%s", diff)
	}
}

func TestIngest_LaggingSource(t *testing.T) {
	policy := newRatePolicy("reqs")
	ms := NewMetricStore()

	// The source lags 90s behind the collection, more than the rate window.
	const lag = 90 * time.Second
	ingestSample(t, ms, policy, newSample("reqs", at(-lag), 0), t0)
	ingestSample(t, ms, policy, newSample("reqs", at(30*time.Second-lag), 300), at(30*time.Second))

	ser := onlySeries(t, ms, "reqs")
	// The window follows the source's clock, so the rate is still computed,
	// over the measurement interval.
	if got, want := apply(t, ser).Value, 10.0; got != want {
		t.Errorf("rate = %v, want %v", got, want)
	}
	if got, want := ser.LastRaw.Timestamp, at(30*time.Second-lag); !got.Equal(want) {
		t.Errorf("LastRaw.Timestamp = %v, want %v", got, want)
	}
	if got, want := ser.LastIngested, at(30*time.Second); !got.Equal(want) {
		t.Errorf("LastIngested = %v, want %v", got, want)
	}
}

func TestIngest_LeadingSource(t *testing.T) {
	policy := newRatePolicy("reqs")
	ms := NewMetricStore()

	// The source's clock runs 15s ahead of the collector's, and the source
	// measures every 8s while the collector scrapes every 10s.
	const skew = 15 * time.Second
	ingestSample(t, ms, policy, newSample("reqs", at(skew), 1), t0)
	ingestSample(t, ms, policy, newSample("reqs", at(skew+8*time.Second), 2), at(10*time.Second))

	// The rate is computed over the source's measurement interval, not over
	// the collection interval.
	ser := onlySeries(t, ms, "reqs")
	if got, want := apply(t, ser).Value, 1.0/8; got != want {
		t.Errorf("rate = %v, want %v", got, want)
	}
}

func TestIngest_FarFutureSampleIsDropped(t *testing.T) {
	policy := newGaugePolicy("queue")
	ms := NewMetricStore()

	// An invalid timestamp must not cause the following samples to be dropped
	// as out of order.
	ingestSample(t, ms, policy, newSample("queue", at(time.Hour), 1), t0)
	ingestSample(t, ms, policy, newSample("queue", at(5*time.Second), 2), at(10*time.Second))

	ser := onlySeries(t, ms, "queue")
	if got, want := ser.LastRaw.Timestamp, at(5*time.Second); !got.Equal(want) {
		t.Errorf("LastRaw.Timestamp = %v, want %v", got, want)
	}
	if got, want := apply(t, ser).Value, 2.0; got != want {
		t.Errorf("value = %v, want %v", got, want)
	}
}

func TestIngest_OutOfOrderRequests(t *testing.T) {
	policy := newGaugePolicy("queue")
	ms := NewMetricStore()

	ingestSample(t, ms, policy, newSample("queue", at(10*time.Second), 1), at(10*time.Second))
	// An older request delivered late. Its sample is newer (the source's
	// clock runs ahead) and is recorded, but the request must not move the
	// liveness of the series back.
	ingestSample(t, ms, policy, newSample("queue", at(20*time.Second), 2), at(5*time.Second))

	ser := onlySeries(t, ms, "queue")
	if got, want := ser.LastIngested, at(10*time.Second); !got.Equal(want) {
		t.Errorf("LastIngested = %v, want %v", got, want)
	}
	if got, want := apply(t, ser).Value, 2.0; got != want {
		t.Errorf("value = %v, want %v", got, want)
	}
}

func TestGCSeries_KeepsLaggingSources(t *testing.T) {
	policy := newGaugePolicy("queue")
	ms := NewMetricStore()

	// The source lags more than the retention period behind the collection.
	ingested := at(2 * seriesRetention)
	ingestSample(t, ms, policy, newSample("queue", t0, 1), ingested)

	ms.gcSeries(ingested.Add(seriesRetention))
	onlySeries(t, ms, "queue") // Still reported: kept.

	ms.gcSeries(ingested.Add(seriesRetention + time.Second))
	if _, ok := ms.Series[metricID{name: "queue"}]; ok {
		t.Errorf("series not garbage collected after %v without being reported", seriesRetention)
	}
}

func TestIngest_SameTimestampSupersedes(t *testing.T) {
	policy := newRatePolicy("reqs")
	ms := NewMetricStore()

	ingestSample(t, ms, policy, newSample("reqs", t0, 0), t0)
	ingestSample(t, ms, policy, newSample("reqs", at(10*time.Second), 100), at(10*time.Second))
	// A re-scrape within the same second supersedes the previous sample.
	ingestSample(t, ms, policy, newSample("reqs", at(10*time.Second), 200), at(10*time.Second))

	ser := onlySeries(t, ms, "reqs")
	if got, want := apply(t, ser).Value, 20.0; got != want {
		t.Errorf("rate = %v, want %v", got, want)
	}
	if got, want := ser.LastRaw.Value, 200.0; got != want {
		t.Errorf("LastRaw.Value = %v, want %v", got, want)
	}
	// The superseding sample was measured over the same interval.
	if got, want := ser.LastRawWindow, 10*time.Second; got != want {
		t.Errorf("LastRawWindow = %v, want %v", got, want)
	}
}

func TestIngest_DecayingDistributionDuplicatesAndOutOfOrder(t *testing.T) {
	policy := newDecayingDistributionPolicy("cpu")
	ms := NewMetricStore()
	totalWeight := func(ser *model.Series) float64 {
		return apply(t, ser).DecayingHistogram.TotalWeight
	}

	ingestSample(t, ms, policy, newSample("cpu", at(-10*time.Second), 1), t0)
	ser := onlySeries(t, ms, "cpu")
	if got, want := totalWeight(ser), 1.0; got != want {
		t.Errorf("TotalWeight = %v, want %v", got, want)
	}

	// Duplicate: the source re-reports its last sample. No new observation,
	// but the series is still being reported.
	ingestSample(t, ms, policy, newSample("cpu", at(-10*time.Second), 1), at(10*time.Second))
	if got, want := totalWeight(ser), 1.0; got != want {
		t.Errorf("after duplicate: TotalWeight = %v, want %v", got, want)
	}
	if got, want := ser.LastIngested, at(10*time.Second); !got.Equal(want) {
		t.Errorf("after duplicate: LastIngested = %v, want %v", got, want)
	}

	// Same timestamp, different value: a histogram cannot supersede an
	// observation, so the first one wins and the sample is not recorded.
	ingestSample(t, ms, policy, newSample("cpu", at(-10*time.Second), 3), at(20*time.Second))
	if got, want := totalWeight(ser), 1.0; got != want {
		t.Errorf("after same timestamp: TotalWeight = %v, want %v", got, want)
	}
	if got, want := ser.LastRaw.Value, 1.0; got != want {
		t.Errorf("after same timestamp: LastRaw.Value = %v, want %v", got, want)
	}
	if got, want := ser.LastIngested, at(20*time.Second); !got.Equal(want) {
		t.Errorf("after same timestamp: LastIngested = %v, want %v", got, want)
	}

	// Out of order: dropped entirely.
	ingestSample(t, ms, policy, newSample("cpu", at(-15*time.Second), 5), at(30*time.Second))
	if got, want := totalWeight(ser), 1.0; got != want {
		t.Errorf("after out of order: TotalWeight = %v, want %v", got, want)
	}
	if got, want := ser.LastIngested, at(20*time.Second); !got.Equal(want) {
		t.Errorf("after out of order: LastIngested = %v, want %v", got, want)
	}

	// New sample: added at its measurement time.
	ingestSample(t, ms, policy, newSample("cpu", t0, 2), at(40*time.Second))
	if got, want := totalWeight(ser), 2.0; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 0.01)) {
		t.Errorf("after new sample: TotalWeight = %v, want ~%v", got, want)
	}
	if got, want := apply(t, ser).DecayingHistogram.ReferenceTime, t0; !got.Equal(want) {
		t.Errorf("after new sample: ReferenceTime = %v, want %v", got, want)
	}
}

func TestIngest_SharedDecayingDistributionRepeats(t *testing.T) {
	// Global decaying distributions share a single workload-wide histogram
	// between all the series.
	policy := newDecayingDistributionPolicy("cpu")
	ms := NewMetricStore()
	ingestPods := func(ingestTime time.Time) {
		t.Helper()
		batches := []*pb.MetricBatch{
			pb.MetricBatch_builder{PodName: proto.String("pod-a"), Samples: []*pb.MetricSample{newSample("cpu", t0, 1)}}.Build(),
			pb.MetricBatch_builder{PodName: proto.String("pod-b"), Samples: []*pb.MetricSample{newSample("cpu", t0, 2)}}.Build(),
		}
		if err := ms.IngestMetrics(policy, readyPods("pod-a", "pod-b"), batches, ingestTime); err != nil {
			t.Fatalf("IngestMetrics() = %v", err)
		}
	}
	totalWeight := func() float64 {
		t.Helper()
		s, ok := ms.Series[metricID{name: "cpu"}].Aggregation.NewTimeWindow(policy.GetMetrics()[0]).Apply()
		if !ok {
			t.Fatalf("TimeWindow.Apply() reported no value")
		}
		return s.DecayingHistogram.TotalWeight
	}

	// Samples from different pods measured at the same time all count...
	ingestPods(t0)
	if got, want := totalWeight(), 2.0; got != want {
		t.Errorf("TotalWeight = %v, want %v", got, want)
	}
	// ...but their re-reports do not.
	ingestPods(at(10 * time.Second))
	if got, want := totalWeight(), 2.0; got != want {
		t.Errorf("after re-reports: TotalWeight = %v, want %v", got, want)
	}
}
