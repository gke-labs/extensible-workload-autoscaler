package store

import (
	"testing"
	"time"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store/model"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// unix returns the time sec seconds after the Unix epoch.
func unix(sec int64) time.Time {
	return time.Unix(sec, 0)
}

// newSample returns a scalar sample. A zero ts leaves the sample timestamp
// unset, so that it defaults to the request (ingestion) timestamp.
func newSample(name string, ts int64, val float64) *pb.MetricSample {
	b := pb.MetricSample_builder{Name: proto.String(name), Value: proto.Float64(val)}
	if ts != 0 {
		b.Timestamp = timestamppb.New(unix(ts))
	}
	return b.Build()
}

func ingestSample(t *testing.T, ms *MetricStore, policy *pb.Policy, m *pb.MetricSample, ingestTime int64) {
	t.Helper()
	batches := []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod-a"), Samples: []*pb.MetricSample{m}}.Build()}
	if err := ms.IngestMetrics(policy, batches, unix(ingestTime)); err != nil {
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

func TestSampleTimestamp(t *testing.T) {
	tests := []struct {
		name string
		ts   int64 // 0 means unset
		want int64
	}{
		{name: "unset defaults to ingest time", ts: 0, want: 1000},
		{name: "past is kept", ts: 970, want: 970},
		{name: "equal is kept", ts: 1000, want: 1000},
		{name: "future is clamped to ingest time", ts: 2000, want: 1000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := sampleTimestamp(newSample("m", tc.ts, 0), unix(1000)); !got.Equal(unix(tc.want)) {
				t.Errorf("sampleTimestamp() = %v, want %v", got, unix(tc.want))
			}
		})
	}
}

func TestIngest_CounterDefaultsToIngestTime(t *testing.T) {
	policy := pb.Policy_builder{Metrics: []*pb.MetricDefinition{
		pb.MetricDefinition_builder{Name: proto.String("reqs"), Rate: pb.Rate_builder{Window: durationpb.New(time.Minute)}.Build()}.Build(),
	}}.Build()
	ms := NewMetricStore()

	ingestSample(t, ms, policy, newSample("reqs", 0, 0), 1000)
	ingestSample(t, ms, policy, newSample("reqs", 0, 100), 1010)

	ser := onlySeries(t, ms, "reqs")
	if got, want := apply(t, ser).Value, 10.0; got != want {
		t.Errorf("rate = %v, want %v", got, want)
	}
	if got, want := ser.LastRaw.Timestamp, unix(1010); !got.Equal(want) {
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
	ingestSample(t, ms, policy, hist(0), 1000)
	ingestSample(t, ms, policy, hist(100), 1010)

	ser := onlySeries(t, ms, "latency")
	if diff := cmp.Diff(map[string]float64{"1": 10}, apply(t, ser).Distribution.Buckets); diff != "" {
		t.Errorf("bucket rates mismatch (-want +got):\n%s", diff)
	}
}

func TestIngest_LaggingSource(t *testing.T) {
	policy := pb.Policy_builder{Metrics: []*pb.MetricDefinition{
		pb.MetricDefinition_builder{Name: proto.String("reqs"), Rate: pb.Rate_builder{Window: durationpb.New(time.Minute)}.Build()}.Build(),
	}}.Build()
	ms := NewMetricStore()

	// The source lags 90s behind the collection, more than the rate window.
	ingestSample(t, ms, policy, newSample("reqs", 910, 0), 1000)
	ingestSample(t, ms, policy, newSample("reqs", 940, 300), 1030)

	ser := onlySeries(t, ms, "reqs")
	// The window follows the source's clock, so the rate is still computed,
	// over the measurement interval.
	if got, want := apply(t, ser).Value, 10.0; got != want {
		t.Errorf("rate = %v, want %v", got, want)
	}
	if got, want := ser.LastRaw.Timestamp, unix(940); !got.Equal(want) {
		t.Errorf("LastRaw.Timestamp = %v, want %v", got, want)
	}
	if got, want := ser.LastIngested, unix(1030); !got.Equal(want) {
		t.Errorf("LastIngested = %v, want %v", got, want)
	}
}

func TestGCSeries_KeepsLaggingSources(t *testing.T) {
	policy := pb.Policy_builder{Metrics: []*pb.MetricDefinition{
		pb.MetricDefinition_builder{Name: proto.String("queue"), Gauge: pb.Gauge_builder{}.Build()}.Build(),
	}}.Build()
	ms := NewMetricStore()

	// The source lags more than the retention period behind the collection.
	ingestSample(t, ms, policy, newSample("queue", 1000, 1), 1000+int64(2*seriesRetention/time.Second))
	ingested := onlySeries(t, ms, "queue").LastIngested

	ms.gcSeries(ingested.Add(seriesRetention))
	onlySeries(t, ms, "queue") // Still reported: kept.

	ms.gcSeries(ingested.Add(seriesRetention + time.Second))
	if _, ok := ms.Series[metricID{name: "queue"}]; ok {
		t.Errorf("series not garbage collected after %v without being reported", seriesRetention)
	}
}

func TestIngest_DecayingDistributionDuplicatesAndOutOfOrder(t *testing.T) {
	policy := pb.Policy_builder{Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{
		Name: proto.String("cpu"),
		DecayingDistribution: pb.DecayingDistribution_builder{
			HalfLife:   durationpb.New(time.Hour),
			BucketSize: proto.String("0.1"),
		}.Build(),
	}.Build()}}.Build()
	ms := NewMetricStore()
	totalWeight := func(ser *model.Series) float64 {
		return apply(t, ser).DecayingHistogram.TotalWeight
	}

	ingestSample(t, ms, policy, newSample("cpu", 990, 1), 1000)
	ser := onlySeries(t, ms, "cpu")
	if got, want := totalWeight(ser), 1.0; got != want {
		t.Errorf("TotalWeight = %v, want %v", got, want)
	}

	// Duplicate: the source re-reports its last sample. No new observation,
	// but the series is still being reported.
	ingestSample(t, ms, policy, newSample("cpu", 990, 1), 1010)
	if got, want := totalWeight(ser), 1.0; got != want {
		t.Errorf("after duplicate: TotalWeight = %v, want %v", got, want)
	}
	if got, want := ser.LastIngested, unix(1010); !got.Equal(want) {
		t.Errorf("after duplicate: LastIngested = %v, want %v", got, want)
	}

	// Out of order: dropped entirely.
	ingestSample(t, ms, policy, newSample("cpu", 985, 5), 1020)
	if got, want := totalWeight(ser), 1.0; got != want {
		t.Errorf("after out of order: TotalWeight = %v, want %v", got, want)
	}
	if got, want := ser.LastIngested, unix(1010); !got.Equal(want) {
		t.Errorf("after out of order: LastIngested = %v, want %v", got, want)
	}

	// New sample: added at its measurement time.
	ingestSample(t, ms, policy, newSample("cpu", 1000, 2), 1030)
	if got, want := totalWeight(ser), 2.0; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 0.01)) {
		t.Errorf("after new sample: TotalWeight = %v, want ~%v", got, want)
	}
	if got, want := apply(t, ser).DecayingHistogram.ReferenceTime, unix(1000); !got.Equal(want) {
		t.Errorf("after new sample: ReferenceTime = %v, want %v", got, want)
	}
}

func TestIngest_SharedDecayingDistributionRepeats(t *testing.T) {
	// Global decaying distributions share a single workload-wide histogram
	// between all the series.
	policy := pb.Policy_builder{Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{
		Name: proto.String("cpu"),
		DecayingDistribution: pb.DecayingDistribution_builder{
			HalfLife:   durationpb.New(time.Hour),
			BucketSize: proto.String("0.1"),
		}.Build(),
	}.Build()}}.Build()
	ms := NewMetricStore()
	ingestPods := func(ingestTime int64) {
		t.Helper()
		batches := []*pb.MetricBatch{
			pb.MetricBatch_builder{PodName: proto.String("pod-a"), Samples: []*pb.MetricSample{newSample("cpu", 1000, 1)}}.Build(),
			pb.MetricBatch_builder{PodName: proto.String("pod-b"), Samples: []*pb.MetricSample{newSample("cpu", 1000, 2)}}.Build(),
		}
		if err := ms.IngestMetrics(policy, batches, unix(ingestTime)); err != nil {
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
	ingestPods(1000)
	if got, want := totalWeight(), 2.0; got != want {
		t.Errorf("TotalWeight = %v, want %v", got, want)
	}
	// ...but their re-reports do not.
	ingestPods(1010)
	if got, want := totalWeight(), 2.0; got != want {
		t.Errorf("after re-reports: TotalWeight = %v, want %v", got, want)
	}
}

func TestIngest_FutureSampleIsClamped(t *testing.T) {
	policy := pb.Policy_builder{Metrics: []*pb.MetricDefinition{
		pb.MetricDefinition_builder{Name: proto.String("queue"), Gauge: pb.Gauge_builder{}.Build()}.Build(),
	}}.Build()
	ms := NewMetricStore()

	// A source clock running ahead must not cause the following samples to be
	// dropped as out of order.
	ingestSample(t, ms, policy, newSample("queue", 2000, 1), 1000)
	ingestSample(t, ms, policy, newSample("queue", 1005, 2), 1010)

	ser := onlySeries(t, ms, "queue")
	if got, want := ser.LastRaw.Timestamp, unix(1005); !got.Equal(want) {
		t.Errorf("LastRaw.Timestamp = %v, want %v", got, want)
	}
	if got, want := apply(t, ser).Value, 2.0; got != want {
		t.Errorf("value = %v, want %v", got, want)
	}
}
