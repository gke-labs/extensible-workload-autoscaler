package store

import (
	"fmt"
	"iter"
	"maps"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/podutil"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store/aggregation"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store/model"
)

// MetricSeries holds the state of a single metric: every series reported for
// it, keyed by series ID.
type MetricSeries struct {
	Series      map[model.SeriesID]*model.Series
	Aggregation aggregation.SeriesAggregation `json:"-"`
	// Definition is the metric definition the state was built from. The state
	// is dropped when the definition changes.
	Definition *pb.MetricDefinition `json:"-"`
	// ResourceName is the name of the resource the metric describes (e.g.
	// "cpu"), as reported by its samples. Empty for non-resource metrics.
	ResourceName string
}

// newMetricSeries creates an initialized MetricSeries.
func newMetricSeries(def *pb.MetricDefinition, podName, containerName string, sample *pb.MetricSample) *MetricSeries {
	return &MetricSeries{
		Series:       make(map[model.SeriesID]*model.Series),
		Aggregation:  aggregation.CreateSeriesAggregation(def, sample.GetResourceName(), podName, containerName),
		Definition:   def,
		ResourceName: sample.GetResourceName(),
	}
}

func (ms *MetricSeries) isEmpty() bool {
	return ms == nil || len(ms.Series) == 0
}

func (ms *MetricSeries) getSeries(sid model.SeriesID) (*model.Series, bool) {
	ser, ok := ms.Series[sid]
	return ser, ok
}

func (ms *MetricSeries) setSeries(sid model.SeriesID, ser *model.Series) {
	ms.Series[sid] = ser
}

// MetricStore holds the time-series and histogram state of a policy's metrics.
type MetricStore struct {
	// Series holds the state of every tracked metric, keyed by metric ID.
	Series map[metricID]*MetricSeries
}

// NewMetricStore creates an initialized MetricStore.
func NewMetricStore() *MetricStore {
	return &MetricStore{
		Series: make(map[metricID]*MetricSeries),
	}
}

// IngestMetrics ingests a slice of metric batches for the given policy.
// IngestMetrics ingests the given metric batches of a policy. workload is
// the current state of the policy's pods, keyed by name.
func (ms *MetricStore) IngestMetrics(policy *pb.Policy, workload map[string]*pb.PodState, batches []*pb.MetricBatch, ingestTime time.Time) error {
	for _, batch := range batches {
		for _, m := range batch.GetSamples() {
			if err := ms.ingestSample(policy, workload, batch.GetPodName(), batch.GetContainerName(), m, ingestTime); err != nil {
				return err
			}
		}
	}
	return nil
}

// findMetricDefinition returns the definition of the metric identified by the
// <name, owner> pair. An empty owner looks the metric up among the policy-wide
// metrics, otherwise among the metrics that recommender owns.
func findMetricDefinition(policy *pb.Policy, owner, name string) *pb.MetricDefinition {
	if policy == nil {
		return nil
	}
	defs := policy.GetMetrics()
	if owner != "" {
		defs = policy.GetRecommenderMetrics()[owner].GetDefinitions()
	}
	for _, d := range defs {
		if d.GetName() == name {
			return d
		}
	}
	return nil
}

func (ms *MetricStore) ingestSample(policy *pb.Policy, workload map[string]*pb.PodState, podName, containerName string, sample *pb.MetricSample, ingestTime time.Time) error {
	owner := sample.GetRecommenderName()
	def := findMetricDefinition(policy, owner, sample.GetName())
	if def == nil {
		if owner != "" {
			return fmt.Errorf("metric %s not defined in policy for recommender %s", sample.GetName(), owner)
		}
		return fmt.Errorf("metric %s not defined in policy", sample.GetName())
	}

	// Filter early
	if !model.MatchFilter(sample.GetLabels(), def.GetFilter()) {
		return nil
	}

	// The samples of the pods that aren't eligible for the metric (e.g.
	// still starting up, within the metric's startup grace period, or
	// unknown to the workload) are dropped: they must not enter the time
	// windows, and the samples of the global decaying distributions can't be
	// told apart once in the workload-wide histogram. Eligibility can also
	// change after a sample was ingested (e.g. the pod starts terminating),
	// which the aggregation handles by filtering the series.
	if podName != "" && !podutil.IsPodEligible(workload, podName, def.GetEligibility(), ingestTime) {
		return nil
	}

	id := newMetricID(def)
	metricSeries, ok := ms.Series[id]
	if !ok {
		metricSeries = newMetricSeries(def, podName, containerName, sample)
		ms.Series[id] = metricSeries
	}
	sid := model.NewSeriesID(podName, containerName, sample.GetLabels())
	series, ok := metricSeries.getSeries(sid)
	if !ok {
		series = model.NewSeries(podName, containerName, sample.GetResourceName(), sample.GetLabels(), metricSeries.Aggregation.NewTimeWindow(def))
		metricSeries.setSeries(sid, series)
	}
	ms.seriesIngestSample(series, sample, ingestTime)
	return nil
}

// seriesIngestSample records sample m, reported at ingestTime, into ser.
//
// Two timestamps are involved:
//   - the sample timestamp (see sampleTimestamp), the time at which the sample
//     was measured, is used for everything that relates to the data itself:
//     rate computation, time windows and decaying histograms.
//   - ingestTime, the time at which the sample was collected, tracks whether
//     the series is still being reported (see gcSeries).
func (ms *MetricStore) seriesIngestSample(ser *model.Series, m *pb.MetricSample, ingestTime time.Time) {
	ts, ok := sampleTimestamp(m, ingestTime)
	if !ok || ts.Before(ser.LastRaw.Timestamp) {
		return // Drop invalid and out of order samples.
	}
	sample := model.Sample{Timestamp: ts, Value: m.GetValue(), Distribution: model.NewDistributionFromHistogram(m.GetHistogram())}
	ser.AddSample(sample, ingestTime)
}

// maxSampleLead is how far ahead of its collection a sample may be timestamped.
//
// The clock of a source may legitimately run slightly ahead of the collector's
// (clock skew), and this is harmless: the samples of a series are processed
// on the clock of their source. A sample timestamped further ahead than this
// is invalid, and it is dropped rather than recorded, as it would cause the
// following samples of its series to be dropped as out of order until the
// source's clock catches up.
const maxSampleLead = 5 * time.Minute

// sampleTimestamp returns the time at which the sample was measured, reporting
// false when the sample is invalid. Samples without their own timestamp default
// to ingestTime, the time at which they were collected.
func sampleTimestamp(m *pb.MetricSample, ingestTime time.Time) (time.Time, bool) {
	if !m.HasTimestamp() {
		return ingestTime, true
	}
	ts := m.GetTimestamp().AsTime()
	if ts.After(ingestTime.Add(maxSampleLead)) {
		return time.Time{}, false
	}
	return ts, true
}

// CleanupOrphaned removes the state of the metrics that are no longer defined
// in policy, or whose definition changed.
func (ms *MetricStore) CleanupOrphaned(policy *pb.Policy) {
	defs := definedMetrics(policy)
	for id, metric := range ms.Series {
		if def, ok := defs[id]; !ok || !proto.Equal(def, metric.Definition) {
			delete(ms.Series, id)
		}
	}
}

// seriesRetention is how long a series is kept without being reported.
const seriesRetention = 10 * time.Minute

// gcSeries drops the series that have not been reported for seriesRetention
// (e.g. the series of pods that disappeared), and the metrics left without any
// series. Series are kept based on when they were last reported rather than on
// their samples' timestamps, so that sources lagging behind are not dropped.
func (ms *MetricStore) gcSeries(now time.Time) {
	cutoff := now.Add(-seriesRetention)
	for id, metric := range ms.Series {
		for sid, ser := range metric.Series {
			if ser.LastIngested.Before(cutoff) {
				delete(metric.Series, sid)
			}
		}
		if metric.isEmpty() {
			delete(ms.Series, id)
		}
	}
}

// definedMetrics returns all the metrics a policy defines, including the ones
// owned by its recommenders, keyed by metric ID.
func definedMetrics(policy *pb.Policy) map[metricID]*pb.MetricDefinition {
	defs := make(map[metricID]*pb.MetricDefinition)
	if policy == nil {
		return defs
	}
	for _, m := range policy.GetMetrics() {
		defs[newMetricID(m)] = m
	}
	for _, owned := range policy.GetRecommenderMetrics() {
		for _, m := range owned.GetDefinitions() {
			defs[newMetricID(m)] = m
		}
	}
	return defs
}

// calculate aggregates the given metric definitions into a single ControlMetrics
// snapshot as of now. All the definitions are expected to share the same
// owner. The caller must hold mu.
func (ms *MetricStore) calculate(defs []*pb.MetricDefinition, workload map[string]*pb.PodState, now time.Time) *pb.ControlMetrics {
	ms.gcSeries(now)

	cm := pb.ControlMetrics_builder{
		Values:              make(map[string]float64),
		PodMetrics:          make(map[string]*pb.MetricValues),
		PodContainerMetrics: make(map[string]*pb.ContainerMetrics),
		ContainerMetrics: pb.ContainerMetrics_builder{
			ContainerMetrics: make(map[string]*pb.MetricValues),
		}.Build(),
	}.Build()

	for _, def := range defs {
		metricSeries := ms.Series[newMetricID(def)]
		if metricSeries.isEmpty() {
			continue
		}
		series := maps.Values(metricSeries.Series)
		if !def.GetEligibility().GetStartupSamples() {
			series = filterInitializingCPU(series, workload, now)
		}
		aggregation.AddControlMetric(cm, def.GetName(), metricSeries.Aggregation.Reduce(series, workload, now))
	}

	// Skip empty ContainerMetrics as, unlike empty maps, an empty proto message
	// is serialized.
	if len(cm.GetContainerMetrics().GetContainerMetrics()) == 0 {
		cm.ClearContainerMetrics()
	}

	return cm
}

// filterInitializingCPU drops the CPU usage series of the pods still in their
// CPU initialization period whose last sample was not entirely measured after
// the pod became ready. See podutil.IsCPUSampleEligible. Series of other
// resources, and of pods not in the workload, are passed through: the
// aggregation filters them on its own.
func filterInitializingCPU(series iter.Seq[*model.Series], workload map[string]*pb.PodState, now time.Time) iter.Seq[*model.Series] {
	return func(yield func(*model.Series) bool) {
		for ser := range series {
			if ser.ResourceName == "cpu" {
				if pod, ok := workload[ser.PodName]; ok && !podutil.IsCPUSampleEligible(podutil.FromPodState(pod), ser.LastRaw.Timestamp, ser.LastRawWindow, now) {
					continue
				}
			}
			if !yield(ser) {
				return
			}
		}
	}
}

// readyReplicasCount returns the number of pods of the workload whose metrics are
// considered (see podutil.IsEligible), as reported to the recommenders. At
// least 1 is reported, so that a recommender scaling the current replicas
// proportionally (e.g. Linear) does not get stuck at 0.
func readyReplicasCount(workload map[string]*pb.PodState) int32 {
	n := 0
	for _, p := range workload {
		if podutil.IsEligible(podutil.FromPodState(p)) {
			n++
		}
	}
	return int32(max(1, n))
}
