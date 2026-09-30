package store

import (
	"fmt"
	"maps"
	"time"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/podutil"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store/aggregation"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store/model"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		Aggregation:  aggregation.CreateSeriesAggregation(def, sample.ResourceName, podName, containerName),
		Definition:   def,
		ResourceName: sample.ResourceName,
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
func (ms *MetricStore) IngestMetrics(policy *pb.Policy, batches []*pb.MetricBatch, ingestTime time.Time) error {
	for _, batch := range batches {
		for _, m := range batch.Samples {
			if err := ms.ingestSample(policy, batch.PodName, batch.ContainerName, m, ingestTime); err != nil {
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
	defs := policy.Metrics
	if owner != "" {
		defs = policy.RecommenderMetrics[owner].GetDefinitions()
	}
	for _, d := range defs {
		if d.Name == name {
			return d
		}
	}
	return nil
}

func (ms *MetricStore) ingestSample(policy *pb.Policy, podName, containerName string, sample *pb.MetricSample, ingestTime time.Time) error {
	owner := sample.GetRecommenderName()
	def := findMetricDefinition(policy, owner, sample.Name)
	if def == nil {
		if owner != "" {
			return fmt.Errorf("metric %s not defined in policy for recommender %s", sample.Name, owner)
		}
		return fmt.Errorf("metric %s not defined in policy", sample.Name)
	}

	// Filter early
	if !model.MatchFilter(sample.Labels, def.Filter) {
		return nil
	}

	id := newMetricID(def)
	metricSeries, ok := ms.Series[id]
	if !ok {
		metricSeries = newMetricSeries(def, podName, containerName, sample)
		ms.Series[id] = metricSeries
	}
	sid := model.NewSeriesID(podName, containerName, sample.Labels)
	series, ok := metricSeries.getSeries(sid)
	if !ok {
		series = model.NewSeries(podName, containerName, sample.ResourceName, sample.Labels, metricSeries.Aggregation.NewTimeWindow(def))
		metricSeries.setSeries(sid, series)
	}
	ms.seriesIngestSample(series, sample, ingestTime)
	return nil
}

func (ms *MetricStore) seriesIngestSample(ser *model.Series, m *pb.MetricSample, ingestTime time.Time) {
	ts := ingestTime
	if m.Timestamp != nil {
		ts = m.Timestamp.AsTime()
	}
	if ts.Before(time.Unix(ser.LastRaw.Timestamp, 0)) {
		return // Drop out of order samples
	}
	sample := model.Sample{Timestamp: ts.Unix(), Value: m.Value, Distribution: model.NewDistributionFromHistogram(m.GetHistogram())}
	ser.AddSample(sample, ingestTime)
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

// seriesRetention is how long a series is kept without receiving any sample.
const seriesRetention = 10 * time.Minute

// gcSeries drops the series that have not received a sample for
// seriesRetention (e.g. the series of pods that disappeared), and the metrics
// left without any series.
func (ms *MetricStore) gcSeries(now time.Time) {
	cutoff := now.Add(-seriesRetention).Unix()
	for id, metric := range ms.Series {
		for sid, ser := range metric.Series {
			if ser.LastRaw.Timestamp < cutoff {
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
	for _, m := range policy.Metrics {
		defs[newMetricID(m)] = m
	}
	for _, owned := range policy.RecommenderMetrics {
		for _, m := range owned.GetDefinitions() {
			defs[newMetricID(m)] = m
		}
	}
	return defs
}

// Calculate aggregates the given metric definitions into a single ControlMetrics
// snapshot at timestamp now. All the definitions are expected to share the same
// owner.
func (ms *MetricStore) Calculate(policy *pb.Policy, defs []*pb.MetricDefinition, workload map[string]*pb.PodState, now int64) *pb.ControlMetrics {
	ms.gcSeries(time.Unix(now, 0))

	eligiblePods := filterEligiblePods(workload)
	readyReplicas := len(eligiblePods)
	cm := &pb.ControlMetrics{
		ReadyReplicas:       int32(max(1, readyReplicas)),
		Timestamp:           timestamppb.New(time.Unix(now, 0)),
		Values:              make(map[string]float64),
		PodMetrics:          make(map[string]*pb.MetricValues),
		PodContainerMetrics: make(map[string]*pb.ContainerMetrics),
		ContainerMetrics: &pb.ContainerMetrics{
			ContainerMetrics: make(map[string]*pb.MetricValues),
		},
	}

	for _, def := range defs {
		metricSeries := ms.Series[newMetricID(def)]
		if metricSeries.isEmpty() {
			continue
		}
		aggregation.AddControlMetric(cm, def.Name, metricSeries.Aggregation.Reduce(maps.Values(metricSeries.Series), workload))
	}

	// Skip empty ContainerMetrics as, unlike empty maps, an empty proto message
	// is serialized.
	if len(cm.ContainerMetrics.ContainerMetrics) == 0 {
		cm.ContainerMetrics = nil
	}

	return cm
}

// filterEligiblePods returns the pods of the workload whose metrics should be
// considered, keyed by name. See podutil.IsEligible.
func filterEligiblePods(workload map[string]*pb.PodState) map[string]*pb.PodState {
	eligible := make(map[string]*pb.PodState, len(workload))
	for name, p := range workload {
		if podutil.IsEligible(podutil.FromPodState(p)) {
			eligible[name] = p
		}
	}
	return eligible
}
