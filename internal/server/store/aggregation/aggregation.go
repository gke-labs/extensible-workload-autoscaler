package aggregation

import (
	"iter"
	"math"
	"slices"
	"time"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store/model"
)

type AggregatedMetrics struct {
	// global is the policy-wide value, reported for model.ScopeGlobal.
	global float64
	// pod maps a pod name to its value, reported for model.ScopePod and
	// model.ScopePodContainer.
	pod map[string]float64
	// podContainer maps a pod name to its per-container values, reported for
	// model.ScopePodContainer.
	podContainer map[string]map[string]float64
	// container maps a container name to its value averaged over every pod
	// reporting that container, reported for model.ScopeContainer.
	container map[string]float64
}

// SeriesAggregation aggregates a set of series.
type SeriesAggregation interface {
	// NewTimeWindow creates a new temporal aggregation for a given metric.
	NewTimeWindow(def *pb.MetricDefinition) model.TimeWindow
	// Reduce aggregates the series of a metric, returning one aggregate for
	// the global scope, one aggregate per pod for the pod scope, etc.
	Reduce(series iter.Seq[*model.Series], workload map[string]*pb.PodState) []*SeriesAggregate
}

// DimensionalSeriesAggregation aggregates series one dimension at a time:
// labels first, then containers, then pods. A nil reducer keeps the
// corresponding dimension.
type DimensionalSeriesAggregation struct {
	// Filters drop the series that must not take part in the aggregation (e.g.
	// because they describe a non-ready pod).
	Filters []SeriesFilter
	// LabelsReducer merges the series that only differ by their labels.
	LabelsReducer SeriesReducer
	// ContainerTransform is applied to every aggregate right before the
	// containers are rolled up into their pod, e.g. to weight them.
	ContainerTransform SeriesTransform
	// ContainerReducer merges the containers of a pod into a pod-level value.
	ContainerReducer SeriesReducer
	// PodReducer merges the pods into a workload-wide value.
	PodReducer SeriesReducer
	// FinalTransform is applied once every dimension has been aggregated, to
	// turn the aggregates into the reported values.
	FinalTransform SeriesTransform
	// ResourceName is the resource described by the series, if any.
	ResourceName string
	// A workload-wide temporal aggregation. Used for decaying distributions
	// over long time windows: those are not an aggregation of per-series
	// distributions.
	WorkloadTimeWindow model.TimeWindow
}

var _ SeriesAggregation = (*DimensionalSeriesAggregation)(nil)

// NewTimeWindow implements SeriesAggregation.
func (a *DimensionalSeriesAggregation) NewTimeWindow(def *pb.MetricDefinition) model.TimeWindow {
	if a.WorkloadTimeWindow != nil {
		return a.WorkloadTimeWindow
	}
	return NewTimeWindow(def)
}

// Reduce implements SeriesAggregation.
func (a *DimensionalSeriesAggregation) Reduce(series iter.Seq[*model.Series], workload map[string]*pb.PodState) []*SeriesAggregate {
	if a.WorkloadTimeWindow != nil {
		sample, ok := a.WorkloadTimeWindow.Apply()
		if !ok {
			return nil
		}
		aggregate := &SeriesAggregate{sample: sample}
		if a.FinalTransform != nil {
			a.FinalTransform(workload, aggregate, a.ResourceName)
		}
		return []*SeriesAggregate{aggregate}
	}

	var aggregates []*SeriesAggregate
	for s := range series {
		if agg, ok := NewSeriesAggregate(s); ok && !filter(agg, workload, a.Filters) {
			aggregates = append(aggregates, agg)
		}
	}

	// Labels are always aggregated
	removeLabelsDimension(aggregates)
	aggregates = groupBy(aggregates, a.LabelsReducer)

	if a.ContainerTransform != nil {
		aggregates = slices.DeleteFunc(aggregates, func(agg *SeriesAggregate) bool {
			return !weightable(workload, agg, a.ResourceName)
		})
		for _, agg := range aggregates {
			a.ContainerTransform(workload, agg, a.ResourceName)
		}
	}
	if a.ContainerReducer != nil {
		removeContainerDimension(aggregates)
		aggregates = groupBy(aggregates, a.ContainerReducer)
	}
	if a.PodReducer != nil {
		removePodDimension(aggregates)
		aggregates = groupBy(aggregates, a.PodReducer)
	}
	if a.FinalTransform != nil {
		for _, agg := range aggregates {
			a.FinalTransform(workload, agg, a.ResourceName)
		}
	}
	return aggregates
}

// CreateSeriesAggregation returns the aggregation for a metric series. The
// resourceName argument is not empty if the series describes a resource
// (e.g. CPU or memory usage).
func CreateSeriesAggregation(def *pb.MetricDefinition, resourceName, podName, containerName string) SeriesAggregation {
	seriesFilters := []SeriesFilter{
		NonEligiblePodSeriesFilter,          // Drops series associated with non-eligible pods
		CreateLabelSeriesFilter(def.Filter), // Apply label filters
	}

	labelsReducer := SumSeriesReducer
	containerReducer := SumSeriesReducer
	podReducer := AverageSeriesReducer

	// Applied once every dimension has been aggregated, to turn the aggregate
	// into the reported value.
	finalTransform := SeriesTransform(nil)

	// If the metric is not pod specific (e.g. a queue depth), compute the
	// per-replica average. This transform may be overridden if the final value
	// is a percentile, in which case dividing by the number of replicas would
	// not make sense.
	if podName == "" {
		finalTransform = ReplicaAverageTransform
	}

	// A workload-wide temporal aggregation. Used by decaying distributions over
	// long time windows.
	workloadDecayingDistribution := model.TimeWindow(nil)

	switch {
	case def.Gauge != nil && def.Gauge.Aggregation != "":
		podReducer = NewSeriesReducer(def.Gauge.Aggregation)
	case def.Rate != nil && def.Rate.Aggregation != "":
		podReducer = NewSeriesReducer(def.Rate.Aggregation)
	case def.Distribution != nil:
		// The bucket rates are merged across every aggregated dimension and
		// the percentile is extracted from the result, so that e.g. a Global
		// p95 is the p95 of all the observations of the workload rather than
		// an average of per-pod p95s.
		merge := MergeDistributionSeriesReducer
		labelsReducer, containerReducer, podReducer = merge, merge, merge
		finalTransform = CreatePercentileTransform(parsePercentile(def.Distribution.Percentile))
	// For global metrics with decaying distributions, we only build one
	// distribution for the whole workload (i.e. not one-per-series).
	case def.DecayingDistribution != nil && (def.Scope == "" || def.Scope == model.ScopeGlobal):
		workloadDecayingDistribution = NewDecayingDistributionAggregation(def.DecayingDistribution)
		// Don't aggregate series, only use the workload-wide time window.
		podReducer, containerReducer, labelsReducer = nil, nil, nil
		finalTransform = CreatePercentileTransform(parsePercentile(def.DecayingDistribution.Percentile))
	case def.DecayingDistribution != nil:
		// Idem, with the decaying histograms.
		merge := CreateMergeDecayingDistributionSeriesReducer(def.DecayingDistribution)
		labelsReducer, containerReducer, podReducer = merge, merge, merge
		finalTransform = CreatePercentileTransform(parsePercentile(def.DecayingDistribution.Percentile))
	}

	// The scope decides which dimensions are kept.
	switch def.Scope {
	case model.ScopePod:
		podReducer = nil
	case model.ScopeContainer:
		containerReducer = nil
	case model.ScopePodContainer:
		podReducer = nil
		containerReducer = nil
	}

	// When the containers are rolled up into their pod, the samples related to
	// a resource are weighted by their container's share of the pod requests
	// for that resource, so that their sum is the pod-level value. Containers
	// without a request for the resource have no meaningful weight and are
	// left out of the rollup.
	containerTransform := SeriesTransform(nil)
	if resourceName != "" && containerReducer != nil {
		containerTransform = ContainerResourceWeightedTransform
	}

	return &DimensionalSeriesAggregation{
		Filters:            seriesFilters,
		LabelsReducer:      labelsReducer,
		ContainerTransform: containerTransform,
		ContainerReducer:   containerReducer,
		PodReducer:         podReducer,
		FinalTransform:     finalTransform,
		ResourceName:       resourceName,
		WorkloadTimeWindow: workloadDecayingDistribution,
	}
}

// SeriesAggregate is the result of aggregating a set of series.
//
// It contains the remaining dimensions after aggregation (i.e. the remaining
// pod, container, and labels) and the aggregated value. Empty/nil fields
// correspond to unset dimensions.
type SeriesAggregate struct {
	podName       string
	containerName string
	labels        map[string]string
	sample        model.Sample
}

func NewSeriesAggregate(s *model.Series) (*SeriesAggregate, bool) {
	sample, ok := s.TimeWindow.Apply()
	if !ok {
		return nil, false
	}
	return &SeriesAggregate{
		podName:       s.PodName,
		containerName: s.ContainerName,
		labels:        s.Labels,
		sample:        sample,
	}, true
}

// AddControlMetric adds the series associated with the metricName metric to a
// ControlMetrics message.
//
// Series are reported according to the dimensions they have left: for example,
// a series with only a pod dimension is reported as a per-pod metric.
func AddControlMetric(cm *pb.ControlMetrics, metricName string, aggregates []*SeriesAggregate) *pb.ControlMetrics {
	for _, s := range aggregates {
		switch {
		case s.podName == "" && s.containerName == "":
			cm.Values[metricName] = s.sample.Value
		case s.containerName == "":
			mv := metricValues(cm.PodMetrics, s.podName)
			mv.Values[metricName] = s.sample.Value
		case s.podName == "":
			mv := metricValues(cm.ContainerMetrics.ContainerMetrics, s.containerName)
			mv.Values[metricName] = s.sample.Value
		default:
			podMetricValues(cm.PodContainerMetrics, s.podName, s.containerName).Values[metricName] = s.sample.Value
		}
	}
	return cm
}

// podMetricValues returns the MetricValues entry for containerName within
// podName, creating the intermediate entries if they do not exist yet.
func podMetricValues(pcm map[string]*pb.ContainerMetrics, podName, containerName string) *pb.MetricValues {
	cm, ok := pcm[podName]
	if !ok {
		cm = &pb.ContainerMetrics{ContainerMetrics: make(map[string]*pb.MetricValues)}
		pcm[podName] = cm
	}
	mv, ok := cm.ContainerMetrics[containerName]
	if !ok {
		mv = &pb.MetricValues{Values: make(map[string]float64)}
		cm.ContainerMetrics[containerName] = mv
	}
	return mv
}

// metricValues returns the MetricValues entry stored under key, creating it
// (and its nested map) if it does not exist yet.
func metricValues(all map[string]*pb.MetricValues, key string) *pb.MetricValues {
	mv, ok := all[key]
	if !ok {
		mv = &pb.MetricValues{Values: make(map[string]float64)}
		all[key] = mv
	}
	return mv
}

func removeLabelsDimension(aggregates []*SeriesAggregate) {
	for _, s := range aggregates {
		s.labels = nil
	}
}

func removeContainerDimension(aggregates []*SeriesAggregate) {
	for _, s := range aggregates {
		s.containerName = ""
	}
}

func removePodDimension(aggregates []*SeriesAggregate) {
	for _, s := range aggregates {
		s.podName = ""
	}
}

// filter returns true if any of the provided filters matches the aggregate, in
// which case it should be dropped.
func filter(agg *SeriesAggregate, workload map[string]*pb.PodState, filters []SeriesFilter) bool {
	for _, f := range filters {
		if f(workload, agg) {
			return true
		}
	}
	return false
}

// groupBy merges aggregates that have identical dimensions using the reducer
// reduce. Input aggregates are modified and the output is not ordered.
func groupBy(aggregates []*SeriesAggregate, reduce SeriesReducer) []*SeriesAggregate {
	groups := make(map[model.SeriesID]*SeriesAggregate)
	samples := make(map[model.SeriesID][]model.Sample)
	for _, s := range aggregates {
		id := model.NewSeriesID(s.podName, s.containerName, s.labels)
		groups[id] = s
		samples[id] = append(samples[id], s.sample)
	}
	result := make([]*SeriesAggregate, 0, len(groups))
	for id, g := range groups {
		g.sample = reduce(slices.Values(samples[id]))
		result = append(result, g)
	}
	return result
}

// NewTimeWindow returns the time window for the given
// metric definition.
func NewTimeWindow(def *pb.MetricDefinition) model.TimeWindow {
	switch {
	case def.Gauge != nil:
		return NewLatestAggregation(time.Minute)
	case def.Rate != nil:
		return NewRateAggregation(def.Rate.GetWindow().AsDuration())
	case def.Distribution != nil:
		// Bucket rates over the same freshness window as gauges.
		return NewDistributionAggregation(time.Minute)
	case def.DecayingDistribution != nil:
		return NewDecayingDistributionAggregation(def.DecayingDistribution)
	default:
		panic("Not implemented")
	}
}

type RateAggregation struct {
	sampleWindow
}

var _ model.TimeWindow = (*RateAggregation)(nil)

func NewRateAggregation(duration time.Duration) *RateAggregation {
	return &RateAggregation{
		sampleWindow: newSamplesWindow(duration),
	}
}

func (r *RateAggregation) Apply() (model.Sample, bool) {
	if r.Len() < 2 {
		return model.Sample{}, false
	}
	oldest, newest := r.oldest(), r.newest()
	deltaTime := newest.Timestamp.Sub(oldest.Timestamp).Seconds()
	if deltaTime == 0 {
		return model.Sample{}, false
	}
	deltaValue := newest.Value - oldest.Value
	v := deltaValue / deltaTime
	return model.Sample{Value: v}, true
}

// DistributionAggregation computes the per-second rate of each bucket of a
// cumulative distribution, from the oldest and the newest distributions of a
// sliding window. Only distribution samples are supported: scalar samples are
// ignored.
//
// Apply returns the bucket rates rather than a percentile, so that the
// distributions of several series can be merged by the series reducers
// before the percentile is extracted.
type DistributionAggregation struct {
	sampleWindow
}

var _ model.TimeWindow = (*DistributionAggregation)(nil)

func NewDistributionAggregation(duration time.Duration) *DistributionAggregation {
	return &DistributionAggregation{
		sampleWindow: newSamplesWindow(duration),
	}
}

func (d *DistributionAggregation) AddSample(sample model.Sample, now time.Time) {
	if sample.Distribution == nil {
		return
	}
	d.sampleWindow.AddSample(sample, now)
}

func (d *DistributionAggregation) Apply() (model.Sample, bool) {
	if d.Len() < 2 {
		return model.Sample{}, false
	}
	oldest, newest := d.oldest(), d.newest()
	deltaTime := newest.Timestamp.Sub(oldest.Timestamp).Seconds()
	if deltaTime == 0 {
		return model.Sample{}, false
	}
	return model.Sample{Distribution: model.RateDistribution(oldest.Distribution, newest.Distribution, deltaTime)}, true
}

type MaxTimeWindow struct {
	sampleWindow
}

var _ model.TimeWindow = (*MaxTimeWindow)(nil)

func NewMaxAggregation(duration time.Duration) *MaxTimeWindow {
	return &MaxTimeWindow{
		sampleWindow: newSamplesWindow(duration),
	}
}

func (m *MaxTimeWindow) Apply() (model.Sample, bool) {
	if m.Len() == 0 {
		return model.Sample{}, false
	}
	maxValue := -math.MaxFloat64
	for s := range m.All() {
		if s.Value > maxValue {
			maxValue = s.Value
		}
	}
	return model.Sample{Value: maxValue}, true
}

type AvgTimeWindow struct {
	sampleWindow
}

var _ model.TimeWindow = (*AvgTimeWindow)(nil)

func NewAverageTimeWindow(duration time.Duration) *AvgTimeWindow {
	return &AvgTimeWindow{
		sampleWindow: newSamplesWindow(duration),
	}
}

func (m *AvgTimeWindow) Apply() (model.Sample, bool) {
	n := m.Len()
	if n == 0 {
		return model.Sample{}, false
	}
	var sum float64
	for s := range m.All() {
		sum += s.Value
	}
	return model.Sample{Value: sum / float64(n)}, true
}

type LatestTimeWindow struct {
	sampleWindow
}

var _ model.TimeWindow = (*LatestTimeWindow)(nil)

func NewLatestAggregation(duration time.Duration) *LatestTimeWindow {
	return &LatestTimeWindow{
		sampleWindow: newSamplesWindow(duration),
	}
}

func (m *LatestTimeWindow) Apply() (model.Sample, bool) {
	if m.Len() == 0 {
		return model.Sample{}, false
	}
	return m.newest(), true
}

// DecayingDistributionAggregation accumulates the samples of a series into a
// time-decaying histogram. When a rate window is configured, the samples are
// treated as a counter and the histogram accumulates the rate computed over
// that window instead.
//
// Apply returns the histogram itself rather than a percentile, so that the
// distributions of several series can be merged by the series reducers
// before the percentile is extracted.
type DecayingDistributionAggregation struct {
	// histogram is nil when the definition could not be parsed, in which case
	// the aggregation never reports a value.
	histogram *model.DecayingHistogram
	// rate converts the counter samples into rates. Nil for gauges.
	rate *RateAggregation
}

var _ model.TimeWindow = (*DecayingDistributionAggregation)(nil)

func NewDecayingDistributionAggregation(def *pb.DecayingDistribution) *DecayingDistributionAggregation {
	halfLife := def.HalfLife.AsDuration()
	// The reference time is set by the first sample.
	histogram, _ := model.NewDecayingHistogram(time.Time{}, halfLife, def.BucketSize)
	a := &DecayingDistributionAggregation{histogram: histogram}
	if def.GetRate().AsDuration() > 0 {
		a.rate = NewRateAggregation(def.GetRate().AsDuration())
	}
	return a
}

func (d *DecayingDistributionAggregation) AddSample(sample model.Sample, now time.Time) {
	if d.rate != nil {
		d.rate.AddSample(sample, now)
		rate, ok := d.rate.Apply()
		if !ok {
			return
		}
		sample.Value = rate.Value
	}
	t := sample.Timestamp
	if d.histogram.ReferenceTime.IsZero() {
		d.histogram.ReferenceTime = t
	}
	d.histogram.Add(sample.Value, t)
}

func (d *DecayingDistributionAggregation) Apply() (model.Sample, bool) {
	if d.histogram == nil || d.histogram.TotalWeight == 0 {
		return model.Sample{}, false
	}
	return model.Sample{DecayingHistogram: d.histogram}, true
}

// sampleWindow holds the samples that fall within a sliding time window.
//
// The samples are stored oldest first in a ring buffer, in the half-open
// index range [start, end). At least one slot is always kept free, so that
// start == end unambiguously means the window is empty.
type sampleWindow struct {
	duration   time.Duration
	samples    []model.Sample
	start, end int
}

func newSamplesWindow(duration time.Duration) sampleWindow {
	return sampleWindow{duration: duration}
}

// AddSample appends sample to the window, after dropping the samples that
// have fallen out of the window relative to now. A sample carrying the same
// timestamp as the newest one supersedes it (e.g. a re-scrape of a source
// that has not produced a new value).
func (m *sampleWindow) AddSample(sample model.Sample, now time.Time) {
	m.prune(now)
	if m.Len() > 0 && m.newest().Timestamp.Equal(sample.Timestamp) {
		m.samples[(m.end+len(m.samples)-1)%len(m.samples)] = sample
		return
	}
	// Never let the buffer fill up completely, so that start == end can
	// only mean that the window is empty.
	if m.Len()+1 >= len(m.samples) {
		m.grow()
	}
	m.samples[m.end] = sample
	m.end = (m.end + 1) % len(m.samples)
}

// prune drops the samples that have fallen out of the window relative to now.
func (m *sampleWindow) prune(now time.Time) {
	cutoff := now.Add(-m.duration)
	for m.start != m.end && m.samples[m.start].Timestamp.Before(cutoff) {
		m.start = (m.start + 1) % len(m.samples)
	}
}

// grow doubles the capacity of the ring buffer, moving the samples to the
// beginning of the new buffer.
func (m *sampleWindow) grow() {
	n := m.Len()
	grown := make([]model.Sample, max(2, 2*len(m.samples)))
	for i := range n {
		grown[i] = m.samples[(m.start+i)%len(m.samples)]
	}
	m.samples = grown
	m.start, m.end = 0, n
}

// Len returns the number of samples currently in the window.
func (m *sampleWindow) Len() int {
	if m.end >= m.start {
		return m.end - m.start
	}
	return m.end - m.start + len(m.samples)
}

// oldest returns the oldest sample in the window, which must not be empty.
func (m *sampleWindow) oldest() model.Sample {
	return m.samples[m.start]
}

// newest returns the newest sample in the window, which must not be empty.
func (m *sampleWindow) newest() model.Sample {
	return m.samples[(m.end+len(m.samples)-1)%len(m.samples)]
}

// All returns an iterator over the samples currently in the window, ordered
// from oldest to newest.
func (m *sampleWindow) All() iter.Seq[model.Sample] {
	return func(yield func(model.Sample) bool) {
		for i := m.start; i != m.end; i = (i + 1) % len(m.samples) {
			if !yield(m.samples[i]) {
				return
			}
		}
	}
}
