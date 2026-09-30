package store

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/metrics"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Metric scopes define the granularity at which a metric is reported.
const (
	// ScopeGlobal aggregates every series into a single policy-wide value,
	// reported in ControlMetrics.Values. This is the default.
	ScopeGlobal = "Global"
	// ScopePod reports one value per pod in ControlMetrics.PodMetrics[pod].Values.
	// Samples reported by individual containers are summed into their pod's value.
	ScopePod = "Pod"
	// ScopePodContainer reports one value per container in
	// ControlMetrics.PodContainerMetrics[pod].ContainerMetrics[container], along
	// with the pod-level rollup in ControlMetrics.PodMetrics[pod].Values.
	ScopePodContainer = "PodContainer"
	// ScopeContainer reports one value per container name in
	// ControlMetrics.ContainerMetrics.ContainerMetrics[container], averaged over
	// every pod that reports that container.
	ScopeContainer = "Container"
)

// isPodBreakdown reports whether the metric keeps one value per pod. Both ScopePod
// and ScopePodContainer keep per-pod state; they differ only in whether the
// per-container breakdown is reported alongside it.
func isPodBreakdown(scope string) bool {
	return scope == ScopePod || scope == ScopePodContainer
}

// isContainerBreakdown reports whether the metric needs the per-pod,
// per-container values to be tracked. ScopePodContainer reports them directly,
// while ScopeContainer averages them across pods.
func isContainerBreakdown(scope string) bool {
	return scope == ScopePodContainer || scope == ScopeContainer
}

// isGlobalScope reports whether the metric is aggregated into a single
// policy-wide value. Any unrecognized scope (including the empty string) is
// treated as ScopeGlobal.
func isGlobalScope(scope string) bool {
	return !isPodBreakdown(scope) && !isContainerBreakdown(scope)
}

// DataPoint represents a single calculated value (ControlMetric)
type DataPoint struct {
	Timestamp   int64 // Freshness Timestamp (Ingest Time)
	Value       float64
	Labels      map[string]string
	RateBuckets map[string]float64 // Rate buckets
}

// Sample represents the raw data from the source
type Sample struct {
	// Source Timestamp.
	Timestamp int64
	// If the sample is a scalar, its value.
	Value float64
	// If the sample is a distribution, its cumulative histogram.
	Histogram *pb.Histogram
}

// Series holds the state of a single metric stream
type Series struct {
	// Identity
	PodName string
	// ContainerName is the container the samples originate from.
	// Empty means the samples apply to the pod (or the policy) as a whole.
	ContainerName string
	// The name of the resource this metric describes (e.g. "cpu or "memory").
	ResourceName string
	Labels       map[string]string

	// State
	LastRaw       Sample
	ControlMetric DataPoint

	// Temporal Aggregation
	Window            *SlidingWindow
	DecayingHistogram *DecayingHistogram
}

// MetricStore holds the time-series and histogram state of a policy's metrics.
type MetricStore struct {
	Series           map[metricID]map[seriesID]*Series
	GlobalHistograms map[metricID]*DecayingHistogram
}

// NewMetricStore creates an initialized MetricStore.
func NewMetricStore() *MetricStore {
	return &MetricStore{
		Series:           make(map[metricID]map[seriesID]*Series),
		GlobalHistograms: make(map[metricID]*DecayingHistogram),
	}
}

// IngestBatch ingests a slice of metric batches for the given policy.
func (ms *MetricStore) IngestBatch(policy *pb.Policy, batches []*pb.MetricBatch, ingestTime int64) error {
	for _, batch := range batches {
		for _, m := range batch.Samples {
			if err := ms.processSample(policy, batch.PodName, batch.ContainerName, m, ingestTime); err != nil {
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

func (ms *MetricStore) processSample(policy *pb.Policy, podName, containerName string, m *pb.MetricSample, ingestTime int64) error {
	owner := m.GetRecommenderName()
	def := findMetricDefinition(policy, owner, m.Name)
	if def == nil {
		if owner != "" {
			return fmt.Errorf("metric %s not defined in policy for recommender %s", m.Name, owner)
		}
		return fmt.Errorf("metric %s not defined in policy", m.Name)
	}

	// Filter early
	if !matchFilter(m.Labels, def.Filter) {
		return nil
	}

	id := newMetricID(def)
	if _, ok := ms.Series[id]; !ok {
		ms.Series[id] = make(map[seriesID]*Series)
	}

	sid := newSeriesID(podName, containerName, m.Labels)

	ser, ok := ms.Series[id][sid]
	if !ok {
		ser = &Series{
			PodName:       podName,
			ContainerName: containerName,
			ResourceName:  m.ResourceName,
			Labels:        m.Labels,
		}

		// INTENT-BASED INITIALIZATION
		if def.Rate != nil {
			d, _ := time.ParseDuration(def.Rate.Window)
			// TEMPORAL Aggregation for Rate is always Avg (averaging instantaneous rates)
			// SPATIAL Aggregation is handled in calculateMetric via def.Rate.Aggregation
			ser.Window = NewSlidingWindow(d, "Avg")
		} else if def.DecayingDistribution != nil && def.DecayingDistribution.Rate != "" {
			// Pre-processing Rate for DecayingDistribution
			d, _ := time.ParseDuration(def.DecayingDistribution.Rate)
			ser.Window = NewSlidingWindow(d, "Avg")
		}

		ms.Series[id][sid] = ser
	}

	var gh *DecayingHistogram
	if def.DecayingDistribution != nil {
		if isPodBreakdown(def.Scope) {
			if ser.DecayingHistogram == nil {
				hl, _ := time.ParseDuration(def.DecayingDistribution.HalfLife)
				ser.DecayingHistogram, _ = NewDecayingHistogram(time.Unix(ingestTime, 0), hl, def.DecayingDistribution.BucketSize)
			}
		} else {
			if ms.GlobalHistograms == nil {
				ms.GlobalHistograms = make(map[metricID]*DecayingHistogram)
			}
			var ok bool
			gh, ok = ms.GlobalHistograms[id]
			if !ok {
				hl, _ := time.ParseDuration(def.DecayingDistribution.HalfLife)
				gh, _ = NewDecayingHistogram(time.Unix(ingestTime, 0), hl, def.DecayingDistribution.BucketSize)
				ms.GlobalHistograms[id] = gh
			}
		}
	}

	ms.updateSeries(ser, def, m, ingestTime, gh)
	return nil
}

func (ms *MetricStore) updateSeries(ser *Series, def *pb.MetricDefinition, m *pb.MetricSample, ingestTime int64, gh *DecayingHistogram) {
	ts := m.Timestamp
	if ts == 0 {
		ts = ingestTime
	}

	var value float64
	var hasValue bool

	// Determine effective type
	defType := "Gauge"
	if def.Rate != nil {
		defType = "Counter"
	} else if def.Distribution != nil {
		defType = "Histogram"
	} else if def.DecayingDistribution != nil {
		if def.DecayingDistribution.Rate != "" {
			defType = "Counter"
		}
	}

	switch defType {
	case "Histogram":
		hist := m.GetHistogram()
		if hist.GetBuckets() == nil {
			return
		}

		if ser.LastRaw.Timestamp == 0 {
			ser.LastRaw = Sample{Timestamp: ts, Histogram: hist}
			ser.ControlMetric = DataPoint{Timestamp: ingestTime, Labels: m.Labels}
		} else if ts > ser.LastRaw.Timestamp {
			dt := float64(ts - ser.LastRaw.Timestamp)
			rateBuckets := calculateBucketRates(hist.GetBuckets(), ser.LastRaw.Histogram.GetBuckets(), dt)
			ser.ControlMetric = DataPoint{Timestamp: ingestTime, Value: 0, Labels: m.Labels, RateBuckets: rateBuckets}
			ser.LastRaw = Sample{Timestamp: ts, Value: 0, Histogram: hist}
		} else if ts == ser.LastRaw.Timestamp {
			ser.ControlMetric.Timestamp = ingestTime
			ser.LastRaw.Value = m.Value
			ser.LastRaw.Histogram = hist
		}

	case "Counter":
		if ser.LastRaw.Timestamp == 0 {
			ser.LastRaw = Sample{Timestamp: ts, Value: m.Value}
			ser.ControlMetric = DataPoint{Timestamp: ingestTime, Value: 0, Labels: m.Labels}
		} else if ts > ser.LastRaw.Timestamp {
			diff := m.Value - ser.LastRaw.Value
			if diff < 0 {
				diff = m.Value
			}
			dt := float64(ts - ser.LastRaw.Timestamp)
			rate := diff / dt
			ser.ControlMetric = DataPoint{Timestamp: ingestTime, Value: rate, Labels: m.Labels}
			ser.LastRaw = Sample{Timestamp: ts, Value: m.Value}
			value = rate
			hasValue = true
		} else if ts == ser.LastRaw.Timestamp {
			ser.ControlMetric.Timestamp = ingestTime
			ser.LastRaw.Value = m.Value
			ser.LastRaw.Histogram = m.GetHistogram()
		}

	default: // Gauge (Default)
		ser.ControlMetric = DataPoint{Timestamp: ingestTime, Value: m.Value, Labels: m.Labels}
		ser.LastRaw = Sample{Timestamp: ts, Value: m.Value}
		value = m.Value
		hasValue = true
	}

	if hasValue {
		t := time.Unix(ingestTime, 0)
		if gh != nil {
			gh.Add(value, t)
		}
		if ser.Window != nil {
			ser.Window.Add(value, t)
		}
		if ser.DecayingHistogram != nil {
			ser.DecayingHistogram.Add(value, t)
		}
	}
}

// CleanupOrphaned removes series and global histograms whose metric is no
// longer defined in policy.
func (ms *MetricStore) CleanupOrphaned(policy *pb.Policy) {
	metricDefs := definedMetricIDs(policy)
	for id := range ms.Series {
		if !metricDefs[id] {
			delete(ms.Series, id)
		}
	}
	for id := range ms.GlobalHistograms {
		if !metricDefs[id] {
			delete(ms.GlobalHistograms, id)
		}
	}
}

// definedMetricIDs returns the IDs of all the metrics a policy defines,
// including the ones owned by its recommenders.
func definedMetricIDs(policy *pb.Policy) map[metricID]bool {
	ids := make(map[metricID]bool)
	if policy == nil {
		return ids
	}
	for _, m := range policy.Metrics {
		ids[newMetricID(m)] = true
	}
	for _, defs := range policy.RecommenderMetrics {
		for _, m := range defs.GetDefinitions() {
			ids[newMetricID(m)] = true
		}
	}
	return ids
}

// Calculate aggregates the given metric definitions into a single ControlMetrics
// snapshot at timestamp now. All the definitions are expected to share the same
// owner.
func (ms *MetricStore) Calculate(policy *pb.Policy, defs []*pb.MetricDefinition, workload map[string]*pb.PodState, now int64) *pb.ControlMetrics {
	cutoff := now - 60
	gcCutoff := now - 600

	readyReplicas := 0
	for _, p := range workload {
		if p.IsReady {
			readyReplicas++
		}
	}
	if readyReplicas == 0 {
		readyReplicas = 1
	}

	currentControlMetrics := make(map[string]float64)
	currentPodMetrics := make(map[string]*pb.MetricValues)
	currentPodContainerMetrics := make(map[string]*pb.ContainerMetrics)
	currentContainerMetrics := make(map[string]*pb.MetricValues)

	for _, def := range defs {
		id := newMetricID(def)
		res, ok := ms.calculateMetric(id, def, ms.Series[id], workload, readyReplicas, now, cutoff, gcCutoff)
		if !ok {
			continue
		}
		switch {
		case res.container != nil:
			for containerName, containerVal := range res.container {
				metricValues(currentContainerMetrics, containerName).Values[def.Name] = containerVal
			}
		case res.pod != nil:
			for podName, podVal := range res.pod {
				metricValues(currentPodMetrics, podName).Values[def.Name] = podVal
			}
			for podName, byContainer := range res.podContainer {
				for containerName, containerVal := range byContainer {
					podContainerMetrics(currentPodContainerMetrics, podName, containerName).Values[def.Name] = containerVal
				}
			}
		default:
			currentControlMetrics[def.Name] = res.global
			if policy.Workload != nil {
				// Metrics owned by a recommender are exported under their
				// metric ID, as their name is only unique within their
				// owner.
				metrics.RecordControlMetric(policy.Id.ClusterName, policy.Id.Namespace, policy.Id.Name, policy.Workload.Group, policy.Workload.Version, policy.Workload.Kind, policy.Workload.Name, id.String(), res.global)
			}
		}
	}

	cm := &pb.ControlMetrics{
		Values:              currentControlMetrics,
		PodMetrics:          currentPodMetrics,
		PodContainerMetrics: currentPodContainerMetrics,
		ReadyReplicas:       int32(readyReplicas),
		Timestamp:           now,
	}
	// Left unset when no Container-scoped metric reported a value, so that the
	// snapshot does not carry an empty message.
	if len(currentContainerMetrics) > 0 {
		cm.ContainerMetrics = &pb.ContainerMetrics{ContainerMetrics: currentContainerMetrics}
	}
	return cm
}

// metricValues returns the MetricValues entry stored under key, creating it
// (and its nested map) if it does not exist yet. The key is a pod name for
// ControlMetrics.PodMetrics and a container name for
// ControlMetrics.ContainerMetrics.
func metricValues(all map[string]*pb.MetricValues, key string) *pb.MetricValues {
	mv, ok := all[key]
	if !ok {
		mv = &pb.MetricValues{Values: make(map[string]float64)}
		all[key] = mv
	}
	return mv
}

// podContainerMetrics returns the MetricValues entry for containerName within
// podName, creating the intermediate entries if they do not exist yet.
func podContainerMetrics(all map[string]*pb.ContainerMetrics, podName, containerName string) *pb.MetricValues {
	pcm, ok := all[podName]
	if !ok {
		pcm = &pb.ContainerMetrics{ContainerMetrics: make(map[string]*pb.MetricValues)}
		all[podName] = pcm
	}
	cm, ok := pcm.ContainerMetrics[containerName]
	if !ok {
		cm = &pb.MetricValues{Values: make(map[string]float64)}
		pcm.ContainerMetrics[containerName] = cm
	}
	return cm
}

// metricResult holds the values calculated for a single metric definition.
// Which field is populated depends on the metric's scope; the others stay nil.
type metricResult struct {
	// global is the policy-wide value, reported for ScopeGlobal.
	global float64
	// pod maps a pod name to its value, reported for ScopePod and
	// ScopePodContainer.
	pod map[string]float64
	// podContainer maps a pod name to its per-container values, reported for
	// ScopePodContainer.
	podContainer map[string]map[string]float64
	// container maps a container name to its value averaged over every pod
	// reporting that container, reported for ScopeContainer.
	container map[string]float64
}

// calculateMetric aggregates the series of a single metric definition. It
// reports false if no value could be computed.
//
// id identifies the metric whose state is tracked.
//
// The fields populated on the result depend on def.Scope:
//   - "Global" (default): only the policy-wide value is set.
//   - "Pod": only the per-pod values are set. Samples reported by individual
//     containers are summed into their pod's value.
//   - "PodContainer": the per-pod values are set along with the per-container
//     breakdown that rolls up into them.
//   - "Container": only the per-container values are set, each averaged over
//     the pods reporting that container.
func (ms *MetricStore) calculateMetric(id metricID, def *pb.MetricDefinition, seriesMap map[seriesID]*Series, workload map[string]*pb.PodState, readyReplicas int, now, cutoff, gcCutoff int64) (metricResult, bool) {
	if isGlobalScope(def.Scope) {
		if gh, ok := ms.GlobalHistograms[id]; ok {
			percentile := "p95"
			if def.DecayingDistribution != nil {
				percentile = def.DecayingDistribution.Percentile
			}
			p := parsePercentile(percentile)
			return metricResult{global: gh.Percentile(p, time.Unix(now, 0))}, true
		}
	}

	if seriesMap == nil {
		return metricResult{}, false
	}

	globalSum := 0.0
	hasGlobal := false
	globalBuckets := make(map[string]float64)
	podBuckets := make(map[string]map[string]float64)
	// PodName -> ContainerName -> buckets
	containerBuckets := make(map[string]map[string]map[string]float64)
	hasBuckets := false
	podSums := make(map[string]float64)
	podFound := make(map[string]bool)
	// PodName -> ContainerName -> value
	containerSums := make(map[string]map[string]float64)
	// Lazily computed weights of the containers' resource requests.
	requestWeights := newRequestWeightCache()

	// Determine effective type & aggregation
	defType := "Gauge"
	agg := "Avg"
	percentile := ""

	if def.Gauge != nil {
		defType = "Gauge"
		agg = def.Gauge.Aggregation
	} else if def.Rate != nil {
		defType = "Counter"
		agg = def.Rate.Aggregation
		if agg == "" {
			agg = "Sum"
		}
	} else if def.Distribution != nil {
		defType = "Histogram"
		agg = def.Distribution.Aggregation
		if agg == "" {
			agg = "Max"
		}
		percentile = def.Distribution.Percentile
	} else if def.DecayingDistribution != nil {
		defType = "Gauge"
	}

	for sid, ser := range seriesMap {
		// GC
		if ser.ControlMetric.Timestamp < gcCutoff {
			delete(seriesMap, sid)
			continue
		}

		// Freshness
		if ser.ControlMetric.Timestamp < cutoff || !matchFilter(ser.Labels, def.Filter) {
			continue
		}

		if ser.PodName == "" {
			if defType == "Histogram" {
				if ser.ControlMetric.RateBuckets != nil {
					sumRateBuckets(globalBuckets, ser.ControlMetric.RateBuckets)
					hasBuckets = true
				}
			} else {
				globalSum += ser.ControlMetric.Value
				hasGlobal = true
			}
			continue
		}

		// Pod readiness
		if podState, ok := workload[ser.PodName]; !ok || !podState.IsReady {
			continue
		}

		if defType == "Histogram" {
			if ser.ControlMetric.RateBuckets != nil {
				if !isGlobalScope(def.Scope) {
					if podBuckets[ser.PodName] == nil {
						podBuckets[ser.PodName] = make(map[string]float64)
					}
					sumRateBuckets(podBuckets[ser.PodName], ser.ControlMetric.RateBuckets)

					// The per-container breakdown backs both the PodContainer
					// scope and the per-container average of the Container scope.
					if isContainerBreakdown(def.Scope) && ser.ContainerName != "" {
						if containerBuckets[ser.PodName] == nil {
							containerBuckets[ser.PodName] = make(map[string]map[string]float64)
						}
						if containerBuckets[ser.PodName][ser.ContainerName] == nil {
							containerBuckets[ser.PodName][ser.ContainerName] = make(map[string]float64)
						}
						sumRateBuckets(containerBuckets[ser.PodName][ser.ContainerName], ser.ControlMetric.RateBuckets)
					}
				} else {
					sumRateBuckets(globalBuckets, ser.ControlMetric.RateBuckets)
					hasBuckets = true
				}
			}
		} else {
			// Scalar Value (Gauge/Counter)
			val := ser.ControlMetric.Value

			// Apply Window
			if ser.Window != nil {
				if v, err := ser.Window.Value(time.Unix(now, 0)); err == nil {
					val = v
				}
			} else if ser.DecayingHistogram != nil {
				percentile := "p95"
				if def.DecayingDistribution != nil && def.DecayingDistribution.Percentile != "" {
					percentile = def.DecayingDistribution.Percentile
				}
				p := parsePercentile(percentile)
				val = ser.DecayingHistogram.Percentile(p, time.Unix(now, 0))
			}

			// The per-container breakdown backs both the PodContainer scope and
			// the per-container average of the Container scope. It holds the raw
			// container value, so it is recorded even when the container declares
			// no request for the resource (e.g. pods sized with pod-level
			// resources only).
			if isContainerBreakdown(def.Scope) && ser.ContainerName != "" {
				if containerSums[ser.PodName] == nil {
					containerSums[ser.PodName] = make(map[string]float64)
				}
				containerSums[ser.PodName][ser.ContainerName] += val
			}

			// When aggregating accross containers into a pod value, resource
			// metrics (e.g. "cpu") are weighted by the container's relative
			// resource request. A container that does not declare a request for
			// the resource has no meaningful weight, so it is left out of the pod
			// value.
			weightedVal := val
			if ser.ResourceName != "" && ser.ContainerName != "" {
				w, ok := requestWeights.weight(workload[ser.PodName], ser.ContainerName, ser.ResourceName)
				if !ok {
					continue
				}
				weightedVal = val * w
			}

			podSums[ser.PodName] += weightedVal
			podFound[ser.PodName] = true
		}
	}

	if defType == "Histogram" {
		if !isGlobalScope(def.Scope) {
			if len(podBuckets) > 0 && percentile != "" {
				for pName, buckets := range podBuckets {
					podSums[pName] = calculatePercentile(buckets, percentile)
				}
				for pName, byContainer := range containerBuckets {
					if containerSums[pName] == nil {
						containerSums[pName] = make(map[string]float64)
					}
					for cName, buckets := range byContainer {
						containerSums[pName][cName] = calculatePercentile(buckets, percentile)
					}
				}
				if def.Scope == ScopeContainer {
					if len(containerSums) == 0 {
						return metricResult{}, false
					}
					return metricResult{container: averageByContainer(containerSums)}, true
				}
				return metricResult{pod: podSums, podContainer: containerSums}, true
			}
			return metricResult{}, false
		}
		if hasBuckets && percentile != "" {
			return metricResult{global: calculatePercentile(globalBuckets, percentile)}, true
		}
	} else if hasGlobal {
		val := globalSum
		if agg == "Avg" {
			val = val / float64(readyReplicas)
		}
		return metricResult{global: val}, true
	} else if len(podFound) > 0 || len(containerSums) > 0 {
		if def.Scope == ScopeContainer {
			if len(containerSums) == 0 {
				return metricResult{}, false
			}
			return metricResult{container: averageByContainer(containerSums)}, true
		}
		// Pods whose containers declare no request for the resource only
		// appear in the per-container breakdown, not in podSums.
		if isPodBreakdown(def.Scope) {
			return metricResult{pod: podSums, podContainer: containerSums}, true
		}
		values := []float64{}
		for pName := range podFound {
			values = append(values, podSums[pName])
		}
		if len(values) > 0 {
			return metricResult{global: aggregate(values, agg)}, true
		}
	}
	return metricResult{}, false
}

// averageByContainer collapses per-pod, per-container values into a single
// value per container name, averaged over the pods that report that container.
// A container missing from a pod does not count against its average.
func averageByContainer(byPod map[string]map[string]float64) map[string]float64 {
	sums := make(map[string]float64)
	counts := make(map[string]int)
	for _, byContainer := range byPod {
		for cName, val := range byContainer {
			sums[cName] += val
			counts[cName]++
		}
	}
	avg := make(map[string]float64, len(sums))
	for cName, sum := range sums {
		avg[cName] = sum / float64(counts[cName])
	}
	return avg
}

// requestWeightCache memoizes the per-container request weights of a pod, keyed
// by pod and resource name, so that the requests are only parsed once per
// calculation cycle.
type requestWeightCache map[string]map[string]float64

func newRequestWeightCache() requestWeightCache {
	return make(requestWeightCache)
}

// weight returns the share of the pod's total request for the given resource
// that belongs to the given container. It reports false if the container does
// not declare a usable request for the resource, in which case the caller
// should drop the sample.
func (c requestWeightCache) weight(pod *pb.PodState, containerName, resourceName string) (float64, bool) {
	key := pod.GetName() + "|" + resourceName
	weights, ok := c[key]
	if !ok {
		weights = containerRequestWeights(pod, resourceName)
		c[key] = weights
	}
	w, ok := weights[containerName]
	return w, ok
}

// containerRequestWeights returns, for every container of the pod declaring a
// request for the given resource, the container's request relative to the sum
// of the requests of all the pod's containers. Containers without a valid
// (parseable, positive) request are absent from the result.
func containerRequestWeights(pod *pb.PodState, resourceName string) map[string]float64 {
	if pod == nil || resourceName == "" {
		return nil
	}

	requests := make(map[string]float64, len(pod.Containers))
	total := 0.0
	for _, c := range pod.Containers {
		raw, ok := c.Requests[resourceName]
		if !ok {
			continue
		}
		q, err := resource.ParseQuantity(raw)
		if err != nil {
			continue
		}
		v := q.AsApproximateFloat64()
		if v <= 0 {
			continue
		}
		requests[c.Name] = v
		total += v
	}

	if total <= 0 {
		return nil
	}
	for name, v := range requests {
		requests[name] = v / total
	}
	return requests
}

func parsePercentile(s string) float64 {
	if len(s) > 0 {
		cleanStr := s
		if len(s) > 1 && (s[0] == 'p' || s[0] == 'P') {
			cleanStr = s[1:]
		}
		if val, err := strconv.ParseFloat(cleanStr, 64); err == nil {
			return val / 100.0
		}
	}
	return 0.95
}

func aggregate(values []float64, method string) float64 {
	if len(values) == 0 {
		return 0
	}
	if method == "Max" {
		max := -math.MaxFloat64
		for _, v := range values {
			if v > max {
				max = v
			}
		}
		return max
	}
	if method == "Min" {
		min := math.MaxFloat64
		for _, v := range values {
			if v < min {
				min = v
			}
		}
		return min
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	if method == "Sum" {
		return sum
	}
	return sum / float64(len(values))
}

func calculateBucketRates(current, last map[string]uint64, duration float64) map[string]float64 {
	if duration <= 0 {
		return nil
	}
	rates := make(map[string]float64)
	for k, v := range current {
		prev := last[k]
		diff := float64(v) - float64(prev)
		if diff < 0 {
			diff = float64(v)
		}
		rates[k] = diff / duration
	}
	return rates
}

func sumRateBuckets(dest, src map[string]float64) {
	for k, v := range src {
		dest[k] += v
	}
}

func calculatePercentile(buckets map[string]float64, percentileStr string) float64 {
	p := 0.90
	if len(percentileStr) > 0 {
		cleanStr := percentileStr
		if len(percentileStr) > 1 && (percentileStr[0] == 'p' || percentileStr[0] == 'P') {
			cleanStr = percentileStr[1:]
		}
		if val, err := strconv.ParseFloat(cleanStr, 64); err == nil {
			p = val / 100.0
		}
	}
	type bucket struct{ le, count float64 }
	var sorted []bucket
	for leStr, count := range buckets {
		var le float64
		if leStr == "+Inf" {
			le = math.Inf(1)
		} else {
			v, err := strconv.ParseFloat(leStr, 64)
			if err != nil {
				continue
			}
			le = v
		}
		sorted = append(sorted, bucket{le, count})
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].le < sorted[j].le })
	var totalCount float64
	if len(sorted) > 0 {
		totalCount = sorted[len(sorted)-1].count
	}
	if totalCount == 0 {
		return 0
	}
	targetRank := totalCount * p
	var prevLe, prevCount float64
	for _, b := range sorted {
		if b.count >= targetRank {
			countDiff := b.count - prevCount
			if countDiff == 0 {
				return b.le
			}
			fraction := (targetRank - prevCount) / countDiff
			bucketWidth := b.le - prevLe
			if math.IsInf(bucketWidth, 1) {
				return prevLe
			}
			return prevLe + (bucketWidth * fraction)
		}
		prevLe, prevCount = b.le, b.count
	}
	return 0
}

func matchFilter(labels, filter map[string]string) bool {
	for k, v := range filter {
		if labels[k] != v {
			return false
		}
	}
	return true
}
