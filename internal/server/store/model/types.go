package model

import (
	"fmt"
	"hash/fnv"
	"sort"
	"time"
)

// Metric scopes define the granularity at which a metric is reported.
const (
	// ScopeGlobal aggregates every series into a single policy-wide value,
	// reported in ControlMetrics.Values. This is the default.
	ScopeGlobal = "Global"
	// ScopePod reports one value per pod in ControlMetrics.PodMetrics[pod].Values.
	// Samples reported by individual containers are summed into their pod's value.
	ScopePod = "Pod"
	// ScopePodContainer reports one value per pod and container in
	// ControlMetrics.PodContainerMetrics[pod].ContainerMetrics[container].
	ScopePodContainer = "PodContainer"
	// ScopeContainer reports one value per container name in
	// ControlMetrics.ContainerMetrics.ContainerMetrics[container], averaged over
	// every pod that reports that container.
	ScopeContainer = "Container"
)

// Sample represents the raw data from the source
type Sample struct {
	// Source Timestamp.
	Timestamp int64
	// If the sample is a scalar, its value.
	Value float64
	// If the sample is a distribution, its cumulative buckets: the counts
	// reported by the source, or values derived from them (e.g. their rates).
	Distribution *Distribution
	// If the sample is a decaying distribution, its time-decaying histogram.
	DecayingHistogram *DecayingHistogram
}

// TimeWindow aggregates the samples of a single series over time.
type TimeWindow interface {
	// Apply returns the aggregate, reporting false when there is nothing to
	// report (e.g. no sample in the window).
	Apply() (Sample, bool)
	AddSample(sample Sample, now time.Time)
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

	// LastRaw is the most recent sample ingested for the series. Its timestamp
	// is used to garbage collect series that stopped reporting.
	LastRaw Sample
	// TimeWindow is the temportal aggregation for the series.
	TimeWindow TimeWindow `json:"-"`
}

// NewSeries returns a series reported by the given container of the given pod
// with the given labels, whose samples are aggregated over time by window.
func NewSeries(podName, containerName, resourceName string, labels map[string]string, window TimeWindow) *Series {
	return &Series{
		PodName:       podName,
		ContainerName: containerName,
		ResourceName:  resourceName,
		Labels:        labels,
		TimeWindow:    window,
	}
}

// AddSample feeds sample, ingested at time now, to the time window of
// the series and records it as the latest raw sample.
func (s *Series) AddSample(sample Sample, now time.Time) {
	s.TimeWindow.AddSample(sample, now)
	s.LastRaw = sample
}

// SeriesID identifies a timeseries within a metric. It's composed of the pod,
// container and labels of the series.
type SeriesID struct {
	// pod is the name of the pod reporting the series, or empty for series
	// not attributed to a pod.
	pod string
	// container is the name of the container reporting the series, or empty
	// for series not attributed to a container.
	container string
	// labelHash is the hash of the series labels.
	labelHash uint64
}

// NewSeriesID returns the ID of the series reported by the given container of
// the given pod with the given labels.
func NewSeriesID(pod, container string, labels map[string]string) SeriesID {
	return SeriesID{pod: pod, container: container, labelHash: hashLabels(labels)}
}

// MarshalText implements encoding.TextMarshaler for JSON encoding.
func (id SeriesID) MarshalText() ([]byte, error) {
	return fmt.Appendf(nil, "%s|%s|%016x", id.pod, id.container, id.labelHash), nil
}

// hashLabels returns the 64-bit FNV-1a hash of labels. The hash does not depend
// on the iteration order of the map. Empty label sets hash to 0.
func hashLabels(labels map[string]string) uint64 {
	if len(labels) == 0 {
		return 0
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, k := range keys {
		// Separate keys and values with a byte that cannot appear in label
		// names, so that e.g. {"a": "bc"} and {"ab": "c"} hash differently.
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(labels[k]))
		h.Write([]byte{0})
	}
	return h.Sum64()
}

// MatchFilter reports whether labels carry every label in filter. An empty
// filter matches every label set.
func MatchFilter(labels, filter map[string]string) bool {
	for k, v := range filter {
		if labels[k] != v {
			return false
		}
	}
	return true
}
