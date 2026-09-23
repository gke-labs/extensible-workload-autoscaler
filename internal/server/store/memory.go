package store

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/metrics"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
)

// isPodScoped reports whether the metric is tracked per pod instead of being
// aggregated into a single policy-wide value. Both ScopePod and
// ScopePodContainer keep per-pod state; they differ only in whether the
// per-container breakdown is reported alongside it. Any unrecognized scope
// (including the empty string) is treated as ScopeGlobal.
func isPodScoped(scope string) bool {
	return scope == ScopePod || scope == ScopePodContainer
}

// DataPoint represents a single calculated value (ControlMetric)
type DataPoint struct {
	Timestamp int64 // Freshness Timestamp (Ingest Time)
	Value     float64
	Labels    map[string]string
	Buckets   map[string]float64 // Rate buckets
}

// Sample represents the raw data from the source
type Sample struct {
	Timestamp         int64 // Source Timestamp
	Value             float64
	CumulativeBuckets map[string]uint64
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

type MetricStore interface {
	AddBatch(req *pb.IngestMetricsRequest) error
	UpdateRecommenderState(req *pb.UpdateRecommenderStateRequest) error
	// SetPolicy stores p as the full definition of the policy, replacing any
	// previous one.
	SetPolicy(clusterName string, p *pb.Policy) error
	// UpdatePolicy applies p to the stored policy, restricted to the fields
	// selected by updateMask, and returns the stored result. An empty mask
	// replaces the whole policy. See policy.ApplyUpdateMask for the supported
	// mask paths.
	UpdatePolicy(clusterName string, p *pb.Policy, updateMask *fieldmaskpb.FieldMask) (*pb.Policy, error)
	DeletePolicy(id *pb.PolicyId) error
	GetPolicy(id *pb.PolicyId) (*pb.Policy, bool)
	ListPolicies(clusterName string) []*pb.Policy
	UpdateWorkload(req *pb.UpdateWorkloadRequest) error
	GetRecommendation(id *pb.PolicyId) (*pb.GetRecommendationResponse, bool)
	// GetControlMetrics returns the aggregated metrics of a policy. An empty
	// recommenderName returns the policy-wide metrics, otherwise only the
	// metrics owned by that recommender are reported.
	GetControlMetrics(id *pb.PolicyId, recommenderName string) (*pb.ControlMetrics, bool)
	CalculateAll()
	Dump() interface{}
}

type PolicyState struct {
	Policy   *pb.Policy
	Workload map[string]*pb.PodState
	// Series and GlobalHistograms are keyed by the metric key returned by
	// metricKey, which identifies a metric by name and owner.
	Series           map[string]map[string]*Series // MetricKey -> SeriesID -> Series
	GlobalHistograms map[string]*DecayingHistogram // MetricKey -> Histogram
	Recommendation   *pb.Recommendation
	LastActive       int64
	Decisions        map[string]*pb.RecommenderStatus
	ControlMetrics   *pb.ControlMetrics
	// RecommenderControlMetrics holds the values of the metrics owned by each
	// recommender, keyed by recommender name.
	RecommenderControlMetrics map[string]*pb.ControlMetrics
}

// metricKey returns the key under which the state of a metric is tracked. A
// metric is identified by the <name, recommender_name> pair: its name is only
// unique within its owner. Policy-wide metrics have no owner and keep their
// bare name as key.
func metricKey(def *pb.MetricDefinition) string {
	if def.GetRecommenderName() == "" {
		return def.GetName()
	}
	return def.GetRecommenderName() + "/" + def.GetName()
}

type MemoryStore struct {
	mu    sync.RWMutex
	clock clock.Clock

	// Storage: PolicyKey -> PolicyState
	state map[string]*PolicyState
}

func NewMemoryStore() *MemoryStore {
	return NewMemoryStoreWithClock(clock.RealClock{})
}

func NewMemoryStoreWithClock(c clock.Clock) *MemoryStore {
	return &MemoryStore{
		clock: c,
		state: make(map[string]*PolicyState),
	}
}

func (s *MemoryStore) genPolicyKey(id *pb.PolicyId) string {
	return id.ClusterName + "/" + id.Namespace + "/" + id.Name
}

func (s *MemoryStore) SetPolicy(clusterName string, p *pb.Policy) error {
	_, err := s.UpdatePolicy(clusterName, p, nil)
	return err
}

func (s *MemoryStore) UpdatePolicy(clusterName string, p *pb.Policy, updateMask *fieldmaskpb.FieldMask) (*pb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.genPolicyKey(&pb.PolicyId{ClusterName: clusterName, Namespace: p.Id.Namespace, Name: p.Id.Name})

	ps := s.state[key]
	var current *pb.Policy
	if ps != nil {
		current = ps.Policy
	}
	// Merge before touching the state: an invalid mask must leave the store as is.
	updated, err := policy.ApplyUpdateMask(current, p, updateMask)
	if err != nil {
		return nil, err
	}

	if ps == nil {
		ps = &PolicyState{
			Workload:         make(map[string]*pb.PodState),
			Series:           make(map[string]map[string]*Series),
			GlobalHistograms: make(map[string]*DecayingHistogram),
			Decisions:        make(map[string]*pb.RecommenderStatus),
		}
		s.state[key] = ps
	}
	ps.Policy = updated
	ps.Recommendation = nil
	ps.ControlMetrics = nil
	ps.RecommenderControlMetrics = nil

	s.cleanupOrphanedSeries(ps)
	s.cleanupOrphanedHistograms(ps)
	s.cleanupOrphanedDecisions(ps)

	return updated, nil
}

func (s *MemoryStore) DeletePolicy(id *pb.PolicyId) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.genPolicyKey(id)
	delete(s.state, key)
	return nil
}

func (s *MemoryStore) GetPolicy(id *pb.PolicyId) (*pb.Policy, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := s.genPolicyKey(id)
	ps, ok := s.state[key]
	if !ok || ps.Policy == nil {
		return nil, false
	}
	return ps.Policy, true
}

func (s *MemoryStore) ListPolicies(clusterName string) []*pb.Policy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var policies []*pb.Policy
	for key, ps := range s.state {
		if ps.Policy == nil {
			continue
		}
		// Key format: cluster/ns/name
		parts := strings.Split(key, "/")
		if len(parts) >= 3 {
			if clusterName != "" && parts[0] != clusterName {
				continue
			}
		}
		policies = append(policies, ps.Policy)
	}
	return policies
}

func (s *MemoryStore) UpdateWorkload(req *pb.UpdateWorkloadRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.genPolicyKey(req.Id)
	ps, ok := s.state[key]
	if !ok || ps.Policy == nil {
		return fmt.Errorf("policy not found")
	}

	newWorkload := make(map[string]*pb.PodState)
	for _, p := range req.Workload.Pods {
		newWorkload[p.Name] = p
	}
	ps.Workload = newWorkload
	return nil
}

func (s *MemoryStore) AddBatch(req *pb.IngestMetricsRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, pBatch := range req.Policies {
		key := s.genPolicyKey(&pb.PolicyId{ClusterName: req.ClusterName, Namespace: pBatch.Namespace, Name: pBatch.Name})
		ps, ok := s.state[key]
		if !ok || ps.Policy == nil {
			return fmt.Errorf("policy not found: %s", key)
		}

		for _, batch := range pBatch.Batches {
			for _, m := range batch.Samples {
				if err := s.processSample(ps, batch.PodName, batch.ContainerName, m, req.Timestamp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// FindMetricDefinition returns the definition of the metric identified by the
// <name, owner> pair. An empty owner looks the metric up among the policy-wide
// metrics, otherwise among the metrics that recommender owns.
func (ps *PolicyState) FindMetricDefinition(owner, name string) *pb.MetricDefinition {
	if ps.Policy == nil {
		return nil
	}
	defs := ps.Policy.Metrics
	if owner != "" {
		defs = ps.Policy.RecommenderMetrics[owner].GetDefinitions()
	}
	for _, d := range defs {
		if d.Name == name {
			return d
		}
	}
	return nil
}

func (s *MemoryStore) processSample(ps *PolicyState, podName, containerName string, m *pb.MetricSample, ingestTime int64) error {
	owner := m.GetRecommenderName()
	def := ps.FindMetricDefinition(owner, m.Name)
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

	key := metricKey(def)
	if _, ok := ps.Series[key]; !ok {
		ps.Series[key] = make(map[string]*Series)
	}

	labelHash := hashLabels(m.Labels)
	// The container name is part of the series identity so that samples coming
	// from different containers of the same pod are tracked independently.
	seriesID := fmt.Sprintf("%s|%s|%s", podName, containerName, labelHash)

	ser, ok := ps.Series[key][seriesID]
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

		ps.Series[key][seriesID] = ser
	}

	var gh *DecayingHistogram
	if def.DecayingDistribution != nil {
		if isPodScoped(def.Scope) {
			if ser.DecayingHistogram == nil {
				hl, _ := time.ParseDuration(def.DecayingDistribution.HalfLife)
				ser.DecayingHistogram, _ = NewDecayingHistogram(time.Unix(ingestTime, 0), hl, def.DecayingDistribution.BucketSize)
			}
		} else {
			if ps.GlobalHistograms == nil {
				ps.GlobalHistograms = make(map[string]*DecayingHistogram)
			}
			var ok bool
			gh, ok = ps.GlobalHistograms[key]
			if !ok {
				hl, _ := time.ParseDuration(def.DecayingDistribution.HalfLife)
				gh, _ = NewDecayingHistogram(time.Unix(ingestTime, 0), hl, def.DecayingDistribution.BucketSize)
				ps.GlobalHistograms[key] = gh
			}
		}
	}

	s.updateSeries(ser, def, m, ingestTime, gh)
	return nil
}

func (s *MemoryStore) updateSeries(ser *Series, def *pb.MetricDefinition, m *pb.MetricSample, ingestTime int64, gh *DecayingHistogram) {
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
		if m.HistogramBuckets == nil {
			return
		}

		if ser.LastRaw.Timestamp == 0 {
			ser.LastRaw = Sample{Timestamp: ts, CumulativeBuckets: m.HistogramBuckets}
			ser.ControlMetric = DataPoint{Timestamp: ingestTime, Labels: m.Labels}
		} else if ts > ser.LastRaw.Timestamp {
			dt := float64(ts - ser.LastRaw.Timestamp)
			rateBuckets := calculateBucketRates(m.HistogramBuckets, ser.LastRaw.CumulativeBuckets, dt)
			ser.ControlMetric = DataPoint{Timestamp: ingestTime, Value: 0, Labels: m.Labels, Buckets: rateBuckets}
			ser.LastRaw = Sample{Timestamp: ts, Value: 0, CumulativeBuckets: m.HistogramBuckets}
		} else if ts == ser.LastRaw.Timestamp {
			ser.ControlMetric.Timestamp = ingestTime
			ser.LastRaw.Value = m.Value
			ser.LastRaw.CumulativeBuckets = m.HistogramBuckets
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
			ser.LastRaw.CumulativeBuckets = m.HistogramBuckets
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

func (s *MemoryStore) UpdateRecommenderState(req *pb.UpdateRecommenderStateRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.genPolicyKey(req.Id)
	ps, ok := s.state[key]
	if !ok || ps.Policy == nil {
		return fmt.Errorf("policy not found")
	}

	if req.Vote == nil {
		delete(ps.Decisions, req.RecommenderName)
		return nil
	}

	// Validate that the recommender exists in the current policy
	var def *pb.RecommenderDefinition
	isActivation := false
	for _, r := range ps.Policy.Scaling {
		if r.Name == req.RecommenderName {
			def = r
			break
		}
	}
	if def == nil {
		for _, r := range ps.Policy.Activation {
			if r.Name == req.RecommenderName {
				def = r
				isActivation = true
				break
			}
		}
	}
	if def == nil {
		return fmt.Errorf("recommender %s not defined in policy", req.RecommenderName)
	}

	// Basic validation of vote
	if req.Vote.Replicas != nil && *req.Vote.Replicas < 0 {
		return fmt.Errorf("desired replicas cannot be negative")
	}

	phase := "Scaling"
	if isActivation {
		phase = "Activation"
	}

	// Create enriched status
	status := &pb.RecommenderStatus{
		Name:              req.RecommenderName,
		Type:              def.Type,
		Phase:             phase,
		Mode:              def.Mode,
		Replicas:          req.Vote.Replicas,
		IsActive:          req.Vote.IsActive,
		Message:           req.Vote.Message,
		LastUpdated:       timestamppb.New(s.clock.Now()),
		WorkloadResources: req.Vote.WorkloadResources,
		PodResources:      req.Vote.PodResources,
	}

	// Wait, I need to check how to correctly create google.protobuf.Timestamp
	// I'll check imports and existing usage.
	return s.updateDecision(ps, req.RecommenderName, status)
}

func (s *MemoryStore) updateDecision(ps *PolicyState, name string, status *pb.RecommenderStatus) error {
	if ps.Decisions == nil {
		ps.Decisions = make(map[string]*pb.RecommenderStatus)
	}
	ps.Decisions[name] = status
	return nil
}

func (s *MemoryStore) GetRecommendation(id *pb.PolicyId) (*pb.GetRecommendationResponse, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := s.genPolicyKey(id)
	ps, ok := s.state[key]
	if !ok || ps.Policy == nil {
		return nil, false
	}

	var metricStatuses []*pb.MetricStatus
	if ps.ControlMetrics != nil {
		for _, def := range ps.Policy.Metrics {
			status := &pb.MetricStatus{
				Name:      def.Name,
				Timestamp: ps.ControlMetrics.Timestamp,
			}
			if val, ok := ps.ControlMetrics.Values[def.Name]; ok {
				status.Value = val
			} else {
				status.Error = "No data available"
			}
			metricStatuses = append(metricStatuses, status)
		}
	}

	return &pb.GetRecommendationResponse{
		Recommendation: ps.Recommendation,
		MetricStatuses: metricStatuses,
	}, true
}

// GetControlMetrics returns the aggregated metrics of a policy. An empty
// recommenderName reports the policy-wide metrics, otherwise only the metrics
// owned by that recommender are reported. Workload-level information (ready
// replicas, timestamp) is reported in both cases, so a recommender that owns no
// metric still observes the state of the workload.
func (s *MemoryStore) GetControlMetrics(id *pb.PolicyId, recommenderName string) (*pb.ControlMetrics, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := s.genPolicyKey(id)
	ps, ok := s.state[key]
	if !ok || ps.ControlMetrics == nil {
		return nil, false
	}
	if recommenderName == "" {
		return ps.ControlMetrics, true
	}
	if cm, ok := ps.RecommenderControlMetrics[recommenderName]; ok {
		return cm, true
	}
	// The recommender owns no metric: report the workload state only.
	return &pb.ControlMetrics{
		Values:              make(map[string]float64),
		PodMetrics:          make(map[string]*pb.MetricValues),
		PodContainerMetrics: make(map[string]*pb.ContainerMetrics),
		ReadyReplicas:       ps.ControlMetrics.ReadyReplicas,
		Timestamp:           ps.ControlMetrics.Timestamp,
	}, true
}

func (s *MemoryStore) CalculateAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now().Unix()
	cutoff := now - 60
	gcCutoff := now - 600

	for _, ps := range s.state {
		policy := ps.Policy
		if policy == nil {
			continue
		}

		workload := ps.Workload
		readyReplicas := 0
		for _, p := range workload {
			if p.IsReady {
				readyReplicas++
			}
		}
		if readyReplicas == 0 {
			readyReplicas = 1
		}

		ps.ControlMetrics = s.calculateControlMetrics(ps, policy.Metrics, workload, readyReplicas, now, cutoff, gcCutoff)

		// Metrics owned by a recommender are aggregated the same way, but kept
		// in a snapshot of their own so that they are only reported to their
		// owner.
		ps.RecommenderControlMetrics = nil
		if len(policy.RecommenderMetrics) > 0 {
			ps.RecommenderControlMetrics = make(map[string]*pb.ControlMetrics, len(policy.RecommenderMetrics))
			for name, defs := range policy.RecommenderMetrics {
				ps.RecommenderControlMetrics[name] = s.calculateControlMetrics(ps, defs.GetDefinitions(), workload, readyReplicas, now, cutoff, gcCutoff)
			}
		}

		s.processDecisions(ps, now)
	}
}

// calculateControlMetrics aggregates the given metric definitions into a single
// snapshot. All the definitions are expected to share the same owner.
func (s *MemoryStore) calculateControlMetrics(ps *PolicyState, defs []*pb.MetricDefinition, workload map[string]*pb.PodState, readyReplicas int, now, cutoff, gcCutoff int64) *pb.ControlMetrics {
	policy := ps.Policy
	currentControlMetrics := make(map[string]float64)
	currentPodMetrics := make(map[string]*pb.MetricValues)
	currentPodContainerMetrics := make(map[string]*pb.ContainerMetrics)

	for _, def := range defs {
		key := metricKey(def)
		val, podVals, containerVals, ok := s.calculateMetric(ps, key, def, ps.Series[key], workload, readyReplicas, now, cutoff, gcCutoff)
		if ok {
			if podVals != nil {
				for podName, podVal := range podVals {
					podMetrics(currentPodMetrics, podName).Values[def.Name] = podVal
				}
				for podName, byContainer := range containerVals {
					for containerName, containerVal := range byContainer {
						containerMetrics(currentPodContainerMetrics, podName, containerName).Values[def.Name] = containerVal
					}
				}
			} else {
				currentControlMetrics[def.Name] = val
				if policy.Workload != nil {
					// Metrics owned by a recommender are exported under their
					// metric key, as their name is only unique within their
					// owner.
					metrics.RecordControlMetric(policy.Id.ClusterName, policy.Id.Namespace, policy.Id.Name, policy.Workload.Group, policy.Workload.Version, policy.Workload.Kind, policy.Workload.Name, key, val)
				}
			}
		}
	}

	return &pb.ControlMetrics{
		Values:              currentControlMetrics,
		PodMetrics:          currentPodMetrics,
		PodContainerMetrics: currentPodContainerMetrics,
		ReadyReplicas:       int32(readyReplicas),
		Timestamp:           now,
	}
}

// podMetrics returns the MetricValues entry for podName, creating it (and its
// nested map) if it does not exist yet.
func podMetrics(all map[string]*pb.MetricValues, podName string) *pb.MetricValues {
	pm, ok := all[podName]
	if !ok {
		pm = &pb.MetricValues{Values: make(map[string]float64)}
		all[podName] = pm
	}
	return pm
}

// containerMetrics returns the MetricValues entry for containerName within
// podName, creating the intermediate entries if they do not exist yet.
func containerMetrics(all map[string]*pb.ContainerMetrics, podName, containerName string) *pb.MetricValues {
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

// definedMetricKeys returns the keys of all the metrics a policy defines,
// including the ones owned by its recommenders.
func definedMetricKeys(policy *pb.Policy) map[string]bool {
	keys := make(map[string]bool)
	for _, m := range policy.Metrics {
		keys[metricKey(m)] = true
	}
	for _, defs := range policy.RecommenderMetrics {
		for _, m := range defs.GetDefinitions() {
			keys[metricKey(m)] = true
		}
	}
	return keys
}

func (s *MemoryStore) cleanupOrphanedSeries(ps *PolicyState) {
	metricDefs := definedMetricKeys(ps.Policy)
	for key := range ps.Series {
		if !metricDefs[key] {
			delete(ps.Series, key)
		}
	}
}

func (s *MemoryStore) cleanupOrphanedHistograms(ps *PolicyState) {
	metricDefs := definedMetricKeys(ps.Policy)
	for key := range ps.GlobalHistograms {
		if !metricDefs[key] {
			delete(ps.GlobalHistograms, key)
		}
	}
}

func (s *MemoryStore) cleanupOrphanedDecisions(ps *PolicyState) {
	recommenderNames := make(map[string]bool)
	for _, r := range ps.Policy.Scaling {
		recommenderNames[r.Name] = true
	}
	for _, r := range ps.Policy.Activation {
		recommenderNames[r.Name] = true
	}
	for rName := range ps.Decisions {
		if !recommenderNames[rName] {
			delete(ps.Decisions, rName)
		}
	}
}

// calculateMetric aggregates the series of a single metric definition.
// It returns, in order: the policy-wide (Global) value, the per-pod values, the
// per-pod/per-container values, and whether any value could be computed.
//
// key is the key the metric state is tracked under (see metricKey).
//
// The returned values depend on def.Scope:
//   - "Global" (default): only the policy-wide value is returned.
//   - "Pod": only per-pod values are returned. Samples reported by individual
//     containers are summed into their pod's value.
//   - "PodContainer": per-pod values are returned along with the per-container
//     breakdown that rolls up into them.
func (s *MemoryStore) calculateMetric(ps *PolicyState, key string, def *pb.MetricDefinition, seriesMap map[string]*Series, workload map[string]*pb.PodState, readyReplicas int, now, cutoff, gcCutoff int64) (float64, map[string]float64, map[string]map[string]float64, bool) {
	if !isPodScoped(def.Scope) {
		if gh, ok := ps.GlobalHistograms[key]; ok {
			percentile := "p95"
			if def.DecayingDistribution != nil {
				percentile = def.DecayingDistribution.Percentile
			}
			p := parsePercentile(percentile)
			return gh.Percentile(p, time.Unix(now, 0)), nil, nil, true
		}
	}

	if seriesMap == nil {
		return 0, nil, nil, false
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

	for id, ser := range seriesMap {
		// GC
		if ser.ControlMetric.Timestamp < gcCutoff {
			delete(seriesMap, id)
			continue
		}

		// Freshness
		if ser.ControlMetric.Timestamp < cutoff || !matchFilter(ser.Labels, def.Filter) {
			continue
		}

		if ser.PodName == "" {
			if defType == "Histogram" {
				if ser.ControlMetric.Buckets != nil {
					sumRateBuckets(globalBuckets, ser.ControlMetric.Buckets)
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
			if ser.ControlMetric.Buckets != nil {
				if isPodScoped(def.Scope) {
					if podBuckets[ser.PodName] == nil {
						podBuckets[ser.PodName] = make(map[string]float64)
					}
					sumRateBuckets(podBuckets[ser.PodName], ser.ControlMetric.Buckets)

					// The per-container breakdown is only reported for PodContainer scope.
					if def.Scope == ScopePodContainer && ser.ContainerName != "" {
						if containerBuckets[ser.PodName] == nil {
							containerBuckets[ser.PodName] = make(map[string]map[string]float64)
						}
						if containerBuckets[ser.PodName][ser.ContainerName] == nil {
							containerBuckets[ser.PodName][ser.ContainerName] = make(map[string]float64)
						}
						sumRateBuckets(containerBuckets[ser.PodName][ser.ContainerName], ser.ControlMetric.Buckets)
					}
				} else {
					sumRateBuckets(globalBuckets, ser.ControlMetric.Buckets)
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

			// When aggregating accross containers, resource metrics (e.g. "cpu") are weighted by the
			// container's relative resource request. A container that does not declare a request
			// for the resource has no meaningful weight, so its sample is dropped.
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

			// The per-container breakdown is only reported for PodContainer scope.
			// Pod-scoped metrics keep the summed pod value only.
			if def.Scope == ScopePodContainer && ser.ContainerName != "" {
				if containerSums[ser.PodName] == nil {
					containerSums[ser.PodName] = make(map[string]float64)
				}
				containerSums[ser.PodName][ser.ContainerName] += val
			}
		}
	}

	if defType == "Histogram" {
		if isPodScoped(def.Scope) {
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
				return 0, podSums, containerSums, true
			}
			return 0, nil, nil, false
		}
		if hasBuckets && percentile != "" {
			return calculatePercentile(globalBuckets, percentile), nil, nil, true
		}
	} else if hasGlobal {
		val := globalSum
		if agg == "Avg" {
			val = val / float64(readyReplicas)
		}
		return val, nil, nil, true
	} else if len(podFound) > 0 {
		values := []float64{}
		for pName := range podFound {
			values = append(values, podSums[pName])
		}
		if len(values) > 0 {
			if isPodScoped(def.Scope) {
				return 0, podSums, containerSums, true
			}
			return aggregate(values, agg), nil, nil, true
		}
	}
	return 0, nil, nil, false
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

func (s *MemoryStore) processDecisions(ps *PolicyState, now int64) {
	policy := ps.Policy
	isActive := false
	var activationStatuses []*pb.RecommenderStatus

	if len(policy.Activation) == 0 {
		isActive = true
	} else {
		for _, recDef := range policy.Activation {
			if d, ok := ps.Decisions[recDef.Name]; ok {
				activationStatuses = append(activationStatuses, d)
				if recDef.Mode == "DryRun" {
					continue
				}
				if d.IsActive {
					isActive = true
				}
			}
		}
	}

	if isActive {
		ps.LastActive = now
	}

	window := int64(300)
	for _, recDef := range policy.Activation {
		if val, ok := recDef.Params["window"]; ok {
			if w, err := strconv.ParseInt(val, 10, 64); err == nil {
				window = w
			}
		}
	}

	if !isActive {
		last := ps.LastActive
		if now-last <= window {
			isActive = true
		}
	}

	// Returns the arbitrated number of replicas and vertical recommendations status.
	// hasRecommendation is true if activators and recommenders have decided on a
	// number of replicas: workload is inactive (0 replicas), or active and a
	// recommender has decided on a positive number of replicas.
	replicas, scalingStatuses := s.calculateTargetReplicas(ps, isActive)

	if replicas == nil && scalingStatuses == nil {
		ps.Recommendation = nil
		return
	}

	explanation := append(activationStatuses, scalingStatuses...)
	ps.Recommendation = &pb.Recommendation{
		TargetReplicas: replicas,
		Explanation:    explanation,
	}

	if policy.Workload != nil {
		if replicas != nil {
			metrics.RecordRecommendation(policy.Id.ClusterName, policy.Id.Namespace, policy.Id.Name, policy.Workload.Group, policy.Workload.Version, policy.Workload.Kind, policy.Workload.Name, *replicas)
		}
		metrics.RecordActive(policy.Id.ClusterName, policy.Id.Namespace, policy.Id.Name, policy.Workload.Group, policy.Workload.Version, policy.Workload.Kind, policy.Workload.Name, isActive)
	}
}

func (s *MemoryStore) calculateTargetReplicas(ps *PolicyState, isActive bool) (*int32, []*pb.RecommenderStatus) {
	if !isActive {
		return new(int32), nil
	}

	var targetReplicas *int32
	var decisionStatuses []*pb.RecommenderStatus

	for _, recDef := range ps.Policy.Scaling {
		d, ok := ps.Decisions[recDef.Name]
		if !ok {
			continue
		}

		// If multiple (non-dry-run) recommendations exist, take the highest.
		if recDef.Mode != "DryRun" && d.IsActive && d.Replicas != nil && (targetReplicas == nil || *d.Replicas >= *targetReplicas) {
			r := *d.Replicas
			targetReplicas = &r
		}

		decisionStatuses = append(decisionStatuses, d)
	}

	if targetReplicas == nil {
		return nil, decisionStatuses
	}

	if ps.Policy.MaxReplicas > 0 {
		*targetReplicas = min(*targetReplicas, ps.Policy.MaxReplicas)
	}
	*targetReplicas = max(*targetReplicas, ps.Policy.MinReplicas)
	return targetReplicas, decisionStatuses
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

func (s *MemoryStore) Dump() interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func matchFilter(labels, filter map[string]string) bool {
	for k, v := range filter {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func hashLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s,", k, labels[k])
	}
	return b.String()
}
