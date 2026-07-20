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
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
	Labels  map[string]string

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
	SetPolicy(clusterName string, policy *pb.Policy) error
	DeletePolicy(id *pb.PolicyId) error
	GetPolicy(id *pb.PolicyId) (*pb.Policy, bool)
	ListPolicies(clusterName string) []*pb.Policy
	UpdateWorkload(req *pb.UpdateWorkloadRequest) error
	GetRecommendation(id *pb.PolicyId) (*pb.GetRecommendationResponse, bool)
	GetControlMetrics(id *pb.PolicyId) (*pb.ControlMetrics, bool)
	CalculateAll()
	Dump() interface{}
}

type PolicyState struct {
	Policy           *pb.Policy
	Workload         map[string]*pb.PodState
	Series           map[string]map[string]*Series // MetricName -> SeriesID -> Series
	GlobalHistograms map[string]*DecayingHistogram // MetricName -> Histogram
	Recommendation   *pb.Recommendation
	LastActive       int64
	Decisions        map[string]*pb.RecommenderStatus
	ControlMetrics   *pb.ControlMetrics
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

func (s *MemoryStore) SetPolicy(clusterName string, policy *pb.Policy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.genPolicyKey(&pb.PolicyId{ClusterName: clusterName, Namespace: policy.Id.Namespace, Name: policy.Id.Name})

	ps, ok := s.state[key]
	if !ok {
		ps = &PolicyState{
			Workload:         make(map[string]*pb.PodState),
			Series:           make(map[string]map[string]*Series),
			GlobalHistograms: make(map[string]*DecayingHistogram),
			Decisions:        make(map[string]*pb.RecommenderStatus),
		}
		s.state[key] = ps
	}
	ps.Policy = policy
	ps.Recommendation = nil
	ps.ControlMetrics = nil

	s.cleanupOrphanedSeries(ps)
	s.cleanupOrphanedHistograms(ps)
	s.cleanupOrphanedDecisions(ps)

	return nil
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
				if err := s.processSample(ps, batch.EntityKey, m, req.Timestamp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ps *PolicyState) FindMetricDefinition(name string) *pb.MetricDefinition {
	if ps.Policy == nil {
		return nil
	}
	for _, d := range ps.Policy.Metrics {
		if d.Name == name {
			return d
		}
	}
	return nil
}

func (s *MemoryStore) processSample(ps *PolicyState, entityKey string, m *pb.MetricSample, ingestTime int64) error {
	def := ps.FindMetricDefinition(m.Name)
	if def == nil {
		return fmt.Errorf("metric %s not defined in policy", m.Name)
	}

	// Filter early
	if !matchFilter(m.Labels, def.Filter) {
		return nil
	}

	if _, ok := ps.Series[def.Name]; !ok {
		ps.Series[def.Name] = make(map[string]*Series)
	}

	labelHash := hashLabels(m.Labels)
	seriesID := fmt.Sprintf("%s|%s", entityKey, labelHash)

	ser, ok := ps.Series[def.Name][seriesID]
	if !ok {
		ser = &Series{
			PodName: entityKey,
			Labels:  m.Labels,
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

		ps.Series[def.Name][seriesID] = ser
	}

	var gh *DecayingHistogram
	if def.DecayingDistribution != nil {
		if def.Scope == "Pod" {
			if ser.DecayingHistogram == nil {
				hl, _ := time.ParseDuration(def.DecayingDistribution.HalfLife)
				ser.DecayingHistogram, _ = NewDecayingHistogram(time.Unix(ingestTime, 0), hl, def.DecayingDistribution.BucketSize)
			}
		} else {
			if ps.GlobalHistograms == nil {
				ps.GlobalHistograms = make(map[string]*DecayingHistogram)
			}
			var ok bool
			gh, ok = ps.GlobalHistograms[def.Name]
			if !ok {
				hl, _ := time.ParseDuration(def.DecayingDistribution.HalfLife)
				gh, _ = NewDecayingHistogram(time.Unix(ingestTime, 0), hl, def.DecayingDistribution.BucketSize)
				ps.GlobalHistograms[def.Name] = gh
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
	if req.Vote.DesiredReplicas < 0 {
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
		DesiredReplicas:   req.Vote.DesiredReplicas,
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

func (s *MemoryStore) GetControlMetrics(id *pb.PolicyId) (*pb.ControlMetrics, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := s.genPolicyKey(id)
	ps, ok := s.state[key]
	if !ok || ps.ControlMetrics == nil {
		return nil, false
	}
	return ps.ControlMetrics, true
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

		currentControlMetrics := make(map[string]float64)
		currentPodMetrics := make(map[string]*pb.PodMetrics)
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

		for _, def := range policy.Metrics {
			val, podVals, ok := s.calculateMetric(ps, def, ps.Series[def.Name], workload, readyReplicas, now, cutoff, gcCutoff)
			if ok {
				if podVals != nil {
					for podName, podVal := range podVals {
						if _, exists := currentPodMetrics[podName]; !exists {
							currentPodMetrics[podName] = &pb.PodMetrics{Values: make(map[string]float64)}
						}
						currentPodMetrics[podName].Values[def.Name] = podVal
					}
				} else {
					currentControlMetrics[def.Name] = val
					if policy.Workload != nil {
						metrics.RecordControlMetric(policy.Id.ClusterName, policy.Id.Namespace, policy.Id.Name, policy.Workload.Group, policy.Workload.Version, policy.Workload.Kind, policy.Workload.Name, def.Name, val)
					}
				}
			}
		}

		ps.ControlMetrics = &pb.ControlMetrics{
			Values:        currentControlMetrics,
			PodMetrics:    currentPodMetrics,
			ReadyReplicas: int32(readyReplicas),
			Timestamp:     now,
		}
		s.processDecisions(ps, now, currentControlMetrics)
	}
}

func (s *MemoryStore) cleanupOrphanedSeries(ps *PolicyState) {
	metricDefs := make(map[string]bool)
	for _, m := range ps.Policy.Metrics {
		metricDefs[m.Name] = true
	}
	for mName := range ps.Series {
		if !metricDefs[mName] {
			delete(ps.Series, mName)
		}
	}
}

func (s *MemoryStore) cleanupOrphanedHistograms(ps *PolicyState) {
	metricDefs := make(map[string]bool)
	for _, m := range ps.Policy.Metrics {
		metricDefs[m.Name] = true
	}
	for mName := range ps.GlobalHistograms {
		if !metricDefs[mName] {
			delete(ps.GlobalHistograms, mName)
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

func (s *MemoryStore) calculateMetric(ps *PolicyState, def *pb.MetricDefinition, seriesMap map[string]*Series, workload map[string]*pb.PodState, readyReplicas int, now, cutoff, gcCutoff int64) (float64, map[string]float64, bool) {
	if def.Scope != "Pod" {
		if gh, ok := ps.GlobalHistograms[def.Name]; ok {
			percentile := "p95"
			if def.DecayingDistribution != nil {
				percentile = def.DecayingDistribution.Percentile
			}
			p := parsePercentile(percentile)
			return gh.Percentile(p, time.Unix(now, 0)), nil, true
		}
	}

	if seriesMap == nil {
		return 0, nil, false
	}

	globalSum := 0.0
	hasGlobal := false
	globalBuckets := make(map[string]float64)
	podBuckets := make(map[string]map[string]float64)
	hasBuckets := false
	podSums := make(map[string]float64)
	podFound := make(map[string]bool)

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
				if def.Scope == "Pod" {
					if podBuckets[ser.PodName] == nil {
						podBuckets[ser.PodName] = make(map[string]float64)
					}
					sumRateBuckets(podBuckets[ser.PodName], ser.ControlMetric.Buckets)
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

			podSums[ser.PodName] += val
			podFound[ser.PodName] = true
		}
	}

	if defType == "Histogram" {
		if def.Scope == "Pod" {
			if len(podBuckets) > 0 && percentile != "" {
				for pName, buckets := range podBuckets {
					podSums[pName] = calculatePercentile(buckets, percentile)
				}
				return 0, podSums, true
			}
			return 0, nil, false
		}
		if hasBuckets && percentile != "" {
			return calculatePercentile(globalBuckets, percentile), nil, true
		}
	} else if hasGlobal {
		val := globalSum
		if agg == "Avg" {
			val = val / float64(readyReplicas)
		}
		return val, nil, true
	} else if len(podFound) > 0 {
		values := []float64{}
		for pName := range podFound {
			values = append(values, podSums[pName])
		}
		if len(values) > 0 {
			if def.Scope == "Pod" {
				return 0, podSums, true
			}
			return aggregate(values, agg), nil, true
		}
	}
	return 0, nil, false
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

func (s *MemoryStore) processDecisions(ps *PolicyState, now int64, controlMetrics map[string]float64) {
	policy := ps.Policy
	isActive := false
	if len(policy.Activation) == 0 {
		isActive = true
	} else {
		for _, recDef := range policy.Activation {
			if d, ok := ps.Decisions[recDef.Name]; ok {
				if recDef.Mode == "DryRun" {
					continue
				}
				if d.IsActive {
					isActive = true
					break
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

	maxReplicas, scalingStatuses := s.calculateTargetReplicas(ps, isActive)

	if policy.MaxReplicas > 0 && maxReplicas > policy.MaxReplicas {
		maxReplicas = policy.MaxReplicas
	}

	var activationStatuses []*pb.RecommenderStatus
	for _, recDef := range policy.Activation {
		d, ok := ps.Decisions[recDef.Name]
		if !ok {
			continue
		}
		activationStatuses = append(activationStatuses, d)
	}

	// Arbitration: If we are active but have no scaling recommenders, we have no recommendation.
	// We only actuate if:
	// 1. We have at least one scaling recommender decision.
	// 2. OR, we are explicitly scaling to 0 (isActive = false).
	if isActive && len(scalingStatuses) == 0 {
		ps.Recommendation = nil
	} else {
		explanation := append(scalingStatuses, activationStatuses...)
		ps.Recommendation = &pb.Recommendation{
			TargetReplicas: maxReplicas,
			Explanation:    explanation,
		}
	}

	if policy.Workload != nil {
		metrics.RecordRecommendation(policy.Id.ClusterName, policy.Id.Namespace, policy.Id.Name, policy.Workload.Group, policy.Workload.Version, policy.Workload.Kind, policy.Workload.Name, maxReplicas)
		metrics.RecordActive(policy.Id.ClusterName, policy.Id.Namespace, policy.Id.Name, policy.Workload.Group, policy.Workload.Version, policy.Workload.Kind, policy.Workload.Name, isActive)
	}
}

func (s *MemoryStore) calculateTargetReplicas(ps *PolicyState, isActive bool) (int32, []*pb.RecommenderStatus) {
	maxReplicas := int32(0)
	var decisionStatuses []*pb.RecommenderStatus

	if isActive {
		for _, recDef := range ps.Policy.Scaling {
			d, ok := ps.Decisions[recDef.Name]
			if !ok {
				continue
			}

			if recDef.Mode != "DryRun" && d.IsActive && d.DesiredReplicas > maxReplicas {
				maxReplicas = d.DesiredReplicas
			}

			decisionStatuses = append(decisionStatuses, d)
		}

		if maxReplicas < ps.Policy.MinReplicas {
			maxReplicas = ps.Policy.MinReplicas
		}
	} else {
		maxReplicas = 0
	}
	return maxReplicas, decisionStatuses
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
