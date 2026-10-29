package store

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/resource"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/metrics"
)

var (
	ErrStaleEtag           = errors.New("etag does not match the stored policy")
	ErrUnknownEtag         = errors.New("etag given for unknown policy")
	ErrPolicyAlreadyExists = errors.New("policy being created already exists")
	ErrUnknownPolicy       = errors.New("policy not found")
)

type ServerStore interface {
	AddBatch(req *pb.IngestMetricsRequest) error
	UpdateRecommenderState(req *pb.UpdateRecommenderStateRequest) error
	CreatePolicy(clusterName string, p *pb.Policy) (*pb.Policy, error)
	// UpdatePolicy replaces the stored policy with p and returns the stored
	// result with a new ETag. If the policy exists, a non-empty p.Etag must
	// match the stored one (ErrStaleEtag); an empty p.Etag overwrites it
	// unconditionally. If the policy does not exist and allowMissing is true,
	// p.Etag must be empty. (ErrUnknownEtag). On error the stored policy is left unchanged.
	UpdatePolicy(clusterName string, allowMissing bool, p *pb.Policy) (*pb.Policy, error)
	DeletePolicy(id *pb.PolicyId) error
	GetPolicy(id *pb.PolicyId) *pb.Policy
	ListPolicies(clusterName string) []*pb.Policy
	UpdateWorkload(req *pb.UpdateWorkloadRequest) error
	GetRecommendation(id *pb.PolicyId) (*pb.GetRecommendationResponse, bool)
	// GetControlMetrics returns the aggregated metrics of a policy: the
	// policy-wide ones and, keyed by recommender name, those owned by each
	// recommender, along with the state of the workload they were calculated
	// against (see calculatePolicyControlMetrics). false is returned if the policy is
	// unknown or has not been calculated yet.
	GetControlMetrics(id *pb.PolicyId) (*pb.GetControlMetricsResponse, bool)
	// GetWorkload returns the pods of a policy's workload, as last reported
	// with UpdateWorkload, sorted by name.
	GetWorkload(id *pb.PolicyId) (*pb.Workload, bool)
	CalculateAll()
	// WatchControlMetrics returns a channel delivering the control metrics
	// of the policies of a cluster each time they change. The returned
	// function unwatches.
	WatchControlMetrics(clusterName string) (<-chan ControlMetricEvent, func())
	// WatchRecommendations returns a channel delivering the arbitrated
	// recommendation of the policies of a cluster each time it changes. The
	// returned function unwatches.
	WatchRecommendations(clusterName string) (<-chan RecommendationEvent, func())
	Dump() interface{}
}

type PolicyState struct {
	Policy *pb.Policy
	// Workload maps a pod name to its state.
	Workload                 map[string]*pb.PodState
	Metrics                  *MetricStore
	ArbitratedRecommendation *pb.Recommendation
	Explanation              []*pb.RecommenderStatus
	LastActive               time.Time
	RecommenderStatuses      map[string]*pb.RecommenderStatus
	// ControlMetrics holds the control metrics of the policy as of its last
	// calculation (see calculatePolicyControlMetrics), nil until then.
	ControlMetrics *pb.GetControlMetricsResponse
}

type MemoryStore struct {
	mu    sync.RWMutex
	clock clock.Clock

	// Storage: PolicyID -> PolicyState
	state map[policyID]*PolicyState

	// cmWatchers holds the control metric watchers of each cluster.
	cmWatchers map[string]*Watchers[ControlMetricEvent]
	// recWatchers holds the recommendation watchers of each cluster.
	recWatchers map[string]*Watchers[RecommendationEvent]
}

func NewMemoryStore() *MemoryStore {
	return NewMemoryStoreWithClock(clock.RealClock{})
}

func NewMemoryStoreWithClock(c clock.Clock) *MemoryStore {
	return &MemoryStore{
		clock:       c,
		state:       make(map[policyID]*PolicyState),
		cmWatchers:  make(map[string]*Watchers[ControlMetricEvent]),
		recWatchers: make(map[string]*Watchers[RecommendationEvent]),
	}
}

// WatchControlMetrics implements ServerStore.
func (s *MemoryStore) WatchControlMetrics(clusterName string) (<-chan ControlMetricEvent, func()) {
	s.mu.Lock()
	watchers, ok := s.cmWatchers[clusterName]
	if !ok {
		watchers = NewWatchers(clusterName, ControlMetricEvent.equal)
		s.cmWatchers[clusterName] = watchers
	}
	s.mu.Unlock()
	return watchers.Watch()
}

// WatchRecommendations implements ServerStore.
func (s *MemoryStore) WatchRecommendations(clusterName string) (<-chan RecommendationEvent, func()) {
	s.mu.Lock()
	watchers, ok := s.recWatchers[clusterName]
	if !ok {
		watchers = NewWatchers(clusterName, RecommendationEvent.equal)
		s.recWatchers[clusterName] = watchers
	}
	s.mu.Unlock()
	return watchers.Watch()
}

func (s *MemoryStore) CreatePolicy(clusterName string, p *pb.Policy) (*pb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ps := s.getPolicyState(clusterName, p)
	if ps != nil {
		return nil, ErrPolicyAlreadyExists
	}
	if p.GetEtag() != "" {
		return nil, ErrUnknownEtag
	}
	return s.setPolicyState(p, ps, key)
}

func (s *MemoryStore) getPolicyState(clusterName string, p *pb.Policy) (policyID, *PolicyState) {
	key := policyID{cluster: clusterName, ns: p.GetId().GetNamespace(), name: p.GetId().GetName()}
	ps := s.state[key]
	return key, ps
}

func (s *MemoryStore) setPolicyState(p *pb.Policy, ps *PolicyState, key policyID) (*pb.Policy, error) {
	created := proto.Clone(p).(*pb.Policy)

	// Drop the metrics of removed recommenders before computing the ETag, so
	// the ETag matches the stored policy, and before CleanupOrphaned below, so
	// their series are freed in this update.
	dropOrphanedRecommenderMetrics(created)

	etag, err := policy.CreateEtag(created)
	if err != nil {
		return nil, fmt.Errorf("unable to create etag: %w", err)
	}
	created.SetEtag(etag)

	if ps == nil {
		ps = &PolicyState{
			Workload:            make(map[string]*pb.PodState),
			Metrics:             NewMetricStore(),
			RecommenderStatuses: make(map[string]*pb.RecommenderStatus),
		}
		s.state[key] = ps
	}
	ps.Policy = created
	ps.ArbitratedRecommendation = nil
	ps.Explanation = nil
	ps.ControlMetrics = nil

	ps.Metrics.CleanupOrphaned(ps.Policy)
	s.cleanupOrphanedRecommenderStatuses(ps)

	return created, nil
}

func (s *MemoryStore) UpdatePolicy(clusterName string, allowMissing bool, p *pb.Policy) (*pb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ps := s.getPolicyState(clusterName, p)
	if ps != nil && ps.Policy != nil {
		// A sent ETag must match the stored one; an empty ETag overwrites.
		if p.GetEtag() != "" && p.GetEtag() != ps.Policy.GetEtag() {
			return nil, fmt.Errorf("%w: got %q, want %q", ErrStaleEtag, p.GetEtag(), ps.Policy.GetEtag())
		}
	} else {
		if !allowMissing {
			return nil, ErrUnknownPolicy
		}
		if p.GetEtag() != "" {
			// An ETag was sent for a policy that does not exist.
			return nil, ErrUnknownEtag
		}
	}

	return s.setPolicyState(p, ps, key)
}

func (s *MemoryStore) DeletePolicy(id *pb.PolicyId) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := newPolicyID(id)
	delete(s.state, key)
	if watchers, ok := s.cmWatchers[key.cluster]; ok {
		watchers.Forget(key)
	}
	if watchers, ok := s.recWatchers[key.cluster]; ok {
		watchers.Forget(key)
	}
	return nil
}

func (s *MemoryStore) GetPolicy(id *pb.PolicyId) *pb.Policy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := newPolicyID(id)
	ps, ok := s.state[key]
	if !ok || ps.Policy == nil {
		return nil
	}
	return ps.Policy
}

func (s *MemoryStore) ListPolicies(clusterName string) []*pb.Policy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var policies []*pb.Policy
	for key, ps := range s.state {
		if ps.Policy == nil {
			continue
		}
		if clusterName != "" && key.cluster != clusterName {
			continue
		}
		policies = append(policies, ps.Policy)
	}
	return policies
}

func (s *MemoryStore) GetWorkload(id *pb.PolicyId) (*pb.Workload, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ps, ok := s.state[newPolicyID(id)]
	if !ok || ps.Policy == nil {
		return nil, false
	}
	workload := pb.Workload_builder{Pods: make([]*pb.PodState, 0, len(ps.Workload))}.Build()
	for _, name := range slices.Sorted(maps.Keys(ps.Workload)) {
		workload.SetPods(append(workload.GetPods(), proto.Clone(ps.Workload[name]).(*pb.PodState)))
	}
	return workload, true
}

func (s *MemoryStore) UpdateWorkload(req *pb.UpdateWorkloadRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := newPolicyID(req.GetId())
	ps, ok := s.state[key]
	if !ok || ps.Policy == nil {
		return fmt.Errorf("policy not found")
	}

	newWorkload := make(map[string]*pb.PodState)
	for _, p := range req.GetWorkload().GetPods() {
		newWorkload[p.GetName()] = p
	}
	ps.Workload = newWorkload
	return nil
}

func (s *MemoryStore) AddBatch(req *pb.IngestMetricsRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, pBatch := range req.GetPolicies() {
		key := policyID{cluster: req.GetClusterName(), ns: pBatch.GetNamespace(), name: pBatch.GetName()}
		ps, ok := s.state[key]
		if !ok || ps.Policy == nil {
			return fmt.Errorf("policy not found: %s", key)
		}

		if err := ps.Metrics.IngestMetrics(ps.Policy, ps.Workload, pBatch.GetBatches(), req.GetTimestamp().AsTime()); err != nil {
			return err
		}
		ps.ControlMetrics = calculatePolicyControlMetrics(ps, s.clock.Now())

		watchers, watchersOk := s.cmWatchers[key.cluster]
		if watchersOk && !watchers.Throttled(key) {
			res := ps.ControlMetrics
			watchers.Notify(key, ControlMetricEvent{
				ID:                        key.toProto(),
				ReadyReplicas:             res.GetReadyReplicas(),
				ControlMetrics:            res.GetControlMetrics(),
				RecommenderControlMetrics: res.GetRecommenderControlMetrics(),
			})
		}
	}
	return nil
}

func (s *MemoryStore) UpdateRecommenderState(req *pb.UpdateRecommenderStateRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := newPolicyID(req.GetId())
	ps, ok := s.state[key]
	if !ok || ps.Policy == nil {
		return fmt.Errorf("policy not found")
	}

	if !req.HasRecommendation() {
		delete(ps.RecommenderStatuses, req.GetRecommenderName())
		s.calculateArbitratedRecommendation(ps, s.clock.Now())
		s.notifyRecommendation(key, ps)
		return nil
	}

	// Validate that the recommender exists in the current policy
	var def *pb.RecommenderDefinition
	isActivation := false
	for _, r := range ps.Policy.GetScaling() {
		if r.GetName() == req.GetRecommenderName() {
			def = r
			break
		}
	}
	if def == nil {
		for _, r := range ps.Policy.GetActivation() {
			if r.GetName() == req.GetRecommenderName() {
				def = r
				isActivation = true
				break
			}
		}
	}
	if def == nil {
		return fmt.Errorf("recommender %s not defined in policy", req.GetRecommenderName())
	}

	// Basic validation of recommendation
	if req.GetRecommendation().HasReplicas() && req.GetRecommendation().GetReplicas() < 0 {
		return fmt.Errorf("desired replicas cannot be negative")
	}

	phase := "Scaling"
	if isActivation {
		phase = "Activation"
	}

	// Create enriched status
	status := pb.RecommenderStatus_builder{
		Name:              proto.String(req.GetRecommenderName()),
		Type:              proto.String(def.GetType()),
		Phase:             proto.String(phase),
		Mode:              proto.String(def.GetMode()),
		Replicas:          proto.ValueOrNil(req.GetRecommendation().HasReplicas(), req.GetRecommendation().GetReplicas),
		IsActive:          proto.Bool(req.GetRecommendation().GetIsActive()),
		Message:           proto.String(req.GetRecommendation().GetMessage()),
		LastUpdated:       timestamppb.New(s.clock.Now()),
		WorkloadResources: req.GetRecommendation().GetWorkloadResources(),
		PodResources:      req.GetRecommendation().GetPodContainerResources(),
	}.Build()

	if err := s.updateRecommenderStatus(ps, req.GetRecommenderName(), status); err != nil {
		return err
	}
	s.calculateArbitratedRecommendation(ps, s.clock.Now())
	s.notifyRecommendation(key, ps)
	return nil
}

func (s *MemoryStore) updateRecommenderStatus(ps *PolicyState, name string, status *pb.RecommenderStatus) error {
	if ps.RecommenderStatuses == nil {
		ps.RecommenderStatuses = make(map[string]*pb.RecommenderStatus)
	}
	ps.RecommenderStatuses[name] = status
	return nil
}

// notifyRecommendation notifies the recommendation watchers of a policy's
// cluster of its current arbitrated recommendation, unless the cluster has
// never been watched or the policy is throttled. The caller must hold mu.
func (s *MemoryStore) notifyRecommendation(key policyID, ps *PolicyState) {
	watchers, ok := s.recWatchers[key.cluster]
	if !ok || watchers.Throttled(key) {
		return
	}
	watchers.Notify(key, recommendationEvent(key, ps))
}

// recommendationEvent returns the event reporting the current arbitrated
// recommendation of a policy: what GetRecommendation returns. The caller must
// hold mu.
func recommendationEvent(key policyID, ps *PolicyState) RecommendationEvent {
	return RecommendationEvent{
		ID:             key.toProto(),
		Recommendation: ps.ArbitratedRecommendation,
		Explanation:    ps.Explanation,
		MetricStatuses: metricStatuses(ps),
	}
}

func (s *MemoryStore) GetRecommendation(id *pb.PolicyId) (*pb.GetRecommendationResponse, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := newPolicyID(id)
	ps, ok := s.state[key]
	if !ok || ps.Policy == nil {
		return nil, false
	}

	return pb.GetRecommendationResponse_builder{
		Recommendation: ps.ArbitratedRecommendation,
		MetricStatuses: metricStatuses(ps),
		Explanation:    ps.Explanation,
	}.Build(), true
}

// metricStatuses returns the status of every metric of a policy as of its
// last calculation, none until then. The policy-wide metrics come first,
// followed by those owned by recommenders, listed as <recommender>/<metric>.
// The caller must hold mu.
func metricStatuses(ps *PolicyState) []*pb.MetricStatus {
	res := ps.ControlMetrics
	if res == nil {
		return nil
	}
	var statuses []*pb.MetricStatus
	for _, def := range ps.Policy.GetMetrics() {
		statuses = append(statuses, metricStatus(def.GetName(), "", res.GetControlMetrics(), res.GetTimestamp()))
	}
	for _, owner := range slices.Sorted(maps.Keys(ps.Policy.GetRecommenderMetrics())) {
		cm, ok := res.GetRecommenderControlMetrics()[owner]
		if !ok || cm == nil {
			continue
		}
		for _, def := range ps.Policy.GetRecommenderMetrics()[owner].GetDefinitions() {
			statuses = append(statuses, metricStatus(def.GetName(), owner, cm, res.GetTimestamp()))
		}
	}
	return statuses
}

// metricStatus returns the status of the metric name in cm, owned by owner
// (empty for policy-wide metrics) and calculated at ts. Owned metrics are
// displayed as "<owner>/<name>". Per-pod and per-container values are averaged.
func metricStatus(name, owner string, cm *pb.ControlMetrics, ts *timestamppb.Timestamp) *pb.MetricStatus {
	displayName := name
	if owner != "" {
		displayName = owner + "/" + name
	}
	status := pb.MetricStatus_builder{
		Name:      proto.String(displayName),
		Timestamp: ts,
	}.Build()
	if val, ok := cm.GetValues()[name]; ok {
		status.SetValue(val)
		return status
	}
	var sum float64
	var count int
	for _, pcm := range cm.GetPodContainerMetrics() {
		for _, c := range pcm.GetContainerMetrics() {
			if v, ok := c.GetValues()[name]; ok {
				sum += v
				count++
			}
		}
	}
	if count == 0 {
		for _, pm := range cm.GetPodMetrics() {
			if v, ok := pm.GetValues()[name]; ok {
				sum += v
				count++
			}
		}
	}
	if count > 0 {
		status.SetValue(sum / float64(count))
	} else {
		status.SetError("No data available")
	}
	return status
}

// GetControlMetrics implements ServerStore. The recommender control metrics
// only have entries for the recommenders that own metrics.
func (s *MemoryStore) GetControlMetrics(id *pb.PolicyId) (*pb.GetControlMetricsResponse, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := newPolicyID(id)
	ps, ok := s.state[key]
	if !ok || ps.ControlMetrics == nil {
		return nil, false
	}
	return ps.ControlMetrics, true
}

func (s *MemoryStore) CalculateAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	for key, ps := range s.state {
		if ps.Policy == nil {
			continue
		}
		// Arbitration depends on time (see the activation window), so it's
		// periodically refreshed.
		s.calculateArbitratedRecommendation(ps, now)
		s.notifyRecommendation(key, ps)
	}
}

// calculatePolicyControlMetrics aggregates the metrics of a policy as of now. It returns the
// policy-wide control metrics and, keyed by recommender name, the control
// metrics owned by each recommender (none if the policy defines none), along
// with the state of the workload they were calculated against: the
// calculation time and the number of ready replicas. The policy state is not
// modified. The caller must hold mu.
func calculatePolicyControlMetrics(ps *PolicyState, now time.Time) *pb.GetControlMetricsResponse {
	policy := ps.Policy
	workload := ps.Workload
	cm := ps.Metrics.calculate(policy.GetMetrics(), workload, now)

	// Process metrics owned by a recommender the same way.
	var rcm map[string]*pb.ControlMetrics
	if len(policy.GetRecommenderMetrics()) > 0 {
		rcm = make(map[string]*pb.ControlMetrics, len(policy.GetRecommenderMetrics()))
		for name, defs := range policy.GetRecommenderMetrics() {
			rcm[name] = ps.Metrics.calculate(defs.GetDefinitions(), workload, now)
		}
	}
	return pb.GetControlMetricsResponse_builder{
		Timestamp:                 timestamppb.New(now),
		ReadyReplicas:             proto.Int32(readyReplicasCount(workload)),
		ControlMetrics:            cm,
		RecommenderControlMetrics: rcm,
	}.Build()
}

func (s *MemoryStore) cleanupOrphanedRecommenderStatuses(ps *PolicyState) {
	names := recommenderNames(ps.Policy)
	for rName := range ps.RecommenderStatuses {
		if !names[rName] {
			delete(ps.RecommenderStatuses, rName)
		}
	}
}

// dropOrphanedRecommenderMetrics removes the metrics owned by recommenders
// that are no longer in the policy. Only the server can do this: the component
// running a removed recommender never sees it again.
func dropOrphanedRecommenderMetrics(p *pb.Policy) {
	names := recommenderNames(p)
	for rName := range p.GetRecommenderMetrics() {
		if !names[rName] {
			delete(p.GetRecommenderMetrics(), rName)
		}
	}
}

// recommenderNames returns the names of the Scaling and Activation
// recommenders of the policy.
func recommenderNames(p *pb.Policy) map[string]bool {
	names := make(map[string]bool, len(p.GetScaling())+len(p.GetActivation()))
	for _, r := range p.GetScaling() {
		names[r.GetName()] = true
	}
	for _, r := range p.GetActivation() {
		names[r.GetName()] = true
	}
	return names
}

func (s *MemoryStore) calculateArbitratedRecommendation(ps *PolicyState, now time.Time) {
	policy := ps.Policy
	isActive := false
	var activationStatuses []*pb.RecommenderStatus

	if len(policy.GetActivation()) == 0 {
		isActive = true
	} else {
		for _, recDef := range policy.GetActivation() {
			if d, ok := ps.RecommenderStatuses[recDef.GetName()]; ok {
				activationStatuses = append(activationStatuses, d)
				if recDef.GetMode() == "DryRun" {
					continue
				}
				if d.GetIsActive() {
					isActive = true
				}
			}
		}
	}

	if isActive {
		ps.LastActive = now
	}

	window := 5 * time.Minute
	for _, recDef := range policy.GetActivation() {
		if val, ok := recDef.GetParams()["window"]; ok {
			if w, err := strconv.ParseInt(val, 10, 64); err == nil {
				window = time.Duration(w) * time.Second
			}
		}
	}

	if !isActive {
		last := ps.LastActive
		if now.Sub(last) <= window {
			isActive = true
		}
	}

	// Arbitrate the horizontal and vertical recommendations. replicas is nil
	// if no recommender has decided on a number of replicas yet.
	scalingStatuses := s.scalingRecommenderStatuses(ps, isActive)
	replicas := s.calculateArbitratedReplicas(ps, isActive, scalingStatuses)
	workloadRes, podRes := s.calculateArbitratedResources(ps, isActive)

	if replicas == nil && len(workloadRes) == 0 && len(podRes) == 0 && len(scalingStatuses) == 0 {
		ps.ArbitratedRecommendation = nil
		ps.Explanation = nil
		return
	}

	ps.ArbitratedRecommendation = pb.Recommendation_builder{
		Replicas:              replicas,
		WorkloadResources:     workloadRes,
		PodContainerResources: podRes,
	}.Build()
	ps.Explanation = append(activationStatuses, scalingStatuses...)

	if policy.HasWorkload() {
		if replicas != nil {
			metrics.RecordRecommendation(policy.GetId().GetClusterName(), policy.GetId().GetNamespace(), policy.GetId().GetName(), policy.GetWorkload().GetGroup(), policy.GetWorkload().GetVersion(), policy.GetWorkload().GetKind(), policy.GetWorkload().GetName(), *replicas)
		}
		metrics.RecordActive(policy.GetId().GetClusterName(), policy.GetId().GetNamespace(), policy.GetId().GetName(), policy.GetWorkload().GetGroup(), policy.GetWorkload().GetVersion(), policy.GetWorkload().GetKind(), policy.GetWorkload().GetName(), isActive)
	}
}

// calculateArbitratedResources combines workload-level and pod-level vertical resource
// recommendations across all active, non-dry-run scaling recommenders.
//
// For workload-level recommendations (WorkloadResources):
//   - Recommendations are grouped by "containerName".
//   - If multiple active recommenders specify resources for the same container,
//     their requests and limits are arbitrated by taking the maximum quantity for each resource.
//
// For pod-level recommendations (PodResources):
//   - Recommendations are grouped by "podName|containerName".
//   - If multiple active recommenders specify resources for the same pod container,
//     their requests and limits are arbitrated by taking the maximum quantity for each resource.
//
// Recommenders running in "DryRun" mode or reporting IsActive = false are excluded from arbitration.
func (s *MemoryStore) calculateArbitratedResources(ps *PolicyState, isActive bool) ([]*pb.ContainerResource, []*pb.PodContainerResource) {
	if !isActive {
		return nil, nil
	}

	// Map key: "containerName" -> ContainerResource
	workloadMap := make(map[string]*pb.ContainerResource)
	workloadKeys := []string{}

	// Map key: "podName|containerName" -> PodContainerResource
	podMap := make(map[string]*pb.PodContainerResource)
	podKeys := []string{} // Maintain insertion order for deterministic outputs

	for _, recDef := range ps.Policy.GetScaling() {
		d, ok := ps.RecommenderStatuses[recDef.GetName()]
		if !ok || recDef.GetMode() == "DryRun" || !d.GetIsActive() {
			continue
		}

		for _, wr := range d.GetWorkloadResources() {
			if wr == nil {
				continue
			}
			cName := wr.GetContainerName()
			existing, ok := workloadMap[cName]
			if !ok {
				newReqs := make(map[string]string)
				newLims := make(map[string]string)
				for k, v := range wr.GetRequests() {
					newReqs[k] = v
				}
				for k, v := range wr.GetLimits() {
					newLims[k] = v
				}
				workloadMap[cName] = pb.ContainerResource_builder{
					ContainerName: proto.String(cName),
					Requests:      newReqs,
					Limits:        newLims,
				}.Build()
				workloadKeys = append(workloadKeys, cName)
			} else {
				arbitrateResourceMap(existing.GetRequests(), wr.GetRequests())
				arbitrateResourceMap(existing.GetLimits(), wr.GetLimits())
			}
		}

		for _, pr := range d.GetPodResources() {
			cName := ""
			var reqs, lims map[string]string
			if pr.HasContainerResources() {
				cName = pr.GetContainerResources().GetContainerName()
				reqs = pr.GetContainerResources().GetRequests()
				lims = pr.GetContainerResources().GetLimits()
			}
			key := fmt.Sprintf("%s|%s", pr.GetPodName(), cName)
			existing, ok := podMap[key]
			if !ok {
				newReqs := make(map[string]string)
				newLims := make(map[string]string)
				for k, v := range reqs {
					newReqs[k] = v
				}
				for k, v := range lims {
					newLims[k] = v
				}
				podMap[key] = pb.PodContainerResource_builder{
					PodName: proto.String(pr.GetPodName()),
					ContainerResources: pb.ContainerResource_builder{
						ContainerName: proto.String(cName),
						Requests:      newReqs,
						Limits:        newLims,
					}.Build(),
				}.Build()
				podKeys = append(podKeys, key)
			} else {
				if existing.HasContainerResources() {
					arbitrateResourceMap(existing.GetContainerResources().GetRequests(), reqs)
					arbitrateResourceMap(existing.GetContainerResources().GetLimits(), lims)
				}
			}
		}
	}

	var arbitratedWorkload []*pb.ContainerResource
	for _, key := range workloadKeys {
		arbitratedWorkload = append(arbitratedWorkload, workloadMap[key])
	}

	var arbitratedPods []*pb.PodContainerResource
	for _, key := range podKeys {
		arbitratedPods = append(arbitratedPods, podMap[key])
	}

	return arbitratedWorkload, arbitratedPods
}

func arbitrateResourceMap(dest map[string]string, src map[string]string) {
	if dest == nil || src == nil {
		return
	}
	for resName, srcVal := range src {
		destVal, ok := dest[resName]
		if !ok || destVal == "" {
			dest[resName] = srcVal
			continue
		}

		destQ, err1 := resource.ParseQuantity(destVal)
		srcQ, err2 := resource.ParseQuantity(srcVal)
		if err1 == nil && err2 == nil {
			if destQ.Cmp(srcQ) < 0 {
				dest[resName] = srcVal
			}
		}
	}
}

// scalingRecommenderStatuses returns the status of every scaling recommender
// of the policy that has reported one, in policy order. Nothing is reported
// for an inactive workload: its replicas are decided by the activators alone.
func (s *MemoryStore) scalingRecommenderStatuses(ps *PolicyState, isActive bool) []*pb.RecommenderStatus {
	if !isActive {
		return nil
	}
	var statuses []*pb.RecommenderStatus
	for _, recDef := range ps.Policy.GetScaling() {
		if d, ok := ps.RecommenderStatuses[recDef.GetName()]; ok {
			statuses = append(statuses, d)
		}
	}
	return statuses
}

// calculateArbitratedReplicas returns the number of replicas arbitrated from
// the given scaling recommender statuses: 0 for an inactive workload,
// otherwise the highest active non-dry-run recommendation clamped to the
// policy's [min, max] range, or nil if no recommender has decided yet.
func (s *MemoryStore) calculateArbitratedReplicas(ps *PolicyState, isActive bool, statuses []*pb.RecommenderStatus) *int32 {
	if !isActive {
		return new(int32)
	}

	// Modes are defined on the policy, not on the status: index them by name.
	modes := make(map[string]string, len(ps.Policy.GetScaling()))
	for _, recDef := range ps.Policy.GetScaling() {
		modes[recDef.GetName()] = recDef.GetMode()
	}

	var targetReplicas *int32
	for _, d := range statuses {
		// If multiple (non-dry-run) recommendations exist, take the highest.
		if modes[d.GetName()] != "DryRun" && d.GetIsActive() && d.HasReplicas() && (targetReplicas == nil || d.GetReplicas() >= *targetReplicas) {
			r := d.GetReplicas()
			targetReplicas = &r
		}
	}

	if targetReplicas == nil {
		return nil
	}

	if ps.Policy.GetMaxReplicas() > 0 {
		*targetReplicas = min(*targetReplicas, ps.Policy.GetMaxReplicas())
	}
	*targetReplicas = max(*targetReplicas, ps.Policy.GetMinReplicas())
	return targetReplicas
}

func (s *MemoryStore) Dump() interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}
