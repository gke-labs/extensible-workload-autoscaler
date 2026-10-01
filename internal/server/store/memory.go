package store

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/metrics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/resource"
)

var (
	ErrStaleEtag   = errors.New("etag does not match the stored policy")
	ErrUnknownEtag = errors.New("etag given for unknown policy")
)

type ServerStore interface {
	AddBatch(req *pb.IngestMetricsRequest) error
	UpdateRecommenderState(req *pb.UpdateRecommenderStateRequest) error
	// UpdatePolicy replaces the stored policy with p and returns the stored
	// result with a new ETag. If the policy exists, a non-empty p.Etag must
	// match the stored one (ErrStaleEtag); an empty p.Etag overwrites it
	// unconditionally. If the policy does not exist, p.Etag must be empty
	// (ErrUnknownEtag). On error the stored policy is left unchanged.
	UpdatePolicy(clusterName string, p *pb.Policy) (*pb.Policy, error)
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
	Policy *pb.Policy
	// Workload maps a pod name to its state.
	Workload            map[string]*pb.PodState
	Metrics             *MetricStore
	Recommendation      *pb.Recommendation
	Explanation         []*pb.RecommenderStatus
	LastActive          int64
	RecommenderStatuses map[string]*pb.RecommenderStatus
	ControlMetrics      *pb.ControlMetrics
	// RecommenderControlMetrics holds the values of the metrics owned by each
	// recommender, keyed by recommender name.
	RecommenderControlMetrics map[string]*pb.ControlMetrics
}

type MemoryStore struct {
	mu    sync.RWMutex
	clock clock.Clock

	// Storage: PolicyID -> PolicyState
	state map[policyID]*PolicyState
}

func NewMemoryStore() *MemoryStore {
	return NewMemoryStoreWithClock(clock.RealClock{})
}

func NewMemoryStoreWithClock(c clock.Clock) *MemoryStore {
	return &MemoryStore{
		clock: c,
		state: make(map[policyID]*PolicyState),
	}
}

func (s *MemoryStore) UpdatePolicy(clusterName string, p *pb.Policy) (*pb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := policyID{cluster: clusterName, ns: p.Id.GetNamespace(), name: p.Id.GetName()}

	ps := s.state[key]
	var current *pb.Policy

	if ps != nil {
		current = ps.Policy
	}
	if current != nil {
		// A sent ETag must match the stored one; an empty ETag overwrites.
		if p.Etag != "" && p.Etag != current.Etag {
			return nil, fmt.Errorf("%w: got %q, want %q", ErrStaleEtag, p.Etag, current.Etag)
		}
	} else if p.Etag != "" {
		// An ETag was sent for a policy that does not exist.
		return nil, ErrUnknownEtag
	}

	updated := proto.Clone(p).(*pb.Policy)
	etag, err := policy.CreateEtag(updated)
	if err != nil {
		return nil, fmt.Errorf("unable to create etag: %w", err)
	}
	updated.Etag = etag

	if ps == nil {
		ps = &PolicyState{
			Workload:            make(map[string]*pb.PodState),
			Metrics:             NewMetricStore(),
			RecommenderStatuses: make(map[string]*pb.RecommenderStatus),
		}
		s.state[key] = ps
	}
	ps.Policy = updated
	ps.Recommendation = nil
	ps.Explanation = nil
	ps.ControlMetrics = nil
	ps.RecommenderControlMetrics = nil

	ps.Metrics.CleanupOrphaned(ps.Policy)
	s.cleanupOrphanedRecommenderStatuses(ps)

	return updated, nil
}

func (s *MemoryStore) DeletePolicy(id *pb.PolicyId) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := newPolicyID(id)
	delete(s.state, key)
	return nil
}

func (s *MemoryStore) GetPolicy(id *pb.PolicyId) (*pb.Policy, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := newPolicyID(id)
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
		if clusterName != "" && key.cluster != clusterName {
			continue
		}
		policies = append(policies, ps.Policy)
	}
	return policies
}

func (s *MemoryStore) UpdateWorkload(req *pb.UpdateWorkloadRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := newPolicyID(req.Id)
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
		key := policyID{cluster: req.ClusterName, ns: pBatch.Namespace, name: pBatch.Name}
		ps, ok := s.state[key]
		if !ok || ps.Policy == nil {
			return fmt.Errorf("policy not found: %s", key)
		}

		if err := ps.Metrics.IngestBatch(ps.Policy, pBatch.Batches, req.Timestamp); err != nil {
			return err
		}
	}
	return nil
}

func (s *MemoryStore) UpdateRecommenderState(req *pb.UpdateRecommenderStateRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := newPolicyID(req.Id)
	ps, ok := s.state[key]
	if !ok || ps.Policy == nil {
		return fmt.Errorf("policy not found")
	}

	if req.Recommendation == nil {
		delete(ps.RecommenderStatuses, req.RecommenderName)
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

	// Basic validation of recommendation
	if req.Recommendation.Replicas != nil && *req.Recommendation.Replicas < 0 {
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
		Replicas:          req.Recommendation.Replicas,
		IsActive:          req.Recommendation.IsActive,
		Message:           req.Recommendation.Message,
		LastUpdated:       timestamppb.New(s.clock.Now()),
		WorkloadResources: req.Recommendation.WorkloadResources,
		PodResources:      req.Recommendation.PodContainerResources,
	}

	// Wait, I need to check how to correctly create google.protobuf.Timestamp
	// I'll check imports and existing usage.
	return s.updateRecommenderStatus(ps, req.RecommenderName, status)
}

func (s *MemoryStore) updateRecommenderStatus(ps *PolicyState, name string, status *pb.RecommenderStatus) error {
	if ps.RecommenderStatuses == nil {
		ps.RecommenderStatuses = make(map[string]*pb.RecommenderStatus)
	}
	ps.RecommenderStatuses[name] = status
	return nil
}

func (s *MemoryStore) GetRecommendation(id *pb.PolicyId) (*pb.GetRecommendationResponse, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := newPolicyID(id)
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
				var sum float64
				var count int
				for _, pcm := range ps.ControlMetrics.PodContainerMetrics {
					for _, cm := range pcm.GetContainerMetrics() {
						if v, ok := cm.Values[def.Name]; ok {
							sum += v
							count++
						}
					}
				}
				if count == 0 {
					for _, pm := range ps.ControlMetrics.PodMetrics {
						if v, ok := pm.Values[def.Name]; ok {
							sum += v
							count++
						}
					}
				}
				if count > 0 {
					status.Value = sum / float64(count)
				} else {
					status.Error = "No data available"
				}
			}
			metricStatuses = append(metricStatuses, status)
		}
	}

	return &pb.GetRecommendationResponse{
		Recommendation: ps.Recommendation,
		MetricStatuses: metricStatuses,
		Explanation:    ps.Explanation,
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
	key := newPolicyID(id)
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
	// ContainerMetrics is left unset, matching Calculate.
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

	for _, ps := range s.state {
		policy := ps.Policy
		if policy == nil {
			continue
		}

		workload := ps.Workload
		ps.ControlMetrics = ps.Metrics.Calculate(policy, policy.Metrics, workload, now)

		// Metrics owned by a recommender are aggregated the same way, but kept
		// in a snapshot of their own so that they are only reported to their
		// owner.
		ps.RecommenderControlMetrics = nil
		if len(policy.RecommenderMetrics) > 0 {
			ps.RecommenderControlMetrics = make(map[string]*pb.ControlMetrics, len(policy.RecommenderMetrics))
			for name, defs := range policy.RecommenderMetrics {
				ps.RecommenderControlMetrics[name] = ps.Metrics.Calculate(policy, defs.GetDefinitions(), workload, now)
			}
		}

		s.processRecommendations(ps, now)
	}
}

func (s *MemoryStore) cleanupOrphanedRecommenderStatuses(ps *PolicyState) {
	recommenderNames := make(map[string]bool)
	for _, r := range ps.Policy.Scaling {
		recommenderNames[r.Name] = true
	}
	for _, r := range ps.Policy.Activation {
		recommenderNames[r.Name] = true
	}
	for rName := range ps.RecommenderStatuses {
		if !recommenderNames[rName] {
			delete(ps.RecommenderStatuses, rName)
		}
	}
}

func (s *MemoryStore) processRecommendations(ps *PolicyState, now int64) {
	policy := ps.Policy
	isActive := false
	var activationStatuses []*pb.RecommenderStatus

	if len(policy.Activation) == 0 {
		isActive = true
	} else {
		for _, recDef := range policy.Activation {
			if d, ok := ps.RecommenderStatuses[recDef.Name]; ok {
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
	workloadRes, podRes := s.calculateArbitratedResources(ps, isActive)

	if replicas == nil && len(workloadRes) == 0 && len(podRes) == 0 && len(scalingStatuses) == 0 {
		ps.Recommendation = nil
		ps.Explanation = nil
		return
	}

	ps.Recommendation = &pb.Recommendation{
		Replicas:              replicas,
		WorkloadResources:     workloadRes,
		PodContainerResources: podRes,
	}
	ps.Explanation = append(activationStatuses, scalingStatuses...)

	if policy.Workload != nil {
		if replicas != nil {
			metrics.RecordRecommendation(policy.Id.ClusterName, policy.Id.Namespace, policy.Id.Name, policy.Workload.Group, policy.Workload.Version, policy.Workload.Kind, policy.Workload.Name, *replicas)
		}
		metrics.RecordActive(policy.Id.ClusterName, policy.Id.Namespace, policy.Id.Name, policy.Workload.Group, policy.Workload.Version, policy.Workload.Kind, policy.Workload.Name, isActive)
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

	for _, recDef := range ps.Policy.Scaling {
		d, ok := ps.RecommenderStatuses[recDef.Name]
		if !ok || recDef.Mode == "DryRun" || !d.IsActive {
			continue
		}

		for _, wr := range d.WorkloadResources {
			if wr == nil {
				continue
			}
			cName := wr.ContainerName
			existing, ok := workloadMap[cName]
			if !ok {
				newReqs := make(map[string]string)
				newLims := make(map[string]string)
				for k, v := range wr.Requests {
					newReqs[k] = v
				}
				for k, v := range wr.Limits {
					newLims[k] = v
				}
				workloadMap[cName] = &pb.ContainerResource{
					ContainerName: cName,
					Requests:      newReqs,
					Limits:        newLims,
				}
				workloadKeys = append(workloadKeys, cName)
			} else {
				arbitrateResourceMap(existing.Requests, wr.Requests)
				arbitrateResourceMap(existing.Limits, wr.Limits)
			}
		}

		for _, pr := range d.PodResources {
			cName := ""
			var reqs, lims map[string]string
			if pr.ContainerResources != nil {
				cName = pr.ContainerResources.ContainerName
				reqs = pr.ContainerResources.Requests
				lims = pr.ContainerResources.Limits
			}
			key := fmt.Sprintf("%s|%s", pr.PodName, cName)
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
				podMap[key] = &pb.PodContainerResource{
					PodName: pr.PodName,
					ContainerResources: &pb.ContainerResource{
						ContainerName: cName,
						Requests:      newReqs,
						Limits:        newLims,
					},
				}
				podKeys = append(podKeys, key)
			} else {
				if existing.ContainerResources != nil {
					arbitrateResourceMap(existing.ContainerResources.Requests, reqs)
					arbitrateResourceMap(existing.ContainerResources.Limits, lims)
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

func (s *MemoryStore) calculateTargetReplicas(ps *PolicyState, isActive bool) (*int32, []*pb.RecommenderStatus) {
	if !isActive {
		return new(int32), nil
	}

	var targetReplicas *int32
	var recommenderStatuses []*pb.RecommenderStatus

	for _, recDef := range ps.Policy.Scaling {
		d, ok := ps.RecommenderStatuses[recDef.Name]
		if !ok {
			continue
		}

		// If multiple (non-dry-run) recommendations exist, take the highest.
		if recDef.Mode != "DryRun" && d.IsActive && d.Replicas != nil && (targetReplicas == nil || *d.Replicas >= *targetReplicas) {
			r := *d.Replicas
			targetReplicas = &r
		}

		recommenderStatuses = append(recommenderStatuses, d)
	}

	if targetReplicas == nil {
		return nil, recommenderStatuses
	}

	if ps.Policy.MaxReplicas > 0 {
		*targetReplicas = min(*targetReplicas, ps.Policy.MaxReplicas)
	}
	*targetReplicas = max(*targetReplicas, ps.Policy.MinReplicas)
	return targetReplicas, recommenderStatuses
}

func (s *MemoryStore) Dump() interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}
