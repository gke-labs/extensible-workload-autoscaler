package engine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	corelisters "k8s.io/client-go/listers/core/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/cmd/core-recommenders/internal/cron"
	"github.com/gke-labs/extensible-workload-autoscaler/cmd/core-recommenders/internal/engine/watchclient"
	"github.com/gke-labs/extensible-workload-autoscaler/cmd/core-recommenders/internal/linear"
	"github.com/gke-labs/extensible-workload-autoscaler/cmd/core-recommenders/internal/perpodvertical"
	"github.com/gke-labs/extensible-workload-autoscaler/cmd/core-recommenders/internal/vpa"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

type Recommender interface {
	// Recommend returns the recommendation of a recommender, given the
	// policy-wide metrics, the metrics it owns (nil if it owns none), the
	// number of ready replicas the metrics were calculated against, and the
	// workload's pods with their current requests (e.g. to only resize the
	// pods that need it).
	Recommend(def *pb.RecommenderDefinition, metrics, ownedMetrics *pb.ControlMetrics, readyReplicas int32, workload *pb.Workload) *pb.Recommendation
}

// The VPA recommender owns the metrics the user doesn't configure.
var _ MetricsOwner = (*vpa.VPARecommender)(nil)

type Engine struct {
	grpcConn               *grpc.ClientConn
	client                 pb.XASServerClient
	state                  State
	recommenderClassLister listers.RecommenderClassLister
	// recommenders holds the implementation backing each RecommenderClass type
	// this binary supports, keyed by type (e.g. "Linear").
	recommenders map[string]Recommender

	clusterName string
}

func NewEngine(recommenderClassLister listers.RecommenderClassLister, nodeLister corelisters.NodeLister, serverAddress, clusterName string) *Engine {
	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("did not connect", "error", err)
		os.Exit(1)
	}
	client := pb.NewXASServerClient(conn)

	return &Engine{
		grpcConn:               conn,
		client:                 client,
		state:                  make(State),
		recommenderClassLister: recommenderClassLister,
		recommenders: map[string]Recommender{
			"Linear":         &linear.LinearRecommender{},
			"Cron":           &cron.Recommender{},
			"VPA":            &vpa.VPARecommender{},
			"PerPodVertical": &perpodvertical.PerPodVerticalRecommender{},
		},
		clusterName: clusterName,
	}
}

func (e *Engine) Run(ctx context.Context) {
	defer e.grpcConn.Close()
	ticker := time.NewTicker(5 * time.Second) // Fast loop
	defer ticker.Stop()

	// Learn the policies before watching their metrics.
	e.Tick()

	ch := make(chan *pb.ControlMetricsEvent, 100)
	cmc := watchclient.NewControlMetricsClient(e.grpcConn, e.clusterName)
	go cmc.Run(ctx, ch)

	for {
		select {
		case metric := <-ch:
			e.handleControlMetricsUpdate(ctx, metric)
		case <-ticker.C:
			e.Tick()
		case <-ctx.Done():
			return
		}
	}
}

// handleControlMetricsUpdate runs the recommenders of a policy on its new
// control metrics and pushes their recommendations. Updates for policies the
// engine does not know about (see Tick) are dropped.
func (e *Engine) handleControlMetricsUpdate(ctx context.Context, metric *pb.ControlMetricsEvent) {
	policyID, clusterName := PolicyIDFromProto(metric.GetId())

	if clusterName != e.clusterName {
		slog.Error("Received metrics from another cluster", "unexpected_cluster_name", clusterName)
		return
	}
	ps, ok := e.state[policyID]
	if !ok {
		slog.Debug("Ignoring the metrics of an unknown policy", "policy", metric.GetId().GetName())
		return
	}
	policy, workload := ps.policy, ps.workload

	// Call each recommender to get their recommendation.
	var recommendations []namedRecommendation
	for _, def := range slices.Concat(policy.GetActivation(), policy.GetScaling()) {
		recName := def.GetName()
		rec, err := e.recommenderFor(def.GetRecommender())
		if err != nil {
			slog.Warn("RecommenderClass not found for policy", "recommender", def.GetRecommender(), "policy", policy.GetId().GetName(), "error", err)
			return
		}
		// Recommenders that own no metric have no entry: ownedMetrics is nil.
		ownedMetrics := metric.GetRecommenderControlMetrics()[recName]
		if v := rec.Recommend(def, metric.GetControlMetrics(), ownedMetrics, metric.GetReadyReplicas(), workload); v != nil {
			recommendations = append(recommendations, namedRecommendation{name: recName, recommendation: v})
		}
	}

	slog.Debug("Recommendations generated", "policy", policy.GetId().GetName(), "count", len(recommendations))
	if len(recommendations) > 0 {
		e.pushRecommendations(ctx, policy, recommendations)
	}
}

// Tick refreshes the engine's state from the policies the Server lists: the
// recommenders run against it when control metrics arrive. Policies that are
// no longer listed, or that can't be processed, are dropped from the state.
func (e *Engine) Tick() {
	policies, err := e.fetchPolicies()
	if err != nil {
		slog.Error("Error fetching policies", "error", err)
		return
	}
	state := make(State, len(policies))
	for _, policy := range policies {
		policyID, clusterName := PolicyIDFromProto(policy.GetId())
		if clusterName != e.clusterName {
			slog.Error("Received a policy from another cluster", "unexpected_cluster_name", clusterName)
			continue
		}
		if ps := e.processPolicy(policy); ps != nil {
			state[policyID] = ps
		}
	}
	e.state = state
}

// processPolicy prepares a policy for its recommenders: it registers the
// metrics they own on the policy and reads the policy's workload. It returns
// nil if the policy is to be skipped: a recommender is misconfigured (see
// MetricsOwner) or the workload can't be read.
func (e *Engine) processPolicy(policy *pb.Policy) *PolicyState {
	// Make sure the metrics owned by our recommenders are registered on the
	// policy, so the providers collect them before we read them back.
	policy, err := e.syncRecommenderMetrics(policy)
	if err != nil {
		slog.Error("Failed to sync recommender metrics", "policy", policy.GetId().String(), "error", err)
		return nil
	}

	workload, err := e.fetchWorkload(policy.GetId().GetNamespace(), policy.GetId().GetName())
	if err != nil {
		slog.Error("Failed to fetch the workload", "policy", policy.GetId().GetName(), "error", err)
		return nil
	}
	return &PolicyState{policy: policy, workload: workload}
}

func (e *Engine) fetchPolicies() ([]*pb.Policy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := e.client.ListPolicies(ctx, pb.ListPoliciesRequest_builder{
		ClusterName: proto.String(e.clusterName),
	}.Build())
	if err != nil {
		return nil, err
	}
	return resp.GetPolicies(), nil
}

// fetchWorkload reads the workload of a policy: its pods and their current
// requests, as last reported by the controller.
func (e *Engine) fetchWorkload(ns, name string) (*pb.Workload, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return e.client.GetWorkload(ctx, pb.GetWorkloadRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String(e.clusterName), Namespace: proto.String(ns), Name: proto.String(name)}.Build(),
	}.Build())
}

type namedRecommendation struct {
	name           string
	recommendation *pb.Recommendation
}

func (e *Engine) pushRecommendations(ctx context.Context, policy *pb.Policy, recommendations []namedRecommendation) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for _, r := range recommendations {
		req := pb.UpdateRecommenderStateRequest_builder{
			Id:              pb.PolicyId_builder{ClusterName: proto.String(e.clusterName), Namespace: proto.String(policy.GetId().GetNamespace()), Name: proto.String(policy.GetId().GetName())}.Build(),
			RecommenderName: proto.String(r.name),
			Recommendation:  r.recommendation,
		}.Build()
		if r.recommendation != nil && r.recommendation.HasReplicas() {
			slog.Debug("Pushing workload replicas recommendation", "policy", policy.GetId().GetName(), "recommender", r.name, "desired", r.recommendation.GetReplicas())
		}
		_, err := e.client.UpdateRecommenderState(ctx, req)
		if err != nil {
			slog.Error("Failed to push recommendation", "policy", policy.GetId().GetName(), "recommender", r.name, "error", err)
		}
	}
}

// recommenderFor returns the implementation backing a RecommenderClass, or an
// error if the class is unknown to this engine.
func (e *Engine) recommenderFor(recommenderClass string) (Recommender, error) {
	class, err := e.recommenderClassLister.Get(recommenderClass)
	if err != nil {
		return nil, err
	}
	rec, ok := e.recommenders[class.Spec.Type]
	if !ok {
		return nil, fmt.Errorf("unknown recommender type %q", class.Spec.Type)
	}
	return rec, nil
}
