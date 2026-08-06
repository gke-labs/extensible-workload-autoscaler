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

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/recommenders/cron"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/recommenders/linear"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/recommenders/vpa"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
)

type Recommender interface {
	Recommend(def *pb.RecommenderDefinition, metrics, ownedMetrics *pb.ControlMetrics) *pb.RecommenderVote
}

type Engine struct {
	grpcConn               *grpc.ClientConn
	client                 pb.XASControlPlaneClient
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
	client := pb.NewXASControlPlaneClient(conn)

	return &Engine{
		grpcConn:               conn,
		client:                 client,
		recommenderClassLister: recommenderClassLister,
		recommenders: map[string]Recommender{
			"Linear": &linear.LinearRecommender{},
			"Cron":   &cron.Recommender{},
			"VPA":    &vpa.VPARecommender{},
		},
		clusterName: clusterName,
	}
}

func (e *Engine) Run(ctx context.Context) {
	defer e.grpcConn.Close()
	ticker := time.NewTicker(5 * time.Second) // Fast loop
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.Tick()
		case <-ctx.Done():
			return
		}
	}
}

func (e *Engine) Tick() {
	// 1. Fetch Policies
	policies, err := e.fetchPolicies()
	if err != nil {
		slog.Error("Error fetching policies", "error", err)
		return
	}

	// 2. Iterate
	for _, policy := range policies {
		e.processPolicy(policy)
	}
}

func (e *Engine) processPolicy(policy *pb.Policy) {
	// Make sure the metrics owned by our recommenders are registered on the
	// policy, so the providers collect them before we read them back.
	policy = e.syncRecommenderMetrics(policy)

	metrics, err := e.fetchControlMetrics(policy.Id.Namespace, policy.Id.Name, "")
	if err != nil {
		return
	}

	// Call each recommender to get their recommendation. Provide both policy-wide
	// metrics and the metrics owned by the recommender.
	var decisions []decision
	for _, def := range slices.Concat(policy.Activation, policy.Scaling) {
		ownedMetrics, err := e.fetchControlMetrics(policy.Id.Namespace, policy.Id.Name, def.Name)
		if err != nil {
			return
		}

		rec, err := e.recommenderFor(def.Recommender)
		if err != nil {
			slog.Warn("RecommenderClass not found for policy", "recommender", def.Recommender, "policy", policy.Id.Name, "error", err)
			return
		}
		if v := rec.Recommend(def, metrics, ownedMetrics); v != nil {
			decisions = append(decisions, decision{name: def.Name, vote: v})
		}
	}

	slog.Debug("Recommender decisions generated", "policy", policy.Id.Name, "count", len(decisions))
	if len(decisions) > 0 {
		e.pushDecisions(policy, decisions)
	}
}

func (e *Engine) fetchPolicies() ([]*pb.Policy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := e.client.ListPolicies(ctx, &pb.ListPoliciesRequest{
		ClusterName: e.clusterName,
	})
	if err != nil {
		return nil, err
	}
	return resp.Policies, nil
}

// fetchControlMetrics reads the control metrics of a policy. An empty recommenderName
// reads the policy-wide metrics, otherwise only the metrics owned by that
// recommender are returned.
func (e *Engine) fetchControlMetrics(ns, name, recommenderName string) (*pb.ControlMetrics, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return e.client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{
		Id:              &pb.PolicyId{ClusterName: e.clusterName, Namespace: ns, Name: name},
		RecommenderName: recommenderName,
	})
}

type decision struct {
	name string
	vote *pb.RecommenderVote
}

func (e *Engine) pushDecisions(policy *pb.Policy, decisions []decision) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, d := range decisions {
		req := &pb.UpdateRecommenderStateRequest{
			Id:              &pb.PolicyId{ClusterName: e.clusterName, Namespace: policy.Id.Namespace, Name: policy.Id.Name},
			RecommenderName: d.name,
			Vote:            d.vote,
		}
		if d.vote != nil && d.vote.Replicas != nil {
			slog.Debug("Pushing workload replicas recommendation", "policy", policy.Id.Name, "recommender", d.name, "desired", *d.vote.Replicas)
		}
		_, err := e.client.UpdateRecommenderState(ctx, req)
		if err != nil {
			slog.Error("Failed to update decision", "policy", policy.Id.Name, "recommender", d.name, "error", err)
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
