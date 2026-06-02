package engine

import (
	"context"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/recommenders/cron"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/recommenders/linear"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
)

type Engine struct {
	grpcConn          *grpc.ClientConn
	client            pb.XASControlPlaneClient
	recommenderLister listers.RecommenderClassLister
	linear            *linear.LinearRecommender
	cron              *cron.Recommender

	clusterName string
}

func NewEngine(recommenderLister listers.RecommenderClassLister, nodeLister corelisters.NodeLister, serverAddress, clusterName string) *Engine {
	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("did not connect", "error", err)
		os.Exit(1)
	}
	client := pb.NewXASControlPlaneClient(conn)

	return &Engine{
		grpcConn:          conn,
		client:            client,
		recommenderLister: recommenderLister,
		linear:            &linear.LinearRecommender{},
		cron:              &cron.Recommender{},
		clusterName:       clusterName,
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
	// Fetch State (Control Metrics + ReadyReplicas)
	state, err := e.fetchState(policy.Id.Namespace, policy.Id.Name)
	if err != nil {
		return
	}

	var decisions []decision

	processDefs := func(defs []*pb.RecommenderDefinition, phase string) {
		for _, def := range defs {
			class, err := e.recommenderLister.Get(def.Recommender)
			if err != nil {
				slog.Warn("RecommenderClass not found for policy", "recommender", def.Recommender, "policy", policy.Id.Name)
				continue
			}

			// Merge Params: Class Config (Defaults) + Def Params (Overrides)
			mergedParams := make(map[string]string)
			for k, v := range class.Spec.Config {
				mergedParams[k] = v
			}
			for k, v := range def.Params {
				mergedParams[k] = v
			}

			// Construct new definition with merged params
			defCopy := &pb.RecommenderDefinition{
				Recommender: def.Recommender,
				Name:        def.Name,
				Mode:        def.Mode,
				Params:      mergedParams,
			}

			var v *pb.RecommenderVote
			switch class.Spec.Type {
			case "Linear":
				v = e.linear.Recommend(defCopy, state)
			case "Cron":
				v = e.cron.Recommend(defCopy, state)
			default:
				slog.Warn("Unknown recommender type in class", "type", class.Spec.Type, "class", class.Name)
			}

			if v != nil {
				decisions = append(decisions, decision{name: def.Name, vote: v})
			}
		}
	}

	processDefs(policy.Activation, "Activation")
	processDefs(policy.Scaling, "Scaling")

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

func (e *Engine) fetchState(ns, name string) (*pb.ControlMetrics, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return e.client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{
		Id: &pb.PolicyId{ClusterName: e.clusterName, Namespace: ns, Name: name},
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

		slog.Debug("Pushing decision", "policy", policy.Id.Name, "recommender", d.name, "desired", d.vote.DesiredReplicas)
		_, err := e.client.UpdateRecommenderState(ctx, req)
		if err != nil {
			slog.Error("Failed to update decision", "policy", policy.Id.Name, "recommender", d.name, "error", err)
		}
	}
}
