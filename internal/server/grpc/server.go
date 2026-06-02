package grpc

import (
	"context"
	"log/slog"
	"strings"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	pb.UnimplementedXASControlPlaneServer
	store store.MetricStore
	clock clock.Clock
}

func NewServer(s store.MetricStore, c clock.Clock) *Server {
	return &Server{store: s, clock: c}
}

// --- Policy Management ---

func (s *Server) UpdatePolicy(ctx context.Context, req *pb.UpdatePolicyRequest) (*pb.Policy, error) {
	if err := validateUpdatePolicyRequest(req); err != nil {
		return nil, err
	}
	if err := s.store.SetPolicy(req.Policy.Id.ClusterName, req.Policy); err != nil {
		return nil, err
	}
	s.store.CalculateAll()
	return req.Policy, nil
}

func (s *Server) DeletePolicy(ctx context.Context, req *pb.DeletePolicyRequest) (*emptypb.Empty, error) {
	if err := validateDeletePolicyRequest(req); err != nil {
		return nil, err
	}
	if err := s.store.DeletePolicy(req.Id); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) ListPolicies(ctx context.Context, req *pb.ListPoliciesRequest) (*pb.ListPoliciesResponse, error) {
	if err := validateListPoliciesRequest(req); err != nil {
		return nil, err
	}
	policies := s.store.ListPolicies(req.ClusterName)
	return &pb.ListPoliciesResponse{Policies: policies}, nil
}

func (s *Server) UpdateWorkload(ctx context.Context, req *pb.UpdateWorkloadRequest) (*pb.Workload, error) {
	if err := validateUpdateWorkloadRequest(req); err != nil {
		return nil, err
	}

	if err := s.store.UpdateWorkload(req); err != nil {
		return nil, status.Errorf(codes.NotFound, "policy not found")
	}
	return req.Workload, nil
}

func (s *Server) GetControlMetrics(ctx context.Context, req *pb.GetControlMetricsRequest) (*pb.ControlMetrics, error) {
	if err := validateGetControlMetricsRequest(req); err != nil {
		return nil, err
	}
	metrics, ok := s.store.GetControlMetrics(req.Id)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "policy not found")
	}
	return metrics, nil
}

func (s *Server) UpdateRecommenderState(ctx context.Context, req *pb.UpdateRecommenderStateRequest) (*pb.RecommenderState, error) {
	if err := validateUpdateRecommenderStateRequest(req); err != nil {
		return nil, err
	}
	if req.Vote != nil {
		slog.Debug("UpdateRecommenderState", "cluster", req.Id.ClusterName, "policy", req.Id.Name, "recommender", req.RecommenderName, "replicas", req.Vote.DesiredReplicas)
	} else {
		slog.Debug("UpdateRecommenderState: Clearing", "cluster", req.Id.ClusterName, "policy", req.Id.Name, "recommender", req.RecommenderName)
	}
	if err := s.store.UpdateRecommenderState(req); err != nil {
		if err.Error() == "policy not found" {
			return nil, status.Errorf(codes.NotFound, "policy not found")
		}
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	res := &pb.RecommenderState{
		Name: req.RecommenderName,
	}
	// Note: We don't necessarily return the full enriched status here unless we fetch it from store
	// For now, let's keep it simple or fetch it if needed by clients.
	// Most clients (recommenders) don't use the response.

	return res, nil
}

func (s *Server) GetRecommendation(ctx context.Context, req *pb.GetRecommendationRequest) (*pb.GetRecommendationResponse, error) {
	if err := validateGetRecommendationRequest(req); err != nil {
		return nil, err
	}
	rec, ok := s.store.GetRecommendation(req.Id)
	if !ok {
		slog.Debug("GetRecommendation: Not Found", "cluster", req.Id.ClusterName, "policy", req.Id.Name)
		return nil, status.Errorf(codes.NotFound, "policy not found")
	}
	target := int32(0)
	if rec.Recommendation != nil {
		target = rec.Recommendation.TargetReplicas
	}
	slog.Debug("GetRecommendation: Serving", "cluster", req.Id.ClusterName, "policy", req.Id.Name, "replicas", target)
	return rec, nil
}

func (s *Server) IngestMetrics(ctx context.Context, req *pb.IngestMetricsRequest) (*pb.IngestMetricsResponse, error) {
	if err := validateIngestMetricsRequest(req); err != nil {
		return nil, err
	}
	slog.Debug("IngestMetrics", "cluster", req.ClusterName, "policies", len(req.Policies))
	if err := s.store.AddBatch(req); err != nil {
		if strings.Contains(err.Error(), "policy not found") {
			return nil, status.Errorf(codes.NotFound, "%v", err)
		}
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &pb.IngestMetricsResponse{Success: true}, nil
}
