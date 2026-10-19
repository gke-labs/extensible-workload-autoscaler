package grpc

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store"
)

type Server struct {
	pb.UnimplementedXASServerServer
	store store.ServerStore
	clock clock.Clock
}

func NewServer(s store.ServerStore, c clock.Clock) *Server {
	return &Server{store: s, clock: c}
}

// --- Policy Management ---

func (s *Server) CreatePolicy(ctx context.Context, req *pb.CreatePolicyRequest) (*pb.Policy, error) {
	if err := validateCreatePolicyRequest(req); err != nil {
		return nil, err
	}
	created, err := s.store.CreatePolicy(req.GetPolicy().GetId().GetClusterName(), req.GetPolicy())
	if errors.Is(err, store.ErrPolicyAlreadyExists) {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	}
	if errors.Is(err, store.ErrUnknownEtag) {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if err != nil {
		return nil, err
	}
	s.store.CalculateAll()
	return created, nil
}

func (s *Server) UpdatePolicy(ctx context.Context, req *pb.UpdatePolicyRequest) (*pb.Policy, error) {
	if err := validateUpdatePolicyRequest(req); err != nil {
		return nil, err
	}
	updated, err := s.store.UpdatePolicy(req.GetPolicy().GetId().GetClusterName(), req.GetAllowMissing(), req.GetPolicy())
	if errors.Is(err, store.ErrStaleEtag) {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	if errors.Is(err, store.ErrUnknownPolicy) || errors.Is(err, store.ErrUnknownEtag) {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	if err != nil {
		return nil, err
	}
	s.store.CalculateAll()
	return updated, nil
}

func (s *Server) DeletePolicy(ctx context.Context, req *pb.DeletePolicyRequest) (*emptypb.Empty, error) {
	if err := validateDeletePolicyRequest(req); err != nil {
		return nil, err
	}
	if err := s.store.DeletePolicy(req.GetId()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) GetPolicy(ctx context.Context, req *pb.GetPolicyRequest) (*pb.Policy, error) {
	if err := validateGetPolicyRequest(req); err != nil {
		return nil, err
	}
	policy := s.store.GetPolicy(req.GetId())
	if policy == nil {
		return nil, status.Errorf(codes.NotFound, "policy not found")
	}
	return policy, nil
}

func (s *Server) ListPolicies(ctx context.Context, req *pb.ListPoliciesRequest) (*pb.ListPoliciesResponse, error) {
	if err := validateListPoliciesRequest(req); err != nil {
		return nil, err
	}
	policies := s.store.ListPolicies(req.GetClusterName())
	return pb.ListPoliciesResponse_builder{Policies: policies}.Build(), nil
}

func (s *Server) UpdateWorkload(ctx context.Context, req *pb.UpdateWorkloadRequest) (*pb.Workload, error) {
	if err := validateUpdateWorkloadRequest(req); err != nil {
		return nil, err
	}

	if err := s.store.UpdateWorkload(req); err != nil {
		return nil, status.Errorf(codes.NotFound, "policy not found")
	}
	return req.GetWorkload(), nil
}

func (s *Server) GetControlMetrics(ctx context.Context, req *pb.GetControlMetricsRequest) (*pb.ControlMetrics, error) {
	if err := validateGetControlMetricsRequest(req); err != nil {
		return nil, err
	}
	metrics, ok := s.store.GetControlMetrics(req.GetId(), req.GetRecommenderName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "policy not found")
	}
	return metrics, nil
}

// WatchControlMetrics streams the control metrics of every policy in the
// requested cluster. The stream stays open until the client cancels it, or
// until the client falls too far behind, in which case it is terminated with
// ResourceExhausted and must be reopened.
func (s *Server) WatchControlMetrics(req *pb.WatchControlMetricsRequest, stream grpc.ServerStreamingServer[pb.WatchControlMetricsResponse]) error {
	if err := validateWatchControlMetricsRequest(req); err != nil {
		return err
	}
	slog.Debug("WatchControlMetrics: Started", "cluster", req.GetClusterName())
	defer slog.Debug("WatchControlMetrics: Stopped", "cluster", req.GetClusterName())

	events, unwatch := s.store.Watch(req.GetClusterName())
	defer unwatch()

	if err := stream.SendHeader(nil); err != nil {
		return err
	}

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt, ok := <-events:
			if !ok {
				return status.Errorf(codes.ResourceExhausted, "watcher too slow, events were dropped: reopen the stream")
			}
			res := pb.WatchControlMetricsResponse_builder{
				Id:                        evt.PolicyID(),
				ControlMetrics:            evt.ControlMetrics(),
				RecommenderControlMetrics: evt.RecommenderControlMetrics(),
			}.Build()
			if err := stream.Send(res); err != nil {
				return err
			}
		}
	}
}

func (s *Server) GetWorkload(ctx context.Context, req *pb.GetWorkloadRequest) (*pb.Workload, error) {
	if err := validateGetWorkloadRequest(req); err != nil {
		return nil, err
	}
	workload, ok := s.store.GetWorkload(req.GetId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "policy not found")
	}
	return workload, nil
}

func (s *Server) UpdateRecommenderState(ctx context.Context, req *pb.UpdateRecommenderStateRequest) (*pb.RecommenderState, error) {
	if err := validateUpdateRecommenderStateRequest(req); err != nil {
		return nil, err
	}
	if req.HasRecommendation() {
		slog.Debug("UpdateRecommenderState", "cluster", req.GetId().GetClusterName(), "policy", req.GetId().GetName(), "recommender", req.GetRecommenderName())
	} else {
		slog.Debug("UpdateRecommenderState: Clearing", "cluster", req.GetId().GetClusterName(), "policy", req.GetId().GetName(), "recommender", req.GetRecommenderName())
	}
	if err := s.store.UpdateRecommenderState(req); err != nil {
		if err.Error() == "policy not found" {
			return nil, status.Errorf(codes.NotFound, "policy not found")
		}
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	res := pb.RecommenderState_builder{
		Name: proto.String(req.GetRecommenderName()),
	}.Build()
	// Note: We don't necessarily return the full enriched status here unless we fetch it from store
	// For now, let's keep it simple or fetch it if needed by clients.
	// Most clients (recommenders) don't use the response.

	return res, nil
}

func (s *Server) GetRecommendation(ctx context.Context, req *pb.GetRecommendationRequest) (*pb.GetRecommendationResponse, error) {
	if err := validateGetRecommendationRequest(req); err != nil {
		return nil, err
	}
	rec, ok := s.store.GetRecommendation(req.GetId())
	if !ok {
		slog.Debug("GetRecommendation: Not Found", "cluster", req.GetId().GetClusterName(), "policy", req.GetId().GetName())
		return nil, status.Errorf(codes.NotFound, "policy not found")
	}
	slog.Debug("GetRecommendation: Serving", "cluster", req.GetId().GetClusterName(), "policy", req.GetId().GetName())
	return rec, nil
}

func (s *Server) IngestMetrics(ctx context.Context, req *pb.IngestMetricsRequest) (*pb.IngestMetricsResponse, error) {
	if err := validateIngestMetricsRequest(req); err != nil {
		return nil, err
	}
	slog.Debug("IngestMetrics", "cluster", req.GetClusterName(), "policies", len(req.GetPolicies()))
	if err := s.store.AddBatch(req); err != nil {
		if strings.Contains(err.Error(), "policy not found") {
			return nil, status.Errorf(codes.NotFound, "%v", err)
		}
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return pb.IngestMetricsResponse_builder{Success: proto.Bool(true)}.Build(), nil
}
