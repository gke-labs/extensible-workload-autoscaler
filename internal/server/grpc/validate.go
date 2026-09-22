package grpc

import (
	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validateUpdatePolicyRequest(req *pb.UpdatePolicyRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	if req.Policy == nil {
		return status.Errorf(codes.InvalidArgument, "policy is required")
	}
	if err := validatePolicyId(req.Policy.Id); err != nil {
		return err
	}
	if err := policy.ValidateUpdateMask(req.GetUpdateMask()); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid update_mask: %v", err)
	}
	for _, m := range req.Policy.Metrics {
		if m.GetRecommenderName() != "" {
			return status.Errorf(codes.InvalidArgument, "metric %s: recommender_name must be empty for policy-wide metrics", m.GetName())
		}
		if err := validateMetricDefinition(m); err != nil {
			return err
		}
	}
	for owner, list := range req.Policy.RecommenderMetrics {
		if owner == "" {
			return status.Errorf(codes.InvalidArgument, "recommender_metrics: recommender name is required")
		}
		for _, m := range list.GetDefinitions() {
			if m.GetRecommenderName() != owner {
				return status.Errorf(codes.InvalidArgument, "metric %s: recommender_name %q must match its owner %q", m.GetName(), m.GetRecommenderName(), owner)
			}
			if err := validateMetricDefinition(m); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateMetricDefinition enforces that a metric definition is named and
// carries exactly one fully specified intent.
func validateMetricDefinition(m *pb.MetricDefinition) error {
	if m == nil {
		return status.Errorf(codes.InvalidArgument, "metric definition is required")
	}
	if m.Name == "" {
		return status.Errorf(codes.InvalidArgument, "metric name is required")
	}
	// INTENT VALIDATION
	intents := 0
	if m.Gauge != nil {
		intents++
	}
	if m.Rate != nil {
		intents++
		if m.Rate.Window == "" {
			return status.Errorf(codes.InvalidArgument, "metric %s: window is required for rate intent", m.Name)
		}
	}
	if m.Distribution != nil {
		intents++
		if m.Distribution.Percentile == "" {
			return status.Errorf(codes.InvalidArgument, "metric %s: percentile is required for distribution intent", m.Name)
		}
	}
	if m.DecayingDistribution != nil {
		intents++
		if m.DecayingDistribution.HalfLife == "" {
			return status.Errorf(codes.InvalidArgument, "metric %s: half_life is required for decaying_distribution intent", m.Name)
		}
		if m.DecayingDistribution.BucketSize == "" {
			return status.Errorf(codes.InvalidArgument, "metric %s: bucket_size is required for decaying_distribution intent", m.Name)
		}
		if m.DecayingDistribution.Percentile == "" {
			return status.Errorf(codes.InvalidArgument, "metric %s: percentile is required for decaying_distribution intent", m.Name)
		}
	}

	if intents == 0 {
		return status.Errorf(codes.InvalidArgument, "metric %s: exactly one intent must be specified", m.Name)
	}
	if intents > 1 {
		return status.Errorf(codes.InvalidArgument, "metric %s: only one intent can be specified", m.Name)
	}
	return nil
}

func validateDeletePolicyRequest(req *pb.DeletePolicyRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validatePolicyId(req.Id)
}

func validateListPoliciesRequest(req *pb.ListPoliciesRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validateClusterName(req.ClusterName)
}

func validateUpdateWorkloadRequest(req *pb.UpdateWorkloadRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	if err := validatePolicyId(req.Id); err != nil {
		return err
	}
	if req.Workload == nil {
		return status.Errorf(codes.InvalidArgument, "workload is required")
	}
	for _, p := range req.Workload.Pods {
		if p.Name == "" {
			return status.Errorf(codes.InvalidArgument, "pod name is required")
		}
	}
	return nil
}

func validateGetControlMetricsRequest(req *pb.GetControlMetricsRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validatePolicyId(req.Id)
}

func validateUpdateRecommenderStateRequest(req *pb.UpdateRecommenderStateRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validatePolicyId(req.Id)
}

func validateGetRecommendationRequest(req *pb.GetRecommendationRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validatePolicyId(req.Id)
}

func validateIngestMetricsRequest(req *pb.IngestMetricsRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validateClusterName(req.ClusterName)
}

func validatePolicyId(id *pb.PolicyId) error {
	if id == nil {
		return status.Errorf(codes.InvalidArgument, "policy id is required")
	}
	if id.ClusterName == "" {
		return status.Errorf(codes.InvalidArgument, "cluster_name is required")
	}
	if id.Namespace == "" {
		return status.Errorf(codes.InvalidArgument, "namespace is required")
	}
	if id.Name == "" {
		return status.Errorf(codes.InvalidArgument, "name is required")
	}
	return nil
}

func validateClusterName(c string) error {
	if c == "" {
		return status.Errorf(codes.InvalidArgument, "cluster_name is required")
	}
	return nil
}
