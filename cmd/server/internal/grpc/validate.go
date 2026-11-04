package grpc

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

func validateGetPolicyRequest(req *pb.GetPolicyRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validatePolicyId(req.GetId())
}

func validateCreatePolicyRequest(req *pb.CreatePolicyRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validateCreateUpdateRequest(req.GetPolicy())
}

func validateUpdatePolicyRequest(req *pb.UpdatePolicyRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validateCreateUpdateRequest(req.GetPolicy())
}

func validateCreateUpdateRequest(policy *pb.Policy) error {

	if policy == nil {
		return status.Errorf(codes.InvalidArgument, "policy is required")
	}
	if err := validatePolicyId(policy.GetId()); err != nil {
		return err
	}

	for _, m := range policy.GetMetrics() {
		if m.GetRecommenderName() != "" {
			return status.Errorf(codes.InvalidArgument, "metric %s: recommender_name must be empty for policy-wide metrics", m.GetName())
		}
		if err := validateMetricDefinition(m); err != nil {
			return err
		}
	}
	for owner, list := range policy.GetRecommenderMetrics() {
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
	if m.GetName() == "" {
		return status.Errorf(codes.InvalidArgument, "metric name is required")
	}
	if e := m.GetEligibility(); e != nil {
		grace := e.GetStartupGracePeriod().AsDuration()
		if grace < 0 {
			return status.Errorf(codes.InvalidArgument, "metric %s: eligibility.startup_grace_period must not be negative", m.GetName())
		}
		if grace > 0 && e.GetStartupSamples() {
			return status.Errorf(codes.InvalidArgument, "metric %s: eligibility.startup_grace_period is incompatible with startup_samples", m.GetName())
		}
	}
	// INTENT VALIDATION
	intents := 0
	if m.HasGauge() {
		intents++
	}
	if m.HasRate() {
		intents++
		if m.GetRate().GetWindow().AsDuration() <= 0 {
			return status.Errorf(codes.InvalidArgument, "metric %s: a positive window is required for rate intent", m.GetName())
		}
	}
	if m.HasDistribution() {
		intents++
		if m.GetDistribution().GetPercentile() == "" {
			return status.Errorf(codes.InvalidArgument, "metric %s: percentile is required for distribution intent", m.GetName())
		}
	}
	if m.HasDecayingDistribution() {
		intents++
		if m.GetDecayingDistribution().GetHalfLife().AsDuration() <= 0 {
			return status.Errorf(codes.InvalidArgument, "metric %s: a positive half_life is required for decaying_distribution intent", m.GetName())
		}
		if m.GetDecayingDistribution().GetBucketSize() == "" {
			return status.Errorf(codes.InvalidArgument, "metric %s: bucket_size is required for decaying_distribution intent", m.GetName())
		}
		if m.GetDecayingDistribution().GetPercentile() == "" {
			return status.Errorf(codes.InvalidArgument, "metric %s: percentile is required for decaying_distribution intent", m.GetName())
		}
	}

	if intents == 0 {
		return status.Errorf(codes.InvalidArgument, "metric %s: exactly one intent must be specified", m.GetName())
	}
	if intents > 1 {
		return status.Errorf(codes.InvalidArgument, "metric %s: only one intent can be specified", m.GetName())
	}
	return nil
}

func validateDeletePolicyRequest(req *pb.DeletePolicyRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validatePolicyId(req.GetId())
}

func validateListPoliciesRequest(req *pb.ListPoliciesRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	if req.GetPageSize() < 0 {
		return status.Errorf(codes.InvalidArgument, "page size must not be negative, got %d", req.GetPageSize())
	}
	return validateClusterName(req.GetClusterName())
}

func validateUpdateWorkloadRequest(req *pb.UpdateWorkloadRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	if err := validatePolicyId(req.GetId()); err != nil {
		return err
	}
	if !req.HasWorkload() {
		return status.Errorf(codes.InvalidArgument, "workload is required")
	}
	for _, p := range req.GetWorkload().GetPods() {
		if p.GetName() == "" {
			return status.Errorf(codes.InvalidArgument, "pod name is required")
		}
	}
	return nil
}

func validateGetControlMetricsRequest(req *pb.GetControlMetricsRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validatePolicyId(req.GetId())
}

func validateWatchControlMetricsRequest(req *pb.WatchControlMetricsRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validateClusterName(req.GetClusterName())
}

func validateWatchArbitratedRecommendationsRequest(req *pb.WatchArbitratedRecommendationsRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validateClusterName(req.GetClusterName())
}

func validateGetWorkloadRequest(req *pb.GetWorkloadRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validatePolicyId(req.GetId())
}

func validateUpdateRecommenderStateRequest(req *pb.UpdateRecommenderStateRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validatePolicyId(req.GetId())
}

func validateGetArbitratedRecommendationRequest(req *pb.GetArbitratedRecommendationRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	return validatePolicyId(req.GetId())
}

func validateIngestMetricsRequest(req *pb.IngestMetricsRequest) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is nil")
	}
	// The request timestamp is the default timestamp of the samples and the
	// freshness timestamp of the series, so it cannot be omitted.
	if !req.HasTimestamp() {
		return status.Errorf(codes.InvalidArgument, "timestamp is required")
	}
	return validateClusterName(req.GetClusterName())
}

func validatePolicyId(id *pb.PolicyId) error {
	if id == nil {
		return status.Errorf(codes.InvalidArgument, "policy id is required")
	}
	if id.GetClusterName() == "" {
		return status.Errorf(codes.InvalidArgument, "cluster_name is required")
	}
	if id.GetNamespace() == "" {
		return status.Errorf(codes.InvalidArgument, "namespace is required")
	}
	if id.GetName() == "" {
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
