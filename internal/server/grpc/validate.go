package grpc

import (
	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
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
	for _, m := range req.Policy.Metrics {
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
