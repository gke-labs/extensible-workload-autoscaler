// Package policy provides helpers to interpret the policies served by the XAS
// Control Plane.
package policy

import (
	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

// MetricDefinitions returns every metric a policy defines, the policy-wide ones as well
// as the ones owned by its recommenders, in no particular order.
func MetricDefinitions(p *pb.Policy) []*pb.MetricDefinition {
	metrics := make([]*pb.MetricDefinition, 0, len(p.Metrics))
	metrics = append(metrics, p.Metrics...)
	for _, ml := range p.RecommenderMetrics {
		metrics = append(metrics, ml.GetDefinitions()...)
	}
	return metrics
}
