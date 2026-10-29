// Package policy provides helpers to interpret the policies served by the xAS
// Server.
package policy

import (
	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

// MetricDefinitions returns every metric a policy defines, the policy-wide ones as well
// as the ones owned by its recommenders, in no particular order.
func MetricDefinitions(p *pb.Policy) []*pb.MetricDefinition {
	metrics := make([]*pb.MetricDefinition, 0, len(p.GetMetrics()))
	metrics = append(metrics, p.GetMetrics()...)
	for _, ml := range p.GetRecommenderMetrics() {
		metrics = append(metrics, ml.GetDefinitions()...)
	}
	return metrics
}
