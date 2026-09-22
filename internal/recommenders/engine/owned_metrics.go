package engine

import (
	"context"
	"log/slog"
	"maps"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
)

// MetricsOwner is implemented by recommenders that need metrics beyond the ones
// the policy declares. The engine registers the returned definitions on the
// policy under the recommender's name, so metric providers collect them like
// any other control metric while they stay scoped to their owner.
type MetricsOwner interface {
	// OwnedMetrics returns the metrics the recommender needs for this
	// definition. The returned definitions are owned by def.Name; the engine
	// stamps the owner on them.
	OwnedMetrics(def *pb.RecommenderDefinition) []*pb.MetricDefinition
}

// syncRecommenderMetrics registers the metrics owned by the recommenders of a
// policy.
//
// It takes the ScalingPolicy from the Server as the argument `pol`, and
// returns an updated version of this ScalingPolicy where all the recommender-owned
// metrics have been added.
//
// Only the metrics of the recommenders this binary manages (e.g. 'linear') are
// updated; others are left untouched.
func (e *Engine) syncRecommenderMetrics(pol *pb.Policy) *pb.Policy {
	owned := make(map[string]*pb.MetricDefinitionList)

	for _, recDef := range pol.Scaling {
		rec, err := e.recommenderFor(recDef.Recommender)
		if err != nil {
			continue
		}
		owner, ok := rec.(MetricsOwner)
		if !ok {
			continue
		}

		ownedMetrics := owner.OwnedMetrics(recDef)
		owned[recDef.Name] = &pb.MetricDefinitionList{Definitions: ownedMetrics}
	}

	// Only send the entries that changed. An entry the mask selects but the
	// request omits is removed from the policy.
	var changedPaths []string
	changed := make(map[string]*pb.MetricDefinitionList)
	for name, list := range owned {
		current, registered := pol.RecommenderMetrics[name]
		switch {
		case len(list.Definitions) == 0 && (!registered || len(current.Definitions) == 0):
			// Server policy correctly has no metric owned by this recommender
			continue
		case proto.Equal(current, list):
			// Server policy already knows about the metrics owned by this recommender
			continue
		default:
			changed[name] = list
			changedPaths = append(changedPaths, policy.RecommenderMetricsPath(name))
		}
	}
	if len(changed) == 0 {
		return pol
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.UpdatePolicyRequest{
		Policy:     &pb.Policy{Id: pol.Id, RecommenderMetrics: changed},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: changedPaths},
	}
	if _, err := e.client.UpdatePolicy(ctx, req); err != nil {
		slog.Error("Failed to register recommender owned metrics", "policy", pol.Id.Name, "error", err)
		return pol
	}
	slog.Debug("Registered recommender owned metrics", "policy", pol.Id.Name, "recommenders", len(changed))

	updated := proto.Clone(pol).(*pb.Policy)
	if updated.RecommenderMetrics == nil {
		updated.RecommenderMetrics = make(map[string]*pb.MetricDefinitionList, len(changed))
	}
	maps.Copy(updated.RecommenderMetrics, changed) // Override new owned metrics
	return updated
}
