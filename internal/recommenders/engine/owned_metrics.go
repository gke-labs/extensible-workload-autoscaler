package engine

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

// MetricsOwner is implemented by recommenders that need metrics beyond the ones
// the policy declares. The engine registers the returned definitions on the
// policy under the recommender's name, so metric providers collect them like
// any other control metric while they stay scoped to their owner.
type MetricsOwner interface {
	// OwnedMetrics returns the metrics the recommender needs for this
	// definition, given the policy-wide metrics (e.g. to reuse the settings of
	// the metrics the user declared). The returned definitions are owned by
	// def.Name; the engine stamps the owner on them.
	//
	// An error means the definition is invalid: the engine then drops the
	// metrics the recommender owns and reports it as inactive with the error,
	// without calling Recommend.
	OwnedMetrics(def *pb.RecommenderDefinition, policyMetrics []*pb.MetricDefinition) ([]*pb.MetricDefinition, error)
}

// syncRecommenderMetrics registers the metrics owned by the recommenders of a
// policy.
//
// It takes the ScalingPolicy from the Server as the argument `pol`, and
// returns an updated version of this ScalingPolicy where all the recommender-owned
// metrics have been added, along with the configuration errors returned by
// OwnedMetrics, keyed by recommender name.
//
// Only the metrics of the recommenders this binary manages (e.g. 'linear') are
// updated; others are left untouched.
func (e *Engine) syncRecommenderMetrics(pol *pb.Policy) (*pb.Policy, map[string]error) {
	owned := make(map[string]*pb.MetricDefinitionList)
	configErrs := make(map[string]error)

	for _, recDef := range slices.Concat(pol.Activation, pol.Scaling) {
		rec, err := e.recommenderFor(recDef.Recommender)
		if err != nil {
			continue
		}
		owner, ok := rec.(MetricsOwner)
		if !ok {
			continue
		}

		ownedMetrics, err := owner.OwnedMetrics(recDef, pol.Metrics)
		if err != nil {
			configErrs[recDef.Name] = err
			ownedMetrics = nil
		}
		stamped := make([]*pb.MetricDefinition, 0, len(ownedMetrics))
		for _, m := range ownedMetrics {
			m = proto.Clone(m).(*pb.MetricDefinition)
			m.RecommenderName = recDef.Name
			stamped = append(stamped, m)
		}
		owned[recDef.Name] = &pb.MetricDefinitionList{Definitions: stamped}
	}

	// changed holds the owned metric lists that differ from the policy on the
	// server: new or updated lists, and nil for entries to delete because
	// their recommender no longer owns any metric. If none differ, the policy
	// is returned as is; otherwise the whole policy is sent with those entries
	// merged in.
	changed := make(map[string]*pb.MetricDefinitionList)
	for name, list := range owned {
		current, registered := pol.RecommenderMetrics[name]
		switch {
		case len(list.Definitions) == 0 && !registered:
			// Server policy correctly has no metric owned by this recommender
			continue
		case len(list.Definitions) == 0:
			// The recommender no longer owns metrics: delete its entry.
			changed[name] = nil
		case proto.Equal(current, list):
			// Server policy already knows about the metrics owned by this recommender
			continue
		default:
			changed[name] = list
		}
	}
	if len(changed) == 0 {
		return pol, configErrs
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	desired := proto.Clone(pol).(*pb.Policy)

	if desired.RecommenderMetrics == nil {
		desired.RecommenderMetrics = make(map[string]*pb.MetricDefinitionList, len(changed))
	}
	for name, list := range changed {
		if list == nil {
			delete(desired.RecommenderMetrics, name)
		} else {
			desired.RecommenderMetrics[name] = list
		}
	}

	req := &pb.UpdatePolicyRequest{
		Policy: desired,
	}
	updated, err := e.client.UpdatePolicy(ctx, req)
	if err != nil {
		slog.Error("Failed to register recommender owned metrics", "policy", pol.Id.Name, "error", err)
		return pol, configErrs
	}
	slog.Debug("Registered recommender owned metrics", "policy", pol.Id.Name, "recommenders", len(changed))

	return updated, configErrs
}
