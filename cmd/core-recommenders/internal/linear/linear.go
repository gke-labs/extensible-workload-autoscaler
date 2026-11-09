package linear

import (
	"fmt"
	"math"
	"strconv"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

type LinearRecommender struct{}

type config struct {
	metric string
	target float64
}

func (r *LinearRecommender) Recommend(def *pb.RecommenderDefinition, state, _ *pb.ControlMetrics, readyReplicas int32, _ *pb.Workload) *pb.Recommendation {
	cfg, err := parseConfig(def)
	if err != nil {
		return pb.Recommendation_builder{
			Message: new(err.Error()),
		}.Build()
	}

	val, ok := state.GetValues()[cfg.metric]
	if !ok {
		return pb.Recommendation_builder{
			Message: new(fmt.Sprintf("metric '%s' not found", cfg.metric)),
		}.Build()
	}

	ratio := val / cfg.target
	desired := int32(math.Ceil(float64(readyReplicas) * ratio))

	return pb.Recommendation_builder{
		Replicas: &desired,
		IsActive: new(true),
	}.Build()
}

func parseConfig(def *pb.RecommenderDefinition) (*config, error) {
	metric := def.GetParams()["metric"]
	if metric == "" {
		return nil, fmt.Errorf("missing metric param")
	}

	targetStr := def.GetParams()["target"]
	if targetStr == "" {
		return nil, fmt.Errorf("missing target param")
	}

	target, err := strconv.ParseFloat(targetStr, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid target format")
	}
	if target <= 0 {
		return nil, fmt.Errorf("target must be > 0")
	}

	return &config{
		metric: metric,
		target: target,
	}, nil
}
