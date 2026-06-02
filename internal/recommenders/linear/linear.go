package linear

import (
	"fmt"
	"math"
	"strconv"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1"
)

type LinearRecommender struct{}

type config struct {
	metric string
	target float64
}

func (r *LinearRecommender) Recommend(def *pb.RecommenderDefinition, state *pb.ControlMetrics) *pb.RecommenderVote {
	cfg, err := parseConfig(def)
	if err != nil {
		return &pb.RecommenderVote{
			Message: err.Error(),
		}
	}

	val, ok := state.Values[cfg.metric]
	if !ok {
		return &pb.RecommenderVote{
			Message: fmt.Sprintf("metric '%s' not found", cfg.metric),
		}
	}

	ratio := val / cfg.target
	desired := int32(math.Ceil(float64(state.ReadyReplicas) * ratio))

	return &pb.RecommenderVote{
		DesiredReplicas: desired,
		IsActive:        true,
	}
}

func parseConfig(def *pb.RecommenderDefinition) (*config, error) {
	metric := def.Params["metric"]
	if metric == "" {
		return nil, fmt.Errorf("missing metric param")
	}

	targetStr := def.Params["target"]
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
