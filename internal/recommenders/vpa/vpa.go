package vpa

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

const (
	minCPUMilli                 = 10   // 10 millicores: represents the minimum CPU value returned by the recommender
	minMEMMiB                   = 10   // 10 MiB: represents the minumum Memory value returned by the recommender
	defaultCPUSafetyMarginFloat = 1.15 // represents 15% headroom
	defaultMemSafetyMarginFloat = 1.15 // represents 15% headroom
)

type VPARecommender struct{}

// config holds the parsed configuration for this recommender instance
type config struct {
	cpuMetric       string
	memMetric       string
	cpuSafetyMargin float64
	memSafetyMargin float64
}

// Recommend calculates the resource recommendations based on control metrics
func (r *VPARecommender) Recommend(def *pb.RecommenderDefinition, state *pb.ControlMetrics) *pb.RecommenderVote {
	var warnings []string

	//Parse the configuration from def.Params using parseConfig.
	cfg, err := parseConfig(def)

	// If parsing fails, return a RecommenderVote with the error message in the Message field.
	if err != nil {
		return &pb.RecommenderVote{
			DesiredReplicas: 0, // TODO: Update after fixing this issue: https://github.com/gke-labs/extensible-workload-autoscaler/issues/28
			IsActive:        false,
			Message:         fmt.Sprintf("Unable to parse recommender configuration: %v", err),
		}
	}

	if state == nil || state.Values == nil {
		return &pb.RecommenderVote{
			DesiredReplicas: 0, // TODO: Update after fixing this issue: https://github.com/gke-labs/extensible-workload-autoscaler/issues/28
			IsActive:        false,
			Message:         "ControlMetrics is missing",
		}
	}

	requests := make(map[string]string, 2)
	limits := make(map[string]string, 2)

	cpuMetricFound := false

	// If cpuMetric is configured (is not empty):
	if cfg.cpuMetric != "" {
		val, found := state.Values[cfg.cpuMetric]
		if found {
			// Convert CPU from fractional cores (e.g., 0.15) to millicores (e.g., 150m)
			// with the safety margin, rounding up to the nearest whole millicore.
			cpuMilli := int64(math.Ceil(val * cfg.cpuSafetyMargin * 1000))
			cpuVal := max(minCPUMilli, cpuMilli)
			cpuValString := fmt.Sprintf("%dm", cpuVal)
			cpuMetricFound = true
			requests["cpu"] = cpuValString
			limits["cpu"] = cpuValString

		} else {
			warnings = append(warnings, fmt.Sprintf("cpuMetric %q not found in state", cfg.cpuMetric))
		}
	}

	// Setting default Mem MiB value
	memMetricFound := false

	if cfg.memMetric != "" {
		val, found := state.Values[cfg.memMetric]
		if found {
			// Convert Memory from bytes (e.g., 268435456) to MiB (e.g., 256MiB)
			// with the safety margin, rounding up to the nearest whole MiB.
			memMiB := int64(math.Ceil(val * cfg.memSafetyMargin / 1024 / 1024))
			memVal := max(memMiB, minMEMMiB)
			memValString := fmt.Sprintf("%dMi", memVal)
			memMetricFound = true
			requests["memory"] = memValString
			limits["memory"] = memValString

		} else {
			warnings = append(warnings, fmt.Sprintf("memMetric %q not found in state", cfg.memMetric))
		}
	}

	// If no valid recommendations were generated, returning a vote with an error
	if !cpuMetricFound && !memMetricFound {
		warnings = append(warnings, "Unable to create recommendation as no value memory or cpu values were found")

		return &pb.RecommenderVote{
			DesiredReplicas: 0, // TODO: Update after fixing this issue: https://github.com/gke-labs/extensible-workload-autoscaler/issues/28
			IsActive:        false,
			Message:         fmt.Sprintf("No Recommendations generated: %s", strings.Join(warnings, "; ")),
		}
	} else {
		return &pb.RecommenderVote{
			DesiredReplicas: 0, // TODO: Update after fixing this issue: https://github.com/gke-labs/extensible-workload-autoscaler/issues/28
			IsActive:        true,
			WorkloadResources: &pb.ResourceRecommendation{
				Requests: requests,
				Limits:   limits,
			},
			Message: func() string {
				if len(warnings) > 0 {
					return fmt.Sprintf("Recommendation generated with warnings: %s", strings.Join(warnings, "; "))
				} else {
					return "Recommendation generated successfully."
				}
			}(),
		}
	}
}

// parseConfig extracts and validates parameters from the recommender definition
func parseConfig(def *pb.RecommenderDefinition) (*config, error) {
	cpuMetric := def.Params["cpu-metric"]
	memMetric := def.Params["mem-metric"]

	if cpuMetric == "" && memMetric == "" {
		return nil, fmt.Errorf("cpu-metric and mem-metric are undefined. For VPA to work at least one of them needs to be defined.")
	}
	config := &config{
		cpuMetric:       cpuMetric,
		memMetric:       memMetric,
		cpuSafetyMargin: defaultCPUSafetyMarginFloat,
		memSafetyMargin: defaultMemSafetyMarginFloat,
	}

	cpuSafetyMargin := def.Params["cpu-safety-margin"]
	if cpuSafetyMargin != "" {
		cpuSafetyMarginFloat, err := strconv.ParseFloat(strings.TrimSpace(cpuSafetyMargin), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid cpu-safety-margin provided %s. The value needs to represent a float64. err: %w", cpuSafetyMargin, err)
		} else {
			config.cpuSafetyMargin = cpuSafetyMarginFloat
		}
	}

	memSafetyMargin := def.Params["mem-safety-margin"]

	if memSafetyMargin != "" {
		memSafetyMarginFloat, err := strconv.ParseFloat(strings.TrimSpace(memSafetyMargin), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid mem-safety-margin provided %s. The value needs to represent a float64. err: %w", memSafetyMargin, err)
		} else {
			config.memSafetyMargin = memSafetyMarginFloat
		}
	}
	return config, nil
}
