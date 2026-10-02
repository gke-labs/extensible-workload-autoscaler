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
	containerName   string
	cpuMetric       string
	memMetric       string
	cpuSafetyMargin float64
	memSafetyMargin float64
	// Optional metrics for the bounds (e.g. P50 for the lower bound and P99
	// for the upper bound). Empty means that side is unbounded.
	cpuLowerBoundMetric string
	cpuUpperBoundMetric string
	memLowerBoundMetric string
	memUpperBoundMetric string
}

// Recommend calculates the resource recommendations based on control metrics
func (r *VPARecommender) Recommend(def *pb.RecommenderDefinition, state, _ *pb.ControlMetrics) *pb.Recommendation {
	var warnings []string

	//Parse the configuration from def.Params using parseConfig.
	cfg, err := parseConfig(def)

	// If parsing fails, return a Recommendation with the error message in the Message field.
	if err != nil {
		return &pb.Recommendation{
			IsActive: false,
			Message:  fmt.Sprintf("Unable to parse recommender configuration: %v", err),
		}
	}

	if state == nil {
		return &pb.Recommendation{
			IsActive: false,
			Message:  "ControlMetrics is missing",
		}
	}
	if len(state.PodContainerMetrics) == 0 {
		return &pb.Recommendation{
			IsActive: false,
			Message:  "PodMetrics is empty (ensure metrics are configured with scope: Container)",
		}
	}

	cpuMetricFound := false
	memMetricFound := false

	requests := make(map[string]string, 2)
	limits := make(map[string]string, 2)
	lowerBound := make(map[string]string, 2)
	upperBound := make(map[string]string, 2)

	// addBounds sets the bounds of a resource from their metrics, clamped so
	// that lower <= target <= upper. A bound whose metric is not configured,
	// or has no data, is left out (unbounded).
	addBounds := func(resName, unit, lowerParam, lowerMetric, upperParam, upperMetric string, target int64, value func(metric string) (int64, bool)) {
		if lowerMetric != "" {
			if v, ok := value(lowerMetric); ok {
				lowerBound[resName] = fmt.Sprintf("%d%s", min(v, target), unit)
			} else {
				warnings = append(warnings, fmt.Sprintf("%s %q not found in state, no lower bound for %s", lowerParam, lowerMetric, resName))
			}
		}
		if upperMetric != "" {
			if v, ok := value(upperMetric); ok {
				upperBound[resName] = fmt.Sprintf("%d%s", max(v, target), unit)
			} else {
				warnings = append(warnings, fmt.Sprintf("%s %q not found in state, no upper bound for %s", upperParam, upperMetric, resName))
			}
		}
	}

	// If cpuMetric is configured (is not empty):
	if cfg.cpuMetric != "" {
		cpuValue := func(metric string) (int64, bool) {
			return cpuMilliFor(state.PodContainerMetrics, cfg.containerName, metric, cfg.cpuSafetyMargin)
		}
		// Getting the max CPU usage value of that container across all pods
		cpuVal, found := cpuValue(cfg.cpuMetric)
		if found {
			cpuValString := fmt.Sprintf("%dm", cpuVal)
			cpuMetricFound = true
			requests["cpu"] = cpuValString
			limits["cpu"] = cpuValString
			addBounds("cpu", "m", "cpu-lower-bound-metric", cfg.cpuLowerBoundMetric, "cpu-upper-bound-metric", cfg.cpuUpperBoundMetric, cpuVal, cpuValue)

		} else {
			warnings = append(warnings, fmt.Sprintf("cpuMetric %q not found in state", cfg.cpuMetric))
		}
	}

	if cfg.memMetric != "" {
		memValue := func(metric string) (int64, bool) {
			return memMiBFor(state.PodContainerMetrics, cfg.containerName, metric, cfg.memSafetyMargin)
		}
		memVal, found := memValue(cfg.memMetric)
		if found {
			memValString := fmt.Sprintf("%dMi", memVal)
			memMetricFound = true
			requests["memory"] = memValString
			limits["memory"] = memValString
			addBounds("memory", "Mi", "mem-lower-bound-metric", cfg.memLowerBoundMetric, "mem-upper-bound-metric", cfg.memUpperBoundMetric, memVal, memValue)

		} else {
			warnings = append(warnings, fmt.Sprintf("memMetric %q not found in state", cfg.memMetric))
		}
	}

	// If no valid recommendations were generated, returning a recommendation with an error
	if !cpuMetricFound && !memMetricFound {
		warnings = append(warnings, "Unable to create recommendation as no value memory or cpu values were found")

		return &pb.Recommendation{
			IsActive: false,
			Message:  fmt.Sprintf("No Recommendations generated: %s", strings.Join(warnings, "; ")),
		}
	} else {
		res := &pb.ContainerResource{
			ContainerName: cfg.containerName,
			Requests:      requests,
			Limits:        limits,
		}
		if len(lowerBound) > 0 {
			res.LowerBound = lowerBound
		}
		if len(upperBound) > 0 {
			res.UpperBound = upperBound
		}
		return &pb.Recommendation{
			IsActive:          true,
			WorkloadResources: []*pb.ContainerResource{res},
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

// cpuMilliFor returns the recommended CPU, in millicores, for the max value of
// the metric across all pods: converted from fractional cores (e.g., 0.15) to
// millicores (e.g., 150m) with the safety margin, rounded up to the nearest
// whole millicore, and at least minCPUMilli.
func cpuMilliFor(podMetrics map[string]*pb.ContainerMetrics, containerName, metric string, safetyMargin float64) (int64, bool) {
	val, found := getMaxVal(podMetrics, containerName, metric)
	if !found {
		return 0, false
	}
	return max(minCPUMilli, int64(math.Ceil(val*safetyMargin*1000))), true
}

// memMiBFor returns the recommended memory, in MiB, for the max value of the
// metric across all pods: converted from bytes (e.g., 268435456) to MiB (e.g.,
// 256Mi) with the safety margin, rounded up to the nearest whole MiB, and at
// least minMEMMiB.
func memMiBFor(podMetrics map[string]*pb.ContainerMetrics, containerName, metric string, safetyMargin float64) (int64, bool) {
	val, found := getMaxVal(podMetrics, containerName, metric)
	if !found {
		return 0, false
	}
	return max(minMEMMiB, int64(math.Ceil(val*safetyMargin/1024/1024))), true
}

// getMaxVal returns the max value of the metric for the container across all
// pods.
func getMaxVal(podMetrics map[string]*pb.ContainerMetrics, containerName, metric string) (float64, bool) {
	var maxVal float64
	found := false

	for _, podMetric := range podMetrics {
		containerMetric, ok := podMetric.GetContainerMetrics()[containerName]
		if !ok || containerMetric == nil {
			continue
		}
		val, ok := containerMetric.Values[metric]
		if ok {
			if !found || val > maxVal {
				maxVal = val
			}
			found = true
		}
	}
	return maxVal, found
}

// parseConfig extracts and validates parameters from the recommender definition
func parseConfig(def *pb.RecommenderDefinition) (*config, error) {
	cpuMetric := def.Params["cpu-metric"]
	memMetric := def.Params["mem-metric"]
	container := def.Params["container"]

	if cpuMetric == "" && memMetric == "" {
		return nil, fmt.Errorf("cpu-metric and mem-metric are undefined. For VPA to work at least one of them needs to be defined.")
	}
	container = strings.TrimSpace(container)
	if container == "" {
		return nil, fmt.Errorf("container is undefined. For VPA to work, one container needs to be defined.")
	}

	config := &config{
		cpuMetric:       cpuMetric,
		memMetric:       memMetric,
		containerName:   container,
		cpuSafetyMargin: defaultCPUSafetyMarginFloat,
		memSafetyMargin: defaultMemSafetyMarginFloat,

		cpuLowerBoundMetric: strings.TrimSpace(def.Params["cpu-lower-bound-metric"]),
		cpuUpperBoundMetric: strings.TrimSpace(def.Params["cpu-upper-bound-metric"]),
		memLowerBoundMetric: strings.TrimSpace(def.Params["mem-lower-bound-metric"]),
		memUpperBoundMetric: strings.TrimSpace(def.Params["mem-upper-bound-metric"]),
	}

	// Bounds are computed around the target, so they need one.
	if cpuMetric == "" && (config.cpuLowerBoundMetric != "" || config.cpuUpperBoundMetric != "") {
		return nil, fmt.Errorf("cpu-lower-bound-metric and cpu-upper-bound-metric require cpu-metric to be defined")
	}
	if memMetric == "" && (config.memLowerBoundMetric != "" || config.memUpperBoundMetric != "") {
		return nil, fmt.Errorf("mem-lower-bound-metric and mem-upper-bound-metric require mem-metric to be defined")
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
