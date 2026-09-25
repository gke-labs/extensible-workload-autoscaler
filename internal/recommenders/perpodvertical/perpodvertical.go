package perpodvertical

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"k8s.io/apimachinery/pkg/api/resource"
)

type PerPodVerticalRecommender struct{}

type config struct {
	metric       string
	target       float64
	safetyMargin float64
	container    string
	resourceType string
	minCpu       int64 // milliCPU
	maxCpu       int64 // milliCPU
	minMemory    int64 // bytes
	maxMemory    int64 // bytes
}

// Recommend sizes the configured container of each pod individually, based on
// that container's own usage.
//
// The metric must be defined with scope "PodContainer" so that the Control
// Plane keeps the per-container breakdown in
// ControlMetrics.pod_container_metrics. The pod-level values in pod_metrics are
// not used: for resource metrics they are a request-weighted mix of all the
// pod's containers, which would size the target container on its neighbors'
// usage.
func (r *PerPodVerticalRecommender) Recommend(def *pb.RecommenderDefinition, state, _ *pb.ControlMetrics) *pb.Recommendation {
	cfg, err := parseConfig(def)
	if err != nil {
		return &pb.Recommendation{
			Message: err.Error(),
		}
	}

	if state == nil || len(state.PodContainerMetrics) == 0 {
		if state != nil && hasPodLevelMetric(state, cfg.metric) {
			return &pb.Recommendation{
				Message: fmt.Sprintf("metric %q has no per-container data: define it with scope \"PodContainer\"", cfg.metric),
			}
		}
		return &pb.Recommendation{
			Message: "no pod container metrics available",
		}
	}

	var podRecs []*pb.PodContainerResource

	for podName, pcm := range state.PodContainerMetrics {
		cm, ok := pcm.GetContainerMetrics()[cfg.container]
		if !ok || cm == nil {
			continue
		}
		val, ok := cm.Values[cfg.metric]
		if !ok {
			continue
		}

		// DesiredRequest = CurrentMetricValue * (SafetyMargin / TargetValue)
		desiredVal := val * (cfg.safetyMargin / cfg.target)

		requests := make(map[string]string)
		limits := make(map[string]string)

		if cfg.resourceType == "cpu" {
			desiredMilli := int64(math.Ceil(desiredVal * 1000.0))
			if cfg.minCpu > 0 && desiredMilli < cfg.minCpu {
				desiredMilli = cfg.minCpu
			}
			if cfg.maxCpu > 0 && desiredMilli > cfg.maxCpu {
				desiredMilli = cfg.maxCpu
			}
			requests["cpu"] = fmt.Sprintf("%dm", desiredMilli)
		} else if cfg.resourceType == "memory" {
			desiredBytes := int64(math.Ceil(desiredVal))
			if cfg.minMemory > 0 && desiredBytes < cfg.minMemory {
				desiredBytes = cfg.minMemory
			}
			if cfg.maxMemory > 0 && desiredBytes > cfg.maxMemory {
				desiredBytes = cfg.maxMemory
			}
			requests["memory"] = fmt.Sprintf("%d", desiredBytes)
		} else {
			requests[cfg.resourceType] = fmt.Sprintf("%.0f", desiredVal)
		}

		podRecs = append(podRecs, &pb.PodContainerResource{
			PodName: podName,
			ContainerResources: &pb.ContainerResource{
				ContainerName: cfg.container,
				Requests:      requests,
				Limits:        limits,
			},
		})
	}

	return &pb.Recommendation{
		IsActive:              true,
		PodContainerResources: podRecs,
	}
}

// hasPodLevelMetric reports whether the metric is only available as a pod-level
// value, i.e. it was defined with scope "Pod" rather than "PodContainer".
func hasPodLevelMetric(state *pb.ControlMetrics, metric string) bool {
	for _, pm := range state.PodMetrics {
		if _, ok := pm.Values[metric]; ok {
			return true
		}
	}
	return false
}

func getParam(params map[string]string, keys ...string) string {
	for _, k := range keys {
		if v, ok := params[k]; ok && v != "" {
			return v
		}
	}
	return ""
}

func parseConfig(def *pb.RecommenderDefinition) (*config, error) {
	metric := getParam(def.Params, "metric")
	if metric == "" {
		return nil, fmt.Errorf("missing metric param")
	}

	targetStr := getParam(def.Params, "target")
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

	safetyMargin := 1.0
	if smStr := getParam(def.Params, "safetyMargin", "safety_margin"); smStr != "" {
		sm, err := strconv.ParseFloat(smStr, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid safetyMargin format")
		}
		if sm <= 0 {
			return nil, fmt.Errorf("safetyMargin must be > 0")
		}
		safetyMargin = sm
	}

	container := getParam(def.Params, "container")
	if container == "" {
		return nil, fmt.Errorf("missing container param")
	}

	resourceType := getParam(def.Params, "resourceType", "resource_type")
	if resourceType == "" {
		if strings.Contains(metric, "memory") || strings.Contains(metric, "mem") {
			resourceType = "memory"
		} else {
			resourceType = "cpu"
		}
	}

	cfg := &config{
		metric:       metric,
		target:       target,
		safetyMargin: safetyMargin,
		container:    container,
		resourceType: resourceType,
	}

	if minCpuStr := getParam(def.Params, "minCpu", "min_cpu"); minCpuStr != "" {
		q, err := resource.ParseQuantity(minCpuStr)
		if err != nil {
			return nil, fmt.Errorf("invalid minCpu quantity: %v", err)
		}
		cfg.minCpu = q.MilliValue()
	}

	if maxCpuStr := getParam(def.Params, "maxCpu", "max_cpu"); maxCpuStr != "" {
		q, err := resource.ParseQuantity(maxCpuStr)
		if err != nil {
			return nil, fmt.Errorf("invalid maxCpu quantity: %v", err)
		}
		cfg.maxCpu = q.MilliValue()
	}

	if minMemStr := getParam(def.Params, "minMemory", "min_memory"); minMemStr != "" {
		q, err := resource.ParseQuantity(minMemStr)
		if err != nil {
			return nil, fmt.Errorf("invalid minMemory quantity: %v", err)
		}
		cfg.minMemory = q.Value()
	}

	if maxMemStr := getParam(def.Params, "maxMemory", "max_memory"); maxMemStr != "" {
		q, err := resource.ParseQuantity(maxMemStr)
		if err != nil {
			return nil, fmt.Errorf("invalid maxMemory quantity: %v", err)
		}
		cfg.maxMemory = q.Value()
	}

	return cfg, nil
}
