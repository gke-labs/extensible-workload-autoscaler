package perpodvertical

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Resource levels supported by the recommender, set with the "resourceLevel"
// param.
const (
	// LevelContainer sizes one named container of each pod, based on that
	// container's own usage.
	LevelContainer = "Container"
	// LevelPod sizes the pod-level resources (pod.spec.resources) of each pod,
	// based on the summed usage of all the pod's containers.
	LevelPod = "Pod"
)

type PerPodVerticalRecommender struct{}

type config struct {
	metric        string
	target        float64
	safetyMargin  float64
	resourceLevel string
	container     string // only set for LevelContainer
	resourceType  string
	limitRatio    float64 // 0 means requests only
	minCpu        int64   // milliCPU
	maxCpu        int64   // milliCPU
	minMemory     int64   // bytes
	maxMemory     int64   // bytes
}

// Recommend sizes each pod individually, based on its own usage.
//
// With resourceLevel "Container" (the default), the configured container is
// sized from that container's usage. With resourceLevel "Pod", the pod-level
// resources are sized from the sum of the usage of all the pod's containers,
// and the recommendation is emitted with an empty container name, which targets
// pod-level resources.
//
// In both modes, the metric must be defined with scope "PodContainer" so that
// the Control Plane keeps the raw per-container breakdown in
// ControlMetrics.pod_container_metrics. The pod-level values in pod_metrics are
// not used: for resource metrics they are a request-weighted average of the
// pod's containers, which is neither a single container's usage nor the pod's
// total usage.
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
		val, ok := podUsage(pcm, cfg)
		if !ok {
			continue
		}

		// DesiredRequest = CurrentMetricValue * (SafetyMargin / TargetValue)
		desiredVal := val * (cfg.safetyMargin / cfg.target)
		requests, limits := cfg.resources(desiredVal)

		podRecs = append(podRecs, &pb.PodContainerResource{
			PodName: podName,
			ContainerResources: &pb.ContainerResource{
				// Empty for LevelPod: targets the pod-level resources.
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

// podUsage returns the usage the pod is sized on: the configured container's
// value for LevelContainer, or the sum over all the pod's containers for
// LevelPod. It reports false if no container of the pod reports the metric.
func podUsage(pcm *pb.ContainerMetrics, cfg *config) (float64, bool) {
	byContainer := pcm.GetContainerMetrics()
	if cfg.resourceLevel == LevelContainer {
		cm, ok := byContainer[cfg.container]
		if !ok || cm == nil {
			return 0, false
		}
		v, ok := cm.Values[cfg.metric]
		return v, ok
	}

	sum, found := 0.0, false
	for _, cm := range byContainer {
		if cm == nil {
			continue
		}
		if v, ok := cm.Values[cfg.metric]; ok {
			sum += v
			found = true
		}
	}
	return sum, found
}

// resources formats the desired value as a resource request, bounded by the
// configured min/max, and derives the limit from limitRatio when set. The
// min/max bounds apply to the request only.
func (cfg *config) resources(desiredVal float64) (requests, limits map[string]string) {
	requests = make(map[string]string)
	limits = make(map[string]string)

	switch cfg.resourceType {
	case "cpu":
		desiredMilli := int64(math.Ceil(desiredVal * 1000.0))
		if cfg.minCpu > 0 && desiredMilli < cfg.minCpu {
			desiredMilli = cfg.minCpu
		}
		if cfg.maxCpu > 0 && desiredMilli > cfg.maxCpu {
			desiredMilli = cfg.maxCpu
		}
		requests["cpu"] = fmt.Sprintf("%dm", desiredMilli)
		if cfg.limitRatio > 0 {
			limits["cpu"] = fmt.Sprintf("%dm", int64(math.Ceil(float64(desiredMilli)*cfg.limitRatio)))
		}
	case "memory":
		desiredBytes := int64(math.Ceil(desiredVal))
		if cfg.minMemory > 0 && desiredBytes < cfg.minMemory {
			desiredBytes = cfg.minMemory
		}
		if cfg.maxMemory > 0 && desiredBytes > cfg.maxMemory {
			desiredBytes = cfg.maxMemory
		}
		requests["memory"] = fmt.Sprintf("%d", desiredBytes)
		if cfg.limitRatio > 0 {
			limits["memory"] = fmt.Sprintf("%d", int64(math.Ceil(float64(desiredBytes)*cfg.limitRatio)))
		}
	default:
		requests[cfg.resourceType] = fmt.Sprintf("%.0f", desiredVal)
		if cfg.limitRatio > 0 {
			limits[cfg.resourceType] = fmt.Sprintf("%.0f", math.Ceil(desiredVal*cfg.limitRatio))
		}
	}
	return requests, limits
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

	resourceLevel := getParam(def.Params, "resourceLevel", "resource_level")
	if resourceLevel == "" {
		resourceLevel = LevelContainer
	}
	container := getParam(def.Params, "container")
	switch resourceLevel {
	case LevelContainer:
		if container == "" {
			return nil, fmt.Errorf("missing container param")
		}
	case LevelPod:
		if container != "" {
			return nil, fmt.Errorf("container param is not allowed with resourceLevel %q", LevelPod)
		}
	default:
		return nil, fmt.Errorf("invalid resourceLevel %q: must be %q or %q", resourceLevel, LevelContainer, LevelPod)
	}

	limitRatio := 0.0
	if lrStr := getParam(def.Params, "limitRatio", "limit_ratio"); lrStr != "" {
		lr, err := strconv.ParseFloat(lrStr, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid limitRatio format")
		}
		if lr < 1 {
			return nil, fmt.Errorf("limitRatio must be >= 1")
		}
		limitRatio = lr
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
		metric:        metric,
		target:        target,
		safetyMargin:  safetyMargin,
		resourceLevel: resourceLevel,
		container:     container,
		resourceType:  resourceType,
		limitRatio:    limitRatio,
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
