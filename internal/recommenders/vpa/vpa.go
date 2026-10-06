package vpa

import (
	"fmt"
	"maps"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

const (
	minCPUMilli                 = 10   // 10 millicores: represents the minimum CPU value returned by the recommender
	minMEMMiB                   = 10   // 10 MiB: represents the minumum Memory value returned by the recommender
	defaultCPUSafetyMarginFloat = 1.15 // represents 15% headroom
	defaultMemSafetyMarginFloat = 1.15 // represents 15% headroom

	// Defaults of the owned metrics of a resource for which the user declares
	// no metric.
	defaultProvider  = "kubelet"
	defaultScope     = "PodContainer"
	defaultHalfLife  = "24h"
	defaultCPUBucket = "0.05"     // 50m
	defaultMemBucket = "10485760" // 10Mi
)

// slot is one of the metrics VPA uses for each resource it controls.
type slot int

const (
	targetSlot slot = iota
	lowerBoundSlot
	upperBoundSlot
	numSlots
)

// slotSpec describes a slot: the suffixes of its param and of its owned
// metric name, and the percentile of its owned metric.
type slotSpec struct {
	paramSuffix string
	ownedSuffix string
	percentile  string
}

var slots = [numSlots]slotSpec{
	targetSlot:     {paramSuffix: "-metric", ownedSuffix: "-target", percentile: "p90"},
	lowerBoundSlot: {paramSuffix: "-lower-bound-metric", ownedSuffix: "-lower-bound", percentile: "p50"},
	upperBoundSlot: {paramSuffix: "-upper-bound-metric", ownedSuffix: "-upper-bound", percentile: "p99"},
}

// resourceSpec describes how VPA sizes a resource.
type resourceSpec struct {
	// name is the resource name in the recommendation, the prefix of the
	// owned metric names, and the "type" param of the default kubelet metric.
	name string
	// paramPrefix is the prefix of the params of the resource.
	paramPrefix       string
	defaultBucketSize string
	defaultMargin     float64
	unit              string
	// value returns the recommended value of the resource, in unit, for the
	// max value of the metric across all pods.
	value func(podMetrics map[string]*pb.ContainerMetrics, containerName, metric string, safetyMargin float64) (int64, bool)
}

var resources = []resourceSpec{
	{name: "cpu", paramPrefix: "cpu", defaultBucketSize: defaultCPUBucket, defaultMargin: defaultCPUSafetyMarginFloat, unit: "m", value: cpuMilliFor},
	{name: "memory", paramPrefix: "mem", defaultBucketSize: defaultMemBucket, defaultMargin: defaultMemSafetyMarginFloat, unit: "Mi", value: memMiBFor},
}

func (s resourceSpec) param(sl slot) string {
	return s.paramPrefix + slots[sl].paramSuffix
}

func (s resourceSpec) ownedName(sl slot) string {
	return s.name + slots[sl].ownedSuffix
}

type VPARecommender struct{}

// metricRef is the metric used for a slot: either a policy-wide metric the
// user configured, or a metric owned by the recommender.
type metricRef struct {
	name  string
	owned bool
}

// resourceConfig holds the configuration of a controlled resource.
type resourceConfig struct {
	safetyMargin float64
	metrics      [numSlots]metricRef
	// limitRatio is the limit/request ratio of the recommended limit. Nil
	// means the pods' current ratio.
	limitRatio *big.Rat
}

// config holds the parsed configuration for this recommender instance
type config struct {
	containerName string
	// resources holds the controlled resources, keyed by resource name.
	resources map[string]*resourceConfig
}

// OwnedMetrics returns the metrics VPA owns: one per slot the user didn't
// configure, for each controlled resource. Owned metrics copy the source
// (provider, params, filter, scope) and the half-life and bucket size of the
// user's metrics for the same resource, if any, so all the metrics of a
// resource share them; otherwise they use the defaults.
//
// It returns an error if the configuration is invalid, including when a
// configured metric isn't defined in the policy, isn't a decaying
// distribution, or differs from the other metrics of its resource in half-life
// or bucket size.
func (r *VPARecommender) OwnedMetrics(def *pb.RecommenderDefinition, policyMetrics []*pb.MetricDefinition) ([]*pb.MetricDefinition, error) {
	cfg, err := parseConfig(def)
	if err != nil {
		return nil, err
	}
	byName := make(map[string]*pb.MetricDefinition, len(policyMetrics))
	for _, m := range policyMetrics {
		byName[m.Name] = m
	}

	var owned []*pb.MetricDefinition
	for _, res := range resources {
		rc, ok := cfg.resources[res.name]
		if !ok {
			continue
		}

		// The user's metrics for this resource. The first one is the
		// template of the owned metrics.
		var template *pb.MetricDefinition
		var templateParam string
		for sl := range numSlots {
			ref := rc.metrics[sl]
			if ref.owned {
				continue
			}
			m, ok := byName[ref.name]
			if !ok {
				return nil, fmt.Errorf("%s %q is not defined in the policy metrics", res.param(sl), ref.name)
			}
			dd := m.GetDecayingDistribution()
			if dd == nil {
				return nil, fmt.Errorf("%s %q must be a decayingDistribution metric", res.param(sl), ref.name)
			}
			if template == nil {
				template, templateParam = m, res.param(sl)
				continue
			}
			tdd := template.GetDecayingDistribution()
			if dd.HalfLife != tdd.HalfLife || dd.BucketSize != tdd.BucketSize {
				return nil, fmt.Errorf("the %s metrics must have the same halfLife and bucketSize: %s %q has %q and %q, %s %q has %q and %q",
					res.name, templateParam, template.Name, tdd.HalfLife, tdd.BucketSize, res.param(sl), ref.name, dd.HalfLife, dd.BucketSize)
			}
		}
		if template == nil {
			template = &pb.MetricDefinition{
				Provider: defaultProvider,
				Params:   map[string]string{"type": res.name},
				Scope:    defaultScope,
				DecayingDistribution: &pb.DecayingDistribution{
					HalfLife:   defaultHalfLife,
					BucketSize: res.defaultBucketSize,
				},
			}
		}

		for sl := range numSlots {
			ref := rc.metrics[sl]
			if !ref.owned {
				continue
			}
			tdd := template.GetDecayingDistribution()
			owned = append(owned, &pb.MetricDefinition{
				Name:     ref.name,
				Provider: template.Provider,
				Params:   maps.Clone(template.Params),
				Filter:   maps.Clone(template.Filter),
				Scope:    template.Scope,
				DecayingDistribution: &pb.DecayingDistribution{
					HalfLife:   tdd.HalfLife,
					BucketSize: tdd.BucketSize,
					Rate:       tdd.Rate,
					Percentile: slots[sl].percentile,
				},
			})
		}
	}
	return owned, nil
}

// Recommend calculates the resource recommendations based on control metrics.
// Slots configured by the user are read from the policy-wide metrics, and the
// others from the metrics owned by the recommender.
//
// It computes the target and the bounds of each resource for the container,
// and recommends the same requests for every pod of the workload: the pods'
// current requests if they all agree and don't need an update (see
// needsUpdate), otherwise the target. The limits keep the limit/request ratio
// of the cpu-limit-ratio / mem-limit-ratio params, or else the pods' current
// one (see limitFor). The target and bounds are reported in the message.
func (r *VPARecommender) Recommend(def *pb.RecommenderDefinition, state, ownedMetrics *pb.ControlMetrics, workload *pb.Workload) *pb.Recommendation {
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

	if state == nil && ownedMetrics == nil {
		return &pb.Recommendation{
			IsActive: false,
			Message:  "ControlMetrics is missing",
		}
	}
	if len(state.GetPodContainerMetrics()) == 0 && len(ownedMetrics.GetPodContainerMetrics()) == 0 {
		return &pb.Recommendation{
			IsActive: false,
			Message:  "PodMetrics is empty (ensure metrics are configured with scope: PodContainer)",
		}
	}

	requests := make(map[string]string, 2)
	recs := make(map[string]resourceRecommendation, 2)
	var summary []string

	for _, res := range resources {
		rc, ok := cfg.resources[res.name]
		if !ok {
			continue
		}
		// describe names the metric of a slot in warnings.
		describe := func(sl slot) string {
			ref := rc.metrics[sl]
			if ref.owned {
				return fmt.Sprintf("owned metric %q", ref.name)
			}
			return fmt.Sprintf("%s %q", res.param(sl), ref.name)
		}
		value := func(sl slot) (int64, bool) {
			ref := rc.metrics[sl]
			src := state
			if ref.owned {
				src = ownedMetrics
			}
			return res.value(src.GetPodContainerMetrics(), cfg.containerName, ref.name, rc.safetyMargin)
		}
		quantity := func(v int64) *resource.Quantity {
			q := resource.MustParse(fmt.Sprintf("%d%s", v, res.unit))
			return &q
		}

		target, found := value(targetSlot)
		if !found {
			warnings = append(warnings, fmt.Sprintf("%s not found in state", describe(targetSlot)))
			continue
		}
		rec := resourceRecommendation{Target: *quantity(target)}

		// The bounds are clamped so that lower <= target <= upper. A bound
		// whose metric has no data is left out (unbounded).
		lower, upper := "-", "-"
		if v, ok := value(lowerBoundSlot); ok {
			rec.Lower = quantity(min(v, target))
			lower = rec.Lower.String()
		} else {
			warnings = append(warnings, fmt.Sprintf("%s not found in state, no lower bound for %s", describe(lowerBoundSlot), res.name))
		}
		if v, ok := value(upperBoundSlot); ok {
			rec.Upper = quantity(max(v, target))
			upper = rec.Upper.String()
		} else {
			warnings = append(warnings, fmt.Sprintf("%s not found in state, no upper bound for %s", describe(upperBoundSlot), res.name))
		}
		requests[res.name] = rec.Target.String()
		recs[res.name] = rec
		summary = append(summary, fmt.Sprintf("%s: target %s [%s, %s]", res.name, rec.Target.String(), lower, upper))
	}

	// If no valid recommendations were generated, returning a recommendation with an error
	if len(requests) == 0 {
		warnings = append(warnings, "Unable to create recommendation as no value memory or cpu values were found")

		return &pb.Recommendation{
			IsActive: false,
			Message:  fmt.Sprintf("No Recommendations generated: %s", strings.Join(warnings, "; ")),
		}
	}

	// Without the pods' current requests, every pod would be resized.
	if len(workload.GetPods()) == 0 {
		return &pb.Recommendation{
			IsActive: false,
			Message:  fmt.Sprintf("%s; no pods reported for the workload yet", strings.Join(summary, "; ")),
		}
	}
	var containers []*pb.ContainerState
	var current []map[string]string
	for _, pod := range workload.GetPods() {
		idx := slices.IndexFunc(pod.GetContainers(), func(c *pb.ContainerState) bool { return c.GetName() == cfg.containerName })
		if idx >= 0 {
			containers = append(containers, pod.Containers[idx])
			current = append(current, pod.Containers[idx].GetRequests())
		}
	}
	if len(current) == 0 {
		return &pb.Recommendation{
			IsActive: false,
			Message:  fmt.Sprintf("%s; no pod of the workload has the container %q", strings.Join(summary, "; "), cfg.containerName),
		}
	}

	// Every pod gets the same requests. Keep the pods' current requests while
	// they agree and don't need an update; otherwise move all pods to the
	// target.
	decision := "resizing to the target"
	if agreed, ok := commonRequests(current, recs); !ok {
		decision += fmt.Sprintf(": pods have different requests (%d pods)", len(current))
	} else if update, reason := needsUpdate(agreed, recs); update {
		decision += ": " + reason
	} else {
		decision = fmt.Sprintf("keeping the current requests: %s", reason)
		for name := range requests {
			requests[name] = agreed[name]
		}
	}

	// The limits keep the configured limit/request ratio, or the pods'
	// current one. Without either, the container gets no limit.
	limits := make(map[string]string, len(requests))
	for name, req := range requests {
		if lim, ok := limitFor(name, req, cfg.resources[name].limitRatio, containers); ok {
			limits[name] = lim
		}
	}

	msg := fmt.Sprintf("%s; %s", strings.Join(summary, "; "), decision)
	if len(warnings) > 0 {
		msg += fmt.Sprintf("; warnings: %s", strings.Join(warnings, "; "))
	}
	return &pb.Recommendation{
		IsActive: true,
		WorkloadResources: []*pb.ContainerResource{{
			ContainerName: cfg.containerName,
			Requests:      requests,
			Limits:        limits,
		}},
		Message: msg,
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

// parseConfig extracts and validates parameters from the recommender
// definition. It doesn't check the configured metrics against the policy; see
// OwnedMetrics.
func parseConfig(def *pb.RecommenderDefinition) (*config, error) {
	container := strings.TrimSpace(def.Params["container"])
	if container == "" {
		return nil, fmt.Errorf("container is undefined. For VPA to work, one container needs to be defined.")
	}

	// The metrics configured by the user, per resource and slot.
	userMetrics := make(map[string][numSlots]string, len(resources))
	for _, res := range resources {
		var names [numSlots]string
		set := false
		for sl := range numSlots {
			names[sl] = strings.TrimSpace(def.Params[res.param(sl)])
			set = set || names[sl] != ""
		}
		if set {
			userMetrics[res.name] = names
		}
	}

	controlled, err := parseControlledResources(def.Params["controlled-resources"], userMetrics)
	if err != nil {
		return nil, err
	}
	for _, res := range resources {
		if _, ok := userMetrics[res.name]; ok && !controlled[res.name] {
			return nil, fmt.Errorf("%s metric params are set, but %s is not in controlled-resources", res.name, res.name)
		}
	}

	cfg := &config{
		containerName: container,
		resources:     make(map[string]*resourceConfig, len(controlled)),
	}
	for _, res := range resources {
		margin := res.defaultMargin
		marginParam := res.paramPrefix + "-safety-margin"
		if s := def.Params[marginParam]; s != "" {
			margin, err = strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid %s provided %s. The value needs to represent a float64. err: %w", marginParam, s, err)
			}
		}
		var limitRatio *big.Rat
		ratioParam := res.paramPrefix + "-limit-ratio"
		if s := strings.TrimSpace(def.Params[ratioParam]); s != "" {
			r, ok := new(big.Rat).SetString(s)
			if !ok || r.Cmp(big.NewRat(1, 1)) < 0 {
				return nil, fmt.Errorf("invalid %s %q: it must be a number >= 1", ratioParam, s)
			}
			if !controlled[res.name] {
				return nil, fmt.Errorf("%s is set, but %s is not in controlled-resources", ratioParam, res.name)
			}
			limitRatio = r
		}
		if !controlled[res.name] {
			continue
		}
		rc := &resourceConfig{safetyMargin: margin, limitRatio: limitRatio}
		names := userMetrics[res.name]
		for sl := range numSlots {
			if names[sl] != "" {
				rc.metrics[sl] = metricRef{name: names[sl]}
			} else {
				rc.metrics[sl] = metricRef{name: res.ownedName(sl), owned: true}
			}
		}
		cfg.resources[res.name] = rc
	}
	return cfg, nil
}

// parseControlledResources parses the controlled-resources param: a comma
// separated list of resources. When unset, VPA controls the resources the user
// set metric params for, or all of them if none.
func parseControlledResources(param string, userMetrics map[string][numSlots]string) (map[string]bool, error) {
	controlled := make(map[string]bool, len(resources))
	if strings.TrimSpace(param) == "" {
		for _, res := range resources {
			_, set := userMetrics[res.name]
			controlled[res.name] = set || len(userMetrics) == 0
		}
		return controlled, nil
	}

	for _, name := range strings.Split(param, ",") {
		name = strings.TrimSpace(name)
		known := false
		for _, res := range resources {
			if res.name == name {
				known = true
				break
			}
		}
		if !known {
			return nil, fmt.Errorf("invalid controlled-resources %q: %q is not one of cpu, memory", param, name)
		}
		controlled[name] = true
	}
	return controlled, nil
}
