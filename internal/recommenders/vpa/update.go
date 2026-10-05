package vpa

import (
	"fmt"
	"maps"
	"math"
	"slices"

	"k8s.io/apimachinery/pkg/api/resource"
)

// significantChangeRatio is the relative change of a request, compared to the
// current one, from which a pod within its bounds is still resized.
const significantChangeRatio = 0.10

// resourceRecommendation is the recommendation for one resource (e.g. cpu): the
// target request, and optional bounds around it. A nil bound means that side
// is unbounded.
type resourceRecommendation struct {
	Target resource.Quantity
	Lower  *resource.Quantity
	Upper  *resource.Quantity
}

// needsUpdate decides whether a pod (or container) with the current requests
// should be resized to the recommended targets, like the OSS VPA updater
// without its minimum pod age. For each recommended resource:
//   - a missing current request needs an update;
//   - without any bound, any difference from the target needs an update;
//   - a current request below the lower bound or above the upper bound needs
//     an update;
//   - within the bounds, only a change of at least significantChangeRatio
//     needs an update.
//
// It returns why the update is needed, or why not.
func needsUpdate(current map[string]string, recs map[string]resourceRecommendation) (bool, string) {
	for _, name := range slices.Sorted(maps.Keys(recs)) {
		rec := recs[name]
		s, ok := current[name]
		if !ok {
			return true, fmt.Sprintf("no current %s request", name)
		}
		cur, err := resource.ParseQuantity(s)
		if err != nil {
			return true, fmt.Sprintf("invalid current %s request %q", name, s)
		}
		if rec.Lower == nil && rec.Upper == nil {
			if cur.Cmp(rec.Target) != 0 {
				return true, fmt.Sprintf("%s request %s differs from the target %s", name, cur.String(), rec.Target.String())
			}
			continue
		}
		if rec.Lower != nil && cur.Cmp(*rec.Lower) < 0 {
			return true, fmt.Sprintf("%s request %s is below the lower bound %s", name, cur.String(), rec.Lower.String())
		}
		if rec.Upper != nil && cur.Cmp(*rec.Upper) > 0 {
			return true, fmt.Sprintf("%s request %s is above the upper bound %s", name, cur.String(), rec.Upper.String())
		}
		curF := cur.AsApproximateFloat64()
		if curF <= 0 || math.Abs(rec.Target.AsApproximateFloat64()-curF)/curF >= significantChangeRatio {
			return true, fmt.Sprintf("%s request %s changes by at least %.0f%% to the target %s", name, cur.String(), significantChangeRatio*100, rec.Target.String())
		}
	}
	return false, fmt.Sprintf("requests are within bounds and change by less than %.0f%%", significantChangeRatio*100)
}

// commonRequests returns the requests, for the recommended resources, that all
// the pods agree on, in canonical form. It returns false if the pods have
// different requests, or some have a request that others don't. A request
// that fails to parse is kept as is, so needsUpdate reports it.
func commonRequests(pods []map[string]string, recs map[string]resourceRecommendation) (map[string]string, bool) {
	canonical := func(s string) string {
		if q, err := resource.ParseQuantity(s); err == nil {
			return q.String()
		}
		return s
	}
	var common map[string]string
	for _, pod := range pods {
		cur := make(map[string]string, len(recs))
		for name := range recs {
			if s, ok := pod[name]; ok {
				cur[name] = canonical(s)
			}
		}
		if common == nil {
			common = cur
		} else if !maps.Equal(common, cur) {
			return nil, false
		}
	}
	return common, true
}
