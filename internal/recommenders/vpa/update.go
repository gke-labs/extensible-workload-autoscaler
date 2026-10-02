package vpa

import (
	"fmt"
	"maps"
	"math"
	"math/big"
	"slices"

	"k8s.io/apimachinery/pkg/api/resource"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
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

// limitFor returns the limit for the recommended request of a resource: the
// request scaled by the configured ratio if any, otherwise by the
// limit/request ratio of the pods' containers. Pods start from the workload's
// template and each resize keeps their ratio, so it is the template's ratio.
// If the pods disagree, the ratio of most pods is used; on a tie, the larger
// one, "no limit" being the largest. It returns false when the container gets
// no limit: the pods have none, or no request to compute the ratio from.
//
// The limit is rounded up to a whole millicore for CPU and to a whole byte
// otherwise, with exact arithmetic so that re-applying the same request gives
// back the same limit.
func limitFor(name, request string, ratio *big.Rat, containers []*pb.ContainerState) (string, bool) {
	req, err := resource.ParseQuantity(request)
	if err != nil {
		return "", false
	}
	if ratio == nil {
		var ok bool
		if ratio, ok = podsLimitRatio(name, containers); !ok {
			return "", false
		}
	}
	scaled := new(big.Rat).Mul(new(big.Rat).SetInt(quantityValue(name, req)), ratio)
	// ceil(num / den), for non-negative values.
	v := new(big.Int).Add(scaled.Num(), new(big.Int).Sub(scaled.Denom(), big.NewInt(1)))
	v.Quo(v, scaled.Denom())
	if name == "cpu" {
		return resource.NewMilliQuantity(v.Int64(), resource.DecimalSI).String(), true
	}
	return resource.NewQuantity(v.Int64(), resource.BinarySI).String(), true
}

// podsLimitRatio returns the limit/request ratio of the resource shared by
// most containers, or false if that is "no limit" or no container has a
// request for the resource.
func podsLimitRatio(name string, containers []*pb.ContainerState) (*big.Rat, bool) {
	type group struct {
		ratio *big.Rat // nil: no limit
		count int
	}
	var groups []*group
	for _, c := range containers {
		reqStr, ok := c.GetRequests()[name]
		if !ok {
			continue
		}
		req, err := resource.ParseQuantity(reqStr)
		if err != nil || req.Sign() <= 0 {
			continue
		}
		var ratio *big.Rat
		if limStr, ok := c.GetLimits()[name]; ok {
			lim, err := resource.ParseQuantity(limStr)
			if err != nil {
				continue
			}
			ratio = new(big.Rat).SetFrac(quantityValue(name, lim), quantityValue(name, req))
		}
		idx := slices.IndexFunc(groups, func(g *group) bool {
			return (g.ratio == nil) == (ratio == nil) && (ratio == nil || g.ratio.Cmp(ratio) == 0)
		})
		if idx < 0 {
			groups = append(groups, &group{ratio: ratio})
			idx = len(groups) - 1
		}
		groups[idx].count++
	}

	var best *group
	for _, g := range groups {
		switch {
		case best == nil || g.count > best.count:
			best = g
		case g.count < best.count || best.ratio == nil:
		case g.ratio == nil || g.ratio.Cmp(best.ratio) > 0:
			best = g
		}
	}
	if best == nil || best.ratio == nil {
		return nil, false
	}
	return best.ratio, true
}

// quantityValue returns the value of the quantity in millicores for CPU, and
// in its base unit (e.g. bytes) otherwise.
func quantityValue(name string, q resource.Quantity) *big.Int {
	if name == "cpu" {
		return big.NewInt(q.MilliValue())
	}
	return big.NewInt(q.Value())
}
