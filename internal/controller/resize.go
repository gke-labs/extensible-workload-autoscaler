package controller

import (
	"encoding/json"
	"fmt"
	"math/big"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// buildResizePatch returns the strategic merge patch to send to the pod's
// "resize" subresource to apply the given resources.
//
// A non-empty containerName targets that container's resources. An empty
// containerName targets the pod-level resources (pod.spec.resources), as
// documented in ContainerResource.container_name.
//
// template is the pod template of the workload (nil if unknown). For a
// container, the template container's limit/request ratios are used to scale
// recommended limits equal to their request, so that every pod of the
// workload gets the same limits whatever its current ones (see scaleLimits).
//
// It returns a nil patch when the pod already has the requested values, and a
// non-empty skipReason when the pod cannot be resized in place.
func buildResizePatch(pod *corev1.Pod, template *corev1.PodSpec, containerName string, requests, limits map[string]string) (patch []byte, skipReason string, err error) {
	// In-place resize cannot change the QoS class of a pod, and a BestEffort pod
	// becomes Burstable or Guaranteed as soon as any request or limit is set.
	if pod.Status.QOSClass == corev1.PodQOSBestEffort {
		return nil, "pod is BestEffort: in-place resize cannot add resources without changing its QoS class", nil
	}

	reqs := parseResourceList(requests)
	lims := parseResourceList(limits)
	if len(reqs) == 0 && len(lims) == 0 {
		return nil, "", nil
	}

	if containerName == "" {
		return buildPodLevelResizePatch(pod, reqs, lims)
	}
	var tmpl *corev1.ResourceRequirements
	if template != nil {
		for i := range template.Containers {
			if template.Containers[i].Name == containerName {
				tmpl = &template.Containers[i].Resources
				break
			}
		}
	}
	return buildContainerResizePatch(pod, tmpl, containerName, reqs, lims)
}

// buildContainerResizePatch adjusts the recommended container resources so that
// the resize is accepted by the API server, then builds the patch:
//   - container limits >= container requests, raising an existing limit that
//     the new request would exceed;
//   - when the pod has pod-level resources, the container fits under them:
//     aggregate container requests <= pod requests, and container limits <=
//     pod limits;
//   - in a Burstable pod without pod-level resources, recommended limits equal
//     to their request are scaled with the template's limit/request ratio (see
//     scaleLimits), so every pod of the workload gets the same limits;
//   - the QoS class is preserved: in a Guaranteed pod without pod-level
//     resources the container keeps limits == requests, and a Burstable pod is
//     not turned into a Guaranteed one. When the resulting limits would still
//     make it Guaranteed, the limits equal to their request are scaled too.
func buildContainerResizePatch(pod *corev1.Pod, tmpl *corev1.ResourceRequirements, containerName string, reqs, lims corev1.ResourceList) ([]byte, string, error) {
	var target *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == containerName {
			target = &pod.Spec.Containers[i]
			break
		}
	}
	if target == nil {
		return nil, fmt.Sprintf("container %q not found", containerName), nil
	}
	current := target.Resources
	podLevel := pod.Spec.Resources

	// With pod-level resources, QoS is derived from them, so container
	// resources only need to fit under them.
	if pod.Status.QOSClass == corev1.PodQOSGuaranteed && podLevel == nil {
		lims = corev1.ResourceList{}
		for name, q := range reqs {
			lims[name] = q.DeepCopy()
		}
	} else {
		if podLevel == nil {
			scaleLimits(reqs, lims, tmpl, current)
		}
		for name, q := range lims {
			req, ok := reqs[name]
			if !ok {
				req = current.Requests[name]
			}
			lims[name] = maxQuantity(q, req)
		}
		// A request raised above an existing limit must raise the limit too.
		for name, q := range reqs {
			if _, ok := lims[name]; ok {
				continue
			}
			if cur, ok := current.Limits[name]; ok && cur.Cmp(q) < 0 {
				lims[name] = q.DeepCopy()
			}
		}
	}

	if podLevel != nil {
		others := aggregateRequestsExcept(pod, containerName)
		for name, q := range reqs {
			podReq, ok := podLevel.Requests[name]
			if !ok {
				continue
			}
			room := podReq.DeepCopy()
			room.Sub(others[name])
			if room.Sign() <= 0 {
				return nil, fmt.Sprintf("no %s left under the pod-level request for container %q", name, containerName), nil
			}
			if q.Cmp(room) > 0 {
				reqs[name] = room
			}
		}
		for name, q := range lims {
			if podLim, ok := podLevel.Limits[name]; ok && q.Cmp(podLim) > 0 {
				lims[name] = podLim.DeepCopy()
			}
		}
		// Capping a limit may have brought it below its request.
		for name, lim := range lims {
			if req, ok := reqs[name]; ok && req.Cmp(lim) > 0 {
				reqs[name] = lim.DeepCopy()
			}
		}
	}

	if podLevel == nil && pod.Status.QOSClass != corev1.PodQOSGuaranteed &&
		containersGuaranteed(pod, containerName, merge(current.Requests, reqs), merge(current.Limits, lims)) {
		scaleLimits(reqs, lims, tmpl, current)
		if containersGuaranteed(pod, containerName, merge(current.Requests, reqs), merge(current.Limits, lims)) {
			return nil, "resize would change the pod QoS class from Burstable to Guaranteed", nil
		}
	}

	if !differs(current.Requests, reqs) && !differs(current.Limits, lims) {
		return nil, "", nil
	}

	resources := resourcesPatch(reqs, lims)
	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"containers": []map[string]any{
				{"name": containerName, "resources": resources},
			},
		},
	})
	return patch, "", err
}

// aggregateRequestsExcept returns the aggregate requests of the pod's
// containers (including restartable init containers), leaving out the named
// container.
func aggregateRequestsExcept(pod *corev1.Pod, containerName string) corev1.ResourceList {
	agg := corev1.ResourceList{}
	add := func(c corev1.Container) {
		if c.Name == containerName {
			return
		}
		for name, q := range c.Resources.Requests {
			sum := agg[name]
			sum.Add(q)
			agg[name] = sum
		}
	}
	for _, c := range pod.Spec.Containers {
		add(c)
	}
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			add(c)
		}
	}
	return agg
}

// containersGuaranteed reports whether, with the named container given the
// requests and limits, every container of the pod would have CPU and memory
// requests == limits, i.e. the pod would be Guaranteed.
func containersGuaranteed(pod *corev1.Pod, containerName string, reqs, lims corev1.ResourceList) bool {
	for _, c := range pod.Spec.Containers {
		r, l := c.Resources.Requests, c.Resources.Limits
		if c.Name == containerName {
			r, l = reqs, lims
		}
		if !isGuaranteed(r, l) {
			return false
		}
	}
	return true
}

// buildPodLevelResizePatch adjusts the recommended pod-level resources so that
// the resize is accepted by the API server, then builds the patch:
//   - pod requests >= aggregate container requests;
//   - pod limits >= pod requests, and >= every container's limit;
//   - the QoS class is preserved: Guaranteed pods keep limits == requests, and
//     Burstable pods are not turned into Guaranteed ones. When the recommended
//     limits would make it Guaranteed, the limits equal to their request are
//     instead scaled with the pod's current limit/request ratio (see
//     scaleLimits).
func buildPodLevelResizePatch(pod *corev1.Pod, reqs, lims corev1.ResourceList) ([]byte, string, error) {
	var current corev1.ResourceRequirements
	if pod.Spec.Resources != nil {
		current = *pod.Spec.Resources
	}

	aggReqs, maxLims := containerResourceBounds(pod)
	for name, q := range reqs {
		if agg, ok := aggReqs[name]; ok && q.Cmp(agg) < 0 {
			reqs[name] = agg.DeepCopy()
		}
	}

	if pod.Status.QOSClass == corev1.PodQOSGuaranteed {
		lims = corev1.ResourceList{}
		for name, q := range reqs {
			lims[name] = q.DeepCopy()
		}
	} else {
		for name, q := range lims {
			lims[name] = maxQuantity(q, reqs[name], maxLims[name])
		}
		// A request raised above an existing limit must raise the limit too.
		for name, q := range reqs {
			if _, ok := lims[name]; ok {
				continue
			}
			if cur, ok := current.Limits[name]; ok && cur.Cmp(q) < 0 {
				lims[name] = q.DeepCopy()
			}
		}
		if isGuaranteed(merge(current.Requests, reqs), merge(current.Limits, lims)) {
			scaleLimits(reqs, lims, nil, current)
			if isGuaranteed(merge(current.Requests, reqs), merge(current.Limits, lims)) {
				return nil, "resize would change the pod QoS class from Burstable to Guaranteed", nil
			}
		}
	}

	if !differs(current.Requests, reqs) && !differs(current.Limits, lims) {
		return nil, "", nil
	}

	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{"resources": resourcesPatch(reqs, lims)},
	})
	return patch, "", err
}

// scaleLimits replaces each recommended limit equal to its recommended request
// with the request scaled by a limit/request ratio, as OSS VPA does, so that a
// resize keeps the pod Burstable. The ratio comes from the template (tmpl,
// may be nil) when it sets both, so that every pod of the workload gets the
// same limits; otherwise from the current resources. A limit is dropped (the
// current limit is kept) when neither has a request and limit to compute the
// ratio from, unless the new request is above the current limit, in which
// case it is kept as is.
func scaleLimits(reqs, lims corev1.ResourceList, tmpl *corev1.ResourceRequirements, current corev1.ResourceRequirements) {
	for name, lim := range lims {
		req, ok := reqs[name]
		if !ok || req.Cmp(lim) != 0 {
			continue
		}
		if tmpl != nil {
			tmplReq, okReq := tmpl.Requests[name]
			tmplLim, okLim := tmpl.Limits[name]
			if okReq && okLim && tmplReq.Sign() > 0 {
				lims[name] = scaleQuantity(name, req, tmplLim, tmplReq)
				continue
			}
		}
		curReq, okReq := current.Requests[name]
		curLim, okLim := current.Limits[name]
		if okReq && okLim && curReq.Sign() > 0 {
			lims[name] = scaleQuantity(name, req, curLim, curReq)
			continue
		}
		if okLim && req.Cmp(curLim) > 0 {
			continue
		}
		delete(lims, name)
	}
}

// scaleQuantity returns q multiplied by the ratio num/den, rounded up to a
// whole millicore for CPU and to a whole unit otherwise. It uses exact integer
// arithmetic, so that scaling a request by the limit/request ratio it already
// has gives back the same limit: with floats, e.g. 1156m * (5780m / 1156m)
// would round up to 5781m and trigger another resize.
func scaleQuantity(name corev1.ResourceName, q, num, den resource.Quantity) resource.Quantity {
	value := func(x resource.Quantity) *big.Int {
		if name == corev1.ResourceCPU {
			return big.NewInt(x.MilliValue())
		}
		return big.NewInt(x.Value())
	}
	// ceil(q * num / den), for non-negative values.
	scaled := new(big.Int).Mul(value(q), value(num))
	d := value(den)
	scaled.Add(scaled, new(big.Int).Sub(d, big.NewInt(1)))
	scaled.Quo(scaled, d)
	if name == corev1.ResourceCPU {
		return *resource.NewMilliQuantity(scaled.Int64(), q.Format)
	}
	return *resource.NewQuantity(scaled.Int64(), q.Format)
}

// containerResourceBounds returns the aggregate requests of the pod's
// containers (including restartable init containers, i.e. native sidecars,
// which run alongside them), and the largest limit set by any container.
func containerResourceBounds(pod *corev1.Pod) (aggReqs, maxLims corev1.ResourceList) {
	aggReqs, maxLims = corev1.ResourceList{}, corev1.ResourceList{}
	add := func(c corev1.Container) {
		for name, q := range c.Resources.Requests {
			sum := aggReqs[name]
			sum.Add(q)
			aggReqs[name] = sum
		}
		for name, q := range c.Resources.Limits {
			if cur, ok := maxLims[name]; !ok || q.Cmp(cur) > 0 {
				maxLims[name] = q.DeepCopy()
			}
		}
	}
	for _, c := range pod.Spec.Containers {
		add(c)
	}
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			add(c)
		}
	}
	return aggReqs, maxLims
}

// isGuaranteed reports whether pod-level resources give the Guaranteed QoS
// class: both CPU and memory set, with requests == limits.
func isGuaranteed(reqs, lims corev1.ResourceList) bool {
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		req, okReq := reqs[name]
		lim, okLim := lims[name]
		if !okReq || !okLim || req.Cmp(lim) != 0 {
			return false
		}
	}
	return true
}

func parseResourceList(values map[string]string) corev1.ResourceList {
	list := corev1.ResourceList{}
	for name, v := range values {
		q, err := resource.ParseQuantity(v)
		if err != nil {
			continue
		}
		list[corev1.ResourceName(name)] = q
	}
	return list
}

// differs reports whether any value in want is missing from, or different in,
// current.
func differs(current, want corev1.ResourceList) bool {
	for name, q := range want {
		if cur, ok := current[name]; !ok || cur.Cmp(q) != 0 {
			return true
		}
	}
	return false
}

// merge returns base overridden by overlay.
func merge(base, overlay corev1.ResourceList) corev1.ResourceList {
	out := corev1.ResourceList{}
	for name, q := range base {
		out[name] = q
	}
	for name, q := range overlay {
		out[name] = q
	}
	return out
}

func maxQuantity(qs ...resource.Quantity) resource.Quantity {
	var out resource.Quantity
	for _, q := range qs {
		if q.Cmp(out) > 0 {
			out = q
		}
	}
	return out.DeepCopy()
}

func resourcesPatch(reqs, lims corev1.ResourceList) map[string]any {
	out := map[string]any{}
	if len(reqs) > 0 {
		out["requests"] = toStringMap(reqs)
	}
	if len(lims) > 0 {
		out["limits"] = toStringMap(lims)
	}
	return out
}

func toStringMap(list corev1.ResourceList) map[string]string {
	out := make(map[string]string, len(list))
	for name, q := range list {
		out[string(name)] = q.String()
	}
	return out
}
