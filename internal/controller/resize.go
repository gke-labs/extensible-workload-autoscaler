package controller

import (
	"encoding/json"
	"fmt"

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
// It returns a nil patch when the pod already has the requested values, and a
// non-empty skipReason when the pod cannot be resized in place.
func buildResizePatch(pod *corev1.Pod, containerName string, requests, limits map[string]string) (patch []byte, skipReason string, err error) {
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
	return buildContainerResizePatch(pod, containerName, reqs, lims)
}

// buildContainerResizePatch adjusts the recommended container resources so that
// the resize is accepted by the API server, then builds the patch:
//   - container limits >= container requests, raising an existing limit that
//     the new request would exceed;
//   - when the pod has pod-level resources, the container fits under them:
//     aggregate container requests <= pod requests, and container limits <=
//     pod limits;
//   - the QoS class is preserved: in a Guaranteed pod without pod-level
//     resources the container keeps limits == requests, and a Burstable pod is
//     not turned into a Guaranteed one.
func buildContainerResizePatch(pod *corev1.Pod, containerName string, reqs, lims corev1.ResourceList) ([]byte, string, error) {
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
		// Drop the recommended limits that would make the pod Guaranteed.
		for name, lim := range lims {
			if req, ok := reqs[name]; ok && req.Cmp(lim) == 0 {
				delete(lims, name)
			}
		}
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
//     Burstable pods are not turned into Guaranteed ones.
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
			// Drop the recommended limits that would make the pod Guaranteed.
			for name, lim := range lims {
				if req, ok := reqs[name]; ok && req.Cmp(lim) == 0 {
					delete(lims, name)
				}
			}
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
