package controller

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func rl(kv ...string) corev1.ResourceList {
	list := corev1.ResourceList{}
	for i := 0; i+1 < len(kv); i += 2 {
		list[corev1.ResourceName(kv[i])] = resource.MustParse(kv[i+1])
	}
	return list
}

type podOpt func(*corev1.Pod)

func withQoS(q corev1.PodQOSClass) podOpt { return func(p *corev1.Pod) { p.Status.QOSClass = q } }

func withPodResources(req, lim corev1.ResourceList) podOpt {
	return func(p *corev1.Pod) {
		p.Spec.Resources = &corev1.ResourceRequirements{Requests: req, Limits: lim}
	}
}

func withContainer(name string, req, lim corev1.ResourceList) podOpt {
	return func(p *corev1.Pod) {
		p.Spec.Containers = append(p.Spec.Containers, corev1.Container{
			Name:      name,
			Resources: corev1.ResourceRequirements{Requests: req, Limits: lim},
		})
	}
}

func newPod(opts ...podOpt) *corev1.Pod {
	p := &corev1.Pod{}
	p.Name = "p"
	p.Status.QOSClass = corev1.PodQOSBurstable
	for _, o := range opts {
		o(p)
	}
	return p
}

// decode returns the resources section of a patch, for either target.
func decode(t *testing.T, patch []byte) (containers []map[string]any, podResources map[string]map[string]string) {
	t.Helper()
	var body struct {
		Spec struct {
			Containers []map[string]any             `json:"containers"`
			Resources  map[string]map[string]string `json:"resources"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(patch, &body); err != nil {
		t.Fatalf("invalid patch %s: %v", patch, err)
	}
	return body.Spec.Containers, body.Spec.Resources
}

func TestBuildResizePatch_PodLevel(t *testing.T) {
	tests := []struct {
		name      string
		pod       *corev1.Pod
		requests  map[string]string
		limits    map[string]string
		wantPatch map[string]map[string]string // nil: no patch
		wantSkip  string                       // substring of the skip reason
	}{
		{
			name:      "Adds pod-level resources to a Burstable pod without them",
			pod:       newPod(withContainer("main", rl("cpu", "100m"), nil), withContainer("sidecar", nil, nil)),
			requests:  map[string]string{"cpu": "300m"},
			limits:    map[string]string{"cpu": "600m"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "300m"}, "limits": {"cpu": "600m"}},
		},
		{
			name:      "Requests only, no limits",
			pod:       newPod(withContainer("main", rl("cpu", "100m"), nil)),
			requests:  map[string]string{"cpu": "300m"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "300m"}},
		},
		{
			name:      "Requests raised to the aggregate container requests",
			pod:       newPod(withContainer("a", rl("cpu", "100m"), nil), withContainer("b", rl("cpu", "150m"), nil)),
			requests:  map[string]string{"cpu": "50m"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "250m"}},
		},
		{
			name:      "Limits raised to the requests and to the largest container limit",
			pod:       newPod(withContainer("a", rl("cpu", "100m"), rl("cpu", "700m"))),
			requests:  map[string]string{"cpu": "400m"},
			limits:    map[string]string{"cpu": "300m"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "400m"}, "limits": {"cpu": "700m"}},
		},
		{
			name:      "Existing pod limit below the new request is raised",
			pod:       newPod(withContainer("a", nil, nil), withPodResources(rl("cpu", "200m"), rl("cpu", "400m"))),
			requests:  map[string]string{"cpu": "900m"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "900m"}, "limits": {"cpu": "900m"}},
		},
		{
			name:      "Guaranteed pod keeps limits == requests",
			pod:       newPod(withQoS(corev1.PodQOSGuaranteed), withContainer("a", nil, nil), withPodResources(rl("cpu", "200m", "memory", "64Mi"), rl("cpu", "200m", "memory", "64Mi"))),
			requests:  map[string]string{"cpu": "300m"},
			limits:    map[string]string{"cpu": "600m"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "300m"}, "limits": {"cpu": "300m"}},
		},
		{
			name:     "Burstable pod is not turned into a Guaranteed one",
			pod:      newPod(withContainer("a", nil, nil), withPodResources(rl("cpu", "200m", "memory", "64Mi"), rl("cpu", "400m", "memory", "64Mi"))),
			requests: map[string]string{"cpu": "300m"},
			limits:   map[string]string{"cpu": "300m"},
			// The limit equal to the request is dropped; the existing 400m
			// limit is kept by the strategic merge, so the pod stays Burstable.
			wantPatch: map[string]map[string]string{"requests": {"cpu": "300m"}},
		},
		{
			name:     "No patch when already at the recommended values",
			pod:      newPod(withContainer("a", nil, nil), withPodResources(rl("cpu", "300m"), rl("cpu", "600m"))),
			requests: map[string]string{"cpu": "300m"},
			limits:   map[string]string{"cpu": "600m"},
		},
		{
			name:     "BestEffort pod is skipped",
			pod:      newPod(withQoS(corev1.PodQOSBestEffort), withContainer("a", nil, nil)),
			requests: map[string]string{"cpu": "300m"},
			wantSkip: "BestEffort",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			patch, skip, err := buildResizePatch(tc.pod, "", tc.requests, tc.limits)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantSkip != "" {
				if !strings.Contains(skip, tc.wantSkip) || patch != nil {
					t.Fatalf("want skip containing %q and no patch, got skip %q, patch %s", tc.wantSkip, skip, patch)
				}
				return
			}
			if skip != "" {
				t.Fatalf("unexpected skip: %s", skip)
			}
			if tc.wantPatch == nil {
				if patch != nil {
					t.Fatalf("want no patch, got %s", patch)
				}
				return
			}
			containers, podRes := decode(t, patch)
			if containers != nil {
				t.Errorf("pod-level patch must not touch containers, got %v", containers)
			}
			if diff := cmp.Diff(tc.wantPatch, podRes); diff != "" {
				t.Errorf("pod-level resources mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildResizePatch_Container(t *testing.T) {
	guaranteedMain := func(cpu string) podOpt {
		return withContainer("main", rl("cpu", cpu, "memory", "64Mi"), rl("cpu", cpu, "memory", "64Mi"))
	}

	tests := []struct {
		name      string
		pod       *corev1.Pod
		container string
		requests  map[string]string
		limits    map[string]string
		wantPatch map[string]map[string]string // nil: no patch
		wantSkip  string                       // substring of the skip reason
	}{
		{
			name:      "Requests only",
			pod:       newPod(withContainer("main", rl("cpu", "100m"), nil), withContainer("sidecar", rl("cpu", "100m"), nil)),
			container: "sidecar",
			requests:  map[string]string{"cpu": "250m"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "250m"}},
		},
		{
			name:      "Requests and limits from limitRatio",
			pod:       newPod(withContainer("main", rl("cpu", "100m"), nil)),
			container: "main",
			requests:  map[string]string{"cpu": "300m"},
			limits:    map[string]string{"cpu": "600m"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "300m"}, "limits": {"cpu": "600m"}},
		},
		{
			name:      "Recommended limit below the request is raised",
			pod:       newPod(withContainer("main", rl("cpu", "100m"), nil)),
			container: "main",
			requests:  map[string]string{"cpu": "300m"},
			limits:    map[string]string{"cpu": "200m"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "300m"}, "limits": {"cpu": "300m"}},
		},
		{
			name:      "Existing limit below the new request is raised",
			pod:       newPod(withContainer("main", rl("cpu", "500m"), rl("cpu", "2"))),
			container: "main",
			requests:  map[string]string{"cpu": "2500m"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "2500m"}, "limits": {"cpu": "2500m"}},
		},
		{
			name:      "Guaranteed pod keeps limits == requests",
			pod:       newPod(withQoS(corev1.PodQOSGuaranteed), guaranteedMain("500m")),
			container: "main",
			requests:  map[string]string{"cpu": "800m"},
			limits:    map[string]string{"cpu": "1600m"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "800m"}, "limits": {"cpu": "800m"}},
		},
		{
			name: "Burstable pod is not turned into a Guaranteed one",
			// sidecar is already Guaranteed; main would become Guaranteed with
			// limit == request, so that limit is dropped and the existing
			// 1 CPU limit is kept by the strategic merge.
			pod: newPod(
				withContainer("main", rl("cpu", "500m", "memory", "64Mi"), rl("cpu", "1", "memory", "64Mi")),
				withContainer("sidecar", rl("cpu", "100m", "memory", "32Mi"), rl("cpu", "100m", "memory", "32Mi")),
			),
			container: "main",
			requests:  map[string]string{"cpu": "700m"},
			limits:    map[string]string{"cpu": "700m"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "700m"}},
		},
		{
			name: "Burstable pod skipped when the request alone would make it Guaranteed",
			pod: newPod(
				withContainer("main", rl("cpu", "500m", "memory", "64Mi"), rl("cpu", "1", "memory", "64Mi")),
				withContainer("sidecar", rl("cpu", "100m", "memory", "32Mi"), rl("cpu", "100m", "memory", "32Mi")),
			),
			container: "main",
			requests:  map[string]string{"cpu": "1"},
			wantSkip:  "Guaranteed",
		},
		{
			name: "Pod-level resources cap the container request and limit",
			pod: newPod(
				withContainer("main", rl("cpu", "100m"), nil),
				withContainer("sidecar", rl("cpu", "200m"), nil),
				withPodResources(rl("cpu", "1"), rl("cpu", "2")),
			),
			container: "main",
			requests:  map[string]string{"cpu": "1500m"},
			limits:    map[string]string{"cpu": "3"},
			// request <= 1 - 200m (sidecar); limit <= pod limit.
			wantPatch: map[string]map[string]string{"requests": {"cpu": "800m"}, "limits": {"cpu": "2"}},
		},
		{
			name: "Pod-level resources: Guaranteed pod QoS comes from the pod, so limits are free",
			pod: newPod(
				withQoS(corev1.PodQOSGuaranteed),
				withContainer("main", rl("cpu", "100m"), nil),
				withPodResources(rl("cpu", "2", "memory", "64Mi"), rl("cpu", "2", "memory", "64Mi")),
			),
			container: "main",
			requests:  map[string]string{"cpu": "500m"},
			limits:    map[string]string{"cpu": "1"},
			wantPatch: map[string]map[string]string{"requests": {"cpu": "500m"}, "limits": {"cpu": "1"}},
		},
		{
			name: "Pod-level request already used by other containers",
			pod: newPod(
				withContainer("main", rl("cpu", "100m"), nil),
				withContainer("sidecar", rl("cpu", "1"), nil),
				withPodResources(rl("cpu", "1"), nil),
			),
			container: "main",
			requests:  map[string]string{"cpu": "500m"},
			wantSkip:  "no cpu left",
		},
		{
			name:      "No patch when already at the recommended values",
			pod:       newPod(withContainer("main", rl("cpu", "100m"), rl("cpu", "200m"))),
			container: "main",
			requests:  map[string]string{"cpu": "100m"},
			limits:    map[string]string{"cpu": "200m"},
		},
		{
			name:      "Container not found",
			pod:       newPod(withContainer("main", rl("cpu", "100m"), nil)),
			container: "missing",
			requests:  map[string]string{"cpu": "1"},
			wantSkip:  "not found",
		},
		{
			name:      "BestEffort pod is skipped",
			pod:       newPod(withQoS(corev1.PodQOSBestEffort), withContainer("main", nil, nil)),
			container: "main",
			requests:  map[string]string{"cpu": "1"},
			wantSkip:  "BestEffort",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			patch, skip, err := buildResizePatch(tc.pod, tc.container, tc.requests, tc.limits)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantSkip != "" {
				if !strings.Contains(skip, tc.wantSkip) || patch != nil {
					t.Fatalf("want skip containing %q and no patch, got skip %q, patch %s", tc.wantSkip, skip, patch)
				}
				return
			}
			if skip != "" {
				t.Fatalf("unexpected skip: %s", skip)
			}
			if tc.wantPatch == nil {
				if patch != nil {
					t.Fatalf("want no patch, got %s", patch)
				}
				return
			}
			containers, podRes := decode(t, patch)
			if podRes != nil {
				t.Errorf("container patch must not touch pod-level resources, got %v", podRes)
			}
			resources := map[string]any{}
			for k, v := range tc.wantPatch {
				m := map[string]any{}
				for rk, rv := range v {
					m[rk] = rv
				}
				resources[k] = m
			}
			want := []map[string]any{{"name": tc.container, "resources": resources}}
			if diff := cmp.Diff(want, containers); diff != "" {
				t.Errorf("container patch mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
