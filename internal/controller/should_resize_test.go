package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestShouldResize(t *testing.T) {
	m := func(kv ...string) map[string]string {
		out := map[string]string{}
		for i := 0; i+1 < len(kv); i += 2 {
			out[kv[i]] = kv[i+1]
		}
		return out
	}
	// Container "app" requesting 500m CPU and 512Mi memory.
	app := withContainer("app", rl("cpu", "500m", "memory", "512Mi"), nil)

	tests := []struct {
		name         string
		pod          *corev1.Pod
		container    string
		requests     map[string]string
		lower, upper map[string]string
		want         bool
	}{
		{
			name:      "no bounds always resizes",
			pod:       newPod(app),
			container: "app",
			requests:  m("cpu", "510m"),
			want:      true,
		},
		{
			name:      "below lower bound resizes",
			pod:       newPod(app),
			container: "app",
			requests:  m("cpu", "800m"),
			lower:     m("cpu", "600m"),
			upper:     m("cpu", "1"),
			want:      true,
		},
		{
			name:      "above upper bound resizes",
			pod:       newPod(app),
			container: "app",
			requests:  m("memory", "256Mi"),
			lower:     m("memory", "128Mi"),
			upper:     m("memory", "300Mi"),
			want:      true,
		},
		{
			name:      "one resource out of bounds is enough",
			pod:       newPod(app),
			container: "app",
			requests:  m("cpu", "500m", "memory", "1Gi"),
			lower:     m("cpu", "400m", "memory", "768Mi"),
			upper:     m("cpu", "600m", "memory", "2Gi"),
			want:      true,
		},
		{
			name:      "within bounds, change below 10% is left as is",
			pod:       newPod(app),
			container: "app",
			requests:  m("cpu", "540m"),
			lower:     m("cpu", "400m"),
			upper:     m("cpu", "600m"),
			want:      false,
		},
		{
			name:      "within bounds, change of at least 10% resizes",
			pod:       newPod(app),
			container: "app",
			requests:  m("cpu", "550m"),
			lower:     m("cpu", "400m"),
			upper:     m("cpu", "600m"),
			want:      true,
		},
		{
			name:      "within bounds, decrease of at least 10% resizes",
			pod:       newPod(app),
			container: "app",
			requests:  m("memory", "400Mi"),
			lower:     m("memory", "256Mi"),
			upper:     m("memory", "1Gi"),
			want:      true,
		},
		{
			name:      "resource without a bound, change below 10% is left as is",
			pod:       newPod(app),
			container: "app",
			requests:  m("cpu", "500m", "memory", "540Mi"),
			lower:     m("cpu", "400m"),
			upper:     m("cpu", "600m"),
			want:      false,
		},
		{
			name:      "resource without a bound, change of at least 10% resizes",
			pod:       newPod(app),
			container: "app",
			requests:  m("cpu", "500m", "memory", "2Gi"),
			lower:     m("cpu", "400m"),
			upper:     m("cpu", "600m"),
			want:      true,
		},
		{
			name:      "missing current request resizes",
			pod:       newPod(withContainer("app", rl("cpu", "500m"), nil)),
			container: "app",
			requests:  m("cpu", "500m", "memory", "256Mi"),
			lower:     m("cpu", "400m"),
			want:      true,
		},
		{
			name:      "unknown container resizes, leaving it to buildResizePatch",
			pod:       newPod(app),
			container: "other",
			requests:  m("cpu", "500m"),
			lower:     m("cpu", "400m"),
			want:      true,
		},
		{
			name:     "pod level within bounds is left as is",
			pod:      newPod(app, withPodResources(rl("cpu", "1", "memory", "1Gi"), nil)),
			requests: m("cpu", "1050m"),
			lower:    m("cpu", "800m"),
			upper:    m("cpu", "2"),
			want:     false,
		},
		{
			name:     "pod level out of bounds resizes",
			pod:      newPod(app, withPodResources(rl("cpu", "1", "memory", "1Gi"), nil)),
			requests: m("cpu", "2"),
			lower:    m("cpu", "1500m"),
			want:     true,
		},
		{
			name:     "pod level without pod resources resizes",
			pod:      newPod(app),
			requests: m("cpu", "1"),
			lower:    m("cpu", "800m"),
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := shouldResize(tt.pod, tt.container, tt.requests, tt.lower, tt.upper)
			if got != tt.want {
				t.Errorf("shouldResize() = %v (reason %q), want %v", got, reason, tt.want)
			}
			if !got && reason == "" {
				t.Errorf("shouldResize() = false without a reason")
			}
		})
	}
}
