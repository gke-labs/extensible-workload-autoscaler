package vpa

import (
	"strings"
	"testing"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

const mib = 1024 * 1024

// boundsState holds the values per pod for container "app". The
// recommendation uses the max across pods; the tests use a safety margin of 1
// to keep the numbers simple: target 200m/200Mi, bounds [150m, 400m] and
// [150Mi, 400Mi].
var boundsState = &pb.ControlMetrics{
	PodContainerMetrics: map[string]*pb.ContainerMetrics{
		"pod-1": {ContainerMetrics: map[string]*pb.MetricValues{"app": {Values: map[string]float64{
			"cpu_p50": 0.1, "cpu_p90": 0.2, "cpu_p99": 0.4,
			"mem_p50": 100 * mib, "mem_p90": 200 * mib, "mem_p99": 400 * mib,
		}}}},
		"pod-2": {ContainerMetrics: map[string]*pb.MetricValues{"app": {Values: map[string]float64{
			"cpu_p50": 0.15, "cpu_p90": 0.1, "cpu_p99": 0.3,
			"mem_p50": 150 * mib, "mem_p90": 100 * mib, "mem_p99": 300 * mib,
		}}}},
	},
}

func boundsParams(kv ...string) map[string]string {
	p := map[string]string{
		"container":         "app",
		"cpu-metric":        "cpu_p90",
		"mem-metric":        "mem_p90",
		"cpu-safety-margin": "1",
		"mem-safety-margin": "1",
	}
	for i := 0; i+1 < len(kv); i += 2 {
		p[kv[i]] = kv[i+1]
	}
	return p
}

var allBounds = []string{
	"cpu-lower-bound-metric", "cpu_p50", "cpu-upper-bound-metric", "cpu_p99",
	"mem-lower-bound-metric", "mem_p50", "mem-upper-bound-metric", "mem_p99",
}

// podWith returns a pod whose "app" container has the given requests, and
// limits equal to them (a limit/request ratio of 1).
func podWith(name string, requests ...string) *pb.PodState {
	reqs := map[string]string{}
	for i := 0; i+1 < len(requests); i += 2 {
		reqs[requests[i]] = requests[i+1]
	}
	return podWithLimits(name, reqs, reqs)
}

// podWithLimits returns a pod whose "app" container has the given requests and
// limits.
func podWithLimits(name string, requests, limits map[string]string) *pb.PodState {
	return &pb.PodState{Name: name, Containers: []*pb.ContainerState{{Name: "app", Requests: requests, Limits: limits}}}
}

// TestRecommendBounds checks the bounds computed around the target, reported
// in the message.
func TestRecommendBounds(t *testing.T) {
	workload := &pb.Workload{Pods: []*pb.PodState{podWith("pod-1", "cpu", "200m", "memory", "200Mi")}}
	tests := []struct {
		name            string
		params          map[string]string
		wantMsgContains string
	}{
		{
			name:   "owned bound metrics without data leave both sides unbounded",
			params: boundsParams(),
			wantMsgContains: `owned metric "cpu-lower-bound" not found in state, no lower bound for cpu; ` +
				`owned metric "cpu-upper-bound" not found in state, no upper bound for cpu`,
		},
		{
			name:            "both bounds for cpu and memory",
			params:          boundsParams(allBounds...),
			wantMsgContains: "cpu: target 200m [150m, 400m]; memory: target 200Mi [150Mi, 400Mi]",
		},
		{
			name:            "only some bounds configured",
			params:          boundsParams("cpu-upper-bound-metric", "cpu_p99", "mem-lower-bound-metric", "mem_p50"),
			wantMsgContains: "cpu: target 200m [-, 400m]; memory: target 200Mi [150Mi, -]",
		},
		{
			name: "bounds are clamped around the target",
			// Swapped metrics: the lower bound metric is above the target and
			// the upper bound metric below it.
			params: boundsParams(
				"cpu-lower-bound-metric", "cpu_p99", "cpu-upper-bound-metric", "cpu_p50",
				"mem-lower-bound-metric", "mem_p99", "mem-upper-bound-metric", "mem_p50",
			),
			wantMsgContains: "cpu: target 200m [200m, 200m]; memory: target 200Mi [200Mi, 200Mi]",
		},
		{
			name:            "bound metric without data is left out with a warning",
			params:          boundsParams("cpu-lower-bound-metric", "missing", "cpu-upper-bound-metric", "cpu_p99"),
			wantMsgContains: `cpu: target 200m [-, 400m]; memory: target 200Mi [-, -]; keeping the current requests: requests are within bounds and change by less than 10%; warnings: cpu-lower-bound-metric "missing" not found in state`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &VPARecommender{}
			got := r.Recommend(&pb.RecommenderDefinition{Params: tt.params}, boundsState, nil, workload)
			if !got.IsActive {
				t.Fatalf("Recommend() inactive: %s", got.Message)
			}
			if !strings.Contains(got.Message, tt.wantMsgContains) {
				t.Errorf("Recommend() Message = %q, want it to contain %q", got.Message, tt.wantMsgContains)
			}
		})
	}
}

// TestRecommendWorkload checks that every pod gets the same requests: the
// pods' current ones while they agree and don't need an update, otherwise the
// target.
func TestRecommendWorkload(t *testing.T) {
	resources := func(cpu, mem string) []*pb.ContainerResource {
		reqs := map[string]string{"cpu": cpu, "memory": mem}
		return []*pb.ContainerResource{{ContainerName: "app", Requests: reqs, Limits: reqs}}
	}
	target := resources("200m", "200Mi")

	tests := []struct {
		name            string
		params          map[string]string
		pods            []*pb.PodState
		wantActive      bool
		want            []*pb.ContainerResource
		wantMsgContains string
	}{
		{
			name:   "pods agreeing within the bounds keep their requests",
			params: boundsParams(allBounds...),
			pods: []*pb.PodState{
				podWith("pod-1", "cpu", "190m", "memory", "210Mi"), // < 10% change
				podWith("pod-2", "cpu", "0.19", "memory", "210Mi"), // same cpu, other format
			},
			wantActive:      true,
			want:            resources("190m", "210Mi"),
			wantMsgContains: "keeping the current requests",
		},
		{
			name:            "pods below the lower bound move to the target",
			params:          boundsParams(allBounds...),
			pods:            []*pb.PodState{podWith("pod-1", "cpu", "100m", "memory", "200Mi"), podWith("pod-2", "cpu", "100m", "memory", "200Mi")},
			wantActive:      true,
			want:            target,
			wantMsgContains: "resizing to the target: cpu request 100m is below the lower bound 150m",
		},
		{
			name:            "pods above the upper bound move to the target",
			params:          boundsParams(allBounds...),
			pods:            []*pb.PodState{podWith("pod-1", "cpu", "200m", "memory", "500Mi")},
			wantActive:      true,
			want:            target,
			wantMsgContains: "memory request 500Mi is above the upper bound 400Mi",
		},
		{
			name:   "pods with different requests move to the target",
			params: boundsParams(allBounds...),
			pods: []*pb.PodState{
				podWith("pod-1", "cpu", "190m", "memory", "210Mi"),
				podWith("pod-2", "cpu", "195m", "memory", "210Mi"), // both within the bounds
			},
			wantActive:      true,
			want:            target,
			wantMsgContains: "resizing to the target: pods have different requests (2 pods)",
		},
		{
			name:            "within the bounds, a change of at least 10% on a single resource moves to the target",
			params:          boundsParams(allBounds...),
			pods:            []*pb.PodState{podWith("pod-1", "cpu", "178m", "memory", "200Mi")}, // 200/178: +12%
			wantActive:      true,
			want:            target,
			wantMsgContains: "changes by at least 10%",
		},
		{
			name:            "within the bounds, changes below 10% are not summed",
			params:          boundsParams(allBounds...),
			pods:            []*pb.PodState{podWith("pod-1", "cpu", "185m", "memory", "185Mi")}, // +8% each
			wantActive:      true,
			want:            resources("185m", "185Mi"),
			wantMsgContains: "keeping the current requests",
		},
		{
			name:            "a resource without bounds moves to the target on any difference",
			params:          boundsParams("cpu-lower-bound-metric", "cpu_p50", "cpu-upper-bound-metric", "cpu_p99"),
			pods:            []*pb.PodState{podWith("pod-1", "cpu", "200m", "memory", "199Mi")},
			wantActive:      true,
			want:            target,
			wantMsgContains: "memory request 199Mi differs from the target 200Mi",
		},
		{
			name:       "a missing request moves to the target",
			params:     boundsParams(allBounds...),
			pods:       []*pb.PodState{podWith("pod-1", "cpu", "200m")},
			wantActive: true,
			// Without a memory request, there's no ratio for the memory limit.
			want: []*pb.ContainerResource{{
				ContainerName: "app",
				Requests:      map[string]string{"cpu": "200m", "memory": "200Mi"},
				Limits:        map[string]string{"cpu": "200m"},
			}},
			wantMsgContains: "no current memory request",
		},
		{
			name:   "pods without the container are ignored",
			params: boundsParams(allBounds...),
			pods: []*pb.PodState{
				{Name: "other", Containers: []*pb.ContainerState{{Name: "sidecar", Requests: map[string]string{"cpu": "1m"}}}},
				podWith("pod-1", "cpu", "190m", "memory", "210Mi"),
			},
			wantActive: true,
			want:       resources("190m", "210Mi"),
		},
		{
			name:            "no pod has the container",
			params:          boundsParams(allBounds...),
			pods:            []*pb.PodState{{Name: "other", Containers: []*pb.ContainerState{{Name: "sidecar"}}}},
			wantActive:      false,
			wantMsgContains: `no pod of the workload has the container "app"`,
		},
		{
			name:            "no pods reported yet",
			params:          boundsParams(allBounds...),
			wantActive:      false,
			wantMsgContains: "cpu: target 200m [150m, 400m]; memory: target 200Mi [150Mi, 400Mi]; no pods reported for the workload yet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &VPARecommender{}
			got := r.Recommend(&pb.RecommenderDefinition{Params: tt.params}, boundsState, nil, &pb.Workload{Pods: tt.pods})
			if got.IsActive != tt.wantActive {
				t.Fatalf("Recommend() IsActive = %v, want %v (message %q)", got.IsActive, tt.wantActive, got.Message)
			}
			if len(got.PodContainerResources) != 0 {
				t.Errorf("Recommend() PodContainerResources = %v, want none", got.PodContainerResources)
			}
			if diff := cmp.Diff(tt.want, got.WorkloadResources, protocmp.Transform()); diff != "" {
				t.Errorf("Recommend() WorkloadResources mismatch (-want +got):\n%s", diff)
			}
			if !strings.Contains(got.Message, tt.wantMsgContains) {
				t.Errorf("Recommend() Message = %q, want it to contain %q", got.Message, tt.wantMsgContains)
			}
		})
	}
}

// TestRecommendLimits checks that the recommended limits keep the configured
// limit/request ratio, or else the pods' current one.
func TestRecommendLimits(t *testing.T) {
	burstable := func(name string) *pb.PodState { // 5x CPU, 4x memory
		return podWithLimits(name, map[string]string{"cpu": "100m", "memory": "100Mi"}, map[string]string{"cpu": "500m", "memory": "400Mi"})
	}
	tests := []struct {
		name       string
		params     map[string]string
		pods       []*pb.PodState
		wantReqs   map[string]string
		wantLimits map[string]string
	}{
		{
			name:       "the pods' ratio is kept",
			params:     boundsParams(),
			pods:       []*pb.PodState{burstable("pod-1"), burstable("pod-2")},
			wantReqs:   map[string]string{"cpu": "200m", "memory": "200Mi"},
			wantLimits: map[string]string{"cpu": "1", "memory": "800Mi"},
		},
		{
			name:       "the limit-ratio params override the pods' ratio",
			params:     boundsParams("cpu-limit-ratio", "1.5", "mem-limit-ratio", "1"),
			pods:       []*pb.PodState{burstable("pod-1")},
			wantReqs:   map[string]string{"cpu": "200m", "memory": "200Mi"},
			wantLimits: map[string]string{"cpu": "300m", "memory": "200Mi"},
		},
		{
			name:       "a Guaranteed pod stays Guaranteed",
			params:     boundsParams(),
			pods:       []*pb.PodState{podWith("pod-1", "cpu", "100m", "memory", "100Mi")},
			wantReqs:   map[string]string{"cpu": "200m", "memory": "200Mi"},
			wantLimits: map[string]string{"cpu": "200m", "memory": "200Mi"},
		},
		{
			name:     "no limit on the pods gives no limit",
			params:   boundsParams(),
			pods:     []*pb.PodState{podWithLimits("pod-1", map[string]string{"cpu": "100m", "memory": "100Mi"}, nil)},
			wantReqs: map[string]string{"cpu": "200m", "memory": "200Mi"},
		},
		{
			name:   "the ratio of most pods is used",
			params: boundsParams(),
			pods: []*pb.PodState{
				burstable("pod-1"), burstable("pod-2"),
				podWith("pod-3", "cpu", "100m", "memory", "100Mi"),
			},
			wantReqs:   map[string]string{"cpu": "200m", "memory": "200Mi"},
			wantLimits: map[string]string{"cpu": "1", "memory": "800Mi"},
		},
		{
			name:   "on a tie, the larger ratio is used",
			params: boundsParams(),
			pods: []*pb.PodState{
				burstable("pod-1"),
				podWithLimits("pod-2", map[string]string{"cpu": "100m", "memory": "100Mi"}, map[string]string{"cpu": "200m", "memory": "800Mi"}),
			},
			wantReqs:   map[string]string{"cpu": "200m", "memory": "200Mi"},
			wantLimits: map[string]string{"cpu": "1", "memory": "1600Mi"},
		},
		{
			name:   "on a tie, no limit wins",
			params: boundsParams(),
			pods: []*pb.PodState{
				burstable("pod-1"),
				podWithLimits("pod-2", map[string]string{"cpu": "100m", "memory": "100Mi"}, map[string]string{"memory": "400Mi"}),
			},
			wantReqs:   map[string]string{"cpu": "200m", "memory": "200Mi"},
			wantLimits: map[string]string{"memory": "800Mi"},
		},
		{
			name:   "keeping the current requests keeps the current limits exactly",
			params: boundsParams(allBounds...),
			pods: []*pb.PodState{
				podWithLimits("pod-1", map[string]string{"cpu": "190m", "memory": "210Mi"}, map[string]string{"cpu": "951m", "memory": "841Mi"}),
			},
			wantReqs:   map[string]string{"cpu": "190m", "memory": "210Mi"},
			wantLimits: map[string]string{"cpu": "951m", "memory": "841Mi"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &VPARecommender{}
			got := r.Recommend(&pb.RecommenderDefinition{Params: tt.params}, boundsState, nil, &pb.Workload{Pods: tt.pods})
			if !got.IsActive {
				t.Fatalf("Recommend() inactive: %s", got.Message)
			}
			want := []*pb.ContainerResource{{ContainerName: "app", Requests: tt.wantReqs, Limits: tt.wantLimits}}
			if diff := cmp.Diff(want, got.WorkloadResources, protocmp.Transform()); diff != "" {
				t.Errorf("Recommend() WorkloadResources mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseConfigLimitRatio(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]string
		wantErr string
	}{
		{name: "valid ratios", params: map[string]string{"container": "app", "cpu-limit-ratio": "5", "mem-limit-ratio": "1.25"}},
		{name: "below 1", params: map[string]string{"container": "app", "cpu-limit-ratio": "0.5"}, wantErr: "it must be a number >= 1"},
		{name: "not a number", params: map[string]string{"container": "app", "mem-limit-ratio": "x"}, wantErr: "it must be a number >= 1"},
		{
			name:    "uncontrolled resource",
			params:  map[string]string{"container": "app", "controlled-resources": "cpu", "mem-limit-ratio": "2"},
			wantErr: "mem-limit-ratio is set, but memory is not in controlled-resources",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseConfig(&pb.RecommenderDefinition{Params: tt.params})
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("parseConfig() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("parseConfig() error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}
