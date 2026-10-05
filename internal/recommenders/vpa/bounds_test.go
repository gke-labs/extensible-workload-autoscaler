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

// podWith returns a pod whose "app" container has the given requests.
func podWith(name string, requests ...string) *pb.PodState {
	reqs := map[string]string{}
	for i := 0; i+1 < len(requests); i += 2 {
		reqs[requests[i]] = requests[i+1]
	}
	return &pb.PodState{Name: name, Containers: []*pb.ContainerState{{Name: "app", Requests: reqs}}}
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
			name:            "no bound metrics leaves both sides unbounded",
			params:          boundsParams(),
			wantMsgContains: "cpu: target 200m [-, -]; memory: target 200Mi [-, -]",
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
			name:            "a missing request moves to the target",
			params:          boundsParams(allBounds...),
			pods:            []*pb.PodState{podWith("pod-1", "cpu", "200m")},
			wantActive:      true,
			want:            target,
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

func TestParseConfigBounds(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]string
		want    *config
		wantErr bool
	}{
		{
			name: "all bound metrics",
			params: map[string]string{
				"container":              "app",
				"cpu-metric":             "cpu_p90",
				"mem-metric":             "mem_p90",
				"cpu-lower-bound-metric": "cpu_p50",
				"cpu-upper-bound-metric": " cpu_p99 ",
				"mem-lower-bound-metric": "mem_p50",
				"mem-upper-bound-metric": "mem_p99",
			},
			want: &config{
				containerName:       "app",
				cpuMetric:           "cpu_p90",
				memMetric:           "mem_p90",
				cpuSafetyMargin:     defaultCPUSafetyMarginFloat,
				memSafetyMargin:     defaultMemSafetyMarginFloat,
				cpuLowerBoundMetric: "cpu_p50",
				cpuUpperBoundMetric: "cpu_p99",
				memLowerBoundMetric: "mem_p50",
				memUpperBoundMetric: "mem_p99",
			},
		},
		{
			name: "cpu bound without cpu-metric",
			params: map[string]string{
				"container":              "app",
				"mem-metric":             "mem_p90",
				"cpu-lower-bound-metric": "cpu_p50",
			},
			wantErr: true,
		},
		{
			name: "memory bound without mem-metric",
			params: map[string]string{
				"container":              "app",
				"cpu-metric":             "cpu_p90",
				"mem-upper-bound-metric": "mem_p99",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseConfig(&pb.RecommenderDefinition{Params: tt.params})
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(config{})); diff != "" {
				t.Errorf("parseConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
