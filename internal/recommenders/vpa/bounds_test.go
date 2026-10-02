package vpa

import (
	"strings"
	"testing"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestRecommendBounds(t *testing.T) {
	const mib = 1024 * 1024
	// Values per pod for container "app". The recommendation uses the max
	// across pods with a safety margin of 1 to keep the numbers simple.
	state := &pb.ControlMetrics{
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
	params := func(kv ...string) map[string]string {
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
	target := func(lower, upper map[string]string) *pb.ContainerResource {
		return &pb.ContainerResource{
			ContainerName: "app",
			Requests:      map[string]string{"cpu": "200m", "memory": "200Mi"},
			Limits:        map[string]string{"cpu": "200m", "memory": "200Mi"},
			LowerBound:    lower,
			UpperBound:    upper,
		}
	}

	tests := []struct {
		name            string
		params          map[string]string
		want            *pb.ContainerResource
		wantMsgContains string
	}{
		{
			name:   "no bound metrics leaves both sides unbounded",
			params: params(),
			want:   target(nil, nil),
		},
		{
			name: "both bounds for cpu and memory",
			params: params(
				"cpu-lower-bound-metric", "cpu_p50", "cpu-upper-bound-metric", "cpu_p99",
				"mem-lower-bound-metric", "mem_p50", "mem-upper-bound-metric", "mem_p99",
			),
			want: target(
				map[string]string{"cpu": "150m", "memory": "150Mi"},
				map[string]string{"cpu": "400m", "memory": "400Mi"},
			),
		},
		{
			name:   "only some bounds configured",
			params: params("cpu-upper-bound-metric", "cpu_p99", "mem-lower-bound-metric", "mem_p50"),
			want: target(
				map[string]string{"memory": "150Mi"},
				map[string]string{"cpu": "400m"},
			),
		},
		{
			name: "bounds are clamped around the target",
			// Swapped metrics: the lower bound metric is above the target and
			// the upper bound metric below it.
			params: params(
				"cpu-lower-bound-metric", "cpu_p99", "cpu-upper-bound-metric", "cpu_p50",
				"mem-lower-bound-metric", "mem_p99", "mem-upper-bound-metric", "mem_p50",
			),
			want: target(
				map[string]string{"cpu": "200m", "memory": "200Mi"},
				map[string]string{"cpu": "200m", "memory": "200Mi"},
			),
		},
		{
			name:            "bound metric without data is left out with a warning",
			params:          params("cpu-lower-bound-metric", "missing", "cpu-upper-bound-metric", "cpu_p99"),
			want:            target(nil, map[string]string{"cpu": "400m"}),
			wantMsgContains: `cpu-lower-bound-metric "missing" not found in state`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &VPARecommender{}
			got := r.Recommend(&pb.RecommenderDefinition{Params: tt.params}, state, nil)
			if !got.IsActive {
				t.Fatalf("Recommend() inactive: %s", got.Message)
			}
			want := []*pb.ContainerResource{tt.want}
			if diff := cmp.Diff(want, got.WorkloadResources, protocmp.Transform()); diff != "" {
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
