package vpa

import (
	"strings"
	"testing"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestRecommend(t *testing.T) {
	tests := []struct {
		name            string
		def             *pb.RecommenderDefinition
		state           *pb.ControlMetrics
		want            *pb.Recommendation
		wantMsgContains string
	}{
		{
			name: "Invalid cfg in def should return with error message in Message",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":            "app",
					"controlled-resources": "gpu", // only cpu and memory are supported
				},
			},
			state: nil, // not required here
			want: &pb.Recommendation{
				IsActive: false,
			},
			wantMsgContains: "Unable to parse recommender configuration",
		},
		{
			name: "Missing container name in params should return with error message in Message",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"cpu-metric": "cpu_p95",
					"mem-metric": "mem_p95",
				},
			},
			state: &pb.ControlMetrics{},
			want: &pb.Recommendation{
				IsActive: false,
			},
			wantMsgContains: "container is undefined",
		},
		{
			name: "Wrong container name (typo) not present in ContainerMetrics should return inactive recommendation",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":  "wrong-container-name",
					"cpu-metric": "cpu_p95",
					"mem-metric": "mem_p95",
				},
			},
			state: &pb.ControlMetrics{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod-1": {
						ContainerMetrics: map[string]*pb.MetricValues{
							"app": {
								Values: map[string]float64{
									"mem_p95": 268435456,
									"cpu_p95": 0.5,
								},
							},
						},
					},
				},
			},
			want: &pb.Recommendation{
				IsActive: false,
			},
			wantMsgContains: "No Recommendations generated",
		},
		{
			name: "Missed scope: Container (metrics in Global Values instead of PodMetrics) should return PodMetrics is empty",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":  "app",
					"cpu-metric": "cpu_p95",
					"mem-metric": "mem_p95",
				},
			},
			state: &pb.ControlMetrics{
				Values: map[string]float64{
					"mem_p95": 268435456,
					"cpu_p95": 0.5,
				},
			},
			want: &pb.Recommendation{
				IsActive: false,
			},
			wantMsgContains: "PodMetrics is empty",
		},
		{
			name: "Missing state should return with error message in Message",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":  "app",
					"cpu-metric": "cpu_p95",
					"mem-metric": "mem_p95",
				},
			},
			state: nil, //purposefully omitted here
			want: &pb.Recommendation{
				IsActive: false,
			},
			wantMsgContains: "ControlMetrics is missing",
		},
		{
			name: "Missing state.PodMetrics should return with error message in Message",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":  "app",
					"cpu-metric": "cpu_p95",
					"mem-metric": "mem_p95",
				},
			},
			state: &pb.ControlMetrics{},
			want: &pb.Recommendation{
				IsActive: false,
			},
			wantMsgContains: "PodMetrics is empty",
		},
		{
			name: "Missing cpu metric definition, present mem metric definition should result in message with warnings, request limit with no cpu values",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":         "app",
					"cpu-metric":        "cpup95", // bad metric name, not in state
					"mem-metric":        "mem_p95",
					"mem-safety-margin": "1.10",
				},
			},
			state: &pb.ControlMetrics{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod-1": {
						ContainerMetrics: map[string]*pb.MetricValues{
							"app": {
								Values: map[string]float64{
									"mem_p95": 268435456,
									"cpu_p95": 0.5,
								},
							},
						},
					},
				},
			},
			want: &pb.Recommendation{
				IsActive: true,
				WorkloadResources: []*pb.ContainerResource{
					{
						ContainerName: "app",
						Requests: map[string]string{
							"memory": "282Mi",
						},
						Limits: map[string]string{
							"memory": "282Mi",
						},
					},
				},
			},
			wantMsgContains: "warnings: cpu-metric \"cpup95\" not found in state",
		},
		{
			name: "Missing mem metric definition, present cpu metric definition should result in message with warnings, request limit with no mem values",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":         "app",
					"cpu-metric":        "cpu_p95",
					"mem-metric":        "memp95", // bad metric name, not in state
					"mem-safety-margin": "1.10",
					"cpu-safety-margin": "1.10",
				},
			},
			state: &pb.ControlMetrics{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod-1": {
						ContainerMetrics: map[string]*pb.MetricValues{
							"app": {
								Values: map[string]float64{
									"mem_p95": 268435456,
									"cpu_p95": 0.5,
								},
							},
						},
					},
				},
			},
			want: &pb.Recommendation{
				IsActive: true,
				WorkloadResources: []*pb.ContainerResource{
					{
						ContainerName: "app",
						Requests: map[string]string{
							"cpu": "550m",
						},
						Limits: map[string]string{
							"cpu": "550m",
						},
					},
				},
			},
			wantMsgContains: "mem-metric \"memp95\" not found in state",
		},
		{
			name: "Missing both cpu and mem metric definition should result in warning and nil WorkloadRecommendation",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":         "app",
					"cpu-metric":        "cpup95", // bad metric name, not in state
					"mem-metric":        "memp95", // bad metric name, not in state
					"mem-safety-margin": "1.10",
					"cpu-safety-margin": "1.10",
				},
			},
			state: &pb.ControlMetrics{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod-1": {
						ContainerMetrics: map[string]*pb.MetricValues{
							"app": {
								Values: map[string]float64{
									"mem_p95": 268435456,
									"cpu_p95": 0.5,
								},
							},
						},
					},
				},
			},
			want: &pb.Recommendation{
				IsActive:          false,
				WorkloadResources: nil,
			},
			wantMsgContains: "Unable to create recommendation as no value memory or cpu values were found",
		},
		{
			name: "verify CPU and Mem fractional core math and max aggregation across multiple pods",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":         "app",
					"cpu-metric":        "cpu_p95",
					"mem-metric":        "mem_p95",
					"mem-safety-margin": "1.10",
					"cpu-safety-margin": "1.10",
				},
			},
			state: &pb.ControlMetrics{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod-1": {
						ContainerMetrics: map[string]*pb.MetricValues{
							"app": {
								Values: map[string]float64{
									"mem_p95": 134217728, // 128 MiB (lower)
									"cpu_p95": 0.5,       // 500m (higher)
								},
							},
							"sidecar": {
								Values: map[string]float64{
									"mem_p95": 999999999, // ignored because container is "sidecar"
									"cpu_p95": 4.0,
								},
							},
						},
					},
					"pod-2": {
						ContainerMetrics: map[string]*pb.MetricValues{
							"app": {
								Values: map[string]float64{
									"mem_p95": 268435456, // 256 MiB (higher)
									"cpu_p95": 0.2,       // 200m (lower)
								},
							},
						},
					},
				},
			},
			want: &pb.Recommendation{
				IsActive: true,
				WorkloadResources: []*pb.ContainerResource{
					{
						ContainerName: "app",
						Requests: map[string]string{
							"cpu":    "550m",
							"memory": "282Mi",
						},
						Limits: map[string]string{
							"cpu":    "550m",
							"memory": "282Mi",
						},
					},
				},
			},
			wantMsgContains: "; warnings: owned metric \"cpu-lower-bound\" not found in state, no lower bound for cpu",
		},
		{
			name: "verify that low cpu and mem values are clamped by the floors",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":         "app",
					"cpu-metric":        "cpu_p95",
					"mem-metric":        "mem_p95",
					"mem-safety-margin": "1.10",
					"cpu-safety-margin": "1.10",
				},
			},
			state: &pb.ControlMetrics{
				PodContainerMetrics: map[string]*pb.ContainerMetrics{
					"pod-1": {
						ContainerMetrics: map[string]*pb.MetricValues{
							"app": {
								Values: map[string]float64{
									"mem_p95": 1048576, // value too low, should get clamped at minMEMMiB
									"cpu_p95": 0.005,   // value too low, should get clamped at minCPUMilli
								},
							},
						},
					},
				},
			},
			want: &pb.Recommendation{
				IsActive: true,
				WorkloadResources: []*pb.ContainerResource{
					{
						ContainerName: "app",
						Requests: map[string]string{
							"cpu":    "10m",
							"memory": "10Mi",
						},
						Limits: map[string]string{
							"cpu":    "10m",
							"memory": "10Mi",
						},
					},
				},
			},
			wantMsgContains: "resizing to the target",
		},
	}

	// The single pod of the workload has requests far from any target, so the
	// recommendation is always the target. Its limits equal its requests, so
	// the recommended limits equal the recommended requests.
	current := map[string]string{"cpu": "1m", "memory": "1Mi"}
	workload := &pb.Workload{Pods: []*pb.PodState{{
		Name:       "pod-1",
		Containers: []*pb.ContainerState{{Name: "app", Requests: current, Limits: current}},
	}}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &VPARecommender{}
			got := r.Recommend(tt.def, tt.state, nil, workload)

			if tt.wantMsgContains != "" {
				if !strings.Contains(got.Message, tt.wantMsgContains) {
					t.Errorf("got.Message = %q, want it to contain %q", got.Message, tt.wantMsgContains)
				}
			}

			// 2. Compare the rest of the fields (ignoring Message if we checked it via substring)
			opts := []cmp.Option{protocmp.Transform()}
			if tt.wantMsgContains != "" {
				opts = append(opts, protocmp.IgnoreFields(&pb.Recommendation{}, "message"))
			}
			if diff := cmp.Diff(tt.want, got, opts...); diff != "" {
				t.Errorf("Recommend() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]string
		want    *config
		wantErr string
	}{
		{
			name: "both targets and custom margins",
			params: map[string]string{
				"container":         "app",
				"cpu-metric":        "cpu_p95",
				"mem-metric":        "mem_p95",
				"cpu-safety-margin": "1.25",
				"mem-safety-margin": "1.10",
			},
			want: &config{
				containerName: "app",
				resources: map[string]*resourceConfig{
					"cpu":    {safetyMargin: 1.25, metrics: refs(user("cpu_p95"), owned("cpu-lower-bound"), owned("cpu-upper-bound"))},
					"memory": {safetyMargin: 1.10, metrics: refs(user("mem_p95"), owned("memory-lower-bound"), owned("memory-upper-bound"))},
				},
			},
		},
		{
			name: "only mem metrics controls memory only",
			params: map[string]string{
				"container":         "app",
				"mem-metric":        "mem_p95",
				"mem-safety-margin": "1.10",
			},
			want: &config{
				containerName: "app",
				resources: map[string]*resourceConfig{
					"memory": {safetyMargin: 1.10, metrics: refs(user("mem_p95"), owned("memory-lower-bound"), owned("memory-upper-bound"))},
				},
			},
		},
		{
			name: "only a cpu bound controls cpu only, and owns the target",
			params: map[string]string{
				"container":              "app",
				"cpu-upper-bound-metric": " cpu_p99 ",
			},
			want: &config{
				containerName: "app",
				resources: map[string]*resourceConfig{
					"cpu": {safetyMargin: defaultCPUSafetyMarginFloat, metrics: refs(owned("cpu-target"), owned("cpu-lower-bound"), user("cpu_p99"))},
				},
			},
		},
		{
			name: "no metrics owns all of them for cpu and memory",
			params: map[string]string{
				"container":         "app",
				"mem-safety-margin": "1.25",
				"cpu-safety-margin": "1.10",
			},
			want: &config{
				containerName: "app",
				resources: map[string]*resourceConfig{
					"cpu":    {safetyMargin: 1.10, metrics: refs(owned("cpu-target"), owned("cpu-lower-bound"), owned("cpu-upper-bound"))},
					"memory": {safetyMargin: 1.25, metrics: refs(owned("memory-target"), owned("memory-lower-bound"), owned("memory-upper-bound"))},
				},
			},
		},
		{
			name: "all slots configured",
			params: map[string]string{
				"container":              "app",
				"cpu-metric":             "cpu_p90",
				"cpu-lower-bound-metric": "cpu_p50",
				"cpu-upper-bound-metric": "cpu_p99",
				"mem-metric":             "mem_p90",
				"mem-lower-bound-metric": "mem_p50",
				"mem-upper-bound-metric": "mem_p99",
			},
			want: &config{
				containerName: "app",
				resources: map[string]*resourceConfig{
					"cpu":    {safetyMargin: defaultCPUSafetyMarginFloat, metrics: refs(user("cpu_p90"), user("cpu_p50"), user("cpu_p99"))},
					"memory": {safetyMargin: defaultMemSafetyMarginFloat, metrics: refs(user("mem_p90"), user("mem_p50"), user("mem_p99"))},
				},
			},
		},
		{
			name: "controlled-resources memory",
			params: map[string]string{
				"container":            "app",
				"controlled-resources": "memory",
			},
			want: &config{
				containerName: "app",
				resources: map[string]*resourceConfig{
					"memory": {safetyMargin: defaultMemSafetyMarginFloat, metrics: refs(owned("memory-target"), owned("memory-lower-bound"), owned("memory-upper-bound"))},
				},
			},
		},
		{
			name: "controlled-resources adds a resource without metric params",
			params: map[string]string{
				"container":            "app",
				"controlled-resources": " cpu , memory ",
				"cpu-metric":           "cpu_p95",
			},
			want: &config{
				containerName: "app",
				resources: map[string]*resourceConfig{
					"cpu":    {safetyMargin: defaultCPUSafetyMarginFloat, metrics: refs(user("cpu_p95"), owned("cpu-lower-bound"), owned("cpu-upper-bound"))},
					"memory": {safetyMargin: defaultMemSafetyMarginFloat, metrics: refs(owned("memory-target"), owned("memory-lower-bound"), owned("memory-upper-bound"))},
				},
			},
		},
		{
			name: "missing container",
			params: map[string]string{
				"cpu-metric": "cpu_p95",
				"mem-metric": "mem_p95",
			},
			wantErr: "container is undefined",
		},
		{
			name: "invalid safety margin",
			params: map[string]string{
				"container":         "app",
				"cpu-safety-margin": "lots",
			},
			wantErr: "invalid cpu-safety-margin",
		},
		{
			name: "unknown controlled resource",
			params: map[string]string{
				"container":            "app",
				"controlled-resources": "cpu,gpu",
			},
			wantErr: `"gpu" is not one of cpu, memory`,
		},
		{
			name: "metric param for an uncontrolled resource",
			params: map[string]string{
				"container":              "app",
				"controlled-resources":   "cpu",
				"mem-lower-bound-metric": "mem_p50",
			},
			wantErr: "memory metric params are set, but memory is not in controlled-resources",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseConfig(&pb.RecommenderDefinition{Params: tt.params})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseConfig() error = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseConfig() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(config{}, resourceConfig{}, metricRef{})); diff != "" {
				t.Errorf("parseConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func user(name string) metricRef  { return metricRef{name: name} }
func owned(name string) metricRef { return metricRef{name: name, owned: true} }

// refs returns the metrics of the target, lower bound and upper bound slots.
func refs(target, lower, upper metricRef) [numSlots]metricRef {
	return [numSlots]metricRef{targetSlot: target, lowerBoundSlot: lower, upperBoundSlot: upper}
}
