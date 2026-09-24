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
		want            *pb.RecommenderVote
		wantMsgContains string
	}{
		{
			name: "Invalid cfg in def should return with error message in Message",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"cpumetric": "cpu_p95", // this is incorrect. It should be cpu-metric
					"memmetric": "mem_p95", // this is incorrect. It should be mem-metric
				},
			},
			state: nil, // not required here
			want: &pb.RecommenderVote{
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
			want: &pb.RecommenderVote{
				IsActive: false,
			},
			wantMsgContains: "container is undefined",
		},
		{
			name: "Wrong container name (typo) not present in ContainerMetrics should return inactive vote",
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
			want: &pb.RecommenderVote{
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
			want: &pb.RecommenderVote{
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
			want: &pb.RecommenderVote{
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
			want: &pb.RecommenderVote{
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
			want: &pb.RecommenderVote{
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
			wantMsgContains: "Recommendation generated with warnings: cpuMetric \"cpup95\" not found in state",
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
			want: &pb.RecommenderVote{
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
			wantMsgContains: "Recommendation generated with warnings: memMetric \"memp95\" not found in state",
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
			want: &pb.RecommenderVote{
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
			want: &pb.RecommenderVote{
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
			wantMsgContains: "Recommendation generated successfully",
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
			want: &pb.RecommenderVote{
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
			wantMsgContains: "Recommendation generated successfully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &VPARecommender{}
			got := r.Recommend(tt.def, tt.state, nil)

			if tt.wantMsgContains != "" {
				if !strings.Contains(got.Message, tt.wantMsgContains) {
					t.Errorf("got.Message = %q, want it to contain %q", got.Message, tt.wantMsgContains)
				}
			}

			// 2. Compare the rest of the fields (ignoring Message if we checked it via substring)
			opts := []cmp.Option{protocmp.Transform()}
			if tt.wantMsgContains != "" {
				opts = append(opts, protocmp.IgnoreFields(&pb.RecommenderVote{}, "message"))
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
		def     *pb.RecommenderDefinition
		want    *config
		wantErr bool
	}{
		{
			name: "valid config with both metrics and custom margins",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":         "app",
					"cpu-metric":        "cpu_p95",
					"mem-metric":        "mem_p95",
					"cpu-safety-margin": "1.25",
					"mem-safety-margin": "1.10",
				},
			},
			want: &config{
				containerName:   "app",
				cpuMetric:       "cpu_p95",
				memMetric:       "mem_p95",
				cpuSafetyMargin: 1.25,
				memSafetyMargin: 1.10,
			},
			wantErr: false,
		},
		{
			name: "valid config with only mem metrics",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":         "app",
					"mem-metric":        "mem_p95",
					"mem-safety-margin": "1.10",
				},
			},
			want: &config{
				containerName:   "app",
				memMetric:       "mem_p95",
				cpuSafetyMargin: 1.15,
				memSafetyMargin: 1.10,
			},
			wantErr: false,
		},
		{
			name: "valid config with only cpu metrics",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":         "app",
					"cpu-metric":        "cpu_p95",
					"cpu-safety-margin": "1.10",
				},
			},
			want: &config{
				containerName:   "app",
				cpuMetric:       "cpu_p95",
				cpuSafetyMargin: 1.10,
				memSafetyMargin: 1.15,
			},
			wantErr: false,
		},
		{
			name: "invalid config with missing container",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"cpu-metric": "cpu_p95",
					"mem-metric": "mem_p95",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "invalid config with only no metrics",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"container":         "app",
					"mem-safety-margin": "1.25",
					"cpu-safety-margin": "1.10",
				},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseConfig(tt.def)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(config{})); diff != "" {
					t.Errorf("parseConfig() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}
