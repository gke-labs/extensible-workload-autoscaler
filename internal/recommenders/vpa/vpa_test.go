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
				DesiredReplicas: 0,
				IsActive:        false,
			},
			wantMsgContains: "Unable to parse recommender configuration",
		},
		{
			name: "Missing state should return with error message in Message",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"cpu-metric": "cpu_p95",
					"mem-metric": "mem_p95",
				},
			},
			state: nil, //purposefully omitted here
			want: &pb.RecommenderVote{
				DesiredReplicas: 0,
				IsActive:        false,
			},
			wantMsgContains: "ControlMetrics is missing",
		},
		{
			name: "Missing state.Values should return with error message in Message",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"cpu-metric": "cpu_p95",
					"mem-metric": "mem_p95",
				},
			},
			state: &pb.ControlMetrics{},
			want: &pb.RecommenderVote{
				DesiredReplicas: 0,
				IsActive:        false,
			},
			wantMsgContains: "ControlMetrics is missing",
		},
		{
			name: "Missing cpu metric definition, present mem metric definition should result in message with warnings, request limit with no cpu values",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"cpu-metric":        "cpup95", // bad metric name, not in state
					"mem-metric":        "mem_p95",
					"mem-safety-margin": "1.10",
				},
			},
			state: &pb.ControlMetrics{
				Values: map[string]float64{
					"mem_p95": 268435456,
					"cpu_p95": 0.5,
				},
			},
			want: &pb.RecommenderVote{
				DesiredReplicas: 0,
				IsActive:        true,
				WorkloadResources: &pb.ResourceRecommendation{
					Requests: map[string]string{
						"memory": "282Mi",
					},
					Limits: map[string]string{
						"memory": "282Mi",
					},
				},
			},
			wantMsgContains: "Recommendation generated with warnings: cpuMetric \"cpup95\" not found in state",
		},
		{
			name: "Missing mem metric definition, present cpu metric definition should result in message with warnings, request limit with no mem values",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"cpu-metric":        "cpu_p95",
					"mem-metric":        "memp95", // bad metric name, not in state
					"mem-safety-margin": "1.10",
					"cpu-safety-margin": "1.10",
				},
			},
			state: &pb.ControlMetrics{
				Values: map[string]float64{
					"mem_p95": 268435456,
					"cpu_p95": 0.5,
				},
			},
			want: &pb.RecommenderVote{
				DesiredReplicas: 0,
				IsActive:        true,
				WorkloadResources: &pb.ResourceRecommendation{
					Requests: map[string]string{
						"cpu": "550m",
					},
					Limits: map[string]string{
						"cpu": "550m",
					},
				},
			},
			wantMsgContains: "Recommendation generated with warnings: memMetric \"memp95\" not found in state",
		},
		{
			name: "Missing both cpu and mem metric definition should result in warning and nil WorkloadRecommendation",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"cpu-metric":        "cpup95", // bad metric name, not in state
					"mem-metric":        "memp95", // bad metric name, not in state
					"mem-safety-margin": "1.10",
					"cpu-safety-margin": "1.10",
				},
			},
			state: &pb.ControlMetrics{
				Values: map[string]float64{
					"mem_p95": 268435456,
					"cpu_p95": 0.5,
				},
			},
			want: &pb.RecommenderVote{
				DesiredReplicas:   0,
				IsActive:          false,
				WorkloadResources: nil,
			},
			wantMsgContains: "Unable to create recommendation as no value memory or cpu values were found",
		},
		{
			name: "verify CPU and Mem fractional core math",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"cpu-metric":        "cpu_p95",
					"mem-metric":        "mem_p95",
					"mem-safety-margin": "1.10",
					"cpu-safety-margin": "1.10",
				},
			},
			state: &pb.ControlMetrics{
				Values: map[string]float64{
					"mem_p95": 268435456,
					"cpu_p95": 0.5,
				},
			},
			want: &pb.RecommenderVote{
				DesiredReplicas: 0,
				IsActive:        true,
				WorkloadResources: &pb.ResourceRecommendation{
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
			wantMsgContains: "Recommendation generated successfully",
		},
		{
			name: "verify that low cpu and mem values are clamped by the floors",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					"cpu-metric":        "cpu_p95",
					"mem-metric":        "mem_p95",
					"mem-safety-margin": "1.10",
					"cpu-safety-margin": "1.10",
				},
			},
			state: &pb.ControlMetrics{
				Values: map[string]float64{
					"mem_p95": 1048576, // value too low, should get clamped at minMEMMiB
					"cpu_p95": 0.005,   // value too low, should get clamped at minCPUMilli
				},
			},
			want: &pb.RecommenderVote{
				DesiredReplicas: 0,
				IsActive:        true,
				WorkloadResources: &pb.ResourceRecommendation{
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
			wantMsgContains: "Recommendation generated successfully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &VPARecommender{}
			got := r.Recommend(tt.def, tt.state)

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
					"cpu-metric":        "cpu_p95",
					"mem-metric":        "mem_p95",
					"cpu-safety-margin": "1.25",
					"mem-safety-margin": "1.10",
				},
			},
			want: &config{
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
					"mem-metric":        "mem_p95",
					"mem-safety-margin": "1.10",
				},
			},
			want: &config{
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
					"cpu-metric":        "cpu_p95",
					"cpu-safety-margin": "1.10",
				},
			},
			want: &config{
				cpuMetric:       "cpu_p95",
				cpuSafetyMargin: 1.10,
				memSafetyMargin: 1.15,
			},
			wantErr: false,
		},
		{
			name: "invalid config with only no metrics",
			def: &pb.RecommenderDefinition{
				Params: map[string]string{
					// missing cpu-metric and mem-metric
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
