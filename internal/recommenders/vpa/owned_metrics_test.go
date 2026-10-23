package vpa

import (
	"strings"
	"testing"
	"time"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
)

// decaying returns a decaying distribution metric definition.
func decaying(name, typ, percentile string, halfLife time.Duration, bucketSize string) *pb.MetricDefinition {
	return pb.MetricDefinition_builder{
		Name:     proto.String(name),
		Provider: proto.String("kubelet"),
		Params:   map[string]string{"type": typ},
		Scope:    proto.String("PodContainer"),
		DecayingDistribution: pb.DecayingDistribution_builder{
			Percentile: proto.String(percentile),
			HalfLife:   durationpb.New(halfLife),
			BucketSize: proto.String(bucketSize),
		}.Build(),
	}.Build()
}

// defaultOwned returns an owned metric definition with the defaults, which
// consider the startup samples.
func defaultOwned(resource, slotSuffix, percentile string) *pb.MetricDefinition {
	bucket := defaultCPUBucket
	if resource == "memory" {
		bucket = defaultMemBucket
	}
	m := decaying(resource+slotSuffix, resource, percentile, defaultHalfLife, bucket)
	m.SetEligibility(pb.MetricDefinition_Eligibility_builder{StartupSamples: proto.Bool(true)}.Build())
	return m
}

func TestOwnedMetrics(t *testing.T) {
	cpuTarget := defaultOwned("cpu", "-target", "p90")
	cpuLower := defaultOwned("cpu", "-lower-bound", "p50")
	cpuUpper := defaultOwned("cpu", "-upper-bound", "p99")
	memTarget := defaultOwned("memory", "-target", "p90")
	memLower := defaultOwned("memory", "-lower-bound", "p50")
	memUpper := defaultOwned("memory", "-upper-bound", "p99")

	// A user CPU metric with its own source, filter, half-life and bucket size.
	userCPU := pb.MetricDefinition_builder{
		Name:     proto.String("cpu_p95"),
		Provider: proto.String("prometheus"),
		Params:   map[string]string{"query": "container_cpu"},
		Filter:   map[string]string{"env": "prod"},
		Scope:    proto.String("PodContainer"),
		// Explicitly without the startup samples, unlike the defaults.
		Eligibility: pb.MetricDefinition_Eligibility_builder{StartupSamples: proto.Bool(false)}.Build(),
		DecayingDistribution: pb.DecayingDistribution_builder{
			Percentile: proto.String("p95"),
			HalfLife:   durationpb.New(12 * time.Hour),
			BucketSize: proto.String("0.01"),
		}.Build(),
	}.Build()
	// The owned metrics copied from the user's metric follow its
	// eligibility setting.
	copied := func(name, percentile string) *pb.MetricDefinition {
		return pb.MetricDefinition_builder{
			Name:        proto.String(name),
			Provider:    proto.String("prometheus"),
			Params:      map[string]string{"query": "container_cpu"},
			Filter:      map[string]string{"env": "prod"},
			Scope:       proto.String("PodContainer"),
			Eligibility: pb.MetricDefinition_Eligibility_builder{StartupSamples: proto.Bool(false)}.Build(),
			DecayingDistribution: pb.DecayingDistribution_builder{
				Percentile: proto.String(percentile),
				HalfLife:   durationpb.New(12 * time.Hour),
				BucketSize: proto.String("0.01"),
			}.Build(),
		}.Build()
	}

	allPolicyMetrics := []*pb.MetricDefinition{
		decaying("cpu_p50", "cpu", "p50", 24*time.Hour, "0.05"),
		decaying("cpu_p90", "cpu", "p90", 24*time.Hour, "0.05"),
		decaying("cpu_p99", "cpu", "p99", 24*time.Hour, "0.05"),
		decaying("mem_p50", "memory", "p50", 24*time.Hour, "10485760"),
		decaying("mem_p90", "memory", "p90", 24*time.Hour, "10485760"),
		decaying("mem_p99", "memory", "p99", 24*time.Hour, "10485760"),
	}

	tests := []struct {
		name          string
		params        map[string]string
		policyMetrics []*pb.MetricDefinition
		want          []*pb.MetricDefinition
		wantErr       string
	}{
		{
			name:   "nothing set owns all metrics with the defaults",
			params: map[string]string{},
			want:   []*pb.MetricDefinition{cpuTarget, cpuLower, cpuUpper, memTarget, memLower, memUpper},
		},
		{
			name:          "a cpu target with controlled memory copies the cpu metric to the cpu bounds",
			params:        map[string]string{"cpu-metric": "cpu_p95", "controlled-resources": "cpu,memory"},
			policyMetrics: []*pb.MetricDefinition{userCPU},
			want: []*pb.MetricDefinition{
				copied("cpu-lower-bound", "p50"), copied("cpu-upper-bound", "p99"),
				memTarget, memLower, memUpper,
			},
		},
		{
			name:          "only a cpu target controls cpu only",
			params:        map[string]string{"cpu-metric": "cpu_p95"},
			policyMetrics: []*pb.MetricDefinition{userCPU},
			want:          []*pb.MetricDefinition{copied("cpu-lower-bound", "p50"), copied("cpu-upper-bound", "p99")},
		},
		{
			name: "all slots set owns nothing",
			params: map[string]string{
				"cpu-metric": "cpu_p90", "cpu-lower-bound-metric": "cpu_p50", "cpu-upper-bound-metric": "cpu_p99",
				"mem-metric": "mem_p90", "mem-lower-bound-metric": "mem_p50", "mem-upper-bound-metric": "mem_p99",
			},
			policyMetrics: allPolicyMetrics,
			want:          nil,
		},
		{
			name:   "controlled-resources memory owns memory metrics only",
			params: map[string]string{"controlled-resources": "memory"},
			want:   []*pb.MetricDefinition{memTarget, memLower, memUpper},
		},
		{
			name:          "configured metric not defined in the policy",
			params:        map[string]string{"cpu-metric": "cpu_p95", "mem-upper-bound-metric": "nope"},
			policyMetrics: []*pb.MetricDefinition{userCPU},
			wantErr:       `mem-upper-bound-metric "nope" is not defined in the policy metrics`,
		},
		{
			name:   "configured metric that isn't a decaying distribution",
			params: map[string]string{"cpu-metric": "cpu_gauge"},
			policyMetrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{
				Name: proto.String("cpu_gauge"), Provider: proto.String("kubelet"), Scope: proto.String("PodContainer"),
				Gauge: pb.Gauge_builder{Aggregation: proto.String("Max")}.Build(),
			}.Build()},
			wantErr: `cpu-metric "cpu_gauge" must be a decayingDistribution metric`,
		},
		{
			name:   "user metrics of one resource with different half-lives",
			params: map[string]string{"mem-metric": "mem_p90", "mem-upper-bound-metric": "mem_p99_short"},
			policyMetrics: []*pb.MetricDefinition{
				decaying("mem_p90", "memory", "p90", 24*time.Hour, "10485760"),
				decaying("mem_p99_short", "memory", "p99", time.Hour, "10485760"),
			},
			wantErr: "the memory metrics must have the same halfLife and bucketSize",
		},
		{
			name:   "user metrics of one resource with different bucket sizes",
			params: map[string]string{"cpu-lower-bound-metric": "cpu_p50", "cpu-upper-bound-metric": "cpu_p99_fine"},
			policyMetrics: []*pb.MetricDefinition{
				decaying("cpu_p50", "cpu", "p50", 24*time.Hour, "0.05"),
				decaying("cpu_p99_fine", "cpu", "p99", 24*time.Hour, "0.01"),
			},
			wantErr: "the cpu metrics must have the same halfLife and bucketSize",
		},
		{
			name:    "metric param for an uncontrolled resource",
			params:  map[string]string{"controlled-resources": "memory", "cpu-metric": "cpu_p90"},
			wantErr: "cpu metric params are set, but cpu is not in controlled-resources",
		},
		{
			name:    "invalid config",
			params:  map[string]string{"container": ""},
			wantErr: "container is undefined",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]string{"container": "app"}
			for k, v := range tt.params {
				params[k] = v
			}
			r := &VPARecommender{}
			got, err := r.OwnedMetrics(pb.RecommenderDefinition_builder{Name: proto.String("vpa-sizing"), Params: params}.Build(), tt.policyMetrics)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("OwnedMetrics() error = %v, want it to contain %q", err, tt.wantErr)
				}
				if got != nil {
					t.Errorf("OwnedMetrics() = %v, want nil on error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("OwnedMetrics() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
				t.Errorf("OwnedMetrics() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRecommendOwnedMetrics(t *testing.T) {
	const mib = 1024 * 1024
	podMetrics := func(values map[string]float64) *pb.ControlMetrics {
		return pb.ControlMetrics_builder{PodContainerMetrics: map[string]*pb.ContainerMetrics{
			"pod-1": pb.ContainerMetrics_builder{ContainerMetrics: map[string]*pb.MetricValues{"app": pb.MetricValues_builder{Values: values}.Build()}}.Build(),
		}}.Build()
	}
	owned := podMetrics(map[string]float64{
		"cpu-target": 0.2, "cpu-lower-bound": 0.1, "cpu-upper-bound": 0.4,
		"memory-target": 200 * mib, "memory-lower-bound": 100 * mib, "memory-upper-bound": 400 * mib,
	})
	margins := map[string]string{"container": "app", "cpu-safety-margin": "1", "mem-safety-margin": "1"}
	with := func(kv ...string) map[string]string {
		p := map[string]string{}
		for k, v := range margins {
			p[k] = v
		}
		for i := 0; i+1 < len(kv); i += 2 {
			p[kv[i]] = kv[i+1]
		}
		return p
	}

	tests := []struct {
		name   string
		params map[string]string
		state  *pb.ControlMetrics
		owned  *pb.ControlMetrics
		want   *pb.ContainerResource
		// wantSummary is the target and bounds reported in the message.
		wantSummary string
	}{
		{
			name:   "all slots owned",
			params: with(),
			owned:  owned,
			want: pb.ContainerResource_builder{
				ContainerName: proto.String("app"),
				Requests:      map[string]string{"cpu": "200m", "memory": "200Mi"},
				Limits:        map[string]string{"cpu": "200m", "memory": "200Mi"},
			}.Build(),
			wantSummary: "cpu: target 200m [100m, 400m]; memory: target 200Mi [100Mi, 400Mi]",
		},
		{
			name:   "user cpu target with owned bounds, all memory owned",
			params: with("cpu-metric", "cpu_p95", "controlled-resources", "cpu,memory"),
			state:  podMetrics(map[string]float64{"cpu_p95": 0.3}),
			owned:  owned,
			want: pb.ContainerResource_builder{
				ContainerName: proto.String("app"),
				Requests:      map[string]string{"cpu": "300m", "memory": "200Mi"},
				Limits:        map[string]string{"cpu": "300m", "memory": "200Mi"},
			}.Build(),
			wantSummary: "cpu: target 300m [100m, 400m]; memory: target 200Mi [100Mi, 400Mi]",
		},
		{
			name:   "user memory upper bound with owned target, memory only",
			params: with("mem-upper-bound-metric", "mem_max"),
			state:  podMetrics(map[string]float64{"mem_max": 800 * mib}),
			owned:  owned,
			want: pb.ContainerResource_builder{
				ContainerName: proto.String("app"),
				Requests:      map[string]string{"memory": "200Mi"},
				Limits:        map[string]string{"memory": "200Mi"},
			}.Build(),
			wantSummary: "memory: target 200Mi [100Mi, 800Mi]",
		},
		{
			name:   "owned metrics only, nil policy-wide metrics",
			params: with("controlled-resources", "cpu"),
			state:  nil,
			owned:  owned,
			want: pb.ContainerResource_builder{
				ContainerName: proto.String("app"),
				Requests:      map[string]string{"cpu": "200m"},
				Limits:        map[string]string{"cpu": "200m"},
			}.Build(),
			wantSummary: "cpu: target 200m [100m, 400m]",
		},
	}

	// The pod's requests are far from any target, so the target is
	// recommended. Its limits equal its requests, so the recommended limits
	// equal the recommended requests.
	current := map[string]string{"cpu": "1m", "memory": "1Mi"}
	workload := pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{
		Name:       proto.String("pod-1"),
		Containers: []*pb.ContainerState{pb.ContainerState_builder{Name: proto.String("app"), Requests: current, Limits: current}.Build()},
	}.Build()}}.Build()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &VPARecommender{}
			got := r.Recommend(pb.RecommenderDefinition_builder{Params: tt.params}.Build(), tt.state, tt.owned, 0, workload)
			if !got.GetIsActive() {
				t.Fatalf("Recommend() inactive: %s", got.GetMessage())
			}
			if !strings.Contains(got.GetMessage(), tt.wantSummary) || strings.Contains(got.GetMessage(), "warnings") {
				t.Errorf("Recommend() Message = %q, want it to contain %q and no warnings", got.GetMessage(), tt.wantSummary)
			}
			if diff := cmp.Diff([]*pb.ContainerResource{tt.want}, got.GetWorkloadResources(), protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
				t.Errorf("Recommend() WorkloadResources mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRecommendNoOwnedData(t *testing.T) {
	r := &VPARecommender{}
	got := r.Recommend(pb.RecommenderDefinition_builder{Params: map[string]string{"container": "app"}}.Build(), &pb.ControlMetrics{}, &pb.ControlMetrics{}, 0, nil)
	if got.GetIsActive() {
		t.Fatalf("Recommend() active, want inactive without data")
	}
	if !strings.Contains(got.GetMessage(), "PodMetrics is empty") {
		t.Errorf("Recommend() Message = %q, want it to contain %q", got.GetMessage(), "PodMetrics is empty")
	}
}
