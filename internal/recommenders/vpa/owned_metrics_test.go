package vpa

import (
	"strings"
	"testing"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// decaying returns a decaying distribution metric definition.
func decaying(name, typ, percentile, halfLife, bucketSize string) *pb.MetricDefinition {
	return &pb.MetricDefinition{
		Name:     name,
		Provider: "kubelet",
		Params:   map[string]string{"type": typ},
		Scope:    "PodContainer",
		DecayingDistribution: &pb.DecayingDistribution{
			Percentile: percentile,
			HalfLife:   halfLife,
			BucketSize: bucketSize,
		},
	}
}

func defaultOwned(resource, slotSuffix, percentile string) *pb.MetricDefinition {
	bucket := defaultCPUBucket
	if resource == "memory" {
		bucket = defaultMemBucket
	}
	return decaying(resource+slotSuffix, resource, percentile, defaultHalfLife, bucket)
}

func TestOwnedMetrics(t *testing.T) {
	cpuTarget := defaultOwned("cpu", "-target", "p90")
	cpuLower := defaultOwned("cpu", "-lower-bound", "p50")
	cpuUpper := defaultOwned("cpu", "-upper-bound", "p99")
	memTarget := defaultOwned("memory", "-target", "p90")
	memLower := defaultOwned("memory", "-lower-bound", "p50")
	memUpper := defaultOwned("memory", "-upper-bound", "p99")

	// A user CPU metric with its own source, filter, half-life and bucket size.
	userCPU := &pb.MetricDefinition{
		Name:     "cpu_p95",
		Provider: "prometheus",
		Params:   map[string]string{"query": "container_cpu"},
		Filter:   map[string]string{"env": "prod"},
		Scope:    "PodContainer",
		DecayingDistribution: &pb.DecayingDistribution{
			Percentile: "p95",
			HalfLife:   "12h",
			BucketSize: "0.01",
		},
	}
	copied := func(name, percentile string) *pb.MetricDefinition {
		return &pb.MetricDefinition{
			Name:     name,
			Provider: "prometheus",
			Params:   map[string]string{"query": "container_cpu"},
			Filter:   map[string]string{"env": "prod"},
			Scope:    "PodContainer",
			DecayingDistribution: &pb.DecayingDistribution{
				Percentile: percentile,
				HalfLife:   "12h",
				BucketSize: "0.01",
			},
		}
	}

	allPolicyMetrics := []*pb.MetricDefinition{
		decaying("cpu_p50", "cpu", "p50", "24h", "0.05"),
		decaying("cpu_p90", "cpu", "p90", "24h", "0.05"),
		decaying("cpu_p99", "cpu", "p99", "24h", "0.05"),
		decaying("mem_p50", "memory", "p50", "24h", "10485760"),
		decaying("mem_p90", "memory", "p90", "24h", "10485760"),
		decaying("mem_p99", "memory", "p99", "24h", "10485760"),
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
			policyMetrics: []*pb.MetricDefinition{{
				Name: "cpu_gauge", Provider: "kubelet", Scope: "PodContainer",
				Gauge: &pb.Gauge{Aggregation: "Max"},
			}},
			wantErr: `cpu-metric "cpu_gauge" must be a decayingDistribution metric`,
		},
		{
			name:   "user metrics of one resource with different half-lives",
			params: map[string]string{"mem-metric": "mem_p90", "mem-upper-bound-metric": "mem_p99_short"},
			policyMetrics: []*pb.MetricDefinition{
				decaying("mem_p90", "memory", "p90", "24h", "10485760"),
				decaying("mem_p99_short", "memory", "p99", "1h", "10485760"),
			},
			wantErr: "the memory metrics must have the same halfLife and bucketSize",
		},
		{
			name:   "user metrics of one resource with different bucket sizes",
			params: map[string]string{"cpu-lower-bound-metric": "cpu_p50", "cpu-upper-bound-metric": "cpu_p99_fine"},
			policyMetrics: []*pb.MetricDefinition{
				decaying("cpu_p50", "cpu", "p50", "24h", "0.05"),
				decaying("cpu_p99_fine", "cpu", "p99", "24h", "0.01"),
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
			got, err := r.OwnedMetrics(&pb.RecommenderDefinition{Name: "vpa-sizing", Params: params}, tt.policyMetrics)
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
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("OwnedMetrics() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRecommendOwnedMetrics(t *testing.T) {
	const mib = 1024 * 1024
	podMetrics := func(values map[string]float64) *pb.ControlMetrics {
		return &pb.ControlMetrics{PodContainerMetrics: map[string]*pb.ContainerMetrics{
			"pod-1": {ContainerMetrics: map[string]*pb.MetricValues{"app": {Values: values}}},
		}}
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
			want: &pb.ContainerResource{
				ContainerName: "app",
				Requests:      map[string]string{"cpu": "200m", "memory": "200Mi"},
				Limits:        map[string]string{"cpu": "200m", "memory": "200Mi"},
			},
			wantSummary: "cpu: target 200m [100m, 400m]; memory: target 200Mi [100Mi, 400Mi]",
		},
		{
			name:   "user cpu target with owned bounds, all memory owned",
			params: with("cpu-metric", "cpu_p95", "controlled-resources", "cpu,memory"),
			state:  podMetrics(map[string]float64{"cpu_p95": 0.3}),
			owned:  owned,
			want: &pb.ContainerResource{
				ContainerName: "app",
				Requests:      map[string]string{"cpu": "300m", "memory": "200Mi"},
				Limits:        map[string]string{"cpu": "300m", "memory": "200Mi"},
			},
			wantSummary: "cpu: target 300m [100m, 400m]; memory: target 200Mi [100Mi, 400Mi]",
		},
		{
			name:   "user memory upper bound with owned target, memory only",
			params: with("mem-upper-bound-metric", "mem_max"),
			state:  podMetrics(map[string]float64{"mem_max": 800 * mib}),
			owned:  owned,
			want: &pb.ContainerResource{
				ContainerName: "app",
				Requests:      map[string]string{"memory": "200Mi"},
				Limits:        map[string]string{"memory": "200Mi"},
			},
			wantSummary: "memory: target 200Mi [100Mi, 800Mi]",
		},
		{
			name:   "owned metrics only, nil policy-wide metrics",
			params: with("controlled-resources", "cpu"),
			state:  nil,
			owned:  owned,
			want: &pb.ContainerResource{
				ContainerName: "app",
				Requests:      map[string]string{"cpu": "200m"},
				Limits:        map[string]string{"cpu": "200m"},
			},
			wantSummary: "cpu: target 200m [100m, 400m]",
		},
	}

	// The pod's requests are far from any target, so the target is
	// recommended. Its limits equal its requests, so the recommended limits
	// equal the recommended requests.
	current := map[string]string{"cpu": "1m", "memory": "1Mi"}
	workload := &pb.Workload{Pods: []*pb.PodState{{
		Name:       "pod-1",
		Containers: []*pb.ContainerState{{Name: "app", Requests: current, Limits: current}},
	}}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &VPARecommender{}
			got := r.Recommend(&pb.RecommenderDefinition{Params: tt.params}, tt.state, tt.owned, workload)
			if !got.IsActive {
				t.Fatalf("Recommend() inactive: %s", got.Message)
			}
			if !strings.Contains(got.Message, tt.wantSummary) || strings.Contains(got.Message, "warnings") {
				t.Errorf("Recommend() Message = %q, want it to contain %q and no warnings", got.Message, tt.wantSummary)
			}
			if diff := cmp.Diff([]*pb.ContainerResource{tt.want}, got.WorkloadResources, protocmp.Transform()); diff != "" {
				t.Errorf("Recommend() WorkloadResources mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRecommendNoOwnedData(t *testing.T) {
	r := &VPARecommender{}
	got := r.Recommend(&pb.RecommenderDefinition{Params: map[string]string{"container": "app"}}, &pb.ControlMetrics{}, &pb.ControlMetrics{}, nil)
	if got.IsActive {
		t.Fatalf("Recommend() active, want inactive without data")
	}
	if !strings.Contains(got.Message, "PodMetrics is empty") {
		t.Errorf("Recommend() Message = %q, want it to contain %q", got.Message, "PodMetrics is empty")
	}
}
