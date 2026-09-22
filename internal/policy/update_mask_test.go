package policy_test

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
)

func metricList(names ...string) *pb.MetricDefinitionList {
	l := &pb.MetricDefinitionList{}
	for _, n := range names {
		l.Definitions = append(l.Definitions, &pb.MetricDefinition{Name: n})
	}
	return l
}

func TestApplyUpdateMask(t *testing.T) {
	id := &pb.PolicyId{ClusterName: "c", Namespace: "ns", Name: "pol"}
	current := &pb.Policy{
		Id:          id,
		MinReplicas: 1,
		MaxReplicas: 10,
		Selector:    "app=web",
		Metrics:     []*pb.MetricDefinition{{Name: "rps"}},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"vpa": metricList("cpu", "memory"),
			"hpa": metricList("qps"),
		},
	}

	tests := []struct {
		name    string
		current *pb.Policy
		update  *pb.Policy
		paths   []string
		want    *pb.Policy
		wantErr bool
	}{
		{
			name:    "No mask replaces the whole policy",
			current: current,
			update:  &pb.Policy{Id: id, MaxReplicas: 3},
			want:    &pb.Policy{Id: id, MaxReplicas: 3},
		},
		{
			name:    "Wildcard only updates the fields the update sets",
			current: current,
			update:  &pb.Policy{Id: id, MaxReplicas: 3},
			paths:   []string{"*"},
			want: &pb.Policy{
				Id:          id,
				MinReplicas: 1,
				MaxReplicas: 3,
				Selector:    "app=web",
				Metrics:     []*pb.MetricDefinition{{Name: "rps"}},
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": metricList("cpu", "memory"),
					"hpa": metricList("qps"),
				},
			},
		},
		{
			name:    "Single field is replaced",
			current: current,
			update:  &pb.Policy{Id: id, Metrics: []*pb.MetricDefinition{{Name: "latency"}}},
			paths:   []string{"metrics"},
			want: &pb.Policy{
				Id:          id,
				MinReplicas: 1,
				MaxReplicas: 10,
				Selector:    "app=web",
				Metrics:     []*pb.MetricDefinition{{Name: "latency"}},
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": metricList("cpu", "memory"),
					"hpa": metricList("qps"),
				},
			},
		},
		{
			name:    "A field of a nested message is replaced",
			current: &pb.Policy{Id: id, MaxReplicas: 10, Workload: &pb.WorkloadRef{Kind: "Deployment", Name: "web", Namespace: "prod"}},
			update:  &pb.Policy{Id: id, Workload: &pb.WorkloadRef{Name: "web-canary"}},
			paths:   []string{"workload.name"},
			want: &pb.Policy{
				Id:          id,
				MaxReplicas: 10,
				Workload:    &pb.WorkloadRef{Kind: "Deployment", Name: "web-canary", Namespace: "prod"},
			},
		},
		{
			name:    "Masked field unset in the update is cleared",
			current: current,
			update:  &pb.Policy{Id: id},
			paths:   []string{"selector", "max_replicas"},
			want: &pb.Policy{
				Id:          id,
				MinReplicas: 1,
				Metrics:     []*pb.MetricDefinition{{Name: "rps"}},
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": metricList("cpu", "memory"),
					"hpa": metricList("qps"),
				},
			},
		},
		{
			name:    "Single recommender_metrics entry is updated",
			current: current,
			update: &pb.Policy{Id: id, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": metricList("cpu"),
				// Entries outside the mask are ignored.
				"hpa": metricList("ignored"),
			}},
			paths: []string{policy.RecommenderMetricsPath("vpa")},
			want: &pb.Policy{
				Id:          id,
				MinReplicas: 1,
				MaxReplicas: 10,
				Selector:    "app=web",
				Metrics:     []*pb.MetricDefinition{{Name: "rps"}},
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": metricList("cpu"),
					"hpa": metricList("qps"),
				},
			},
		},
		{
			name:    "New recommender_metrics entry is added",
			current: current,
			update: &pb.Policy{Id: id, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"cron": metricList("schedule"),
			}},
			paths: []string{policy.RecommenderMetricsPath("cron")},
			want: &pb.Policy{
				Id:          id,
				MinReplicas: 1,
				MaxReplicas: 10,
				Selector:    "app=web",
				Metrics:     []*pb.MetricDefinition{{Name: "rps"}},
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa":  metricList("cpu", "memory"),
					"hpa":  metricList("qps"),
					"cron": metricList("schedule"),
				},
			},
		},
		{
			name:    "Masked recommender_metrics entry missing from the update is removed",
			current: current,
			update:  &pb.Policy{Id: id},
			paths:   []string{policy.RecommenderMetricsPath("vpa")},
			want: &pb.Policy{
				Id:          id,
				MinReplicas: 1,
				MaxReplicas: 10,
				Selector:    "app=web",
				Metrics:     []*pb.MetricDefinition{{Name: "rps"}},
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"hpa": metricList("qps"),
				},
			},
		},
		{
			name:    "Removing an unknown recommender_metrics entry is a no-op",
			current: &pb.Policy{Id: id, MaxReplicas: 10},
			update:  &pb.Policy{Id: id},
			paths:   []string{policy.RecommenderMetricsPath("vpa")},
			want:    &pb.Policy{Id: id, MaxReplicas: 10},
		},
		{
			name:    "Masked update creates a missing policy",
			current: nil,
			update: &pb.Policy{Id: id, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": metricList("cpu"),
			}},
			paths: []string{policy.RecommenderMetricsPath("vpa")},
			want: &pb.Policy{Id: id, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": metricList("cpu"),
			}},
		},
		{
			name:    "Unknown field is rejected",
			current: current,
			update:  &pb.Policy{Id: id},
			paths:   []string{"not_a_field"},
			wantErr: true,
		},
		{
			name:    "Key on a non-map field is rejected",
			current: current,
			update:  &pb.Policy{Id: id},
			paths:   []string{"metrics.cpu"},
			wantErr: true,
		},
		{
			name:    "Empty map key is rejected",
			current: current,
			update:  &pb.Policy{Id: id},
			paths:   []string{"recommender_metrics."},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var mask *fieldmaskpb.FieldMask
			if tc.paths != nil {
				mask = &fieldmaskpb.FieldMask{Paths: tc.paths}
			}
			currentBefore := proto.Clone(tc.current)
			updateBefore := proto.Clone(tc.update)

			got, err := policy.ApplyUpdateMask(tc.current, tc.update, mask)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("ApplyUpdateMask() error = %v, wantErr %t", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("ApplyUpdateMask() mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(currentBefore, tc.current, protocmp.Transform()); diff != "" {
				t.Errorf("ApplyUpdateMask() modified the current policy (-before +after):\n%s", diff)
			}
			if diff := cmp.Diff(updateBefore, tc.update, protocmp.Transform()); diff != "" {
				t.Errorf("ApplyUpdateMask() modified the update (-before +after):\n%s", diff)
			}
		})
	}
}

// TestApplyUpdateMaskCopies makes sure the merged policy does not alias the
// messages of its inputs: mutating it must not corrupt the stored policy.
func TestApplyUpdateMaskCopies(t *testing.T) {
	id := &pb.PolicyId{ClusterName: "c", Namespace: "ns", Name: "pol"}
	current := &pb.Policy{Id: id, Metrics: []*pb.MetricDefinition{{Name: "rps"}}}
	update := &pb.Policy{Id: id, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
		"vpa": metricList("cpu"),
	}}

	merged, err := policy.ApplyUpdateMask(current, update, &fieldmaskpb.FieldMask{
		Paths: []string{policy.RecommenderMetricsPath("vpa")},
	})
	if err != nil {
		t.Fatalf("ApplyUpdateMask() returned an unexpected error: %v", err)
	}

	merged.Metrics[0].Name = "mutated"
	merged.RecommenderMetrics["vpa"].Definitions[0].Name = "mutated"

	if got := current.Metrics[0].Name; got != "rps" {
		t.Errorf("current policy metric name = %q, want %q", got, "rps")
	}
	if got := update.RecommenderMetrics["vpa"].Definitions[0].Name; got != "cpu" {
		t.Errorf("update metric name = %q, want %q", got, "cpu")
	}
}

func TestValidateUpdateMask(t *testing.T) {
	tests := []struct {
		name    string
		mask    *fieldmaskpb.FieldMask
		wantErr string
	}{
		{name: "Nil mask"},
		{name: "Wildcard", mask: &fieldmaskpb.FieldMask{Paths: []string{"*"}}},
		{name: "Fields", mask: &fieldmaskpb.FieldMask{Paths: []string{"metrics", "min_replicas"}}},
		{name: "Map entry", mask: &fieldmaskpb.FieldMask{Paths: []string{"recommender_metrics.vpa"}}},
		{name: "Nested field", mask: &fieldmaskpb.FieldMask{Paths: []string{"workload.name"}}},
		{
			name:    "Unknown field",
			mask:    &fieldmaskpb.FieldMask{Paths: []string{"metrics", "replicas"}},
			wantErr: `unknown path: "replicas"`,
		},
		{
			name:    "Unknown nested field",
			mask:    &fieldmaskpb.FieldMask{Paths: []string{"scaling.linear"}},
			wantErr: `unknown path: "scaling.linear"`,
		},
		{
			name:    "Nested field of a scalar",
			mask:    &fieldmaskpb.FieldMask{Paths: []string{"selector.app"}},
			wantErr: "can't get nested fields",
		},
		{
			name:    "Missing map key",
			mask:    &fieldmaskpb.FieldMask{Paths: []string{"recommender_metrics."}},
			wantErr: "empty segment",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := policy.ValidateUpdateMask(tc.mask)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateUpdateMask() returned an unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ValidateUpdateMask() error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}
