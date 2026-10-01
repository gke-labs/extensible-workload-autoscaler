package policy_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
)

// etagTestPolicy returns a policy that sets every field, including several
// map entries, so that non-deterministic map ordering would show up.
func etagTestPolicy() *pb.Policy {
	return &pb.Policy{
		Id:          &pb.PolicyId{ClusterName: "c", Namespace: "ns", Name: "pol"},
		Workload:    &pb.WorkloadRef{Kind: "Deployment", Name: "web"},
		MinReplicas: 1,
		MaxReplicas: 10,
		Selector:    "app=web",
		Metrics: []*pb.MetricDefinition{{
			Name:   "rps",
			Params: map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"},
		}},
		Scaling:    []*pb.RecommenderDefinition{{Name: "linear"}},
		Activation: []*pb.RecommenderDefinition{{Name: "cron"}},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"vpa":  {Definitions: []*pb.MetricDefinition{{Name: "cpu"}, {Name: "memory"}}},
			"hpa":  {Definitions: []*pb.MetricDefinition{{Name: "qps"}}},
			"cron": {Definitions: []*pb.MetricDefinition{{Name: "tick"}}},
			"ml":   {Definitions: []*pb.MetricDefinition{{Name: "forecast"}}},
		},
	}
}

// etagOf returns the ETag computed for p. It fails the test on error.
func etagOf(t *testing.T, p *pb.Policy) string {
	t.Helper()
	etag, err := policy.CreateEtag(p)
	if err != nil {
		t.Fatalf("CreateEtag() error = %v", err)
	}
	return etag
}

func TestCreateEtag_ReturnsNonEmptyEtag(t *testing.T) {
	if got := etagOf(t, etagTestPolicy()); got == "" {
		t.Errorf("CreateEtag() returned an empty ETag")
	}
}

func TestCreateEtag_SameContentSameEtag(t *testing.T) {
	want := etagOf(t, etagTestPolicy())
	// Repeat many times: map iteration order is random in Go, so a
	// non-deterministic marshaling would eventually produce another ETag.
	for i := 0; i < 100; i++ {
		if got := etagOf(t, etagTestPolicy()); got != want {
			t.Fatalf("run %d: ETag = %q, want %q", i, got, want)
		}
	}
}

func TestCreateEtag_MapInsertionOrderDoesNotMatter(t *testing.T) {
	a := etagTestPolicy()
	b := etagTestPolicy()
	b.RecommenderMetrics = map[string]*pb.MetricDefinitionList{}
	for _, k := range []string{"ml", "cron", "hpa", "vpa"} {
		b.RecommenderMetrics[k] = proto.Clone(a.RecommenderMetrics[k]).(*pb.MetricDefinitionList)
	}

	if ea, eb := etagOf(t, a), etagOf(t, b); ea != eb {
		t.Errorf("ETags differ for equal policies: %q vs %q", ea, eb)
	}
}

func TestCreateEtag_AnyFieldChangeChangesEtag(t *testing.T) {
	base := etagOf(t, etagTestPolicy())

	tests := []struct {
		name   string
		mutate func(p *pb.Policy)
	}{
		{"id", func(p *pb.Policy) { p.Id.Name = "other" }},
		{"workload", func(p *pb.Policy) { p.Workload.Name = "api" }},
		{"min_replicas", func(p *pb.Policy) { p.MinReplicas = 2 }},
		{"max_replicas", func(p *pb.Policy) { p.MaxReplicas = 11 }},
		{"selector", func(p *pb.Policy) { p.Selector = "app=api" }},
		{"metrics", func(p *pb.Policy) { p.Metrics[0].Name = "latency" }},
		{"metric params", func(p *pb.Policy) { p.Metrics[0].Params["a"] = "9" }},
		{"scaling", func(p *pb.Policy) { p.Scaling[0].Name = "vpa" }},
		{"activation", func(p *pb.Policy) { p.Activation = nil }},
		{"recommender_metrics entry", func(p *pb.Policy) { delete(p.RecommenderMetrics, "hpa") }},
		{"recommender_metrics value", func(p *pb.Policy) {
			p.RecommenderMetrics["vpa"].Definitions[0].Name = "gpu"
		}},
		{"list order", func(p *pb.Policy) {
			d := p.RecommenderMetrics["vpa"].Definitions
			d[0], d[1] = d[1], d[0]
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := etagTestPolicy()
			tt.mutate(p)
			if got := etagOf(t, p); got == base {
				t.Errorf("changing %s did not change the ETag %q", tt.name, got)
			}
		})
	}
}

func TestCreateEtag_DoesNotModifyInput(t *testing.T) {
	p := etagTestPolicy()
	p.Etag = "previous-etag"
	before := proto.Clone(p).(*pb.Policy)

	if _, err := policy.CreateEtag(p); err != nil {
		t.Fatalf("CreateEtag() error = %v", err)
	}

	if diff := cmp.Diff(before, p, protocmp.Transform()); diff != "" {
		t.Errorf("CreateEtag() modified its input (-before +after):\n%s", diff)
	}
}

// TestCreateEtag_IgnoresExistingEtag checks that the ETag a client sends has no
// effect on the computed one: the store hashes a copy of the client's policy.
func TestCreateEtag_IgnoresExistingEtag(t *testing.T) {
	empty := etagTestPolicy()
	stale := etagTestPolicy()
	stale.Etag = "stale-etag"

	if e1, e2 := etagOf(t, empty), etagOf(t, stale); e1 != e2 {
		t.Errorf("the input etag changed the result: %q vs %q", e1, e2)
	}
}

// TestCreateEtag_CoversAllPolicyFields fails when a field is added to or
// removed from Policy, so that the field list in CreateEtag is kept complete.
func TestCreateEtag_CoversAllPolicyFields(t *testing.T) {
	const nrPolicyFields = 10
	lenFields := (&pb.Policy{}).ProtoReflect().Descriptor().Fields().Len()
	if lenFields != nrPolicyFields {
		t.Errorf("policy wants %d fields, but found %d. A field was added or removed: update CreateEtag(), or the change won't be part of the ETag hash. Then add a case to TestCreateEtag_AnyFieldChangeChangesEtag and update nrPolicyFields.", nrPolicyFields, lenFields)
	}
}
