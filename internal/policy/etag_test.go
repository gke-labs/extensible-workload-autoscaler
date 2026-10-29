package policy_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
)

// etagTestPolicy returns a policy that sets every field, including several
// map entries, so that non-deterministic map ordering would show up.
func etagTestPolicy() *pb.Policy {
	return pb.Policy_builder{
		Id:          pb.PolicyId_builder{ClusterName: proto.String("c"), Namespace: proto.String("ns"), Name: proto.String("pol")}.Build(),
		Workload:    pb.WorkloadRef_builder{Kind: proto.String("Deployment"), Name: proto.String("web")}.Build(),
		MinReplicas: proto.Int32(1),
		MaxReplicas: proto.Int32(10),
		Selector:    proto.String("app=web"),
		Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{
			Name:   proto.String("rps"),
			Params: map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"},
		}.Build()},
		Scaling:    []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("linear")}.Build()},
		Activation: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("cron")}.Build()},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"vpa":  pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("cpu")}.Build(), pb.MetricDefinition_builder{Name: proto.String("memory")}.Build()}}.Build(),
			"hpa":  pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("qps")}.Build()}}.Build(),
			"cron": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("tick")}.Build()}}.Build(),
			"ml":   pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("forecast")}.Build()}}.Build(),
		},
	}.Build()
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
	b.SetRecommenderMetrics(map[string]*pb.MetricDefinitionList{})
	for _, k := range []string{"ml", "cron", "hpa", "vpa"} {
		b.GetRecommenderMetrics()[k] = proto.Clone(a.GetRecommenderMetrics()[k]).(*pb.MetricDefinitionList)
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
		{"id", func(p *pb.Policy) { p.GetId().SetName("other") }},
		{"workload", func(p *pb.Policy) { p.GetWorkload().SetName("api") }},
		{"min_replicas", func(p *pb.Policy) { p.SetMinReplicas(2) }},
		{"max_replicas", func(p *pb.Policy) { p.SetMaxReplicas(11) }},
		{"selector", func(p *pb.Policy) { p.SetSelector("app=api") }},
		{"metrics", func(p *pb.Policy) { p.GetMetrics()[0].SetName("latency") }},
		{"metric params", func(p *pb.Policy) { p.GetMetrics()[0].GetParams()["a"] = "9" }},
		{"scaling", func(p *pb.Policy) { p.GetScaling()[0].SetName("vpa") }},
		{"activation", func(p *pb.Policy) { p.SetActivation(nil) }},
		{"recommender_metrics entry", func(p *pb.Policy) { delete(p.GetRecommenderMetrics(), "hpa") }},
		{"recommender_metrics value", func(p *pb.Policy) {
			p.GetRecommenderMetrics()["vpa"].GetDefinitions()[0].SetName("gpu")
		}},
		{"list order", func(p *pb.Policy) {
			d := p.GetRecommenderMetrics()["vpa"].GetDefinitions()
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
	p.SetEtag("previous-etag")
	before := proto.Clone(p).(*pb.Policy)

	if _, err := policy.CreateEtag(p); err != nil {
		t.Fatalf("CreateEtag() error = %v", err)
	}

	if diff := cmp.Diff(before, p, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("CreateEtag() modified its input (-before +after):\n%s", diff)
	}
}

// TestCreateEtag_IgnoresExistingEtag checks that the ETag a client sends has no
// effect on the computed one: the store hashes a copy of the client's policy.
func TestCreateEtag_IgnoresExistingEtag(t *testing.T) {
	empty := etagTestPolicy()
	stale := etagTestPolicy()
	stale.SetEtag("stale-etag")

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
