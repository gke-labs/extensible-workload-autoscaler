package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

// fakeControlPlaneClient records the policy updates the engine pushes. Only
// UpdatePolicy is exercised here; the embedded interface makes the other
// methods panic if they are ever called.
type fakeControlPlaneClient struct {
	pb.XASControlPlaneClient
	requests []*pb.UpdatePolicyRequest
	err      error
}

func (c *fakeControlPlaneClient) UpdatePolicy(_ context.Context, req *pb.UpdatePolicyRequest, _ ...grpc.CallOption) (*pb.Policy, error) {
	c.requests = append(c.requests, proto.Clone(req).(*pb.UpdatePolicyRequest))
	if c.err != nil {
		return nil, c.err
	}
	return req.Policy, nil
}

// owningRecommender owns the metrics declared for each recommender instance
// name it knows about.
type owningRecommender struct {
	metrics map[string][]*pb.MetricDefinition
}

func (r *owningRecommender) Recommend(*pb.RecommenderDefinition, *pb.ControlMetrics, *pb.ControlMetrics) *pb.RecommenderVote {
	return nil
}

func (r *owningRecommender) OwnedMetrics(def *pb.RecommenderDefinition) []*pb.MetricDefinition {
	return r.metrics[def.Name]
}

// plainRecommender does not own any metric.
type plainRecommender struct{}

func (plainRecommender) Recommend(*pb.RecommenderDefinition, *pb.ControlMetrics, *pb.ControlMetrics) *pb.RecommenderVote {
	return nil
}

// newTestEngine builds an engine backed by fakes. classes maps the name of a
// RecommenderClass to its type, recommenders maps a type to its implementation.
func newTestEngine(t *testing.T, client pb.XASControlPlaneClient, classes map[string]string, recommenders map[string]Recommender) *Engine {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for name, typ := range classes {
		if err := indexer.Add(&xasv1.RecommenderClass{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       xasv1.RecommenderClassSpec{Type: typ},
		}); err != nil {
			t.Fatalf("Failed to seed the RecommenderClass %q: %v", name, err)
		}
	}
	return &Engine{
		client:                 client,
		recommenderClassLister: listers.NewRecommenderClassLister(indexer),
		recommenders:           recommenders,
		clusterName:            "default",
	}
}

func cpuMetric(owner string) *pb.MetricDefinition {
	return &pb.MetricDefinition{Name: "cpu", RecommenderName: owner, Gauge: &pb.Gauge{Aggregation: "Max"}}
}

func memoryMetric(owner string) *pb.MetricDefinition {
	return &pb.MetricDefinition{Name: "memory", RecommenderName: owner, Gauge: &pb.Gauge{Aggregation: "Max"}}
}

func TestSyncRecommenderMetrics(t *testing.T) {
	id := &pb.PolicyId{ClusterName: "default", Namespace: "prod", Name: "web"}
	// "vpa" is backed by a recommender that owns metrics, "hpa" by one that does
	// not, and "custom" by a class this engine does not know about.
	classes := map[string]string{"owning-class": "Owning", "plain-class": "Plain", "third-party-class": "ThirdParty"}
	scaling := []*pb.RecommenderDefinition{
		{Name: "vpa", Recommender: "owning-class"},
		{Name: "hpa", Recommender: "plain-class"},
		{Name: "custom", Recommender: "third-party-class"},
		{Name: "ghost", Recommender: "missing-class"},
	}

	tests := []struct {
		name string
		// owned is what the owning recommender claims, keyed by recommender name.
		owned        map[string][]*pb.MetricDefinition
		policy       *pb.Policy
		clientErr    error
		wantRequests []*pb.UpdatePolicyRequest
		wantPolicy   *pb.Policy
	}{
		{
			name:   "Registers the metrics of an owner on a policy that has none",
			owned:  map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa")}},
			policy: &pb.Policy{Id: id, Scaling: scaling},
			wantRequests: []*pb.UpdatePolicyRequest{{
				Policy: &pb.Policy{Id: id, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}},
				}},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"recommender_metrics.vpa"}},
			}},
			wantPolicy: &pb.Policy{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}},
			}},
		},
		{
			name:  "Updates the metrics already registered",
			owned: map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa"), memoryMetric("vpa")}},
			policy: &pb.Policy{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}},
			}},
			wantRequests: []*pb.UpdatePolicyRequest{{
				Policy: &pb.Policy{Id: id, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa"), memoryMetric("vpa")}},
				}},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"recommender_metrics.vpa"}},
			}},
			wantPolicy: &pb.Policy{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa"), memoryMetric("vpa")}},
			}},
		},
		{
			name:  "No update when the metrics are already registered",
			owned: map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa")}},
			policy: &pb.Policy{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}},
			}},
			wantPolicy: &pb.Policy{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}},
			}},
		},
		{
			name:       "No update when no recommender owns metrics",
			owned:      nil,
			policy:     &pb.Policy{Id: id, Scaling: scaling},
			wantPolicy: &pb.Policy{Id: id, Scaling: scaling},
		},
		{
			name:  "Entries owned by other writers are left untouched",
			owned: map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa")}},
			policy: &pb.Policy{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				// Registered by another binary: outside the mask, and kept as is.
				"custom": {Definitions: []*pb.MetricDefinition{cpuMetric("custom")}},
			}},
			wantRequests: []*pb.UpdatePolicyRequest{{
				Policy: &pb.Policy{Id: id, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}},
				}},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"recommender_metrics.vpa"}},
			}},
			wantPolicy: &pb.Policy{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa":    {Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}},
				"custom": {Definitions: []*pb.MetricDefinition{cpuMetric("custom")}},
			}},
		},
		{
			name:  "Metrics an owner no longer needs are dropped",
			owned: nil,
			policy: &pb.Policy{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}},
			}},
			wantRequests: []*pb.UpdatePolicyRequest{{
				Policy: &pb.Policy{Id: id, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": {},
				}},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"recommender_metrics.vpa"}},
			}},
			wantPolicy: &pb.Policy{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": {},
			}},
		},
		{
			name:      "A failed update leaves the policy alone",
			owned:     map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa")}},
			policy:    &pb.Policy{Id: id, Scaling: scaling},
			clientErr: errors.New("server unavailable"),
			wantRequests: []*pb.UpdatePolicyRequest{{
				Policy: &pb.Policy{Id: id, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}},
				}},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"recommender_metrics.vpa"}},
			}},
			wantPolicy: &pb.Policy{Id: id, Scaling: scaling},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeControlPlaneClient{err: tc.clientErr}
			e := newTestEngine(t, client, classes, map[string]Recommender{
				"Owning": &owningRecommender{metrics: tc.owned},
				"Plain":  plainRecommender{},
			})

			got := e.syncRecommenderMetrics(tc.policy)

			if diff := cmp.Diff(tc.wantRequests, client.requests, protocmp.Transform(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("UpdatePolicy requests mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantPolicy, got, protocmp.Transform()); diff != "" {
				t.Errorf("syncRecommenderMetrics() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSyncRecommenderMetricsSendsOnlyChangedEntries checks that a recommender
// whose metrics are up to date is left out of the update, so the engine does not
// rewrite the entries of the other recommenders it manages.
func TestSyncRecommenderMetricsSendsOnlyChangedEntries(t *testing.T) {
	id := &pb.PolicyId{ClusterName: "default", Namespace: "prod", Name: "web"}
	client := &fakeControlPlaneClient{}
	e := newTestEngine(t, client,
		map[string]string{"owning-class": "Owning"},
		map[string]Recommender{"Owning": &owningRecommender{metrics: map[string][]*pb.MetricDefinition{
			"vpa":  {cpuMetric("vpa"), memoryMetric("vpa")},
			"vpa2": {cpuMetric("vpa2")},
		}}},
	)

	pol := &pb.Policy{
		Id: id,
		Scaling: []*pb.RecommenderDefinition{
			{Name: "vpa", Recommender: "owning-class"},
			{Name: "vpa2", Recommender: "owning-class"},
		},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			// Already up to date.
			"vpa2": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa2")}},
		},
	}

	got := e.syncRecommenderMetrics(pol)

	wantRequests := []*pb.UpdatePolicyRequest{{
		Policy: &pb.Policy{Id: id, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"vpa": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa"), memoryMetric("vpa")}},
		}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"recommender_metrics.vpa"}},
	}}
	if diff := cmp.Diff(wantRequests, client.requests, protocmp.Transform()); diff != "" {
		t.Errorf("UpdatePolicy requests mismatch (-want +got):\n%s", diff)
	}

	wantMetrics := map[string]*pb.MetricDefinitionList{
		"vpa":  {Definitions: []*pb.MetricDefinition{cpuMetric("vpa"), memoryMetric("vpa")}},
		"vpa2": {Definitions: []*pb.MetricDefinition{cpuMetric("vpa2")}},
	}
	if diff := cmp.Diff(wantMetrics, got.RecommenderMetrics, protocmp.Transform()); diff != "" {
		t.Errorf("RecommenderMetrics mismatch (-want +got):\n%s", diff)
	}
}

// TestSyncRecommenderMetricsDoesNotMutateInput makes sure the policy the Server
// returned is not modified in place: the caller keeps using it if the update
// fails.
func TestSyncRecommenderMetricsDoesNotMutateInput(t *testing.T) {
	id := &pb.PolicyId{ClusterName: "default", Namespace: "prod", Name: "web"}
	client := &fakeControlPlaneClient{}
	e := newTestEngine(t, client,
		map[string]string{"owning-class": "Owning"},
		map[string]Recommender{"Owning": &owningRecommender{metrics: map[string][]*pb.MetricDefinition{
			"vpa": {cpuMetric("vpa")},
		}}},
	)

	pol := &pb.Policy{
		Id:      id,
		Scaling: []*pb.RecommenderDefinition{{Name: "vpa", Recommender: "owning-class"}},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"custom": {Definitions: []*pb.MetricDefinition{cpuMetric("custom")}},
		},
	}
	before := proto.Clone(pol).(*pb.Policy)

	e.syncRecommenderMetrics(pol)

	if diff := cmp.Diff(before, pol, protocmp.Transform()); diff != "" {
		t.Errorf("syncRecommenderMetrics() modified its input (-before +after):\n%s", diff)
	}
}
