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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

// fakeXASServerClient records the policy updates the engine pushes. Only
// UpdatePolicy is exercised here; the embedded interface makes the other
// methods panic if they are ever called.
type fakeXASServerClient struct {
	pb.XASServerClient
	requests []*pb.UpdatePolicyRequest
	err      error
	// etag, when set, is the ETag the fake Server puts on the returned policy,
	// like the real Server does after a successful write.
	etag string
}

func (c *fakeXASServerClient) UpdatePolicy(_ context.Context, req *pb.UpdatePolicyRequest, _ ...grpc.CallOption) (*pb.Policy, error) {
	c.requests = append(c.requests, proto.Clone(req).(*pb.UpdatePolicyRequest))
	if c.err != nil {
		return nil, c.err
	}
	resp := proto.Clone(req.GetPolicy()).(*pb.Policy)
	if c.etag != "" {
		resp.SetEtag(c.etag)
	}
	return resp, nil
}

// owningRecommender owns the metrics declared for each recommender instance
// name it knows about, or fails with the error declared for it.
type owningRecommender struct {
	metrics map[string][]*pb.MetricDefinition
	errs    map[string]error
	// gotPolicyMetrics records the policy-wide metrics passed to OwnedMetrics.
	gotPolicyMetrics []*pb.MetricDefinition
}

func (r *owningRecommender) Recommend(*pb.RecommenderDefinition, *pb.ControlMetrics, *pb.ControlMetrics, int32, *pb.Workload) *pb.Recommendation {
	return nil
}

func (r *owningRecommender) OwnedMetrics(def *pb.RecommenderDefinition, policyMetrics []*pb.MetricDefinition) ([]*pb.MetricDefinition, error) {
	r.gotPolicyMetrics = policyMetrics
	if err := r.errs[def.GetName()]; err != nil {
		return nil, err
	}
	return r.metrics[def.GetName()], nil
}

// plainRecommender does not own any metric.
type plainRecommender struct{}

func (plainRecommender) Recommend(*pb.RecommenderDefinition, *pb.ControlMetrics, *pb.ControlMetrics, int32, *pb.Workload) *pb.Recommendation {
	return nil
}

// newTestEngine builds an engine backed by fakes. classes maps the name of a
// RecommenderClass to its type, recommenders maps a type to its implementation.
func newTestEngine(t *testing.T, client pb.XASServerClient, classes map[string]string, recommenders map[string]Recommender) *Engine {
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
		state:                  make(State),
		recommenderClassLister: listers.NewRecommenderClassLister(indexer),
		recommenders:           recommenders,
		clusterName:            "default",
	}
}

func cpuMetric(owner string) *pb.MetricDefinition {
	return pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String(owner), Gauge: pb.Gauge_builder{Aggregation: proto.String("Max")}.Build()}.Build()
}

func memoryMetric(owner string) *pb.MetricDefinition {
	return pb.MetricDefinition_builder{Name: proto.String("memory"), RecommenderName: proto.String(owner), Gauge: pb.Gauge_builder{Aggregation: proto.String("Max")}.Build()}.Build()
}

func TestSyncRecommenderMetrics(t *testing.T) {
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()
	// "vpa" is backed by a recommender that owns metrics, "hpa" by one that does
	// not, and "custom" by a class this engine does not know about.
	classes := map[string]string{"owning-class": "Owning", "plain-class": "Plain", "third-party-class": "ThirdParty"}
	scaling := []*pb.RecommenderDefinition{
		pb.RecommenderDefinition_builder{Name: proto.String("vpa"), Recommender: proto.String("owning-class")}.Build(),
		pb.RecommenderDefinition_builder{Name: proto.String("hpa"), Recommender: proto.String("plain-class")}.Build(),
		pb.RecommenderDefinition_builder{Name: proto.String("custom"), Recommender: proto.String("third-party-class")}.Build(),
		pb.RecommenderDefinition_builder{Name: proto.String("ghost"), Recommender: proto.String("missing-class")}.Build(),
	}

	tests := []struct {
		name string
		// owned is what the owning recommender claims, keyed by recommender name.
		owned     map[string][]*pb.MetricDefinition
		policy    *pb.Policy
		clientErr error
		// serverEtag is the ETag the fake Server puts on the policy it returns.
		serverEtag   string
		wantRequests []*pb.UpdatePolicyRequest
		wantPolicy   *pb.Policy
	}{
		{
			name:   "Registers the metrics of an owner on a policy that has none",
			owned:  map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa")}},
			policy: pb.Policy_builder{Id: id, Scaling: scaling}.Build(),
			wantRequests: []*pb.UpdatePolicyRequest{pb.UpdatePolicyRequest_builder{
				Policy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}}.Build(),
				}}.Build(),
			}.Build()},
			wantPolicy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}}.Build(),
			}}.Build(),
		},
		{
			name:  "Updates the metrics already registered",
			owned: map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa"), memoryMetric("vpa")}},
			policy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}}.Build(),
			}}.Build(),
			wantRequests: []*pb.UpdatePolicyRequest{pb.UpdatePolicyRequest_builder{
				Policy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa"), memoryMetric("vpa")}}.Build(),
				}}.Build(),
			}.Build()},
			wantPolicy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa"), memoryMetric("vpa")}}.Build(),
			}}.Build(),
		},
		{
			name:  "No update when the metrics are already registered",
			owned: map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa")}},
			policy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}}.Build(),
			}}.Build(),
			wantPolicy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}}.Build(),
			}}.Build(),
		},
		{
			name:       "No update when no recommender owns metrics",
			owned:      nil,
			policy:     pb.Policy_builder{Id: id, Scaling: scaling}.Build(),
			wantPolicy: pb.Policy_builder{Id: id, Scaling: scaling}.Build(),
		},
		{
			name:  "Entries owned by other writers are left untouched",
			owned: map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa")}},
			policy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				// Registered by another binary: sent back unchanged.
				"custom": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("custom")}}.Build(),
			}}.Build(),
			wantRequests: []*pb.UpdatePolicyRequest{pb.UpdatePolicyRequest_builder{
				Policy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa":    pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}}.Build(),
					"custom": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("custom")}}.Build(),
				}}.Build(),
			}.Build()},
			wantPolicy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa":    pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}}.Build(),
				"custom": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("custom")}}.Build(),
			}}.Build(),
		},
		{
			name:  "The entry of an owner that no longer needs metrics is deleted",
			owned: nil,
			policy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa":    pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}}.Build(),
				"custom": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("custom")}}.Build(),
			}}.Build(),
			wantRequests: []*pb.UpdatePolicyRequest{pb.UpdatePolicyRequest_builder{
				Policy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"custom": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("custom")}}.Build(),
				}}.Build(),
			}.Build()},
			wantPolicy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"custom": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("custom")}}.Build(),
			}}.Build(),
		},
		{
			name:  "An empty entry left by an earlier version is deleted",
			owned: nil,
			policy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": {},
			}}.Build(),
			wantRequests: []*pb.UpdatePolicyRequest{pb.UpdatePolicyRequest_builder{
				Policy: pb.Policy_builder{Id: id, Scaling: scaling}.Build(),
			}.Build()},
			wantPolicy: pb.Policy_builder{Id: id, Scaling: scaling}.Build(),
		},
		{
			name: "Stamps the owner on the returned metrics",
			owned: map[string][]*pb.MetricDefinition{"vpa": {
				pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Max")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("memory"), RecommenderName: proto.String("someone-else"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Max")}.Build()}.Build(),
			}},
			policy: pb.Policy_builder{Id: id, Scaling: scaling}.Build(),
			wantRequests: []*pb.UpdatePolicyRequest{pb.UpdatePolicyRequest_builder{
				Policy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa"), memoryMetric("vpa")}}.Build(),
				}}.Build(),
			}.Build()},
			wantPolicy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa"), memoryMetric("vpa")}}.Build(),
			}}.Build(),
		},
		{
			name:       "Sends the listed ETag and returns the policy with the new one",
			owned:      map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa")}},
			policy:     pb.Policy_builder{Id: id, Scaling: scaling, Etag: proto.String("listed")}.Build(),
			serverEtag: "after-write",
			wantRequests: []*pb.UpdatePolicyRequest{pb.UpdatePolicyRequest_builder{
				Policy: pb.Policy_builder{Id: id, Scaling: scaling, Etag: proto.String("listed"), RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}}.Build(),
				}}.Build(),
			}.Build()},
			wantPolicy: pb.Policy_builder{Id: id, Scaling: scaling, Etag: proto.String("after-write"), RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}}.Build(),
			}}.Build(),
		},
		{
			name:      "A failed update leaves the policy alone",
			owned:     map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa")}},
			policy:    pb.Policy_builder{Id: id, Scaling: scaling}.Build(),
			clientErr: errors.New("server unavailable"),
			wantRequests: []*pb.UpdatePolicyRequest{pb.UpdatePolicyRequest_builder{
				Policy: pb.Policy_builder{Id: id, Scaling: scaling, RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}}.Build(),
				}}.Build(),
			}.Build()},
			wantPolicy: pb.Policy_builder{Id: id, Scaling: scaling}.Build(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeXASServerClient{err: tc.clientErr, etag: tc.serverEtag}
			e := newTestEngine(t, client, classes, map[string]Recommender{
				"Owning": &owningRecommender{metrics: tc.owned},
				"Plain":  plainRecommender{},
			})

			got, _ := e.syncRecommenderMetrics(tc.policy)

			if diff := cmp.Diff(tc.wantRequests, client.requests, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("UpdatePolicy requests mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantPolicy, got, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
				t.Errorf("syncRecommenderMetrics() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSyncRecommenderMetricsKeepsUpToDateEntries checks that when only one
// recommender's metrics changed, the entries of the other recommenders the
// engine manages are sent back as they were, not dropped or rewritten.
func TestSyncRecommenderMetricsKeepsUpToDateEntries(t *testing.T) {
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()
	client := &fakeXASServerClient{}
	e := newTestEngine(t, client,
		map[string]string{"owning-class": "Owning"},
		map[string]Recommender{"Owning": &owningRecommender{metrics: map[string][]*pb.MetricDefinition{
			"vpa":  {cpuMetric("vpa"), memoryMetric("vpa")},
			"vpa2": {cpuMetric("vpa2")},
		}}},
	)

	pol := pb.Policy_builder{
		Id: id,
		Scaling: []*pb.RecommenderDefinition{
			pb.RecommenderDefinition_builder{Name: proto.String("vpa"), Recommender: proto.String("owning-class")}.Build(),
			pb.RecommenderDefinition_builder{Name: proto.String("vpa2"), Recommender: proto.String("owning-class")}.Build(),
		},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			// Already up to date.
			"vpa2": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa2")}}.Build(),
		},
	}.Build()

	got, _ := e.syncRecommenderMetrics(pol)

	wantRequests := []*pb.UpdatePolicyRequest{pb.UpdatePolicyRequest_builder{
		Policy: pb.Policy_builder{Id: id, Scaling: pol.GetScaling(), RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"vpa":  pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa"), memoryMetric("vpa")}}.Build(),
			"vpa2": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa2")}}.Build(),
		}}.Build(),
	}.Build()}
	if diff := cmp.Diff(wantRequests, client.requests, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("UpdatePolicy requests mismatch (-want +got):\n%s", diff)
	}

	wantMetrics := map[string]*pb.MetricDefinitionList{
		"vpa":  pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa"), memoryMetric("vpa")}}.Build(),
		"vpa2": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa2")}}.Build(),
	}
	if diff := cmp.Diff(wantMetrics, got.GetRecommenderMetrics(), protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("RecommenderMetrics mismatch (-want +got):\n%s", diff)
	}
}

// TestSyncRecommenderMetricsDoesNotMutateInput makes sure the policy the Server
// returned is not modified in place: the caller keeps using it if the update
// fails.
func TestSyncRecommenderMetricsDoesNotMutateInput(t *testing.T) {
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()
	client := &fakeXASServerClient{}
	e := newTestEngine(t, client,
		map[string]string{"owning-class": "Owning"},
		map[string]Recommender{"Owning": &owningRecommender{metrics: map[string][]*pb.MetricDefinition{
			"vpa": {cpuMetric("vpa")},
		}}},
	)

	pol := pb.Policy_builder{
		Id:      id,
		Scaling: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("vpa"), Recommender: proto.String("owning-class")}.Build()},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"custom": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("custom")}}.Build(),
		},
	}.Build()
	before := proto.Clone(pol).(*pb.Policy)

	e.syncRecommenderMetrics(pol)

	if diff := cmp.Diff(before, pol, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("syncRecommenderMetrics() modified its input (-before +after):\n%s", diff)
	}
}

// TestSyncRecommenderMetricsActivation checks that the recommenders of the
// activation phase can own metrics too, matching the Server garbage
// collection, which keeps the metrics of both phases.
func TestSyncRecommenderMetricsActivation(t *testing.T) {
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()
	client := &fakeXASServerClient{}
	e := newTestEngine(t, client,
		map[string]string{"owning-class": "Owning"},
		map[string]Recommender{"Owning": &owningRecommender{metrics: map[string][]*pb.MetricDefinition{
			"gate": {cpuMetric("gate")},
		}}},
	)
	pol := pb.Policy_builder{Id: id, Activation: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("gate"), Recommender: proto.String("owning-class")}.Build()}}.Build()

	got, _ := e.syncRecommenderMetrics(pol)

	want := map[string]*pb.MetricDefinitionList{"gate": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("gate")}}.Build()}
	if diff := cmp.Diff(want, got.GetRecommenderMetrics(), protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("RecommenderMetrics mismatch (-want +got):\n%s", diff)
	}
}

// TestSyncRecommenderMetricsPassesPolicyMetrics checks that OwnedMetrics gets
// the policy-wide metrics.
func TestSyncRecommenderMetricsPassesPolicyMetrics(t *testing.T) {
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()
	rec := &owningRecommender{}
	e := newTestEngine(t, &fakeXASServerClient{}, map[string]string{"owning-class": "Owning"}, map[string]Recommender{"Owning": rec})
	policyMetrics := []*pb.MetricDefinition{cpuMetric("")}
	pol := pb.Policy_builder{Id: id, Metrics: policyMetrics, Scaling: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("vpa"), Recommender: proto.String("owning-class")}.Build()}}.Build()

	e.syncRecommenderMetrics(pol)

	if diff := cmp.Diff(policyMetrics, rec.gotPolicyMetrics, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("OwnedMetrics() policy metrics mismatch (-want +got):\n%s", diff)
	}
}

// TestSyncRecommenderMetricsConfigError checks that the OwnedMetrics errors of
// every misconfigured recommender are returned, joined, and that the metrics
// they owned are dropped while those of the valid recommenders are kept.
func TestSyncRecommenderMetricsConfigError(t *testing.T) {
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()
	client := &fakeXASServerClient{}
	badErr := errors.New("cpu-metric \"gauge\" is not a decaying distribution")
	worseErr := errors.New("memory-metric \"mem\" is not defined in the policy metrics")
	e := newTestEngine(t, client,
		map[string]string{"owning-class": "Owning"},
		map[string]Recommender{"Owning": &owningRecommender{
			metrics: map[string][]*pb.MetricDefinition{"ok": {cpuMetric("ok")}},
			errs:    map[string]error{"bad": badErr, "worse": worseErr},
		}},
	)
	pol := pb.Policy_builder{
		Id: id,
		Scaling: []*pb.RecommenderDefinition{
			pb.RecommenderDefinition_builder{Name: proto.String("ok"), Recommender: proto.String("owning-class")}.Build(),
			pb.RecommenderDefinition_builder{Name: proto.String("bad"), Recommender: proto.String("owning-class")}.Build(),
			pb.RecommenderDefinition_builder{Name: proto.String("worse"), Recommender: proto.String("owning-class")}.Build(),
		},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"bad":   pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("bad")}}.Build(),
			"worse": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{memoryMetric("worse")}}.Build(),
		},
	}.Build()

	got, err := e.syncRecommenderMetrics(pol)

	for _, want := range []error{badErr, worseErr} {
		if !errors.Is(err, want) {
			t.Errorf("syncRecommenderMetrics() error = %v, want it to wrap %q", err, want)
		}
	}
	want := map[string]*pb.MetricDefinitionList{"ok": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("ok")}}.Build()}
	if diff := cmp.Diff(want, got.GetRecommenderMetrics(), protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("RecommenderMetrics mismatch (-want +got):\n%s", diff)
	}
}

// TestSyncRecommenderMetricsNoConfigError checks that a nil error is returned
// when every recommender is well configured, so that the caller can rely on
// it to skip the policy.
func TestSyncRecommenderMetricsNoConfigError(t *testing.T) {
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()
	e := newTestEngine(t, &fakeXASServerClient{},
		map[string]string{"owning-class": "Owning"},
		map[string]Recommender{"Owning": &owningRecommender{metrics: map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa")}}}},
	)
	pol := pb.Policy_builder{Id: id, Scaling: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("vpa"), Recommender: proto.String("owning-class")}.Build()}}.Build()

	if _, err := e.syncRecommenderMetrics(pol); err != nil {
		t.Errorf("syncRecommenderMetrics() error = %v, want nil", err)
	}
}
