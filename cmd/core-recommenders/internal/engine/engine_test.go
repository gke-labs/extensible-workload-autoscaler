package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

// recommendClient serves what Tick and processPolicy read, echoes policy
// updates back like the Server does, and records the recommendations
// handleControlMetricsUpdate pushes. Other methods panic.
type recommendClient struct {
	pb.XASServerClient
	policies    []*pb.Policy
	policiesErr error
	workload    *pb.Workload
	workloadErr error
	pushed      []*pb.UpdateRecommenderStateRequest
}

func (c *recommendClient) ListPolicies(context.Context, *pb.ListPoliciesRequest, ...grpc.CallOption) (*pb.ListPoliciesResponse, error) {
	if c.policiesErr != nil {
		return nil, c.policiesErr
	}
	return pb.ListPoliciesResponse_builder{Policies: c.policies}.Build(), nil
}

func (c *recommendClient) UpdatePolicy(_ context.Context, req *pb.UpdatePolicyRequest, _ ...grpc.CallOption) (*pb.Policy, error) {
	return req.GetPolicy(), nil
}

func (c *recommendClient) GetWorkload(context.Context, *pb.GetWorkloadRequest, ...grpc.CallOption) (*pb.Workload, error) {
	return c.workload, c.workloadErr
}

func (c *recommendClient) UpdateRecommenderState(_ context.Context, req *pb.UpdateRecommenderStateRequest, _ ...grpc.CallOption) (*pb.RecommenderState, error) {
	c.pushed = append(c.pushed, req)
	return &pb.RecommenderState{}, nil
}

// recommenderInput is what a recommender instance was given to recommend.
type recommenderInput struct {
	metrics, ownedMetrics *pb.ControlMetrics
	readyReplicas         int32
	workload              *pb.Workload
}

// recordingRecommender records what each recommender instance is given, keyed
// by instance name, and recommends activation.
type recordingRecommender struct {
	got map[string]recommenderInput
}

func (r *recordingRecommender) Recommend(def *pb.RecommenderDefinition, metrics, ownedMetrics *pb.ControlMetrics, readyReplicas int32, workload *pb.Workload) *pb.Recommendation {
	if r.got == nil {
		r.got = make(map[string]recommenderInput)
	}
	r.got[def.GetName()] = recommenderInput{metrics: metrics, ownedMetrics: ownedMetrics, readyReplicas: readyReplicas, workload: workload}
	return pb.Recommendation_builder{IsActive: proto.Bool(true)}.Build()
}

func policyID(cluster, ns, name string) *pb.PolicyId {
	return pb.PolicyId_builder{ClusterName: proto.String(cluster), Namespace: proto.String(ns), Name: proto.String(name)}.Build()
}

// testPolicy returns a policy with an activation recommender "act" and a
// scaling recommender "vpa", both backed by the "rec-class" RecommenderClass.
func testPolicy(id *pb.PolicyId) *pb.Policy {
	return pb.Policy_builder{
		Id:         id,
		Activation: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("act"), Recommender: proto.String("rec-class")}.Build()},
		Scaling:    []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("vpa"), Recommender: proto.String("rec-class")}.Build()},
	}.Build()
}

func testWorkload(pod string) *pb.Workload {
	return pb.Workload_builder{Pods: []*pb.PodState{
		pb.PodState_builder{Name: proto.String(pod), Containers: []*pb.ContainerState{pb.ContainerState_builder{Name: proto.String("app"), Requests: map[string]string{"cpu": "100m"}}.Build()}}.Build(),
	}}.Build()
}

// stateCmpOpts compares States, whose keys and values have unexported fields
// and hold protos.
var stateCmpOpts = []cmp.Option{cmp.AllowUnexported(PolicyID{}, PolicyState{}), protocmp.Transform(), protocmp.IgnoreDefaultScalars()}

func TestProcessPolicy(t *testing.T) {
	policy := testPolicy(policyID("default", "prod", "web"))
	workload := testWorkload("web-1")

	t.Run("returns the policy and its workload", func(t *testing.T) {
		client := &recommendClient{workload: workload}
		e := newTestEngine(t, client, map[string]string{"rec-class": "Plain"}, map[string]Recommender{"Plain": plainRecommender{}})

		got := e.processPolicy(policy)

		want := &PolicyState{policy: policy, workload: workload}
		if diff := cmp.Diff(want, got, stateCmpOpts...); diff != "" {
			t.Errorf("processPolicy() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("returns the policy with the owned metrics registered", func(t *testing.T) {
		client := &recommendClient{workload: workload}
		rec := &owningRecommender{metrics: map[string][]*pb.MetricDefinition{"vpa": {cpuMetric("vpa")}}}
		e := newTestEngine(t, client, map[string]string{"rec-class": "Owning"}, map[string]Recommender{"Owning": rec})

		got := e.processPolicy(policy)

		want := map[string]*pb.MetricDefinitionList{"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{cpuMetric("vpa")}}.Build()}
		if diff := cmp.Diff(want, got.policy.GetRecommenderMetrics(), protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
			t.Errorf("processPolicy() RecommenderMetrics mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("skips the policy when the workload can't be read", func(t *testing.T) {
		client := &recommendClient{workloadErr: context.DeadlineExceeded}
		e := newTestEngine(t, client, map[string]string{"rec-class": "Plain"}, map[string]Recommender{"Plain": plainRecommender{}})

		if got := e.processPolicy(policy); got != nil {
			t.Errorf("processPolicy() = %+v, want nil", got)
		}
	})

	t.Run("skips the policy when a recommender is misconfigured", func(t *testing.T) {
		client := &recommendClient{workload: workload}
		rec := &owningRecommender{errs: map[string]error{"vpa": errors.New("cpu-metric \"gauge\" is not a decaying distribution")}}
		e := newTestEngine(t, client, map[string]string{"rec-class": "Owning"}, map[string]Recommender{"Owning": rec})

		if got := e.processPolicy(policy); got != nil {
			t.Errorf("processPolicy() = %+v, want nil", got)
		}
	})
}

func TestTick(t *testing.T) {
	web := testPolicy(policyID("default", "prod", "web"))
	api := testPolicy(policyID("default", "prod", "api"))
	gone := testPolicy(policyID("default", "prod", "gone"))
	workload := testWorkload("pod-1")
	webID, _ := PolicyIDFromProto(web.GetId())
	apiID, _ := PolicyIDFromProto(api.GetId())
	goneID, _ := PolicyIDFromProto(gone.GetId())

	t.Run("replaces the state with the policies the Server lists", func(t *testing.T) {
		client := &recommendClient{policies: []*pb.Policy{web, api}, workload: workload}
		e := newTestEngine(t, client, map[string]string{"rec-class": "Plain"}, map[string]Recommender{"Plain": plainRecommender{}})
		e.state[goneID] = &PolicyState{policy: gone, workload: workload}

		e.Tick()

		want := State{
			webID: {policy: web, workload: workload},
			apiID: {policy: api, workload: workload},
		}
		if diff := cmp.Diff(want, e.state, stateCmpOpts...); diff != "" {
			t.Errorf("state after Tick() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("drops the policies that can't be processed", func(t *testing.T) {
		client := &recommendClient{policies: []*pb.Policy{web}, workloadErr: context.DeadlineExceeded}
		e := newTestEngine(t, client, map[string]string{"rec-class": "Plain"}, map[string]Recommender{"Plain": plainRecommender{}})
		e.state[webID] = &PolicyState{policy: web, workload: workload}

		e.Tick()

		if diff := cmp.Diff(State{}, e.state, stateCmpOpts...); diff != "" {
			t.Errorf("state after Tick() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("skips the policies of another cluster", func(t *testing.T) {
		other := testPolicy(policyID("other", "prod", "web"))
		client := &recommendClient{policies: []*pb.Policy{other, api}, workload: workload}
		e := newTestEngine(t, client, map[string]string{"rec-class": "Plain"}, map[string]Recommender{"Plain": plainRecommender{}})

		e.Tick()

		want := State{apiID: {policy: api, workload: workload}}
		if diff := cmp.Diff(want, e.state, stateCmpOpts...); diff != "" {
			t.Errorf("state after Tick() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("keeps the state when the policies can't be listed", func(t *testing.T) {
		client := &recommendClient{policiesErr: context.DeadlineExceeded}
		e := newTestEngine(t, client, map[string]string{"rec-class": "Plain"}, map[string]Recommender{"Plain": plainRecommender{}})
		e.state[webID] = &PolicyState{policy: web, workload: workload}

		e.Tick()

		want := State{webID: {policy: web, workload: workload}}
		if diff := cmp.Diff(want, e.state, stateCmpOpts...); diff != "" {
			t.Errorf("state after Tick() mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestHandleControlMetricsUpdate(t *testing.T) {
	id := policyID("default", "prod", "web")
	policy := testPolicy(id)
	workload := testWorkload("web-1")
	webID, _ := PolicyIDFromProto(id)
	policyWide := pb.ControlMetrics_builder{Values: map[string]float64{"cpu": 2}}.Build()
	vpaOwned := pb.ControlMetrics_builder{Values: map[string]float64{"cpu": 30}}.Build()
	update := pb.ControlMetricsEvent_builder{
		Id:                        id,
		ControlMetrics:            policyWide,
		RecommenderControlMetrics: map[string]*pb.ControlMetrics{"vpa": vpaOwned},
		ReadyReplicas:             proto.Int32(3),
	}.Build()

	// newEngine builds an engine that knows the policy, backed by rec.
	newEngine := func(t *testing.T, client *recommendClient, rec Recommender) *Engine {
		t.Helper()
		e := newTestEngine(t, client, map[string]string{"rec-class": "Rec"}, map[string]Recommender{"Rec": rec})
		e.state[webID] = &PolicyState{policy: policy, workload: workload}
		return e
	}

	// A single update feeds every recommender: all get the policy-wide
	// metrics, the ready replicas and the stored workload, and each gets the
	// metrics it owns, or nil if it owns none.
	t.Run("feeds every recommender", func(t *testing.T) {
		rec := &recordingRecommender{}
		e := newEngine(t, &recommendClient{}, rec)

		e.handleControlMetricsUpdate(context.Background(), update)

		want := map[string]recommenderInput{
			"act": {metrics: policyWide, ownedMetrics: nil, readyReplicas: 3, workload: workload},
			"vpa": {metrics: policyWide, ownedMetrics: vpaOwned, readyReplicas: 3, workload: workload},
		}
		if diff := cmp.Diff(want, rec.got, cmp.AllowUnexported(recommenderInput{}), protocmp.Transform()); diff != "" {
			t.Errorf("inputs given to Recommend mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("pushes the recommendations", func(t *testing.T) {
		client := &recommendClient{}
		e := newEngine(t, client, &recordingRecommender{})

		e.handleControlMetricsUpdate(context.Background(), update)

		active := pb.Recommendation_builder{IsActive: proto.Bool(true)}.Build()
		want := []*pb.UpdateRecommenderStateRequest{
			pb.UpdateRecommenderStateRequest_builder{Id: id, RecommenderName: proto.String("act"), Recommendation: active}.Build(),
			pb.UpdateRecommenderStateRequest_builder{Id: id, RecommenderName: proto.String("vpa"), Recommendation: active}.Build(),
		}
		if diff := cmp.Diff(want, client.pushed, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
			t.Errorf("pushed recommendations mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("pushes nothing when the recommenders have no recommendation", func(t *testing.T) {
		client := &recommendClient{}
		e := newEngine(t, client, plainRecommender{})

		e.handleControlMetricsUpdate(context.Background(), update)

		if len(client.pushed) != 0 {
			t.Errorf("pushed %d recommendations, want none", len(client.pushed))
		}
	})

	t.Run("drops the metrics of a policy it does not know", func(t *testing.T) {
		client := &recommendClient{}
		rec := &recordingRecommender{}
		e := newTestEngine(t, client, map[string]string{"rec-class": "Rec"}, map[string]Recommender{"Rec": rec})

		e.handleControlMetricsUpdate(context.Background(), update)

		if len(rec.got) != 0 || len(client.pushed) != 0 {
			t.Errorf("Recommend called %d times and %d recommendations pushed, want none", len(rec.got), len(client.pushed))
		}
		if len(e.state) != 0 {
			t.Errorf("state has %d entries after an update for an unknown policy, want none", len(e.state))
		}
	})

	t.Run("drops the metrics of another cluster", func(t *testing.T) {
		client := &recommendClient{}
		rec := &recordingRecommender{}
		e := newEngine(t, client, rec)
		foreign := proto.Clone(update).(*pb.ControlMetricsEvent)
		foreign.SetId(policyID("other", "prod", "web"))

		e.handleControlMetricsUpdate(context.Background(), foreign)

		if len(rec.got) != 0 || len(client.pushed) != 0 {
			t.Errorf("Recommend called %d times and %d recommendations pushed, want none", len(rec.got), len(client.pushed))
		}
	})
}
