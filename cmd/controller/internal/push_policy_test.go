package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

// fakePolicyClient serves GetPolicy from a fixed list and records the
// UpdatePolicy requests. The embedded interface makes any other method panic.
type fakePolicyClient struct {
	pb.XASServerClient
	policies  []*pb.Policy
	getErr    error
	updateErr error
	requests  []*pb.UpdatePolicyRequest
}

func (f *fakePolicyClient) GetPolicy(_ context.Context, req *pb.GetPolicyRequest, _ ...grpc.CallOption) (*pb.Policy, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	for _, p := range f.policies {
		if proto.Equal(p.GetId(), req.GetId()) {
			return proto.Clone(p).(*pb.Policy), nil
		}
	}
	return nil, status.Error(codes.NotFound, "policy not found")
}

func (f *fakePolicyClient) UpdatePolicy(_ context.Context, req *pb.UpdatePolicyRequest, _ ...grpc.CallOption) (*pb.Policy, error) {
	f.requests = append(f.requests, proto.Clone(req).(*pb.UpdatePolicyRequest))
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	return req.GetPolicy(), nil
}

// newPushPolicyController builds a Controller with just what pushPolicy needs.
func newPushPolicyController(t *testing.T, client pb.XASServerClient) *Controller {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(&xasv1.RecommenderClass{
		ObjectMeta: metav1.ObjectMeta{Name: "linear-class"},
		Spec:       xasv1.RecommenderClassSpec{Type: "Linear"},
	}); err != nil {
		t.Fatalf("Failed to seed the RecommenderClass: %v", err)
	}
	return &Controller{
		grpcClient:        client,
		recommenderLister: listers.NewRecommenderClassLister(indexer),
		clusterName:       "default",
	}
}

func TestPushPolicy(t *testing.T) {
	minReplicas := int32(2)
	sp := &xasv1.ScalingPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web"},
		Spec: xasv1.ScalingPolicySpec{
			ScaleTargetRef: xasv1.CrossVersionObjectReference{Kind: "Deployment", Name: "web", APIVersion: "apps/v1"},
			MinReplicas:    &minReplicas,
			MaxReplicas:    10,
			Scaling:        []xasv1.RecommenderDefinition{{Name: "linear", Recommender: "linear-class"}},
		},
	}
	selector := "app=web"

	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()
	// fromCRD is what pushPolicy builds from sp alone.
	fromCRD := func() *pb.Policy {
		return pb.Policy_builder{
			Id:          id,
			Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("web"), Namespace: proto.String("prod")}.Build(),
			MinReplicas: proto.Int32(2),
			MaxReplicas: proto.Int32(10),
			Scaling:     []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("linear"), Recommender: proto.String("linear-class"), Type: proto.String("Linear")}.Build()},
			Selector:    proto.String("app=web"),
		}.Build()
	}
	engineMetrics := map[string]*pb.MetricDefinitionList{
		"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("vpa")}.Build()}}.Build(),
	}

	tests := []struct {
		name         string
		serverHas    []*pb.Policy
		getErr       error
		updateErr    error
		wantErr      bool
		wantRequests []*pb.UpdatePolicyRequest
	}{
		{
			name: "Creates the policy with an empty ETag when the Server does not have it",
			serverHas: []*pb.Policy{
				pb.Policy_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("other")}.Build(), Etag: proto.String("other-etag")}.Build(),
			},
			wantRequests: []*pb.UpdatePolicyRequest{pb.UpdatePolicyRequest_builder{Policy: fromCRD(), AllowMissing: proto.Bool(true)}.Build()},
		},
		{
			name: "Sends the stored ETag and keeps the engine's metrics when the policy exists",
			serverHas: []*pb.Policy{
				pb.Policy_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("other")}.Build(), Etag: proto.String("other-etag")}.Build(),
				pb.Policy_builder{
					Id: id,
					// Outdated CRD fields: overwritten by the ScalingPolicy.
					MaxReplicas:        proto.Int32(3),
					Selector:           proto.String("app=old"),
					RecommenderMetrics: engineMetrics,
					Etag:               proto.String("v1"),
				}.Build(),
			},
			wantRequests: func() []*pb.UpdatePolicyRequest {
				want := fromCRD()
				want.SetRecommenderMetrics(engineMetrics)
				want.SetEtag("v1")
				return []*pb.UpdatePolicyRequest{pb.UpdatePolicyRequest_builder{Policy: want, AllowMissing: proto.Bool(true)}.Build()}
			}(),
		},
		{
			name:    "Does not write when the policy cannot be read",
			getErr:  errors.New("server unavailable"),
			wantErr: true,
		},
		{
			name:         "Returns the error of a rejected write so the policy is requeued",
			updateErr:    errors.New("etag mismatch"),
			wantErr:      true,
			wantRequests: []*pb.UpdatePolicyRequest{pb.UpdatePolicyRequest_builder{Policy: fromCRD(), AllowMissing: proto.Bool(true)}.Build()},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakePolicyClient{policies: tc.serverHas, getErr: tc.getErr, updateErr: tc.updateErr}
			c := newPushPolicyController(t, client)

			err := c.pushPolicy(sp, selector)

			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("pushPolicy() error = %v, wantErr %v", err, tc.wantErr)
			}
			if diff := cmp.Diff(tc.wantRequests, client.requests, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
				t.Errorf("UpdatePolicy requests mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// storeClient serves the policy calls of pushPolicy from a single stored
// policy, mimicking the Server's behavior of dropping the metrics owned by
// recommenders that are no longer in the policy on UpdatePolicy.
type storeClient struct {
	pb.XASServerClient
	policy *pb.Policy
}

func (c *storeClient) GetPolicy(_ context.Context, req *pb.GetPolicyRequest, _ ...grpc.CallOption) (*pb.Policy, error) {
	if c.policy == nil || !proto.Equal(c.policy.GetId(), req.GetId()) {
		return nil, status.Error(codes.NotFound, "policy not found")
	}
	return proto.Clone(c.policy).(*pb.Policy), nil
}

func (c *storeClient) UpdatePolicy(_ context.Context, req *pb.UpdatePolicyRequest, _ ...grpc.CallOption) (*pb.Policy, error) {
	p := proto.Clone(req.GetPolicy()).(*pb.Policy)
	names := make(map[string]bool, len(p.GetScaling())+len(p.GetActivation()))
	for _, r := range p.GetScaling() {
		names[r.GetName()] = true
	}
	for _, r := range p.GetActivation() {
		names[r.GetName()] = true
	}
	for rName := range p.GetRecommenderMetrics() {
		if !names[rName] {
			delete(p.GetRecommenderMetrics(), rName)
		}
	}
	c.policy = p
	return proto.Clone(p).(*pb.Policy), nil
}

// TestPushPolicyRemovedRecommenderOwnedMetrics checks that removing a
// recommender from the ScalingPolicy drops the metrics it owns on the Server,
// although pushPolicy copies all the owned metrics from the Server's copy.
func TestPushPolicyRemovedRecommenderOwnedMetrics(t *testing.T) {
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()
	owned := func(owner string) *pb.MetricDefinitionList {
		return pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("cpu-target"), RecommenderName: proto.String(owner)}.Build()}}.Build()
	}
	// The Server has the policy with a vpa recommender and the metrics it owns.
	s := &storeClient{policy: pb.Policy_builder{
		Id:      id,
		Scaling: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("linear")}.Build(), pb.RecommenderDefinition_builder{Name: proto.String("vpa-sizing")}.Build()},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"linear":     owned("linear"),
			"vpa-sizing": owned("vpa-sizing"),
		},
	}.Build()}

	// The user removes vpa-sizing from the ScalingPolicy.
	sp := &xasv1.ScalingPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web"},
		Spec: xasv1.ScalingPolicySpec{
			ScaleTargetRef: xasv1.CrossVersionObjectReference{Kind: "Deployment", Name: "web", APIVersion: "apps/v1"},
			MaxReplicas:    10,
			Scaling:        []xasv1.RecommenderDefinition{{Name: "linear", Recommender: "linear-class"}},
		},
	}
	c := newPushPolicyController(t, s)
	if err := c.pushPolicy(sp, "app=web"); err != nil {
		t.Fatalf("pushPolicy() error = %v", err)
	}

	got := s.policy
	if got == nil {
		t.Fatal("policy not found")
	}
	want := map[string]*pb.MetricDefinitionList{"linear": owned("linear")}
	if diff := cmp.Diff(want, got.GetRecommenderMetrics(), protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("RecommenderMetrics mismatch (-want +got):\n%s", diff)
	}
}
