package controller

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
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1"
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
	deployment := &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}

	id := &pb.PolicyId{ClusterName: "default", Namespace: "prod", Name: "web"}
	// fromCRD is what pushPolicy builds from sp alone.
	fromCRD := func() *pb.Policy {
		return &pb.Policy{
			Id:          id,
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "web", Namespace: "prod"},
			MinReplicas: 2,
			MaxReplicas: 10,
			Scaling:     []*pb.RecommenderDefinition{{Name: "linear", Recommender: "linear-class", Type: "Linear"}},
			Selector:    "app=web",
		}
	}
	engineMetrics := map[string]*pb.MetricDefinitionList{
		"vpa": {Definitions: []*pb.MetricDefinition{{Name: "cpu", RecommenderName: "vpa"}}},
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
				{Id: &pb.PolicyId{ClusterName: "default", Namespace: "prod", Name: "other"}, Etag: "other-etag"},
			},
			wantRequests: []*pb.UpdatePolicyRequest{{Policy: fromCRD(), AllowMissing: true}},
		},
		{
			name: "Sends the stored ETag and keeps the engine's metrics when the policy exists",
			serverHas: []*pb.Policy{
				{Id: &pb.PolicyId{ClusterName: "default", Namespace: "prod", Name: "other"}, Etag: "other-etag"},
				{
					Id: id,
					// Outdated CRD fields: overwritten by the ScalingPolicy.
					MaxReplicas:        3,
					Selector:           "app=old",
					RecommenderMetrics: engineMetrics,
					Etag:               "v1",
				},
			},
			wantRequests: func() []*pb.UpdatePolicyRequest {
				want := fromCRD()
				want.RecommenderMetrics = engineMetrics
				want.Etag = "v1"
				return []*pb.UpdatePolicyRequest{{Policy: want, AllowMissing: true}}
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
			wantRequests: []*pb.UpdatePolicyRequest{{Policy: fromCRD(), AllowMissing: true}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakePolicyClient{policies: tc.serverHas, getErr: tc.getErr, updateErr: tc.updateErr}
			c := newPushPolicyController(t, client)

			err := c.pushPolicy(sp, deployment)

			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("pushPolicy() error = %v, wantErr %v", err, tc.wantErr)
			}
			if diff := cmp.Diff(tc.wantRequests, client.requests, protocmp.Transform()); diff != "" {
				t.Errorf("UpdatePolicy requests mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// storeClient serves the policy calls of pushPolicy from a real MemoryStore.
type storeClient struct {
	pb.XASServerClient
	store *store.MemoryStore
}

func (c *storeClient) GetPolicy(_ context.Context, req *pb.GetPolicyRequest, _ ...grpc.CallOption) (*pb.Policy, error) {
	p := c.store.GetPolicy(req.GetId())
	if p == nil {
		return nil, status.Error(codes.NotFound, "policy not found")
	}
	return p, nil
}

func (c *storeClient) UpdatePolicy(_ context.Context, req *pb.UpdatePolicyRequest, _ ...grpc.CallOption) (*pb.Policy, error) {
	return c.store.UpdatePolicy(req.GetPolicy().GetId().GetClusterName(), req.GetAllowMissing(), req.GetPolicy())
}

// TestPushPolicyRemovedRecommenderOwnedMetrics checks that removing a
// recommender from the ScalingPolicy drops the metrics it owns on the Server,
// although pushPolicy copies all the owned metrics from the Server's copy.
func TestPushPolicyRemovedRecommenderOwnedMetrics(t *testing.T) {
	s := store.NewMemoryStore()
	id := &pb.PolicyId{ClusterName: "default", Namespace: "prod", Name: "web"}
	owned := func(owner string) *pb.MetricDefinitionList {
		return &pb.MetricDefinitionList{Definitions: []*pb.MetricDefinition{{Name: "cpu-target", RecommenderName: owner}}}
	}
	// The Server has the policy with a vpa recommender and the metrics it owns.
	if _, err := s.UpdatePolicy("default", true, &pb.Policy{
		Id:      id,
		Scaling: []*pb.RecommenderDefinition{{Name: "linear"}, {Name: "vpa-sizing"}},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"linear":     owned("linear"),
			"vpa-sizing": owned("vpa-sizing"),
		},
	}); err != nil {
		t.Fatalf("UpdatePolicy() error = %v", err)
	}

	// The user removes vpa-sizing from the ScalingPolicy.
	sp := &xasv1.ScalingPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web"},
		Spec: xasv1.ScalingPolicySpec{
			ScaleTargetRef: xasv1.CrossVersionObjectReference{Kind: "Deployment", Name: "web", APIVersion: "apps/v1"},
			MaxReplicas:    10,
			Scaling:        []xasv1.RecommenderDefinition{{Name: "linear", Recommender: "linear-class"}},
		},
	}
	deployment := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
	}}
	c := newPushPolicyController(t, &storeClient{store: s})
	if err := c.pushPolicy(sp, deployment); err != nil {
		t.Fatalf("pushPolicy() error = %v", err)
	}

	got := s.GetPolicy(id)
	if got == nil {
		t.Fatal("GetPolicy() not found")
	}
	want := map[string]*pb.MetricDefinitionList{"linear": owned("linear")}
	if diff := cmp.Diff(want, got.GetRecommenderMetrics(), protocmp.Transform()); diff != "" {
		t.Errorf("RecommenderMetrics mismatch (-want +got):\n%s", diff)
	}
}
