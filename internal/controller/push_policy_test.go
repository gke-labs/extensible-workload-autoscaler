package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

// fakePolicyClient serves ListPolicies from a fixed list and records the
// UpdatePolicy requests. The embedded interface makes any other method panic.
type fakePolicyClient struct {
	pb.XASServerClient
	policies  []*pb.Policy
	listErr   error
	updateErr error
	requests  []*pb.UpdatePolicyRequest
}

func (f *fakePolicyClient) ListPolicies(_ context.Context, _ *pb.ListPoliciesRequest, _ ...grpc.CallOption) (*pb.ListPoliciesResponse, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	resp := &pb.ListPoliciesResponse{}
	for _, p := range f.policies {
		resp.Policies = append(resp.Policies, proto.Clone(p).(*pb.Policy))
	}
	return resp, nil
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
		listErr      error
		updateErr    error
		wantErr      bool
		wantRequests []*pb.UpdatePolicyRequest
	}{
		{
			name: "Creates the policy with an empty ETag when the Server does not have it",
			serverHas: []*pb.Policy{
				{Id: &pb.PolicyId{ClusterName: "default", Namespace: "prod", Name: "other"}, Etag: "other-etag"},
			},
			wantRequests: []*pb.UpdatePolicyRequest{{Policy: fromCRD()}},
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
				return []*pb.UpdatePolicyRequest{{Policy: want}}
			}(),
		},
		{
			name:    "Does not write when the policies cannot be read",
			listErr: errors.New("server unavailable"),
			wantErr: true,
		},
		{
			name:         "Returns the error of a rejected write so the policy is requeued",
			updateErr:    errors.New("etag mismatch"),
			wantErr:      true,
			wantRequests: []*pb.UpdatePolicyRequest{{Policy: fromCRD()}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakePolicyClient{policies: tc.serverHas, listErr: tc.listErr, updateErr: tc.updateErr}
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
