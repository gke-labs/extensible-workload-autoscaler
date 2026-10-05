package engine

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

// recommendClient serves what processPolicy reads, and records the
// recommendations it pushes. Other methods panic.
type recommendClient struct {
	pb.XASServerClient
	workload    *pb.Workload
	workloadErr error
	pushed      []*pb.UpdateRecommenderStateRequest
}

func (c *recommendClient) GetControlMetrics(context.Context, *pb.GetControlMetricsRequest, ...grpc.CallOption) (*pb.ControlMetrics, error) {
	return &pb.ControlMetrics{}, nil
}

func (c *recommendClient) GetWorkload(context.Context, *pb.GetWorkloadRequest, ...grpc.CallOption) (*pb.Workload, error) {
	return c.workload, c.workloadErr
}

func (c *recommendClient) UpdateRecommenderState(_ context.Context, req *pb.UpdateRecommenderStateRequest, _ ...grpc.CallOption) (*pb.RecommenderState, error) {
	c.pushed = append(c.pushed, req)
	return &pb.RecommenderState{}, nil
}

// workloadRecommender records the workload it is given.
type workloadRecommender struct {
	got []*pb.Workload
}

func (r *workloadRecommender) Recommend(_ *pb.RecommenderDefinition, _, _ *pb.ControlMetrics, workload *pb.Workload) *pb.Recommendation {
	r.got = append(r.got, workload)
	return &pb.Recommendation{IsActive: true}
}

func TestProcessPolicyPassesWorkload(t *testing.T) {
	workload := &pb.Workload{Pods: []*pb.PodState{
		{Name: "web-1", Containers: []*pb.ContainerState{{Name: "app", Requests: map[string]string{"cpu": "100m"}}}},
	}}
	policy := &pb.Policy{
		Id:         &pb.PolicyId{ClusterName: "default", Namespace: "prod", Name: "web"},
		Activation: []*pb.RecommenderDefinition{{Name: "act", Recommender: "rec-class"}},
		Scaling:    []*pb.RecommenderDefinition{{Name: "vpa", Recommender: "rec-class"}},
	}

	t.Run("every recommender gets the workload", func(t *testing.T) {
		client := &recommendClient{workload: workload}
		rec := &workloadRecommender{}
		e := newTestEngine(t, client, map[string]string{"rec-class": "Rec"}, map[string]Recommender{"Rec": rec})

		e.processPolicy(policy)

		if diff := cmp.Diff([]*pb.Workload{workload, workload}, rec.got, protocmp.Transform()); diff != "" {
			t.Errorf("workloads given to Recommend mismatch (-want +got):\n%s", diff)
		}
		if len(client.pushed) != 2 {
			t.Errorf("pushed %d recommendations, want 2", len(client.pushed))
		}
	})

	t.Run("the policy is skipped when the workload can't be read", func(t *testing.T) {
		client := &recommendClient{workloadErr: context.DeadlineExceeded}
		rec := &workloadRecommender{}
		e := newTestEngine(t, client, map[string]string{"rec-class": "Rec"}, map[string]Recommender{"Rec": rec})

		e.processPolicy(policy)

		if len(rec.got) != 0 || len(client.pushed) != 0 {
			t.Errorf("Recommend called %d times and %d recommendations pushed, want none", len(rec.got), len(client.pushed))
		}
	})
}
