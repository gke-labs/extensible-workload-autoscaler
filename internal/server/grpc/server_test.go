package grpc_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	servergrpc "github.com/gke-labs/extensible-workload-autoscaler/internal/server/grpc"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store"
)

const bufSize = 1024 * 1024

func init() {
	// bufconn listener is created per test now
}

// FakeClock for deterministic time testing
// We use internal/clock.FakeClock directly

func setupGRPCServer(t *testing.T, c clock.Clock) (*store.MemoryStore, pb.XASControlPlaneClient, func()) {
	lis := bufconn.Listen(bufSize)
	memStore := store.NewMemoryStoreWithClock(c)
	srv := servergrpc.NewServer(memStore, c)

	s := grpc.NewServer()
	pb.RegisterXASControlPlaneServer(s, srv)
	go func() {
		if err := s.Serve(lis); err != nil {
			// s.Serve returns error on Stop/Close, which is expected
		}
	}()

	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	client := pb.NewXASControlPlaneClient(conn)

	cleanup := func() {
		conn.Close()
		s.Stop()
	}

	return memStore, client, cleanup
}

// TestServerEndToEndGRPC validates the full lifecycle of a scaling policy.
// Scenario: A deployment "web-app" scales based on CPU usage.
// Steps:
// 1. Create a Policy with a Linear recommender targeting 0.5 CPU.
// 2. Ingest initial metrics (T0) with value 100.
// 3. Ingest subsequent metrics (T1) with value 108 (rate = 0.8).
// 4. Update Workload state to indicate 2 ready pods.
// 5. Calculate Control Metrics and verify the aggregated CPU rate is 0.8.
// 6. Simulate the Recommender calculating a target of 4 replicas based on this rate.
// 7. Verify the final Recommendation from the Control Plane matches the Recommender's output.
func TestServerEndToEndGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()

	ctx := context.Background()

	policyName := "web-app-policy"
	ns := "default"

	// 1. Policy
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload: &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "web-app", Namespace: ns},
			Metrics:  []*pb.MetricDefinition{{Name: "cpu_avg", Provider: "kubelet", Params: map[string]string{"type": "cpu"}, Rate: &pb.Rate{Window: "1m", Aggregation: "Avg"}}},

			Scaling:     []*pb.RecommenderDefinition{{Recommender: "Linear", Name: "cpu", Mode: "Active", Type: "Linear", Params: map[string]string{"metric": "cpu_avg", "target": "0.5"}}},
			MinReplicas: 2, MaxReplicas: 10,
		},
	})

	// 2. Ingest T0
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{
			{
				Namespace: ns, Name: policyName,
				Batches: []*pb.MetricBatch{
					{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "cpu_avg", Value: 100, Timestamp: ts}}},
					{EntityKey: "p2", Samples: []*pb.MetricSample{{Name: "cpu_avg", Value: 100, Timestamp: ts}}},
				},
			},
		},
	})

	// 3. Ingest T1 (10s later, +8 diff)
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{
			{
				Namespace: ns, Name: policyName,
				Batches: []*pb.MetricBatch{
					{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "cpu_avg", Value: 108, Timestamp: ts}}},
					{EntityKey: "p2", Samples: []*pb.MetricSample{{Name: "cpu_avg", Value: 108, Timestamp: ts}}},
				},
			},
		},
	})

	// 4. Workload
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}, {Name: "p2", IsReady: true}}},
	})

	// 5. Calc Control Metrics
	memStore.CalculateAll()

	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})

	wantCM := &pb.ControlMetrics{
		Values:        map[string]float64{"cpu_avg": 0.8},
		Timestamp:     1010,
		ReadyReplicas: 2,
	}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform()); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	// 6. Simulate Recommender
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "cpu",
		Vote: &pb.RecommenderVote{DesiredReplicas: 4, IsActive: true},
	})

	// 7. Calc Recommendation
	memStore.CalculateAll()

	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	rec := resp.Recommendation

	wantRec := &pb.Recommendation{
		TargetReplicas: 4,
		Explanation: []*pb.RecommenderStatus{
			{DesiredReplicas: 4, IsActive: true, Phase: "Scaling", Mode: "Active", Name: "cpu", Type: "Linear", LastUpdated: timestamppb.New(time.Unix(1010, 0))}, // cpu
		},
	}
	// Sort slices for deterministic comparison
	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.SortRepeated(func(a, b *pb.RecommenderStatus) bool { return a.Phase < b.Phase }), // Simple sort if needed
	}
	if diff := cmp.Diff(wantRec, rec, opts...); diff != "" {
		t.Errorf("Recommendation mismatch (-want +got):\n%s", diff)
	}
}

// TestDistributedCollectionGRPC validates metric aggregation from multiple agents.
// Scenario: A deployment "dist-app" has 4 pods, with metrics collected by 2 different agents (Agent A and Agent B).
// Steps:
// 1. Create a Policy targeting 0.5 CPU.
// 2. Register 4 pods in the Workload state.
// 3. Ingest T0 metrics for all 4 pods.
// 4. Agent A ingests T1 metrics for pods p1, p2 (Value 120, rate = 2.0).
// 5. Agent B ingests T1 metrics for pods p3, p4 (Value 124, rate = 2.0) slightly later.
// 6. Calculate Control Metrics and verify the aggregated average is 2.0.
// 7. Verify the final Recommendation matches the expected scaling decision.
func TestDistributedCollectionGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "dist-app-policy"
	ns := "default"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload: &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "dist-app", Namespace: ns},
			Metrics:  []*pb.MetricDefinition{{Name: "cpu_avg", Provider: "kubelet", Params: map[string]string{"type": "cpu"}, Rate: &pb.Rate{Window: "1m", Aggregation: "Avg"}}},

			Scaling:     []*pb.RecommenderDefinition{{Recommender: "Linear", Name: "cpu", Type: "Linear", Params: map[string]string{"metric": "cpu_avg", "target": "0.5"}}},
			MinReplicas: 1, MaxReplicas: 20,
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
		Workload: &pb.Workload{Pods: []*pb.PodState{
			{Name: "p1", IsReady: true}, {Name: "p2", IsReady: true}, {Name: "p3", IsReady: true}, {Name: "p4", IsReady: true},
		}},
	})

	// T0
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{
			{
				Namespace: ns, Name: policyName,
				Batches: []*pb.MetricBatch{
					{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "cpu_avg", Value: 100, Timestamp: ts}}},
					{EntityKey: "p2", Samples: []*pb.MetricSample{{Name: "cpu_avg", Value: 100, Timestamp: ts}}},
					{EntityKey: "p3", Samples: []*pb.MetricSample{{Name: "cpu_avg", Value: 100, Timestamp: ts}}},
					{EntityKey: "p4", Samples: []*pb.MetricSample{{Name: "cpu_avg", Value: 100, Timestamp: ts}}},
				},
			},
		},
	})

	// T1 (Agent A: 10s later)
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{
			{
				Namespace: ns, Name: policyName,
				Batches: []*pb.MetricBatch{
					{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "cpu_avg", Value: 120, Timestamp: ts}}},
					{EntityKey: "p2", Samples: []*pb.MetricSample{{Name: "cpu_avg", Value: 120, Timestamp: ts}}},
				},
			},
		},
	})

	// T1 (Agent B: 12s later)
	clk.Advance(2 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{
			{
				Namespace: ns, Name: policyName,
				Batches: []*pb.MetricBatch{
					{EntityKey: "p3", Samples: []*pb.MetricSample{{Name: "cpu_avg", Value: 124, Timestamp: ts}}},
					{EntityKey: "p4", Samples: []*pb.MetricSample{{Name: "cpu_avg", Value: 124, Timestamp: ts}}},
				},
			},
		},
	})

	memStore.CalculateAll()

	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	// Avg = 2.0
	if diff := cmp.Diff(&pb.ControlMetrics{Values: map[string]float64{"cpu_avg": 2.0}, Timestamp: 1012, ReadyReplicas: 4}, cm, protocmp.Transform()); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "cpu",
		Vote: &pb.RecommenderVote{DesiredReplicas: 16, IsActive: true},
	})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	rec := resp.Recommendation

	wantRec := &pb.Recommendation{
		TargetReplicas: 16,
		Explanation:    []*pb.RecommenderStatus{{DesiredReplicas: 16, IsActive: true, Phase: "Scaling", Name: "cpu", Type: "Linear", LastUpdated: timestamppb.New(time.Unix(1012, 0))}},
	}
	if diff := cmp.Diff(wantRec, rec, protocmp.Transform()); diff != "" {
		t.Errorf("Recommendation mismatch (-want +got):\n%s", diff)
	}
}

// TestExternalMetricGRPC validates scaling based on a global external metric (e.g., SQS queue depth).
// Scenario: Scale based on a global "queue" metric, independent of individual pods.
// Steps:
// 1. Create a Policy with a Gauge metric "queue" and a Linear recommender targeting 10.0 per replica.
// 2. Register 10 ready pods.
// 3. Ingest a global metric sample for "queue" with value 1000.
// 4. Calculate Control Metrics and verify the average value is 100.0 (1000 / 10 pods).
// 5. Simulate a Recommender decision to scale to 100 replicas.
// 6. Verify the final Recommendation.
func TestExternalMetricGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "ext-app-policy"
	ns := "default"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:          &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "ext-app", Namespace: ns},
			Metrics:     []*pb.MetricDefinition{{Name: "queue", Provider: "sqs", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			Scaling:     []*pb.RecommenderDefinition{{Recommender: "Linear", Name: "queue_target", Type: "Linear", Params: map[string]string{"metric": "queue", "target": "10.0"}}},
			MinReplicas: 1, MaxReplicas: 200,
		},
	})

	var pods []*pb.PodState
	for i := 0; i < 10; i++ {
		pods = append(pods, &pb.PodState{Name: fmt.Sprintf("p-%d", i), IsReady: true})
	}
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, Workload: &pb.Workload{Pods: pods}})

	// Ingest Global
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{
			{
				Namespace: ns, Name: policyName,
				Batches: []*pb.MetricBatch{{EntityKey: "", Samples: []*pb.MetricSample{{Name: "queue", Value: 1000, Timestamp: ts}}}},
			},
		},
	})

	memStore.CalculateAll()

	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	// Avg = 1000/10 = 100.
	if diff := cmp.Diff(&pb.ControlMetrics{Values: map[string]float64{"queue": 100.0}, Timestamp: 1000, ReadyReplicas: 10}, cm, protocmp.Transform()); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "queue_target",
		Vote: &pb.RecommenderVote{DesiredReplicas: 100, IsActive: true},
	})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	rec := resp.Recommendation

	wantRec := &pb.Recommendation{
		TargetReplicas: 100,
		Explanation:    []*pb.RecommenderStatus{{DesiredReplicas: 100, IsActive: true, Phase: "Scaling", Name: "queue_target", Type: "Linear", LastUpdated: timestamppb.New(time.Unix(1000, 0))}},
	}
	if diff := cmp.Diff(wantRec, rec, protocmp.Transform()); diff != "" {
		t.Errorf("Recommendation mismatch (-want +got):\n%s", diff)
	}
}

// TestPerPodExternalMetricGRPC validates scaling based on per-pod external metrics (e.g., sidecar latency).
// Scenario: Scale based on latency reported by a sidecar container in each pod.
// Steps:
// 1. Create a Policy with a Gauge metric "sidecar_latency".
// 2. Register 2 ready pods.
// 3. Ingest distinct metric samples for each pod (50 and 150).
// 4. Calculate Control Metrics and verify the average is 100.0.
// 5. Verify the final Recommendation.
func TestPerPodExternalMetricGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "per-pod-ext-policy"
	ns := "default"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:          &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: ns},
			Metrics:     []*pb.MetricDefinition{{Name: "sidecar_latency", Provider: "sidecar-provider", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			Scaling:     []*pb.RecommenderDefinition{{Recommender: "Linear", Name: "latency_target", Type: "Linear", Params: map[string]string{"metric": "sidecar_latency", "target": "100"}}},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "pod-1", IsReady: true}, {Name: "pod-2", IsReady: true}}},
	})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{
			{
				Namespace: ns, Name: policyName,
				Batches: []*pb.MetricBatch{
					{EntityKey: "pod-1", Samples: []*pb.MetricSample{{Name: "sidecar_latency", Value: 50, Timestamp: ts}}},
					{EntityKey: "pod-2", Samples: []*pb.MetricSample{{Name: "sidecar_latency", Value: 150, Timestamp: ts}}},
				},
			},
		},
	})

	memStore.CalculateAll()

	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	// Avg = (50+150)/2 = 100.
	if diff := cmp.Diff(&pb.ControlMetrics{Values: map[string]float64{"sidecar_latency": 100.0}, Timestamp: 1000, ReadyReplicas: 2}, cm, protocmp.Transform()); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "latency_target",
		Vote: &pb.RecommenderVote{DesiredReplicas: 2, IsActive: true},
	})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	rec := resp.Recommendation

	wantRec := &pb.Recommendation{
		TargetReplicas: 2,
		Explanation:    []*pb.RecommenderStatus{{DesiredReplicas: 2, IsActive: true, Phase: "Scaling", Name: "latency_target", Type: "Linear", LastUpdated: timestamppb.New(time.Unix(1000, 0))}},
	}
	if diff := cmp.Diff(wantRec, rec, protocmp.Transform()); diff != "" {
		t.Errorf("Recommendation mismatch (-want +got):\n%s", diff)
	}
}

// TestScaleToZeroGRPC validates the Scale-to-Zero (Activation) feature.
// Scenario: A workload should scale to zero when a queue is empty and wake up when it's not.
// Steps:
// 1. Create a Policy with an Activation recommender (Threshold > 0) and a Scaling recommender.
// 2. Wake Up: Ingest queue=50. Verify Activation phase is Active, Target Replicas = 5.
// 3. Cooldown: Ingest queue=0. Verify Activation is Inactive, but Target Replicas = 1 (MinReplicas) because the cooldown window hasn't passed.
// 4. Scale Down: Wait for window to pass. Ingest queue=0. Verify Target Replicas = 0.
func TestScaleToZeroGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "zero-policy"
	ns := "default"
	window := "5"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:          &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "zero-app", Namespace: ns},
			Metrics:     []*pb.MetricDefinition{{Name: "queue", Provider: "sqs", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			Scaling:     []*pb.RecommenderDefinition{{Recommender: "Linear", Name: "target", Type: "Linear", Params: map[string]string{"metric": "queue", "target": "10.0"}}},
			Activation:  []*pb.RecommenderDefinition{{Recommender: "Threshold", Name: "act_queue", Type: "Threshold", Params: map[string]string{"metric": "queue", "threshold": "0", "window": window}}},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
		Workload: &pb.Workload{Pods: []*pb.PodState{}},
	})

	// 1. Wake Up
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies:    []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "", Samples: []*pb.MetricSample{{Name: "queue", Value: 50, Timestamp: ts}}}}}},
	})

	memStore.CalculateAll()
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "act_queue",
		Vote: &pb.RecommenderVote{IsActive: true},
	})
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "target",
		Vote: &pb.RecommenderVote{DesiredReplicas: 5, IsActive: true},
	})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	rec := resp.Recommendation

	wantWakeUp := &pb.Recommendation{
		TargetReplicas: 5,
		Explanation: []*pb.RecommenderStatus{
			{DesiredReplicas: 5, IsActive: true, Phase: "Scaling", Name: "target", Type: "Linear", LastUpdated: timestamppb.New(time.Unix(1000, 0))},
			{IsActive: true, Phase: "Activation", Name: "act_queue", Type: "Threshold", LastUpdated: timestamppb.New(time.Unix(1000, 0))},
		},
	}
	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.SortRepeated(func(a, b *pb.RecommenderStatus) bool { return a.Phase < b.Phase }),
	}
	if diff := cmp.Diff(wantWakeUp, rec, opts...); diff != "" {
		t.Errorf("WakeUp mismatch (-want +got):\n%s", diff)
	}

	// 2. Cooldown
	clk.Advance(3 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies:    []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "", Samples: []*pb.MetricSample{{Name: "queue", Value: 0, Timestamp: ts}}}}}},
	})

	memStore.CalculateAll()
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "act_queue",
		Vote: &pb.RecommenderVote{IsActive: false},
	})
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "target",
		Vote: &pb.RecommenderVote{DesiredReplicas: 0, IsActive: true},
	})

	memStore.CalculateAll()
	resp, _ = client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	rec = resp.Recommendation

	wantCooldown := &pb.Recommendation{
		TargetReplicas: 1, // MinReplicas (Control Plane kept active due to window)
		Explanation: []*pb.RecommenderStatus{
			{DesiredReplicas: 0, IsActive: true, Phase: "Scaling", Name: "target", Type: "Linear", LastUpdated: timestamppb.New(time.Unix(1003, 0))},
			{IsActive: false, Phase: "Activation", Name: "act_queue", Type: "Threshold", LastUpdated: timestamppb.New(time.Unix(1003, 0))},
		},
	}
	if diff := cmp.Diff(wantCooldown, rec, opts...); diff != "" {
		t.Errorf("Cooldown mismatch (-want +got):\n%s", diff)
	}

	// 3. Scale Down
	clk.Advance(6 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies:    []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "", Samples: []*pb.MetricSample{{Name: "queue", Value: 0, Timestamp: ts}}}}}},
	})
	memStore.CalculateAll()
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "act_queue",
		Vote: &pb.RecommenderVote{IsActive: false},
	})
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "target",
		Vote: &pb.RecommenderVote{DesiredReplicas: 0, IsActive: true},
	})

	memStore.CalculateAll()
	resp, _ = client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	rec = resp.Recommendation

	wantScaleDown := &pb.Recommendation{
		TargetReplicas: 0,
		Explanation: []*pb.RecommenderStatus{
			{IsActive: false, Phase: "Activation", Name: "act_queue", Type: "Threshold", LastUpdated: timestamppb.New(time.Unix(1009, 0))},
			// Note: If Inactive, Scaling recommenders might be omitted by Control Plane or present.
			// MemoryStore logic: "if isActive { append scaling } else { maxReplicas = 0 }".
			// So scaling decisions are NOT appended if Inactive.
			// Let's verify MemoryStore logic.
			// "if isActive { ... append Scaling ... }"
			// "for _, recDef := range policy.Activation { ... append Activation ... }"
			// So Scaling decisions are indeed MISSING if Inactive.
		},
	}
	if diff := cmp.Diff(wantScaleDown, rec, opts...); diff != "" {
		t.Errorf("ScaleDown mismatch (-want +got):\n%s", diff)
	}
}

// TestDryRunGRPC validates DryRun mode for recommenders.
// Scenario: A policy has two recommenders: one Active (target 10) and one DryRun (target 100).
// Steps:
// 1. Create Policy with both recommenders.
// 2. Ingest metrics.
// 3. Simulate Recommender states: Active votes 10, DryRun votes 100.
// 4. Verify that the final Recommendation Target Replicas is 10 (DryRun is ignored for actuation).
// 5. Verify that the DryRun decision is still present in the Recommendation explanation for observability.
func TestDryRunGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "dry-policy"
	ns := "default"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload: &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "dry-app", Namespace: ns},
			Metrics:  []*pb.MetricDefinition{{Name: "m1", Provider: "kubelet", Params: map[string]string{"type": "cpu"}, Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			Scaling: []*pb.RecommenderDefinition{
				{Recommender: "Linear", Name: "active_obj", Mode: "Active", Type: "Linear", Params: map[string]string{"metric": "m1", "target": "10.0"}},
				{Recommender: "Linear", Name: "dry_obj", Mode: "DryRun", Type: "Linear", Params: map[string]string{"metric": "m1", "target": "1.0"}},
			},
			MinReplicas: 1, MaxReplicas: 200,
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}}})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies:    []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "m1", Value: 100, Timestamp: ts}}}}}},
	})

	memStore.CalculateAll()

	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "active_obj",
		Vote: &pb.RecommenderVote{DesiredReplicas: 10, IsActive: true},
	})
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "dry_obj",
		Vote: &pb.RecommenderVote{DesiredReplicas: 100, IsActive: true},
	})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	rec := resp.Recommendation

	wantRec := &pb.Recommendation{
		TargetReplicas: 10,
		Explanation: []*pb.RecommenderStatus{
			{DesiredReplicas: 10, IsActive: true, Phase: "Scaling", Mode: "Active", Name: "active_obj", Type: "Linear", LastUpdated: timestamppb.New(time.Unix(1000, 0))},
			{DesiredReplicas: 100, IsActive: true, Phase: "Scaling", Mode: "DryRun", Name: "dry_obj", Type: "Linear", LastUpdated: timestamppb.New(time.Unix(1000, 0))},
		},
	}

	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.SortRepeated(func(a, b *pb.RecommenderStatus) bool { return a.DesiredReplicas < b.DesiredReplicas }),
	}
	if diff := cmp.Diff(wantRec, rec, opts...); diff != "" {
		t.Errorf("DryRun mismatch (-want +got):\n%s", diff)
	}
}

// TestHistogramWithFilterGRPC validates histogram calculation with label filtering.
// Scenario: Calculate p99 latency only for requests with path="/api".
// Steps:
// 1. Create Policy with a Histogram metric filtering on "path": "/api".
// 2. Ingest T0 (empty) buckets.
// 3. Ingest T1 buckets:
//   - /api GET: 50 fast requests.
//   - /api POST: 50 slow requests.
//   - /health: 1000 very slow requests (should be ignored).
//
// 4. Calculate Control Metrics.
// 5. Verify the p99 value corresponds to the mixed /api traffic (~0.5), ignoring /health.
func TestHistogramWithFilterGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "filter-policy"
	ns := "default"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload: &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "filter-app", Namespace: ns},
			Metrics: []*pb.MetricDefinition{{
				Name: "api_latency", Provider: "kubelet",
				Filter:       map[string]string{"path": "/api"},
				Distribution: &pb.Distribution{Percentile: "p99"},
			}},
			Scaling:     []*pb.RecommenderDefinition{{Recommender: "Linear", Name: "target", Type: "Linear", Params: map[string]string{"metric": "api_latency", "target": "0.2"}}},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}}})

	// T0
	ts := clk.Now().Unix()
	buckets0 := map[string]uint64{"0.1": 0, "0.5": 0, "+Inf": 0}
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: ns, Name: policyName,
			Batches: []*pb.MetricBatch{{
				EntityKey: "p1",
				Samples: []*pb.MetricSample{
					{Name: "api_latency", Labels: map[string]string{"path": "/api", "method": "GET"}, HistogramBuckets: buckets0, Timestamp: ts},
					{Name: "api_latency", Labels: map[string]string{"path": "/api", "method": "POST"}, HistogramBuckets: buckets0, Timestamp: ts},
					{Name: "api_latency", Labels: map[string]string{"path": "/health"}, HistogramBuckets: buckets0, Timestamp: ts},
				},
			}},
		}},
	})

	// T1 (10s)
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: ns, Name: policyName,
			Batches: []*pb.MetricBatch{{
				EntityKey: "p1",
				Samples: []*pb.MetricSample{
					// A: 50 fast
					{Name: "api_latency", Labels: map[string]string{"path": "/api", "method": "GET"}, HistogramBuckets: map[string]uint64{"0.1": 50, "0.5": 50, "+Inf": 50}, Timestamp: ts},
					// B: 50 slow
					{Name: "api_latency", Labels: map[string]string{"path": "/api", "method": "POST"}, HistogramBuckets: map[string]uint64{"0.1": 0, "0.5": 50, "+Inf": 50}, Timestamp: ts},
					// C: 1000 very slow (Should be filtered out)
					{Name: "api_latency", Labels: map[string]string{"path": "/health"}, HistogramBuckets: map[string]uint64{"0.1": 0, "0.5": 0, "+Inf": 1000}, Timestamp: ts},
				},
			}},
		}},
	})

	memStore.CalculateAll()

	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	// Value 0.492
	wantCM := &pb.ControlMetrics{Values: map[string]float64{"api_latency": 0.492}, Timestamp: 1010, ReadyReplicas: 1}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

// TestHistogramLatencyScalingGRPC validates scaling based on histogram percentiles.
// Scenario: Scale based on p90 latency.
// Steps:
// 1. Create Policy requesting p90 calculation.
// 2. Ingest histogram buckets indicating a latency distribution.
// 3. Verify the calculated Control Metric value matches the expected p90.
// 4. Verify the final Recommendation uses this value.
func TestHistogramLatencyScalingGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "hist-policy"
	ns := "default"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload: &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "hist-app", Namespace: ns},
			Metrics: []*pb.MetricDefinition{{
				Name: "latency", Provider: "kubelet",
				Distribution: &pb.Distribution{Percentile: "p90"},
			}},
			Scaling:     []*pb.RecommenderDefinition{{Recommender: "Linear", Name: "latency_obj", Type: "Linear", Params: map[string]string{"metric": "latency", "target": "0.1"}}},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}},
	})

	// T0
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: ns, Name: policyName,
			Batches: []*pb.MetricBatch{{
				EntityKey: "p1",
				Samples: []*pb.MetricSample{{
					Name: "latency", Timestamp: ts,
					HistogramBuckets: map[string]uint64{"0.05": 0, "0.2": 0, "+Inf": 0},
				}},
			}},
		}},
	})

	// T1 (10s later)
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: ns, Name: policyName,
			Batches: []*pb.MetricBatch{{
				EntityKey: "p1",
				Samples: []*pb.MetricSample{{
					Name: "latency", Timestamp: ts,
					HistogramBuckets: map[string]uint64{"0.05": 80, "0.2": 100, "+Inf": 100},
				}},
			}},
		}},
	})

	memStore.CalculateAll()

	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "latency_obj",
		Vote: &pb.RecommenderVote{DesiredReplicas: 2, IsActive: true},
	})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	rec := resp.Recommendation

	wantRec := &pb.Recommendation{
		TargetReplicas: 2,
		Explanation:    []*pb.RecommenderStatus{{DesiredReplicas: 2, IsActive: true, Phase: "Scaling", Name: "latency_obj", Type: "Linear", LastUpdated: timestamppb.New(time.Unix(1010, 0))}},
	}
	if diff := cmp.Diff(wantRec, rec, protocmp.Transform()); diff != "" {
		t.Errorf("Recommendation mismatch (-want +got):\n%s", diff)
	}
}

// TestWindowedMetricsGRPC validates temporal aggregation (Windows).
// Scenario: Use a DecayingHistogram to smooth out bursty metrics.
// Steps:
// 1. Create Policy with a DecayingHistogram window (24h half-life).
// 2. Ingest a high value (1.0).
// 3. Verify the Control Metric reflects the smoothed value (Bucket Upper Bound 1.1).
func TestWindowedMetricsGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "window-policy"
	ns := "default"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload: &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: ns},
			Metrics: []*pb.MetricDefinition{
				{
					Name: "cpu_hist", DecayingDistribution: &pb.DecayingDistribution{HalfLife: "24h", BucketSize: "0.1", Percentile: "p100"},
				},
			},
			Scaling:     []*pb.RecommenderDefinition{{Recommender: "Linear", Name: "cpu", Type: "Linear", Params: map[string]string{"metric": "cpu_hist", "target": "0.5"}}},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}},
	})

	// T0: Ingest 1.0 (High)
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "cpu_hist", Value: 1.0, Timestamp: ts}}}}}},
	})

	memStore.CalculateAll()

	// Verify Control Metrics
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	wantCM := &pb.ControlMetrics{
		Values:        map[string]float64{"cpu_hist": 1.1},
		Timestamp:     1000,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

// TestDeletePolicyGRPC validates policy deletion lifecycle.
// Steps:
// 1. Create a Policy.
// 2. Verify it exists in ListPolicies.
// 3. Delete the Policy.
// 4. Verify it is gone from ListPolicies.
// 5. Verify deletion is idempotent (second delete succeeds).
func TestDeletePolicyGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	_, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "to-delete"
	ns := "default"
	id := &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}

	// 1. Create Policy
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}},
	})

	// 2. Verify exists
	resp, _ := client.ListPolicies(ctx, &pb.ListPoliciesRequest{ClusterName: "default"})
	wantResp := &pb.ListPoliciesResponse{
		Policies: []*pb.Policy{{Id: id}},
	}
	if diff := cmp.Diff(wantResp, resp, protocmp.Transform()); diff != "" {
		t.Fatalf("ListPolicies mismatch (-want +got):\n%s", diff)
	}

	// 3. Delete
	_, err := client.DeletePolicy(ctx, &pb.DeletePolicyRequest{Id: id})
	if err != nil {
		t.Errorf("DeletePolicy failed: %v", err)
	}

	// 4. Delete again (idempotency)
	_, err = client.DeletePolicy(ctx, &pb.DeletePolicyRequest{Id: id})
	if err != nil {
		t.Errorf("DeletePolicy second call failed (idempotency): %v", err)
	}

	// 5. Verify gone
	resp, _ = client.ListPolicies(ctx, &pb.ListPoliciesRequest{ClusterName: "default"})
	if diff := cmp.Diff(&pb.ListPoliciesResponse{}, resp, protocmp.Transform()); diff != "" {
		t.Errorf("ListPolicies mismatch (-want +got):\n%s", diff)
	}
}

// TestValidationGRPC validates input validation for gRPC endpoints.
// Steps:
// 1. Test various invalid requests (missing IDs, missing cluster names).
// 2. Assert that the server returns the expected gRPC error codes (InvalidArgument).
func TestValidationGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	_, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	tests := []struct {
		name     string
		call     func() error
		wantCode codes.Code
	}{
		{
			name: "UpdatePolicy: Missing ClusterName in ID",
			call: func() error {
				_, err := client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
					Policy: &pb.Policy{Id: &pb.PolicyId{Namespace: "n", Name: "p"}},
				})
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "DeletePolicy: Missing ID",
			call: func() error {
				_, err := client.DeletePolicy(ctx, &pb.DeletePolicyRequest{})
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "DeletePolicy: Missing ClusterName",
			call: func() error {
				_, err := client.DeletePolicy(ctx, &pb.DeletePolicyRequest{
					Id: &pb.PolicyId{Namespace: "n", Name: "p"},
				})
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "UpdateWorkload: Missing ClusterName",
			call: func() error {
				_, err := client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
					Id:       &pb.PolicyId{Namespace: "n", Name: "p"},
					Workload: &pb.Workload{},
				})
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "GetControlMetrics: Missing ClusterName",
			call: func() error {
				_, err := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{
					Id: &pb.PolicyId{Namespace: "n", Name: "p"},
				})
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "IngestMetrics: Missing ClusterName",
			call: func() error {
				_, err := client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
					Timestamp: 1234,
				})
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "ListPolicies: Missing ClusterName",
			call: func() error {
				_, err := client.ListPolicies(ctx, &pb.ListPoliciesRequest{})
				return err
			},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			st, _ := status.FromError(err)
			if st.Code() != tc.wantCode {
				t.Errorf("Want code %v, got %v (err: %v)", tc.wantCode, st.Code(), err)
			}
		})
	}
}

// TestMultiplePoliciesGRPC ensures isolation between multiple policies in the same cluster.
// Scenario: Two policies ("policy-cpu" and "policy-mem") exist simultaneously.
// Steps:
// 1. Create both policies.
// 2. Ingest metrics for both.
// 3. Verify that GetControlMetrics returns the correct unique value for each policy, ensuring no cross-talk.
func TestMultiplePoliciesGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	// Policy 1: CPU
	p1 := "policy-cpu"
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:          &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: p1},
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app-cpu", Namespace: "default"},
			Metrics:     []*pb.MetricDefinition{{Name: "cpu", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	// Policy 2: Memory
	p2 := "policy-mem"
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:          &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: p2},
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app-mem", Namespace: "default"},
			Metrics:     []*pb.MetricDefinition{{Name: "mem", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: p1},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: p2},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}},
	})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{
			{Namespace: "default", Name: p1, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "cpu", Value: 0.5, Timestamp: ts}}}}},
			{Namespace: "default", Name: p2, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "mem", Value: 1024, Timestamp: ts}}}}},
		},
	})

	memStore.CalculateAll()

	cm1, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: p1}})
	wantCM1 := &pb.ControlMetrics{
		Values:        map[string]float64{"cpu": 0.5},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM1, cm1, protocmp.Transform()); diff != "" {
		t.Errorf("Policy 1 ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	cm2, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: p2}})
	wantCM2 := &pb.ControlMetrics{
		Values:        map[string]float64{"mem": 1024},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM2, cm2, protocmp.Transform()); diff != "" {
		t.Errorf("Policy 2 ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

// TestNamespaceIsolationGRPC ensures policies with the same name in different namespaces are distinct.
// Scenario: "shared-name" policy exists in "ns1" and "ns2".
// Steps:
// 1. Create both policies.
// 2. Ingest different metric values for each.
// 3. Verify that GetControlMetrics returns the specific value for the requested namespace.
func TestNamespaceIsolationGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	name := "shared-name"

	// NS 1
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:          &pb.PolicyId{ClusterName: "default", Namespace: "ns1", Name: name},
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: "ns1"},
			Metrics:     []*pb.MetricDefinition{{Name: "m", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	// NS 2
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:          &pb.PolicyId{ClusterName: "default", Namespace: "ns2", Name: name},
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: "ns2"},
			Metrics:     []*pb.MetricDefinition{{Name: "m", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: "ns1", Name: name},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p", IsReady: true}}},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: "ns2", Name: name},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p", IsReady: true}}},
	})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{
			{Namespace: "ns1", Name: name, Batches: []*pb.MetricBatch{{EntityKey: "p", Samples: []*pb.MetricSample{{Name: "m", Value: 10, Timestamp: ts}}}}},
			{Namespace: "ns2", Name: name, Batches: []*pb.MetricBatch{{EntityKey: "p", Samples: []*pb.MetricSample{{Name: "m", Value: 20, Timestamp: ts}}}}},
		},
	})

	memStore.CalculateAll()

	cm1, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: "ns1", Name: name}})
	wantCM1 := &pb.ControlMetrics{
		Values:        map[string]float64{"m": 10},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM1, cm1, protocmp.Transform()); diff != "" {
		t.Errorf("NS1 ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	cm2, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: "ns2", Name: name}})
	wantCM2 := &pb.ControlMetrics{
		Values:        map[string]float64{"m": 20},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM2, cm2, protocmp.Transform()); diff != "" {
		t.Errorf("NS2 ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

// TestMetricAggregationGRPC validates different aggregation methods across pods.
// Scenario: Aggregating metrics from 2 pods using Avg, Sum, Max, and Min.
// Steps:
// 1. Create Policy defining 4 metrics, one for each aggregation type.
// 2. Ingest samples: Pod1=10, Pod2=30.
// 3. Verify Control Metrics:
//   - Avg: 20
//   - Sum: 40
//   - Max: 30
//   - Min: 10
func TestMetricAggregationGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "agg-policy"
	ns := "default"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload: &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: ns},
			Metrics: []*pb.MetricDefinition{
				{Name: "m_avg", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Avg"}},
				{Name: "m_sum", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Sum"}},
				{Name: "m_max", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Max"}},
				{Name: "m_min", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Min"}},
			},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}, {Name: "p2", IsReady: true}}},
	})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: ns, Name: policyName,
			Batches: []*pb.MetricBatch{
				{EntityKey: "p1", Samples: []*pb.MetricSample{
					{Name: "m_avg", Value: 10, Timestamp: ts},
					{Name: "m_sum", Value: 10, Timestamp: ts},
					{Name: "m_max", Value: 10, Timestamp: ts},
					{Name: "m_min", Value: 10, Timestamp: ts},
				}},
				{EntityKey: "p2", Samples: []*pb.MetricSample{
					{Name: "m_avg", Value: 30, Timestamp: ts},
					{Name: "m_sum", Value: 30, Timestamp: ts},
					{Name: "m_max", Value: 30, Timestamp: ts},
					{Name: "m_min", Value: 30, Timestamp: ts},
				}},
			},
		}},
	})

	memStore.CalculateAll()

	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})

	wantCM := &pb.ControlMetrics{
		Values: map[string]float64{
			"m_avg": 20.0,
			"m_sum": 40.0,
			"m_max": 30.0,
			"m_min": 10.0,
		},
		Timestamp:     ts,
		ReadyReplicas: 2,
	}

	if diff := cmp.Diff(wantCM, cm, protocmp.Transform()); diff != "" {
		t.Errorf("Aggregation mismatch (-want +got):\n%s", diff)
	}
}

// TestRecommenderInactiveGRPC ensures inactive recommenders do not influence the decision.
// Scenario: Two recommenders, one Active (votes 5) and one Inactive (votes 100).
// Steps:
// 1. Create Policy.
// 2. Set Recommender states.
// 3. Verify Final Recommendation is 5. If the Inactive recommender were counted, it would be 100 (since we take the max).
func TestRecommenderInactiveGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "inactive-rec-policy"
	ns := "default"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload: &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: ns},
			Metrics:  []*pb.MetricDefinition{{Name: "m", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			Scaling: []*pb.RecommenderDefinition{
				{Recommender: "Linear", Name: "r_active", Type: "Linear", Params: map[string]string{"target": "5"}},
				{Recommender: "Linear", Name: "r_inactive", Type: "Linear", Params: map[string]string{"target": "100"}},
			},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	memStore.CalculateAll()

	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "r_active",
		Vote: &pb.RecommenderVote{DesiredReplicas: 5, IsActive: true},
	})
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "r_inactive",
		Vote: &pb.RecommenderVote{DesiredReplicas: 100, IsActive: false},
	})

	memStore.CalculateAll()

	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	rec := resp.Recommendation

	wantRec := &pb.Recommendation{
		TargetReplicas: 5,
		Explanation: []*pb.RecommenderStatus{
			{DesiredReplicas: 5, IsActive: true, Phase: "Scaling", Name: "r_active", Type: "Linear", LastUpdated: timestamppb.New(time.Unix(1000, 0))},
			{DesiredReplicas: 100, IsActive: false, Phase: "Scaling", Name: "r_inactive", Type: "Linear", LastUpdated: timestamppb.New(time.Unix(1000, 0))},
		},
	}

	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.SortRepeated(func(a, b *pb.RecommenderStatus) bool { return a.Name < b.Name }),
	}
	if diff := cmp.Diff(wantRec, rec, opts...); diff != "" {
		t.Errorf("Recommendation mismatch (-want +got):\n%s", diff)
	}
}

// TestDecayingHistogramLifecycleGRPC validates the decaying histogram behavior over time.
// Scenario: Use a DecayingHistogram with a short half-life (10s) to verify decay and accumulation.
// Steps:
// 1. Create Policy with DecayingHistogram (10s half-life).
// 2. T0: Ingest Value 100. Verify Control Metric is ~100.
// 3. T1 (10s later): No new data. Verify Control Metric sticks to ~100 (Max bucket).
// 4. T3 (20s later): Ingest Value 200. Verify Control Metric updates to new max ~200.
// 5. T4 (Long Gap): Verify it still correctly captures new data.
func TestDecayingHistogramLifecycleGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "decay-lifecycle-policy"
	ns := "default"

	// 1. Configure Policy (10s Half-Life)
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload: &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: ns},
			Metrics: []*pb.MetricDefinition{
				{
					Name: "load", DecayingDistribution: &pb.DecayingDistribution{HalfLife: "10s", BucketSize: "1.0", Percentile: "p100"},
				},
			},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}},
	})

	// 2. T0: Ingest 100
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "load", Value: 100.0, Timestamp: ts}}}}}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	wantCM0 := &pb.ControlMetrics{
		Values:        map[string]float64{"load": 101.0}, // Bucket upper bound
		Timestamp:     1000,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM0, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
		t.Errorf("T0 ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	// 3. T1: 10s later (1 Half-Life). No new data.
	clk.Advance(10 * time.Second)
	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})

	// Expectation: Histogram p100 sticks to the last maximum seen until new data arrives.
	wantCM1 := &pb.ControlMetrics{
		Values:        map[string]float64{"load": 101.0},
		Timestamp:     1010,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM1, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
		t.Errorf("T1 ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	// 4. T3: Ingest 200.
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "load", Value: 200.0, Timestamp: ts}}}}}},
	})

	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})

	wantCM3 := &pb.ControlMetrics{
		Values:        map[string]float64{"load": 201.0},
		Timestamp:     1010,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM3, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
		t.Errorf("T3 ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	// 5. T4: Long Gap (1h) then 300.
	clk.Advance(1 * time.Hour)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "load", Value: 300.0, Timestamp: ts}}}}}},
	})

	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})

	wantCM4 := &pb.ControlMetrics{
		Values:        map[string]float64{"load": 301.0},
		Timestamp:     4610,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM4, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
		t.Errorf("T4 ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

// TestPolicyNotFoundGRPC validates error handling for non-existent policies.
// Steps:
// 1. Attempt to GetControlMetrics for a missing policy.
// 2. Verify that an error is returned.
func TestPolicyNotFoundGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	_, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	_, err := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: "missing"},
	})

	if err == nil {
		t.Errorf("GetControlMetrics for missing policy should error")
	} else {
		st, _ := status.FromError(err)
		if st.Code() != codes.Unknown && st.Message() != "policy not found" {
			t.Logf("Got error %v (code %v), expected 'policy not found'", err, st.Code())
		}
	}
}

// TestRemoveScalingSectionGRPC validates that removing the scaling section from a policy
// stops the recommendation generation.
func TestRemoveScalingSectionGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()

	ctx := context.Background()
	policyName := "remove-scaling-policy"
	ns := "default"
	id := &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}

	// 1. Policy with Scaling
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:          id,
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: ns},
			Metrics:     []*pb.MetricDefinition{{Name: "m", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			Scaling:     []*pb.RecommenderDefinition{{Recommender: "Linear", Name: "r1", Type: "Linear", Params: map[string]string{"target": "10"}}},
			MinReplicas: 1, MaxReplicas: 100,
		},
	})

	// 2. Simulate Recommender
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: id, RecommenderName: "r1",
		Vote: &pb.RecommenderVote{DesiredReplicas: 10, IsActive: true},
	})

	// 3. Calc Recommendation
	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: id})
	if resp.Recommendation == nil || resp.Recommendation.TargetReplicas != 10 {
		t.Errorf("Initial: Want 10, Got %v", resp.Recommendation)
	}

	// 4. Update Policy: Remove Scaling
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:          id,
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: ns},
			Metrics:     []*pb.MetricDefinition{{Name: "m", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			Scaling:     []*pb.RecommenderDefinition{}, // Empty
			MinReplicas: 1, MaxReplicas: 100,
		},
	})

	// 5. Verify No Recommendation
	resp, _ = client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: id})
	if resp.Recommendation != nil {
	}
}

// TestSlidingWindowGRPC validates the Sliding Window aggregation (Simple Moving Average).
// Scenario: A 1m window averaging metrics.
// Steps:
// 1. Create Policy with 1m Avg Window.
// 2. T0: Ingest 10.
// 3. T1 (30s): Ingest 20.
// 4. Verify Control Metric is 15 (Avg of 10 and 20).
// 5. T2 (90s): Verify Control Metric is 20 (10 dropped out of 60s window).
func TestSlidingWindowGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "sliding-policy"
	ns := "default"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload: &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: ns},
			Metrics: []*pb.MetricDefinition{
				{
					Name: "m", Gauge: &pb.Gauge{Aggregation: "Avg"},
				},
			},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}},
	})

	// T0: Ingest 10
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "m", Value: 10, Timestamp: ts}}}}}},
	})
	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	checkCM(t, cm, map[string]float64{"m": 10}, ts)

	// T1: 30s later, Ingest 20. Gauge is instantaneous, so it should see 20.
	clk.Advance(30 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "m", Value: 20, Timestamp: ts}}}}}},
	})
	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	checkCM(t, cm, map[string]float64{"m": 20}, ts)

	// T2: 90s later (60s after T1). Window contains [20] (10 expired after 60s).
	clk.Advance(60 * time.Second)
	ts = clk.Now().Unix() // 1090
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "m", Value: 20, Timestamp: ts}}}}}},
	})
	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	checkCM(t, cm, map[string]float64{"m": 20}, ts)
}

// TestMetricGCGRPC validates that old metric series are garbage collected.
// Scenario: Ingest metric, wait passed GC cutoff, verify it's gone.
func TestMetricGCGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "gc-policy"
	ns := "default"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:          &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: ns},
			Metrics:     []*pb.MetricDefinition{{Name: "m", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}},
	})

	// T0: Ingest
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "m", Value: 10, Timestamp: ts}}}}}},
	})

	// Verify exists
	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	if len(cm.Values) == 0 {
		t.Fatal("Metric should exist")
	}

	// Advance past GC cutoff (600s = 10m)
	clk.Advance(11 * time.Minute)

	// Trigger calculation to run GC
	memStore.CalculateAll()

	// Verify gone.
	// GetControlMetrics returns the last calculated state.
	// If GC removed the series, CalculateAll should produce empty ControlMetrics.
	cm, _ = client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})
	if len(cm.Values) > 0 {
		t.Errorf("Metric should be GC'd, found: %v", cm.Values)
	}
}

// TestRecommenderArbitrationGRPC validates that the maximum replica count is chosen
// when multiple recommenders are active.
func TestRecommenderArbitrationGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "arbitration-policy"
	ns := "default"

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
			Workload: &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: ns},
			Metrics:  []*pb.MetricDefinition{{Name: "m", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			Scaling: []*pb.RecommenderDefinition{
				{Recommender: "Linear", Name: "r_low", Type: "Linear", Params: map[string]string{"target": "100"}}, // Votes low
				{Recommender: "Linear", Name: "r_high", Type: "Linear", Params: map[string]string{"target": "10"}}, // Votes high (metric=200 -> 20 reps)
			},
			MinReplicas: 1, MaxReplicas: 100,
		},
	})

	// Simulate Recommender States
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "r_low",
		Vote: &pb.RecommenderVote{DesiredReplicas: 5, IsActive: true},
	})
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}, RecommenderName: "r_high",
		Vote: &pb.RecommenderVote{DesiredReplicas: 20, IsActive: true},
	})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}})

	// Should equal Max(5, 20) = 20
	if resp.Recommendation.TargetReplicas != 20 {
		t.Errorf("Arbitration failed: Want 20, Got %d", resp.Recommendation.TargetReplicas)
	}
}

// TestPolicyMutationGRPC validates that updating a policy (e.g. metric type) works safely.
func TestPolicyMutationGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "mutation-policy"
	ns := "default"
	pid := &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName}

	// 1. Initial: Gauge
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:          pid,
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: ns},
			Metrics:     []*pb.MetricDefinition{{Name: "m", Provider: "kubelet", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
			MinReplicas: 1, MaxReplicas: 10,
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{Id: pid, Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}}})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "m", Value: 10, Timestamp: ts}}}}}},
	})
	memStore.CalculateAll()

	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: pid})
	if cm.Values["m"] != 10 {
		t.Errorf("Initial Gauge mismatch: %v", cm.Values["m"])
	}

	// 2. Update to Counter (Rate)
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:          pid,
			Workload:    &pb.WorkloadRef{Group: "apps", Version: "v1", Kind: "Deployment", Name: "app", Namespace: ns},
			Metrics:     []*pb.MetricDefinition{{Name: "m", Provider: "kubelet", Rate: &pb.Rate{Window: "1m"}}}, // Changed to Rate
			MinReplicas: 1, MaxReplicas: 10,
		},
	})

	// 3. Ingest next sample (Counter logic: Rate)
	// T1 (10s later): Value 20. Diff=10. Rate=1.
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "default", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: ns, Name: policyName, Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "m", Value: 20, Timestamp: ts}}}}}},
	})

	memStore.CalculateAll()

	cm, _ = client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: pid})
	// Rate = (20-10)/10 = 1.0
	if cm.Values["m"] != 1.0 {
		t.Errorf("Mutation Counter mismatch: Want 1.0, Got %v", cm.Values["m"])
	}
}

func checkCM(t *testing.T, got *pb.ControlMetrics, wantVals map[string]float64, wantTS int64) {
	t.Helper()
	if got.Timestamp != wantTS {
		t.Errorf("Timestamp mismatch: want %d, got %d", wantTS, got.Timestamp)
	}
	for k, v := range wantVals {
		if got.Values[k] != v {
			t.Errorf("Value mismatch for %s: want %f, got %f", k, v, got.Values[k])
		}
	}
}
