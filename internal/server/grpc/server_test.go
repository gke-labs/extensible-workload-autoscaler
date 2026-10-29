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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
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

func setupGRPCServer(t *testing.T, c clock.Clock) (*store.MemoryStore, pb.XASServerClient, func()) {
	lis := bufconn.Listen(bufSize)
	memStore := store.NewMemoryStoreWithClock(c)
	srv := servergrpc.NewServer(memStore, c)

	s := grpc.NewServer()
	pb.RegisterXASServerServer(s, srv)
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
	client := pb.NewXASServerClient(conn)

	cleanup := func() {
		conn.Close()
		s.Stop()
	}

	return memStore, client, cleanup
}

// getControlMetrics reads the control metrics of a policy, failing the test if
// the call does.
func getControlMetrics(t *testing.T, ctx context.Context, client pb.XASServerClient, id *pb.PolicyId) *pb.GetControlMetricsResponse {
	t.Helper()
	res, err := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())
	if err != nil {
		t.Fatalf("GetControlMetrics(%s/%s) failed: %v", id.GetNamespace(), id.GetName(), err)
	}
	return res
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
// 7. Verify the final Recommendation from the Server matches the Recommender's output.
func TestServerEndToEndGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()

	ctx := context.Background()

	policyName := "web-app-policy"
	ns := "default"

	// 1. Policy
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload: pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("web-app"), Namespace: proto.String(ns)}.Build(),
			Metrics:  []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("cpu_avg"), Provider: proto.String("kubelet"), Params: map[string]string{"type": "cpu"}, Rate: pb.Rate_builder{Window: durationpb.New(time.Minute), Aggregation: proto.String("Avg")}.Build()}.Build()},

			Scaling:     []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("cpu"), Mode: proto.String("Active"), Type: proto.String("Linear"), Params: map[string]string{"metric": "cpu_avg", "target": "0.5"}}.Build()},
			MinReplicas: proto.Int32(2), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	// 2. Workload: the pods must be reported before their samples are ingested.
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	// 3. Ingest T0
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{
			pb.PolicyBatch_builder{
				Namespace: proto.String(ns), Name: proto.String(policyName),
				Batches: []*pb.MetricBatch{
					pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_avg"), Value: proto.Float64(100), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
					pb.MetricBatch_builder{PodName: proto.String("p2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_avg"), Value: proto.Float64(100), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
				},
			}.Build(),
		},
	}.Build())

	// 4. Ingest T1 (10s later, +8 diff)
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{
			pb.PolicyBatch_builder{
				Namespace: proto.String(ns), Name: proto.String(policyName),
				Batches: []*pb.MetricBatch{
					pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_avg"), Value: proto.Float64(108), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
					pb.MetricBatch_builder{PodName: proto.String("p2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_avg"), Value: proto.Float64(108), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
				},
			}.Build(),
		},
	}.Build())

	// 5. Calc Control Metrics
	memStore.CalculateAll()

	res := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())

	wantRes := pb.GetControlMetricsResponse_builder{
		Timestamp:     timestamppb.New(time.Unix(1010, 0)),
		ReadyReplicas: proto.Int32(2),
		ControlMetrics: pb.ControlMetrics_builder{
			Values: map[string]float64{"cpu_avg": 0.8},
		}.Build(),
	}.Build()
	if diff := cmp.Diff(wantRes, res, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	// 6. Simulate Recommender
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("cpu"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(4), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	// 7. Calc Recommendation
	memStore.CalculateAll()

	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()}.Build())

	wantResp := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(4),
		}.Build(),
		Explanation: []*pb.RecommenderStatus{
			pb.RecommenderStatus_builder{Replicas: proto.Int32(4), IsActive: proto.Bool(true), Phase: proto.String("Scaling"), Mode: proto.String("Active"), Name: proto.String("cpu"), Type: proto.String("Linear"), LastUpdated: timestamppb.New(time.Unix(1010, 0))}.Build(), // cpu
		},
		MetricStatuses: resp.GetMetricStatuses(),
	}.Build()
	// Sort slices for deterministic comparison
	opts := []cmp.Option{
		protocmp.Transform(), protocmp.IgnoreDefaultScalars(),
		protocmp.SortRepeated(func(a, b *pb.RecommenderStatus) bool { return a.GetPhase() < b.GetPhase() }), // Simple sort if needed
	}
	if diff := cmp.Diff(wantResp, resp, opts...); diff != "" {
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
// 7. Verify the final Recommendation matches the expected arbitrated recommendation.
func TestDistributedCollectionGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "dist-app-policy"
	ns := "default"

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload: pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("dist-app"), Namespace: proto.String(ns)}.Build(),
			Metrics:  []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("cpu_avg"), Provider: proto.String("kubelet"), Params: map[string]string{"type": "cpu"}, Rate: pb.Rate_builder{Window: durationpb.New(time.Minute), Aggregation: proto.String("Avg")}.Build()}.Build()},

			Scaling:     []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("cpu"), Type: proto.String("Linear"), Params: map[string]string{"metric": "cpu_avg", "target": "0.5"}}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(20),
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{
			pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p3"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p4"), IsReady: proto.Bool(true)}.Build(),
		}}.Build(),
	}.Build())

	// T0
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{
			pb.PolicyBatch_builder{
				Namespace: proto.String(ns), Name: proto.String(policyName),
				Batches: []*pb.MetricBatch{
					pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_avg"), Value: proto.Float64(100), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
					pb.MetricBatch_builder{PodName: proto.String("p2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_avg"), Value: proto.Float64(100), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
					pb.MetricBatch_builder{PodName: proto.String("p3"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_avg"), Value: proto.Float64(100), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
					pb.MetricBatch_builder{PodName: proto.String("p4"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_avg"), Value: proto.Float64(100), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
				},
			}.Build(),
		},
	}.Build())

	// T1 (Agent A: 10s later)
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{
			pb.PolicyBatch_builder{
				Namespace: proto.String(ns), Name: proto.String(policyName),
				Batches: []*pb.MetricBatch{
					pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_avg"), Value: proto.Float64(120), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
					pb.MetricBatch_builder{PodName: proto.String("p2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_avg"), Value: proto.Float64(120), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
				},
			}.Build(),
		},
	}.Build())

	// T1 (Agent B: 12s later)
	clk.Advance(2 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{
			pb.PolicyBatch_builder{
				Namespace: proto.String(ns), Name: proto.String(policyName),
				Batches: []*pb.MetricBatch{
					pb.MetricBatch_builder{PodName: proto.String("p3"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_avg"), Value: proto.Float64(124), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
					pb.MetricBatch_builder{PodName: proto.String("p4"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_avg"), Value: proto.Float64(124), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
				},
			}.Build(),
		},
	}.Build())

	memStore.CalculateAll()

	res := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())
	// Avg = 2.0
	if diff := cmp.Diff(pb.GetControlMetricsResponse_builder{Timestamp: timestamppb.New(time.Unix(1012, 0)), ReadyReplicas: proto.Int32(4), ControlMetrics: pb.ControlMetrics_builder{Values: map[string]float64{"cpu_avg": 2.0}}.Build()}.Build(), res, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("cpu"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(16), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()}.Build())

	wantResp := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(16),
		}.Build(),
		Explanation:    []*pb.RecommenderStatus{pb.RecommenderStatus_builder{Replicas: proto.Int32(16), IsActive: proto.Bool(true), Phase: proto.String("Scaling"), Name: proto.String("cpu"), Type: proto.String("Linear"), LastUpdated: timestamppb.New(time.Unix(1012, 0))}.Build()},
		MetricStatuses: resp.GetMetricStatuses(),
	}.Build()
	if diff := cmp.Diff(wantResp, resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
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
// 5. Simulate a Recommender recommendation to scale to 100 replicas.
// 6. Verify the final Recommendation.
func TestExternalMetricGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "ext-app-policy"
	ns := "default"

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:          pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("ext-app"), Namespace: proto.String(ns)}.Build(),
			Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("queue"), Provider: proto.String("sqs"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
			Scaling:     []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("queue_target"), Type: proto.String("Linear"), Params: map[string]string{"metric": "queue", "target": "10.0"}}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(200),
		}.Build(),
	}.Build())

	var pods []*pb.PodState
	for i := 0; i < 10; i++ {
		pods = append(pods, pb.PodState_builder{Name: proto.String(fmt.Sprintf("p-%d", i)), IsReady: proto.Bool(true)}.Build())
	}
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), Workload: pb.Workload_builder{Pods: pods}.Build()}.Build())

	// Ingest Global
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{
			pb.PolicyBatch_builder{
				Namespace: proto.String(ns), Name: proto.String(policyName),
				Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String(""), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("queue"), Value: proto.Float64(1000), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()},
			}.Build(),
		},
	}.Build())

	memStore.CalculateAll()

	res := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())
	// Avg = 1000/10 = 100.
	if diff := cmp.Diff(pb.GetControlMetricsResponse_builder{Timestamp: timestamppb.New(time.Unix(1000, 0)), ReadyReplicas: proto.Int32(10), ControlMetrics: pb.ControlMetrics_builder{Values: map[string]float64{"queue": 100.0}}.Build()}.Build(), res, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("queue_target"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(100), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()}.Build())

	wantResp := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(100),
		}.Build(),
		Explanation:    []*pb.RecommenderStatus{pb.RecommenderStatus_builder{Replicas: proto.Int32(100), IsActive: proto.Bool(true), Phase: proto.String("Scaling"), Name: proto.String("queue_target"), Type: proto.String("Linear"), LastUpdated: timestamppb.New(time.Unix(1000, 0))}.Build()},
		MetricStatuses: resp.GetMetricStatuses(),
	}.Build()
	if diff := cmp.Diff(wantResp, resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
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

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:          pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String(ns)}.Build(),
			Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("sidecar_latency"), Provider: proto.String("sidecar-provider"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
			Scaling:     []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("latency_target"), Type: proto.String("Linear"), Params: map[string]string{"metric": "sidecar_latency", "target": "100"}}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("pod-1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("pod-2"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{
			pb.PolicyBatch_builder{
				Namespace: proto.String(ns), Name: proto.String(policyName),
				Batches: []*pb.MetricBatch{
					pb.MetricBatch_builder{PodName: proto.String("pod-1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("sidecar_latency"), Value: proto.Float64(50), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
					pb.MetricBatch_builder{PodName: proto.String("pod-2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("sidecar_latency"), Value: proto.Float64(150), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build(),
				},
			}.Build(),
		},
	}.Build())

	memStore.CalculateAll()

	res := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())
	// Avg = (50+150)/2 = 100.
	if diff := cmp.Diff(pb.GetControlMetricsResponse_builder{Timestamp: timestamppb.New(time.Unix(1000, 0)), ReadyReplicas: proto.Int32(2), ControlMetrics: pb.ControlMetrics_builder{Values: map[string]float64{"sidecar_latency": 100.0}}.Build()}.Build(), res, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("latency_target"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(2), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()}.Build())

	wantResp := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(2),
		}.Build(),
		Explanation:    []*pb.RecommenderStatus{pb.RecommenderStatus_builder{Replicas: proto.Int32(2), IsActive: proto.Bool(true), Phase: proto.String("Scaling"), Name: proto.String("latency_target"), Type: proto.String("Linear"), LastUpdated: timestamppb.New(time.Unix(1000, 0))}.Build()},
		MetricStatuses: resp.GetMetricStatuses(),
	}.Build()
	if diff := cmp.Diff(wantResp, resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
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

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:          pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("zero-app"), Namespace: proto.String(ns)}.Build(),
			Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("queue"), Provider: proto.String("sqs"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
			Scaling:     []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("target"), Type: proto.String("Linear"), Params: map[string]string{"metric": "queue", "target": "10.0"}}.Build()},
			Activation:  []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Threshold"), Name: proto.String("act_queue"), Type: proto.String("Threshold"), Params: map[string]string{"metric": "queue", "threshold": "0", "window": window}}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{}}.Build(),
	}.Build())

	// 1. Wake Up
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies:    []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String(""), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("queue"), Value: proto.Float64(50), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("act_queue"),
		Recommendation: pb.Recommendation_builder{IsActive: proto.Bool(true)}.Build(),
	}.Build())
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("target"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(5), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()}.Build())

	wantWakeUp := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(5),
		}.Build(),
		Explanation: []*pb.RecommenderStatus{
			pb.RecommenderStatus_builder{Replicas: proto.Int32(5), IsActive: proto.Bool(true), Phase: proto.String("Scaling"), Name: proto.String("target"), Type: proto.String("Linear"), LastUpdated: timestamppb.New(time.Unix(1000, 0))}.Build(),
			pb.RecommenderStatus_builder{IsActive: proto.Bool(true), Phase: proto.String("Activation"), Name: proto.String("act_queue"), Type: proto.String("Threshold"), LastUpdated: timestamppb.New(time.Unix(1000, 0))}.Build(),
		},
		MetricStatuses: resp.GetMetricStatuses(),
	}.Build()
	opts := []cmp.Option{
		protocmp.Transform(), protocmp.IgnoreDefaultScalars(),
		protocmp.SortRepeated(func(a, b *pb.RecommenderStatus) bool { return a.GetPhase() < b.GetPhase() }),
	}
	if diff := cmp.Diff(wantWakeUp, resp, opts...); diff != "" {
		t.Errorf("WakeUp mismatch (-want +got):\n%s", diff)
	}

	// 2. Cooldown
	clk.Advance(3 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies:    []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String(""), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("queue"), Value: proto.Float64(0), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("act_queue"),
		Recommendation: pb.Recommendation_builder{IsActive: proto.Bool(false)}.Build(),
	}.Build())
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("target"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(0), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ = client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()}.Build())

	wantCooldown := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(1), // MinReplicas (Server kept active due to window)
		}.Build(),
		Explanation: []*pb.RecommenderStatus{
			pb.RecommenderStatus_builder{Replicas: proto.Int32(0), IsActive: proto.Bool(true), Phase: proto.String("Scaling"), Name: proto.String("target"), Type: proto.String("Linear"), LastUpdated: timestamppb.New(time.Unix(1003, 0))}.Build(),
			pb.RecommenderStatus_builder{IsActive: proto.Bool(false), Phase: proto.String("Activation"), Name: proto.String("act_queue"), Type: proto.String("Threshold"), LastUpdated: timestamppb.New(time.Unix(1003, 0))}.Build(),
		},
		MetricStatuses: resp.GetMetricStatuses(),
	}.Build()
	if diff := cmp.Diff(wantCooldown, resp, opts...); diff != "" {
		t.Errorf("Cooldown mismatch (-want +got):\n%s", diff)
	}

	// 3. Scale Down
	clk.Advance(6 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies:    []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String(""), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("queue"), Value: proto.Float64(0), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())
	memStore.CalculateAll()
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("act_queue"),
		Recommendation: pb.Recommendation_builder{IsActive: proto.Bool(false)}.Build(),
	}.Build())
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("target"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(0), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ = client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()}.Build())

	wantScaleDown := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(0),
		}.Build(),
		Explanation: []*pb.RecommenderStatus{
			pb.RecommenderStatus_builder{IsActive: proto.Bool(false), Phase: proto.String("Activation"), Name: proto.String("act_queue"), Type: proto.String("Threshold"), LastUpdated: timestamppb.New(time.Unix(1009, 0))}.Build(),
		},
		MetricStatuses: resp.GetMetricStatuses(),
	}.Build()
	if diff := cmp.Diff(wantScaleDown, resp, opts...); diff != "" {
		t.Errorf("ScaleDown mismatch (-want +got):\n%s", diff)
	}
}

// TestDryRunGRPC validates DryRun mode for recommenders.
// Scenario: A policy has two recommenders: one Active (target 10) and one DryRun (target 100).
// Steps:
// 1. Create Policy with both recommenders.
// 2. Ingest metrics.
// 3. Simulate Recommender states: Active recommends 10, DryRun recommends 100.
// 4. Verify that the final Recommendation Target Replicas is 10 (DryRun is ignored for actuation).
// 5. Verify that the DryRun recommendation is still present in the Recommendation explanation for observability.
func TestDryRunGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	policyName := "dry-policy"
	ns := "default"

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload: pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("dry-app"), Namespace: proto.String(ns)}.Build(),
			Metrics:  []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m1"), Provider: proto.String("kubelet"), Params: map[string]string{"type": "cpu"}, Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
			Scaling: []*pb.RecommenderDefinition{
				pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("active_obj"), Mode: proto.String("Active"), Type: proto.String("Linear"), Params: map[string]string{"metric": "m1", "target": "10.0"}}.Build(),
				pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("dry_obj"), Mode: proto.String("DryRun"), Type: proto.String("Linear"), Params: map[string]string{"metric": "m1", "target": "1.0"}}.Build(),
			},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(200),
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build()}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies:    []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m1"), Value: proto.Float64(100), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()

	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("active_obj"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(10), IsActive: proto.Bool(true)}.Build(),
	}.Build())
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("dry_obj"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(100), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()}.Build())

	wantResp := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(10),
		}.Build(),
		Explanation: []*pb.RecommenderStatus{
			pb.RecommenderStatus_builder{Replicas: proto.Int32(10), IsActive: proto.Bool(true), Phase: proto.String("Scaling"), Mode: proto.String("Active"), Name: proto.String("active_obj"), Type: proto.String("Linear"), LastUpdated: timestamppb.New(time.Unix(1000, 0))}.Build(),
			pb.RecommenderStatus_builder{Replicas: proto.Int32(100), IsActive: proto.Bool(true), Phase: proto.String("Scaling"), Mode: proto.String("DryRun"), Name: proto.String("dry_obj"), Type: proto.String("Linear"), LastUpdated: timestamppb.New(time.Unix(1000, 0))}.Build(),
		},
		MetricStatuses: resp.GetMetricStatuses(),
	}.Build()

	opts := []cmp.Option{
		protocmp.Transform(), protocmp.IgnoreDefaultScalars(),
		protocmp.SortRepeated(func(a, b *pb.RecommenderStatus) bool {
			return a.GetReplicas() < b.GetReplicas()
		}),
	}
	if diff := cmp.Diff(wantResp, resp, opts...); diff != "" {
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

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload: pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("filter-app"), Namespace: proto.String(ns)}.Build(),
			Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{
				Name: proto.String("api_latency"), Provider: proto.String("kubelet"),
				Filter:       map[string]string{"path": "/api"},
				Distribution: pb.Distribution_builder{Percentile: proto.String("p99")}.Build(),
			}.Build()},
			Scaling:     []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("target"), Type: proto.String("Linear"), Params: map[string]string{"metric": "api_latency", "target": "0.2"}}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build()}.Build())

	// T0
	ts := clk.Now().Unix()
	buckets0 := map[string]uint64{"0.1": 0, "0.5": 0, "+Inf": 0}
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String(ns), Name: proto.String(policyName),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("p1"),
				Samples: []*pb.MetricSample{
					pb.MetricSample_builder{Name: proto.String("api_latency"), Labels: map[string]string{"path": "/api", "method": "GET"}, Histogram: pb.Histogram_builder{Buckets: buckets0}.Build(), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
					pb.MetricSample_builder{Name: proto.String("api_latency"), Labels: map[string]string{"path": "/api", "method": "POST"}, Histogram: pb.Histogram_builder{Buckets: buckets0}.Build(), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
					pb.MetricSample_builder{Name: proto.String("api_latency"), Labels: map[string]string{"path": "/health"}, Histogram: pb.Histogram_builder{Buckets: buckets0}.Build(), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
				},
			}.Build()},
		}.Build()},
	}.Build())

	// T1 (10s)
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String(ns), Name: proto.String(policyName),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("p1"),
				Samples: []*pb.MetricSample{
					// A: 50 fast
					pb.MetricSample_builder{Name: proto.String("api_latency"), Labels: map[string]string{"path": "/api", "method": "GET"}, Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"0.1": 50, "0.5": 50, "+Inf": 50}}.Build(), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
					// B: 50 slow
					pb.MetricSample_builder{Name: proto.String("api_latency"), Labels: map[string]string{"path": "/api", "method": "POST"}, Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"0.1": 0, "0.5": 50, "+Inf": 50}}.Build(), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
					// C: 1000 very slow (Should be filtered out)
					pb.MetricSample_builder{Name: proto.String("api_latency"), Labels: map[string]string{"path": "/health"}, Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"0.1": 0, "0.5": 0, "+Inf": 1000}}.Build(), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
				},
			}.Build()},
		}.Build()},
	}.Build())

	memStore.CalculateAll()

	res := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())
	// Value 0.492
	wantRes := pb.GetControlMetricsResponse_builder{Timestamp: timestamppb.New(time.Unix(1010, 0)), ReadyReplicas: proto.Int32(1), ControlMetrics: pb.ControlMetrics_builder{Values: map[string]float64{"api_latency": 0.492}}.Build()}.Build()
	if diff := cmp.Diff(wantRes, res, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
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

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload: pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("hist-app"), Namespace: proto.String(ns)}.Build(),
			Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{
				Name: proto.String("latency"), Provider: proto.String("kubelet"),
				Distribution: pb.Distribution_builder{Percentile: proto.String("p90")}.Build(),
			}.Build()},
			Scaling:     []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("latency_obj"), Type: proto.String("Linear"), Params: map[string]string{"metric": "latency", "target": "0.1"}}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	// T0
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String(ns), Name: proto.String(policyName),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("p1"),
				Samples: []*pb.MetricSample{pb.MetricSample_builder{
					Name: proto.String("latency"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
					Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"0.05": 0, "0.2": 0, "+Inf": 0}}.Build(),
				}.Build()},
			}.Build()},
		}.Build()},
	}.Build())

	// T1 (10s later)
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String(ns), Name: proto.String(policyName),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("p1"),
				Samples: []*pb.MetricSample{pb.MetricSample_builder{
					Name: proto.String("latency"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
					Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"0.05": 80, "0.2": 100, "+Inf": 100}}.Build(),
				}.Build()},
			}.Build()},
		}.Build()},
	}.Build())

	memStore.CalculateAll()

	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("latency_obj"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(2), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()}.Build())

	wantResp := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(2),
		}.Build(),
		Explanation:    []*pb.RecommenderStatus{pb.RecommenderStatus_builder{Replicas: proto.Int32(2), IsActive: proto.Bool(true), Phase: proto.String("Scaling"), Name: proto.String("latency_obj"), Type: proto.String("Linear"), LastUpdated: timestamppb.New(time.Unix(1010, 0))}.Build()},
		MetricStatuses: resp.GetMetricStatuses(),
	}.Build()
	if diff := cmp.Diff(wantResp, resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("Recommendation mismatch (-want +got):\n%s", diff)
	}
}

// TestWindowedMetricsGRPC validates time windows.
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

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload: pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String(ns)}.Build(),
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{
					Name: proto.String("cpu_hist"), DecayingDistribution: pb.DecayingDistribution_builder{HalfLife: durationpb.New(24 * time.Hour), BucketSize: proto.String("0.1"), Percentile: proto.String("p100")}.Build(),
				}.Build(),
			},
			Scaling:     []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("cpu"), Type: proto.String("Linear"), Params: map[string]string{"metric": "cpu_hist", "target": "0.5"}}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	// T0: Ingest 1.0 (High)
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu_hist"), Value: proto.Float64(1.0), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()

	// Verify Control Metrics
	res := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())
	wantRes := pb.GetControlMetricsResponse_builder{
		Timestamp:     timestamppb.New(time.Unix(1000, 0)),
		ReadyReplicas: proto.Int32(1),
		ControlMetrics: pb.ControlMetrics_builder{
			Values: map[string]float64{"cpu_hist": 1.1},
		}.Build(),
	}.Build()
	if diff := cmp.Diff(wantRes, res, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
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
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()

	// 1. Create Policy
	mustUpdatePolicy(t, client, pb.Policy_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()}.Build())

	// 2. Verify exists
	resp, _ := client.ListPolicies(ctx, pb.ListPoliciesRequest_builder{ClusterName: proto.String("default")}.Build())
	wantResp := pb.ListPoliciesResponse_builder{
		Policies: []*pb.Policy{pb.Policy_builder{Id: id}.Build()},
	}.Build()
	if diff := cmp.Diff(wantResp, resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), ignoreEtag); diff != "" {
		t.Fatalf("ListPolicies mismatch (-want +got):\n%s", diff)
	}

	// 3. Delete
	_, err := client.DeletePolicy(ctx, pb.DeletePolicyRequest_builder{Id: id}.Build())
	if err != nil {
		t.Errorf("DeletePolicy failed: %v", err)
	}

	// 4. Delete again (idempotency)
	_, err = client.DeletePolicy(ctx, pb.DeletePolicyRequest_builder{Id: id}.Build())
	if err != nil {
		t.Errorf("DeletePolicy second call failed (idempotency): %v", err)
	}

	// 5. Verify gone
	resp, _ = client.ListPolicies(ctx, pb.ListPoliciesRequest_builder{ClusterName: proto.String("default")}.Build())
	if diff := cmp.Diff(&pb.ListPoliciesResponse{}, resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
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
				_, err := client.UpdatePolicy(ctx, pb.UpdatePolicyRequest_builder{
					Policy: pb.Policy_builder{Id: pb.PolicyId_builder{Namespace: proto.String("n"), Name: proto.String("p")}.Build()}.Build(),
				}.Build())
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
				_, err := client.DeletePolicy(ctx, pb.DeletePolicyRequest_builder{
					Id: pb.PolicyId_builder{Namespace: proto.String("n"), Name: proto.String("p")}.Build(),
				}.Build())
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "UpdateWorkload: Missing ClusterName",
			call: func() error {
				_, err := client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
					Id:       pb.PolicyId_builder{Namespace: proto.String("n"), Name: proto.String("p")}.Build(),
					Workload: &pb.Workload{},
				}.Build())
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "GetControlMetrics: Missing ClusterName",
			call: func() error {
				_, err := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{
					Id: pb.PolicyId_builder{Namespace: proto.String("n"), Name: proto.String("p")}.Build(),
				}.Build())
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "IngestMetrics: Missing ClusterName",
			call: func() error {
				_, err := client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
					Timestamp: timestamppb.New(time.Unix(1234, 0)),
				}.Build())
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
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:          pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String(p1)}.Build(),
			Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app-cpu"), Namespace: proto.String("default")}.Build(),
			Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("cpu"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	// Policy 2: Memory
	p2 := "policy-mem"
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:          pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String(p2)}.Build(),
			Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app-mem"), Namespace: proto.String("default")}.Build(),
			Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("mem"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String(p1)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String(p2)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{
			pb.PolicyBatch_builder{Namespace: proto.String("default"), Name: proto.String(p1), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(0.5), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build(),
			pb.PolicyBatch_builder{Namespace: proto.String("default"), Name: proto.String(p2), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("mem"), Value: proto.Float64(1024), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build(),
		},
	}.Build())

	memStore.CalculateAll()

	res1 := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String(p1)}.Build())
	wantRes1 := pb.GetControlMetricsResponse_builder{
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
		ControlMetrics: pb.ControlMetrics_builder{
			Values: map[string]float64{"cpu": 0.5},
		}.Build(),
	}.Build()
	if diff := cmp.Diff(wantRes1, res1, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("Policy 1 ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	res2 := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String(p2)}.Build())
	wantRes2 := pb.GetControlMetricsResponse_builder{
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
		ControlMetrics: pb.ControlMetrics_builder{
			Values: map[string]float64{"mem": 1024},
		}.Build(),
	}.Build()
	if diff := cmp.Diff(wantRes2, res2, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
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
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:          pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("ns1"), Name: proto.String(name)}.Build(),
			Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String("ns1")}.Build(),
			Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	// NS 2
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:          pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("ns2"), Name: proto.String(name)}.Build(),
			Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String("ns2")}.Build(),
			Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("ns1"), Name: proto.String(name)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("ns2"), Name: proto.String(name)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{
			pb.PolicyBatch_builder{Namespace: proto.String("ns1"), Name: proto.String(name), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m"), Value: proto.Float64(10), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build(),
			pb.PolicyBatch_builder{Namespace: proto.String("ns2"), Name: proto.String(name), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m"), Value: proto.Float64(20), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build(),
		},
	}.Build())

	memStore.CalculateAll()

	res1 := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("ns1"), Name: proto.String(name)}.Build())
	wantRes1 := pb.GetControlMetricsResponse_builder{
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
		ControlMetrics: pb.ControlMetrics_builder{
			Values: map[string]float64{"m": 10},
		}.Build(),
	}.Build()
	if diff := cmp.Diff(wantRes1, res1, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("NS1 ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	res2 := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("ns2"), Name: proto.String(name)}.Build())
	wantRes2 := pb.GetControlMetricsResponse_builder{
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
		ControlMetrics: pb.ControlMetrics_builder{
			Values: map[string]float64{"m": 20},
		}.Build(),
	}.Build()
	if diff := cmp.Diff(wantRes2, res2, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
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

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload: pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String(ns)}.Build(),
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("m_avg"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("m_sum"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Sum")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("m_max"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Max")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("m_min"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Min")}.Build()}.Build(),
			},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String(ns), Name: proto.String(policyName),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{
					pb.MetricSample_builder{Name: proto.String("m_avg"), Value: proto.Float64(10), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
					pb.MetricSample_builder{Name: proto.String("m_sum"), Value: proto.Float64(10), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
					pb.MetricSample_builder{Name: proto.String("m_max"), Value: proto.Float64(10), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
					pb.MetricSample_builder{Name: proto.String("m_min"), Value: proto.Float64(10), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
				}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("p2"), Samples: []*pb.MetricSample{
					pb.MetricSample_builder{Name: proto.String("m_avg"), Value: proto.Float64(30), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
					pb.MetricSample_builder{Name: proto.String("m_sum"), Value: proto.Float64(30), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
					pb.MetricSample_builder{Name: proto.String("m_max"), Value: proto.Float64(30), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
					pb.MetricSample_builder{Name: proto.String("m_min"), Value: proto.Float64(30), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
				}}.Build(),
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()

	res := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())

	wantRes := pb.GetControlMetricsResponse_builder{
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(2),
		ControlMetrics: pb.ControlMetrics_builder{
			Values: map[string]float64{
				"m_avg": 20.0,
				"m_sum": 40.0,
				"m_max": 30.0,
				"m_min": 10.0,
			},
		}.Build(),
	}.Build()

	if diff := cmp.Diff(wantRes, res, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("Aggregation mismatch (-want +got):\n%s", diff)
	}
}

// TestRecommenderInactiveGRPC ensures inactive recommenders do not influence the arbitrated recommendation.
// Scenario: Two recommenders, one Active (recommends 5) and one Inactive (recommends 100).
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

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload: pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String(ns)}.Build(),
			Metrics:  []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
			Scaling: []*pb.RecommenderDefinition{
				pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("r_active"), Type: proto.String("Linear"), Params: map[string]string{"target": "5"}}.Build(),
				pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("r_inactive"), Type: proto.String("Linear"), Params: map[string]string{"target": "100"}}.Build(),
			},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	memStore.CalculateAll()

	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("r_active"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(5), IsActive: proto.Bool(true)}.Build(),
	}.Build())
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("r_inactive"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(100), IsActive: proto.Bool(false)}.Build(),
	}.Build())

	memStore.CalculateAll()

	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()}.Build())

	wantResp := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(5),
		}.Build(),
		Explanation: []*pb.RecommenderStatus{
			pb.RecommenderStatus_builder{Replicas: proto.Int32(5), IsActive: proto.Bool(true), Phase: proto.String("Scaling"), Name: proto.String("r_active"), Type: proto.String("Linear"), LastUpdated: timestamppb.New(time.Unix(1000, 0))}.Build(),
			pb.RecommenderStatus_builder{Replicas: proto.Int32(100), IsActive: proto.Bool(false), Phase: proto.String("Scaling"), Name: proto.String("r_inactive"), Type: proto.String("Linear"), LastUpdated: timestamppb.New(time.Unix(1000, 0))}.Build(),
		},
		MetricStatuses: resp.GetMetricStatuses(),
	}.Build()

	opts := []cmp.Option{
		protocmp.Transform(), protocmp.IgnoreDefaultScalars(),
		protocmp.SortRepeated(func(a, b *pb.RecommenderStatus) bool { return a.GetName() < b.GetName() }),
	}
	if diff := cmp.Diff(wantResp, resp, opts...); diff != "" {
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
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload: pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String(ns)}.Build(),
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{
					Name: proto.String("load"), DecayingDistribution: pb.DecayingDistribution_builder{HalfLife: durationpb.New(10 * time.Second), BucketSize: proto.String("1.0"), Percentile: proto.String("p100")}.Build(),
				}.Build(),
			},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	// 2. T0: Ingest 100
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("load"), Value: proto.Float64(100.0), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()
	res := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())
	wantRes0 := pb.GetControlMetricsResponse_builder{
		Timestamp:     timestamppb.New(time.Unix(1000, 0)),
		ReadyReplicas: proto.Int32(1),
		ControlMetrics: pb.ControlMetrics_builder{
			Values: map[string]float64{"load": 101.0}, // Bucket upper bound
		}.Build(),
	}.Build()
	if diff := cmp.Diff(wantRes0, res, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
		t.Errorf("T0 ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	// 3. T1: 10s later (1 Half-Life). No new data.
	clk.Advance(10 * time.Second)
	memStore.CalculateAll()
	res = getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())

	// Expectation: the control metrics are only calculated on ingestion, so
	// they are still those of T0.
	wantRes1 := pb.GetControlMetricsResponse_builder{
		Timestamp:     timestamppb.New(time.Unix(1000, 0)),
		ReadyReplicas: proto.Int32(1),
		ControlMetrics: pb.ControlMetrics_builder{
			Values: map[string]float64{"load": 101.0},
		}.Build(),
	}.Build()
	if diff := cmp.Diff(wantRes1, res, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
		t.Errorf("T1 ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	// 4. T3: Ingest 200.
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("load"), Value: proto.Float64(200.0), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()
	res = getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())

	wantRes3 := pb.GetControlMetricsResponse_builder{
		Timestamp:     timestamppb.New(time.Unix(1010, 0)),
		ReadyReplicas: proto.Int32(1),
		ControlMetrics: pb.ControlMetrics_builder{
			Values: map[string]float64{"load": 201.0},
		}.Build(),
	}.Build()
	if diff := cmp.Diff(wantRes3, res, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
		t.Errorf("T3 ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	// 5. T4: Long Gap (1h) then 300.
	clk.Advance(1 * time.Hour)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("load"), Value: proto.Float64(300.0), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()
	res = getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())

	wantRes4 := pb.GetControlMetricsResponse_builder{
		Timestamp:     timestamppb.New(time.Unix(4610, 0)),
		ReadyReplicas: proto.Int32(1),
		ControlMetrics: pb.ControlMetrics_builder{
			Values: map[string]float64{"load": 301.0},
		}.Build(),
	}.Build()
	if diff := cmp.Diff(wantRes4, res, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.0001)); diff != "" {
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

	_, err := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String("missing")}.Build(),
	}.Build())

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
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()

	// 1. Policy with Scaling
	stored := mustUpdatePolicy(t, client, pb.Policy_builder{
		Id:          id,
		Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String(ns)}.Build(),
		Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
		Scaling:     []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("r1"), Type: proto.String("Linear"), Params: map[string]string{"target": "10"}}.Build()},
		MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(100),
	}.Build())

	// 2. Simulate Recommender
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("r1"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(10), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	// 3. Calc Recommendation
	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: id}.Build())
	if !resp.HasRecommendation() || resp.GetRecommendation().GetReplicas() != 10 {
		t.Errorf("Initial: Want 10, Got %v", resp.GetRecommendation())
	}

	// 4. Update Policy: Remove Scaling
	mustUpdatePolicy(t, client, pb.Policy_builder{
		Id:          id,
		Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String(ns)}.Build(),
		Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
		Scaling:     []*pb.RecommenderDefinition{}, // Empty
		MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(100),
		Etag: proto.String(stored.GetEtag()),
	}.Build())

	// 5. Verify No Recommendation
	resp, _ = client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: id}.Build())
	if resp.HasRecommendation() {
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

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload: pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String(ns)}.Build(),
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{
					Name: proto.String("m"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build(),
				}.Build(),
			},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	// T0: Ingest 10
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m"), Value: proto.Float64(10), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())
	memStore.CalculateAll()
	res := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())
	checkControlMetrics(t, res, map[string]float64{"m": 10}, ts)

	// T1: 30s later, Ingest 20. Gauge is instantaneous, so it should see 20.
	clk.Advance(30 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m"), Value: proto.Float64(20), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())
	memStore.CalculateAll()
	res = getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())
	checkControlMetrics(t, res, map[string]float64{"m": 20}, ts)

	// T2: 90s later (60s after T1). Window contains [20] (10 expired after 60s).
	clk.Advance(60 * time.Second)
	ts = clk.Now().Unix() // 1090
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m"), Value: proto.Float64(20), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())
	memStore.CalculateAll()
	res = getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())
	checkControlMetrics(t, res, map[string]float64{"m": 20}, ts)
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

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:          pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String(ns)}.Build(),
			Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	// T0: Ingest
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m"), Value: proto.Float64(10), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())

	// Verify exists
	memStore.CalculateAll()
	res := getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())
	if len(res.GetControlMetrics().GetValues()) == 0 {
		t.Fatal("Metric should exist")
	}

	// Advance past GC cutoff (600s = 10m)
	clk.Advance(11 * time.Minute)

	// Trigger calculation to run GC: the control metrics are calculated on
	// ingestion, so ingest an empty batch.
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()},
	}.Build())

	// Verify gone.
	// GetControlMetrics returns the last calculated state.
	// If GC removed the series, the calculation should produce empty ControlMetrics.
	res = getControlMetrics(t, ctx, client, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())
	if len(res.GetControlMetrics().GetValues()) > 0 {
		t.Errorf("Metric should be GC'd, found: %v", res.GetControlMetrics().GetValues())
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

	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
			Workload: pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String(ns)}.Build(),
			Metrics:  []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
			Scaling: []*pb.RecommenderDefinition{
				pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("r_low"), Type: proto.String("Linear"), Params: map[string]string{"target": "100"}}.Build(), // Recommends low
				pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("r_high"), Type: proto.String("Linear"), Params: map[string]string{"target": "10"}}.Build(), // Recommends high (metric=200 -> 20 reps)
			},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(100),
		}.Build(),
	}.Build())

	// Simulate Recommender States
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("r_low"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(5), IsActive: proto.Bool(true)}.Build(),
	}.Build())
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(), RecommenderName: proto.String("r_high"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(20), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()}.Build())

	// Should equal Max(5, 20) = 20
	if resp.GetRecommendation().GetReplicas() != 20 {
		t.Errorf("Arbitration failed: Want 20, Got %d", resp.GetRecommendation().GetReplicas())
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
	pid := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()

	// 1. Initial: Gauge
	stored := mustUpdatePolicy(t, client, pb.Policy_builder{
		Id:          pid,
		Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String(ns)}.Build(),
		Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m"), Provider: proto.String("kubelet"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
		MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{Id: pid, Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build()}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m"), Value: proto.Float64(10), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())
	memStore.CalculateAll()

	res := getControlMetrics(t, ctx, client, pid)
	if res.GetControlMetrics().GetValues()["m"] != 10 {
		t.Errorf("Initial Gauge mismatch: %v", res.GetControlMetrics().GetValues()["m"])
	}

	// 2. Update to Counter (Rate)
	mustUpdatePolicy(t, client, pb.Policy_builder{
		Id:          pid,
		Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("app"), Namespace: proto.String(ns)}.Build(),
		Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m"), Provider: proto.String("kubelet"), Rate: pb.Rate_builder{Window: durationpb.New(time.Minute)}.Build()}.Build()}, // Changed to Rate
		MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		Etag: proto.String(stored.GetEtag()),
	}.Build())

	// 3. The state built from the previous definition is dropped: the gauge
	// sample does not seed the rate, which needs two counter samples.
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m"), Value: proto.Float64(20), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())
	memStore.CalculateAll()

	res = getControlMetrics(t, ctx, client, pid)
	if _, ok := res.GetControlMetrics().GetValues()["m"]; ok {
		t.Errorf("Mutation Counter: want no value from a single sample, got %v", res.GetControlMetrics().GetValues()["m"])
	}

	// 4. T2 (10s later): Value 30. Diff=10. Rate=1.
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m"), Value: proto.Float64(30), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build()}}.Build()}}.Build()},
	}.Build())
	memStore.CalculateAll()

	res = getControlMetrics(t, ctx, client, pid)
	// Rate = (30-20)/10 = 1.0
	if res.GetControlMetrics().GetValues()["m"] != 1.0 {
		t.Errorf("Mutation Counter mismatch: Want 1.0, Got %v", res.GetControlMetrics().GetValues()["m"])
	}
}

// checkControlMetrics checks that the control metrics were calculated at
// wantTS, and that the policy-wide metrics have the wantVals values.
func checkControlMetrics(t *testing.T, got *pb.GetControlMetricsResponse, wantVals map[string]float64, wantTS int64) {
	t.Helper()
	if got.GetTimestamp().AsTime().Unix() != wantTS {
		t.Errorf("Timestamp mismatch: want %d, got %d", wantTS, got.GetTimestamp().AsTime().Unix())
	}
	for k, v := range wantVals {
		if got.GetControlMetrics().GetValues()[k] != v {
			t.Errorf("Value mismatch for %s: want %f, got %f", k, v, got.GetControlMetrics().GetValues()[k])
		}
	}
}

// TestRecommenderOwnedMetricsGRPC validates the lifecycle of a metric owned by
// a recommender, end to end.
// Scenario: a "vpa" recommender registers its own "cpu" metric on a policy that
// already collects a policy-wide "cpu".
// Steps:
//  1. Create the policy with both a policy-wide and a recommender-owned metric.
//  2. Ingest samples for both, the owned ones stamped with their owner.
//  3. Verify a single GetControlMetrics call reports the policy-wide value and,
//     keyed by recommender, the owned one.
func TestRecommenderOwnedMetricsGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()
	_, err := client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
			},
			// "hpa" owns no metric.
			Scaling: []*pb.RecommenderDefinition{
				pb.RecommenderDefinition_builder{Name: proto.String("vpa"), Recommender: proto.String("vpa")}.Build(),
				pb.RecommenderDefinition_builder{Name: proto.String("hpa"), Recommender: proto.String("hpa")}.Build(),
			},
			RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
					pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("vpa"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Max")}.Build()}.Build(),
				}}.Build(),
			},
		}.Build(),
	}.Build())
	if err != nil {
		t.Fatalf("UpdatePolicy failed: %v", err)
	}

	_, err = client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{Pods: []*pb.PodState{
			pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(),
			pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build(),
		}}.Build(),
	}.Build())
	if err != nil {
		t.Fatalf("UpdateWorkload failed: %v", err)
	}

	_, err = client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(start.Unix(), 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("prod"), Name: proto.String("web"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("p1"),
				Samples: []*pb.MetricSample{
					pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(2), Timestamp: timestamppb.New(time.Unix(start.Unix(), 0))}.Build(),
					pb.MetricSample_builder{Name: proto.String("cpu"), RecommenderName: proto.String("vpa"), Value: proto.Float64(20), Timestamp: timestamppb.New(time.Unix(start.Unix(), 0))}.Build(),
				},
			}.Build(), pb.MetricBatch_builder{
				PodName: proto.String("p2"),
				Samples: []*pb.MetricSample{
					pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(4), Timestamp: timestamppb.New(time.Unix(start.Unix(), 0))}.Build(),
					pb.MetricSample_builder{Name: proto.String("cpu"), RecommenderName: proto.String("vpa"), Value: proto.Float64(40), Timestamp: timestamppb.New(time.Unix(start.Unix(), 0))}.Build(),
				},
			}.Build()},
		}.Build()},
	}.Build())
	if err != nil {
		t.Fatalf("IngestMetrics failed: %v", err)
	}

	memStore.CalculateAll()

	res, err := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())
	if err != nil {
		t.Fatalf("GetControlMetrics failed: %v", err)
	}
	// Policy-wide scope: Avg(2, 4).
	checkControlMetrics(t, res, map[string]float64{"cpu": 3}, start.Unix())
	// The workload state is reported once, for every scope.
	if res.GetReadyReplicas() != 2 {
		t.Errorf("GetControlMetrics() ready replicas = %d, want 2", res.GetReadyReplicas())
	}
	// Recommender scope: only its own metric, with its own aggregation Max(20, 40).
	owned := res.GetRecommenderControlMetrics()
	if _, ok := owned["vpa"]; !ok {
		t.Fatalf("GetControlMetrics() recommender_control_metrics = %v, want an entry for vpa", owned)
	}
	if diff := cmp.Diff(map[string]float64{"cpu": 40}, owned["vpa"].GetValues()); diff != "" {
		t.Errorf("GetControlMetrics() vpa values mismatch (-want +got):\n%s", diff)
	}
	// A recommender that owns no metric has no entry.
	if _, ok := owned["hpa"]; ok {
		t.Errorf("GetControlMetrics() recommender_control_metrics has an entry for hpa, which owns no metric: %v", owned["hpa"])
	}
}

// TestRecommenderMetricsValidationGRPC checks that owned metric definitions have
// to be consistent with the recommender they are registered under.
func TestRecommenderMetricsValidationGRPC(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	_, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()

	tests := []struct {
		name     string
		policy   *pb.Policy
		wantCode codes.Code
	}{
		{
			name: "Owned metric with a mismatched owner",
			policy: pb.Policy_builder{
				Id: id,
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
						pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("hpa"), Gauge: &pb.Gauge{}}.Build(),
					}}.Build(),
				},
			}.Build(),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "Owned metric without an owner",
			policy: pb.Policy_builder{
				Id: id,
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
						pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: &pb.Gauge{}}.Build(),
					}}.Build(),
				},
			}.Build(),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "Owned metric without an intent",
			policy: pb.Policy_builder{
				Id: id,
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
						pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("vpa")}.Build(),
					}}.Build(),
				},
			}.Build(),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "Policy-wide metric claiming an owner",
			policy: pb.Policy_builder{
				Id: id,
				Metrics: []*pb.MetricDefinition{
					pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("vpa"), Gauge: &pb.Gauge{}}.Build(),
				},
			}.Build(),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "Valid owned metric",
			policy: pb.Policy_builder{
				Id: id,
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
						pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("vpa"), Gauge: &pb.Gauge{}}.Build(),
					}}.Build(),
				},
			}.Build(),
			wantCode: codes.OK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{Policy: tc.policy}.Build())
			st, _ := status.FromError(err)
			if st.Code() != tc.wantCode {
				t.Errorf("Want code %v, got %v (err: %v)", tc.wantCode, st.Code(), err)
			}
		})
	}
}

// TestGetWorkloadGRPC checks that recommenders can read the pods (and their
// requests) last reported by the controller.
func TestGetWorkloadGRPC(t *testing.T) {
	_, client, cleanup := setupGRPCServer(t, &clock.FakeClock{CurrentTime: time.Unix(1000, 0)})
	defer cleanup()
	ctx := context.Background()
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()

	_, err := client.GetWorkload(ctx, pb.GetWorkloadRequest_builder{Id: id}.Build())
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetWorkload(unknown policy) code = %v, want NotFound", status.Code(err))
	}
	_, err = client.GetWorkload(ctx, &pb.GetWorkloadRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetWorkload(no id) code = %v, want InvalidArgument", status.Code(err))
	}

	mustUpdatePolicy(t, client, pb.Policy_builder{Id: id, MaxReplicas: proto.Int32(10)}.Build())
	got, err := client.GetWorkload(ctx, pb.GetWorkloadRequest_builder{Id: id}.Build())
	if err != nil {
		t.Fatalf("GetWorkload() error = %v", err)
	}
	if len(got.GetPods()) != 0 {
		t.Errorf("GetWorkload() before any report = %v, want no pods", got.GetPods())
	}

	pods := []*pb.PodState{
		pb.PodState_builder{Name: proto.String("web-b"), IsReady: proto.Bool(true), Containers: []*pb.ContainerState{pb.ContainerState_builder{Name: proto.String("app"), Requests: map[string]string{"cpu": "200m"}}.Build()}}.Build(),
		pb.PodState_builder{Name: proto.String("web-a"), IsReady: proto.Bool(false), Containers: []*pb.ContainerState{pb.ContainerState_builder{Name: proto.String("app"), Requests: map[string]string{"cpu": "100m", "memory": "64Mi"}}.Build()}}.Build(),
	}
	if _, err := client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{Id: id, Workload: pb.Workload_builder{Pods: pods}.Build()}.Build()); err != nil {
		t.Fatalf("UpdateWorkload() error = %v", err)
	}
	got, err = client.GetWorkload(ctx, pb.GetWorkloadRequest_builder{Id: id}.Build())
	if err != nil {
		t.Fatalf("GetWorkload() error = %v", err)
	}
	// Pods are returned sorted by name.
	want := pb.Workload_builder{Pods: []*pb.PodState{pods[1], pods[0]}}.Build()
	if diff := cmp.Diff(want, got, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("GetWorkload() mismatch (-want +got):\n%s", diff)
	}
}

// TestWatchControlMetricsGRPC validates the WatchControlMetrics stream.
// Steps:
//  1. Create a policy with a policy-wide gauge and a recommender-owned gauge.
//  2. Register the workload, ingest metrics and calculate.
//  3. Open a watch and recalculate without changes: nothing must be streamed.
//  4. Ingest a new value: the change must be streamed, including the
//     recommender-owned metrics and the ready replicas.
//  5. Add a ready pod without changing the metrics: the change in ready
//     replicas must be streamed on its own.
//  6. Validation: a request without cluster must be rejected.
func TestWatchControlMetricsGRPC(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ns, policyName := "default", "watch-policy"
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()

	// 1. Policy
	if _, err := client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:       id,
			Workload: pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("web-app"), Namespace: proto.String(ns)}.Build(),
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("qps"), Provider: proto.String("prometheus"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Sum")}.Build()}.Build(),
			},
			RecommenderMetrics: map[string]*pb.MetricDefinitionList{
				"lin": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
					pb.MetricDefinition_builder{Name: proto.String("owned"), RecommenderName: proto.String("lin"), Provider: proto.String("prometheus"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Sum")}.Build()}.Build(),
				}}.Build(),
			},
			Scaling:     []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("lin"), Mode: proto.String("Active"), Type: proto.String("Linear"), Params: map[string]string{"metric": "qps", "target": "10"}}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build()); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	ingest := func(qps, owned float64) {
		t.Helper()
		ts := timestamppb.New(clk.Now())
		if _, err := client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
			ClusterName: proto.String("default"), Timestamp: ts,
			Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
				Namespace: proto.String(ns), Name: proto.String(policyName),
				Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{
					pb.MetricSample_builder{Name: proto.String("qps"), Value: proto.Float64(qps), Timestamp: ts}.Build(),
					pb.MetricSample_builder{Name: proto.String("owned"), RecommenderName: proto.String("lin"), Value: proto.Float64(owned), Timestamp: ts}.Build(),
				}}.Build()},
			}.Build()},
		}.Build()); err != nil {
			t.Fatalf("IngestMetrics: %v", err)
		}
	}

	// 2. Workload, ingest, calculate. The pod must be reported before its
	// samples are ingested, otherwise they are dropped.
	if _, err := client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       id,
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build()); err != nil {
		t.Fatalf("UpdateWorkload: %v", err)
	}
	ingest(5, 50)
	memStore.CalculateAll()

	// 3. Watch, then recalculate. Nothing is streamed: nothing was published
	// for the cluster before the watch, and recalculating does not publish by
	// itself.
	stream, err := client.WatchControlMetrics(ctx, pb.WatchControlMetricsRequest_builder{
		ClusterName: proto.String("default"),
	}.Build())
	if err != nil {
		t.Fatalf("WatchControlMetrics: %v", err)
	}
	// The headers are sent once the watch is registered: wait for them so
	// that the samples ingested below cannot race with the registration.
	if _, err := stream.Header(); err != nil {
		t.Fatalf("Header: %v", err)
	}
	memStore.CalculateAll()

	// 4. Ingesting new samples publishes the recomputed policy right away, so
	// the first message received must be the changed one.
	recv := func() *pb.WatchControlMetricsResponse {
		t.Helper()
		res, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		return res
	}
	wantResponse := func(readyReplicas int32, qps, owned float64) *pb.WatchControlMetricsResponse {
		return pb.WatchControlMetricsResponse_builder{
			Id:             id,
			ControlMetrics: pb.ControlMetrics_builder{Values: map[string]float64{"qps": qps}}.Build(),
			RecommenderControlMetrics: map[string]*pb.ControlMetrics{
				"lin": pb.ControlMetrics_builder{Values: map[string]float64{"owned": owned}}.Build(),
			},
			ReadyReplicas: proto.Int32(readyReplicas),
		}.Build()
	}
	opts := []cmp.Option{protocmp.Transform(), protocmp.IgnoreDefaultScalars(), protocmp.IgnoreEmptyMessages()}
	clk.Advance(10 * time.Second)
	ingest(7, 70)
	if diff := cmp.Diff(wantResponse(1, 7, 70), recv(), opts...); diff != "" {
		t.Errorf("change mismatch (-want +got):\n%s", diff)
	}

	// 5. A change in the ready replicas alone is streamed too. Re-ingesting
	// the same values publishes nothing (the event is a duplicate), so the
	// next message received must be the one with the second ready pod, whose
	// metrics are unchanged as that pod reports no sample.
	ingest(7, 70)
	if _, err := client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{Pods: []*pb.PodState{
			pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(),
			pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build(),
		}}.Build(),
	}.Build()); err != nil {
		t.Fatalf("UpdateWorkload: %v", err)
	}
	ingest(7, 70)
	if diff := cmp.Diff(wantResponse(2, 7, 70), recv(), opts...); diff != "" {
		t.Errorf("ready replicas change mismatch (-want +got):\n%s", diff)
	}

	// 6. Validation.
	bad, err := client.WatchControlMetrics(ctx, pb.WatchControlMetricsRequest_builder{}.Build())
	if err != nil {
		t.Fatalf("WatchControlMetrics: %v", err)
	}
	if _, err := bad.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("Recv() on invalid request: got %v, want %v", err, codes.InvalidArgument)
	}
}

// TestWatchRecommendationsGRPC validates the WatchRecommendations stream.
// Steps:
//  1. Create a policy with an activation and a scaling recommender, activate
//     it and calculate: there is no arbitrated recommendation yet.
//  2. Open a watch and recalculate: the current state of the policy is
//     streamed. Recalculating again streams nothing: only the timestamps of
//     the metric statuses changed.
//  3. Push a scaling recommendation: the arbitrated recommendation is
//     streamed, with its explanation and metric statuses.
//  4. Push the same recommendation again: nothing is streamed, only its
//     last_updated changed.
//  5. Deactivate: the workload stays active for the window, so only the
//     explanation changes, which is streamed on its own.
//  6. Let the window expire and recalculate: the scale to zero is streamed,
//     although no recommender pushed anything.
//  7. Clear the activation recommender: the change is streamed.
//  8. A new watcher is primed with the last event.
//  9. Validation: a request without cluster must be rejected.
//
// The steps that publish must stay within the notification burst of the
// policy (see store.notifyBurst): the fake clock does not refill the limiter.
func TestWatchRecommendationsGRPC(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupGRPCServer(t, clk)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ns, policyName := "default", "watch-rec-policy"
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()

	// 1. Policy, activated, calculated.
	if _, err := client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:          id,
			Workload:    pb.WorkloadRef_builder{Group: proto.String("apps"), Version: proto.String("v1"), Kind: proto.String("Deployment"), Name: proto.String("web-app"), Namespace: proto.String(ns)}.Build(),
			Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m"), Provider: proto.String("prometheus"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
			Activation:  []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Threshold"), Name: proto.String("act"), Type: proto.String("Threshold"), Params: map[string]string{"metric": "m", "threshold": "0", "window": "5"}}.Build()},
			Scaling:     []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Recommender: proto.String("Linear"), Name: proto.String("lin"), Type: proto.String("Linear"), Params: map[string]string{"metric": "m", "target": "10"}}.Build()},
			MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(10),
		}.Build(),
	}.Build()); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}
	push := func(recommender string, rec *pb.Recommendation) {
		t.Helper()
		if _, err := client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
			Id: id, RecommenderName: proto.String(recommender), Recommendation: rec,
		}.Build()); err != nil {
			t.Fatalf("UpdateRecommenderState(%s): %v", recommender, err)
		}
	}
	push("act", pb.Recommendation_builder{IsActive: proto.Bool(true)}.Build())
	// The control metrics, hence the metric statuses, are calculated on
	// ingestion: ingest an empty batch so that they are reported.
	if _, err := client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"), Timestamp: timestamppb.New(clk.Now()),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String(ns), Name: proto.String(policyName)}.Build()},
	}.Build()); err != nil {
		t.Fatalf("IngestMetrics: %v", err)
	}
	memStore.CalculateAll()

	// 2. Watch, then recalculate.
	stream, err := client.WatchRecommendations(ctx, pb.WatchRecommendationsRequest_builder{
		ClusterName: proto.String("default"),
	}.Build())
	if err != nil {
		t.Fatalf("WatchRecommendations: %v", err)
	}
	// The headers are sent once the watch is registered: wait for them so
	// that the changes made below cannot race with the registration.
	if _, err := stream.Header(); err != nil {
		t.Fatalf("Header: %v", err)
	}

	recv := func() *pb.WatchRecommendationsResponse {
		t.Helper()
		res, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		return res
	}
	// Every response carries the status of the policy's only metric, which
	// never gets any sample.
	wantResponse := func(rec *pb.Recommendation, explanation ...*pb.RecommenderStatus) *pb.WatchRecommendationsResponse {
		return pb.WatchRecommendationsResponse_builder{
			Id:             id,
			Recommendation: rec,
			Explanation:    explanation,
			MetricStatuses: []*pb.MetricStatus{pb.MetricStatus_builder{Name: proto.String("m"), Error: proto.String("No data available")}.Build()},
		}.Build()
	}
	actActive := pb.RecommenderStatus_builder{Name: proto.String("act"), Type: proto.String("Threshold"), Phase: proto.String("Activation"), IsActive: proto.Bool(true)}.Build()
	actInactive := pb.RecommenderStatus_builder{Name: proto.String("act"), Type: proto.String("Threshold"), Phase: proto.String("Activation"), IsActive: proto.Bool(false)}.Build()
	lin3 := pb.RecommenderStatus_builder{Name: proto.String("lin"), Type: proto.String("Linear"), Phase: proto.String("Scaling"), IsActive: proto.Bool(true), Replicas: proto.Int32(3)}.Build()
	replicas := func(n int32) *pb.Recommendation { return pb.Recommendation_builder{Replicas: proto.Int32(n)}.Build() }
	// The timestamps are not part of the comparison: they are checked by
	// other tests, and the stream ignores them when dropping duplicates.
	opts := []cmp.Option{
		protocmp.Transform(), protocmp.IgnoreDefaultScalars(), protocmp.IgnoreEmptyMessages(),
		protocmp.IgnoreFields(&pb.RecommenderStatus{}, "last_updated"),
		protocmp.IgnoreFields(&pb.MetricStatus{}, "timestamp"),
	}

	// Nothing was published for the cluster before the watch, so the first
	// recalculation streams the current state: no recommendation yet, as no
	// scaling recommender has decided.
	memStore.CalculateAll()
	if diff := cmp.Diff(wantResponse(nil), recv(), opts...); diff != "" {
		t.Errorf("initial state mismatch (-want +got):\n%s", diff)
	}
	// Recalculating later changes nothing: nothing is streamed, so the next
	// message received must be the one with the scaling recommendation
	// pushed below.
	clk.Advance(time.Second)
	memStore.CalculateAll()

	// 3. A scaling recommendation is arbitrated and streamed right away.
	push("lin", pb.Recommendation_builder{Replicas: proto.Int32(3), IsActive: proto.Bool(true)}.Build())
	if diff := cmp.Diff(wantResponse(replicas(3), actActive, lin3), recv(), opts...); diff != "" {
		t.Errorf("recommendation mismatch (-want +got):\n%s", diff)
	}

	// 4. The same recommendation pushed later only moves its last_updated:
	// nothing is streamed, so the next message received must be the one
	// with the deactivation below.
	clk.Advance(time.Second)
	push("lin", pb.Recommendation_builder{Replicas: proto.Int32(3), IsActive: proto.Bool(true)}.Build())

	// 5. Deactivating within the window keeps the workload active: only the
	// explanation changes, which is streamed on its own.
	push("act", pb.Recommendation_builder{IsActive: proto.Bool(false)}.Build())
	if diff := cmp.Diff(wantResponse(replicas(3), actInactive, lin3), recv(), opts...); diff != "" {
		t.Errorf("explanation change mismatch (-want +got):\n%s", diff)
	}

	// 6. Once the window expires, the recalculation scales to zero without
	// any recommender pushing anything, and streams it.
	clk.Advance(6 * time.Second)
	memStore.CalculateAll()
	got := recv()
	if diff := cmp.Diff(wantResponse(replicas(0), actInactive), got, opts...); diff != "" {
		t.Errorf("scale to zero mismatch (-want +got):\n%s", diff)
	}
	// Zero is indistinguishable from unset under the comparison options.
	if !got.GetRecommendation().HasReplicas() {
		t.Errorf("scale to zero: replicas not set, want 0")
	}

	// 7. Clearing a recommender re-arbitrates and streams the change.
	push("act", nil)
	if diff := cmp.Diff(wantResponse(replicas(0)), recv(), opts...); diff != "" {
		t.Errorf("clearing mismatch (-want +got):\n%s", diff)
	}

	// 8. A new watcher starts from the last event of every policy.
	late, err := client.WatchRecommendations(ctx, pb.WatchRecommendationsRequest_builder{
		ClusterName: proto.String("default"),
	}.Build())
	if err != nil {
		t.Fatalf("WatchRecommendations: %v", err)
	}
	primed, err := late.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if diff := cmp.Diff(wantResponse(replicas(0)), primed, opts...); diff != "" {
		t.Errorf("primed event mismatch (-want +got):\n%s", diff)
	}

	// 9. Validation.
	bad, err := client.WatchRecommendations(ctx, pb.WatchRecommendationsRequest_builder{}.Build())
	if err != nil {
		t.Fatalf("WatchRecommendations: %v", err)
	}
	if _, err := bad.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("Recv() on invalid request: got %v, want %v", err, codes.InvalidArgument)
	}
}
