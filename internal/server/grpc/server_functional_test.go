package grpc_test

import (
	"context"
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

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	servergrpc "github.com/gke-labs/extensible-workload-autoscaler/internal/server/grpc"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store"
)

const bufSizeFunctional = 1024 * 1024

func setupFunctionalGRPCServer(t *testing.T, c clock.Clock) (*store.MemoryStore, pb.XASServerClient, func()) {
	lis := bufconn.Listen(bufSizeFunctional)
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

// --- UpdatePolicy ---

func TestUpdatePolicy_InvalidID(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	tests := []struct {
		name string
		id   *pb.PolicyId
	}{
		{"Missing Cluster", pb.PolicyId_builder{Namespace: proto.String("ns"), Name: proto.String("name")}.Build()},
		{"Missing Namespace", pb.PolicyId_builder{ClusterName: proto.String("c1"), Name: proto.String("name")}.Build()},
		{"Missing Name", pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns")}.Build()},
		{"Empty ID", &pb.PolicyId{}},
		{"Nil ID", nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.UpdatePolicy(ctx, pb.UpdatePolicyRequest_builder{
				Policy: pb.Policy_builder{Id: tc.id}.Build(),
			}.Build())
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("Expected InvalidArgument, got %v", err)
			}
		})
	}
}

func TestUpdatePolicy_Validation(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	req := pb.UpdatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{
					Name: proto.String("m1"),
				}.Build(),
			},
		}.Build(),
	}.Build()

	_, err := client.UpdatePolicy(ctx, req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument for missing intent, got %v", err)
	}
}

func TestUpdatePolicy_CrossCluster(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id1 := pb.PolicyId_builder{ClusterName: proto.String("cluster-1"), Namespace: proto.String("default"), Name: proto.String("policy")}.Build()
	id2 := pb.PolicyId_builder{ClusterName: proto.String("cluster-2"), Namespace: proto.String("default"), Name: proto.String("policy")}.Build()

	p1 := pb.Policy_builder{Id: id1, MinReplicas: proto.Int32(1)}.Build()
	p2 := pb.Policy_builder{Id: id2, MinReplicas: proto.Int32(5)}.Build()

	mustUpdatePolicy(t, client, p1)
	mustUpdatePolicy(t, client, p2)

	resp1, _ := client.ListPolicies(ctx, pb.ListPoliciesRequest_builder{ClusterName: proto.String("cluster-1")}.Build())
	if diff := cmp.Diff(pb.ListPoliciesResponse_builder{Policies: []*pb.Policy{p1}}.Build(), resp1, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), ignoreEtag); diff != "" {
		t.Errorf("Cluster 1 mismatch (-want +got):\n%s", diff)
	}

	resp2, _ := client.ListPolicies(ctx, pb.ListPoliciesRequest_builder{ClusterName: proto.String("cluster-2")}.Build())
	if diff := cmp.Diff(pb.ListPoliciesResponse_builder{Policies: []*pb.Policy{p2}}.Build(), resp2, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), ignoreEtag); diff != "" {
		t.Errorf("Cluster 2 mismatch (-want +got):\n%s", diff)
	}
}

func TestUpdatePolicy_FullUpdate(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	p1 := pb.Policy_builder{
		Id:          id,
		MinReplicas: proto.Int32(1),
		MaxReplicas: proto.Int32(10),
		Workload:    pb.WorkloadRef_builder{Name: proto.String("app1")}.Build(),
		Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
	}.Build()

	stored := mustUpdatePolicy(t, client, p1)

	p2 := pb.Policy_builder{
		Id:          id,
		MinReplicas: proto.Int32(2),
		MaxReplicas: proto.Int32(20),
		Workload:    pb.WorkloadRef_builder{Name: proto.String("app2")}.Build(),
		Metrics:     []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
		Etag:        proto.String(stored.GetEtag()),
	}.Build()
	mustUpdatePolicy(t, client, p2)

	resp, _ := client.ListPolicies(ctx, pb.ListPoliciesRequest_builder{ClusterName: proto.String("c1")}.Build())
	if diff := cmp.Diff(pb.ListPoliciesResponse_builder{Policies: []*pb.Policy{p2}}.Build(), resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), ignoreEtag); diff != "" {
		t.Errorf("Policy update mismatch (-want +got):\n%s", diff)
	}
}

// --- GetPolicy & CreatePolicy ---

func TestGetPolicy_GRPC(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.GetPolicy(ctx, pb.GetPolicyRequest_builder{Id: &pb.PolicyId{}}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetPolicy(empty ID) code = %v, want InvalidArgument", status.Code(err))
	}

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	_, err = client.GetPolicy(ctx, pb.GetPolicyRequest_builder{Id: id}.Build())
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetPolicy(missing) code = %v, want NotFound", status.Code(err))
	}

	created, err := client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{Id: id, MinReplicas: proto.Int32(2), MaxReplicas: proto.Int32(8)}.Build(),
	}.Build())
	if err != nil {
		t.Fatalf("CreatePolicy() error = %v", err)
	}

	got, err := client.GetPolicy(ctx, pb.GetPolicyRequest_builder{Id: id}.Build())
	if err != nil {
		t.Fatalf("GetPolicy() error = %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("GetPolicy() mismatch (-want +got):\n%s", diff)
	}
}

func TestCreatePolicy_GRPC(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{Policy: pb.Policy_builder{Id: &pb.PolicyId{}}.Build()}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreatePolicy(empty ID) code = %v, want InvalidArgument", status.Code(err))
	}

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	_, err = client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{Id: id, Etag: proto.String("some-etag")}.Build(),
	}.Build())
	if status.Code(err) != codes.NotFound {
		t.Errorf("CreatePolicy(with etag) code = %v, want NotFound", status.Code(err))
	}

	created, err := client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{Id: id, MaxReplicas: proto.Int32(10)}.Build(),
	}.Build())
	if err != nil {
		t.Fatalf("CreatePolicy() error = %v", err)
	}
	if created.GetEtag() == "" {
		t.Error("CreatePolicy() returned an empty ETag")
	}

	_, err = client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{Id: id, MaxReplicas: proto.Int32(20)}.Build(),
	}.Build())
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("CreatePolicy(duplicate) code = %v, want AlreadyExists", status.Code(err))
	}
}

// --- DeletePolicy ---

func TestDeletePolicy_InvalidID(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.DeletePolicy(ctx, pb.DeletePolicyRequest_builder{Id: &pb.PolicyId{}}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestDeletePolicy_Effective(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{Policy: pb.Policy_builder{Id: id}.Build()}.Build())

	client.DeletePolicy(ctx, pb.DeletePolicyRequest_builder{Id: id}.Build())

	resp, _ := client.ListPolicies(ctx, pb.ListPoliciesRequest_builder{ClusterName: proto.String("c1")}.Build())
	if diff := cmp.Diff(&pb.ListPoliciesResponse{}, resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("Expected empty response after delete (-want +got):\n%s", diff)
	}
}

func TestDeletePolicy_Idempotent(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	_, err := client.DeletePolicy(ctx, pb.DeletePolicyRequest_builder{Id: id}.Build())
	if err != nil {
		t.Errorf("First delete failed: %v", err)
	}
	_, err = client.DeletePolicy(ctx, pb.DeletePolicyRequest_builder{Id: id}.Build())
	if err != nil {
		t.Errorf("Second delete failed: %v", err)
	}
}

// --- ListPolicies ---

func TestListPolicies_InvalidCluster(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.ListPolicies(ctx, pb.ListPoliciesRequest_builder{ClusterName: proto.String("")}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestListPolicies_Filtering(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	p1 := pb.Policy_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()}.Build()
	p2 := pb.Policy_builder{Id: pb.PolicyId_builder{ClusterName: proto.String("c2"), Namespace: proto.String("ns"), Name: proto.String("p2")}.Build()}.Build()

	mustUpdatePolicy(t, client, p1)
	mustUpdatePolicy(t, client, p2)

	resp, _ := client.ListPolicies(ctx, pb.ListPoliciesRequest_builder{ClusterName: proto.String("c1")}.Build())
	if diff := cmp.Diff(pb.ListPoliciesResponse_builder{Policies: []*pb.Policy{p1}}.Build(), resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), ignoreEtag); diff != "" {
		t.Errorf("ListPolicies filtering mismatch (-want +got):\n%s", diff)
	}
}

func TestListPolicies_Empty(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	resp, _ := client.ListPolicies(ctx, pb.ListPoliciesRequest_builder{ClusterName: proto.String("non-existent")}.Build())
	if diff := cmp.Diff(&pb.ListPoliciesResponse{}, resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("Expected empty response for non-existent cluster (-want +got):\n%s", diff)
	}
}

// --- UpdateWorkload ---

func TestUpdateWorkload_InvalidID(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("")}.Build(),
		Workload: &pb.Workload{},
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestUpdateWorkload_NotFound(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("non-existent")}.Build(),
		Workload: &pb.Workload{},
	}.Build())
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", err)
	}
}

func TestUpdateWorkload_InvalidPodState(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{Policy: pb.Policy_builder{Id: id}.Build()}.Build())

	_, err := client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("")}.Build()}, // Missing Name
		}.Build(),
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument for missing pod name, got %v", err)
	}
}

// --- GetControlMetrics ---

func TestGetControlMetrics_InvalidID(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: &pb.PolicyId{}}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestGetControlMetrics_NotFound(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("non-existent")}.Build(),
	}.Build())
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", err)
	}
}

func TestGetControlMetrics_Aggregation(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("g_avg"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("c_sum"), Rate: pb.Rate_builder{Window: durationpb.New(time.Minute), Aggregation: proto.String("Sum")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("h_p99"), Distribution: pb.Distribution_builder{Percentile: proto.String("p99"), Aggregation: proto.String("Max")}.Build()}.Build(),
			},
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{
				pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(),
				pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build(),
			},
		}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	// T0 for counter
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("c_sum"), Value: proto.Float64(0)}.Build(), pb.MetricSample_builder{Name: proto.String("h_p99"), Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"1.0": 0, "+Inf": 0}}.Build()}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("p2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("c_sum"), Value: proto.Float64(0)}.Build(), pb.MetricSample_builder{Name: proto.String("h_p99"), Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"1.0": 0, "+Inf": 0}}.Build()}.Build()}}.Build(),
			},
		}.Build()},
	}.Build())

	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{
					pb.MetricSample_builder{Name: proto.String("g_avg"), Value: proto.Float64(10)}.Build(),
					pb.MetricSample_builder{Name: proto.String("c_sum"), Value: proto.Float64(10)}.Build(), // Diff=10, Rate=1.0
					pb.MetricSample_builder{Name: proto.String("h_p99"), Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"1.0": 99, "+Inf": 100}}.Build()}.Build(),
				}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("p2"), Samples: []*pb.MetricSample{
					pb.MetricSample_builder{Name: proto.String("g_avg"), Value: proto.Float64(30)}.Build(),
					pb.MetricSample_builder{Name: proto.String("c_sum"), Value: proto.Float64(20)}.Build(), // Diff=20, Rate=2.0
					pb.MetricSample_builder{Name: proto.String("h_p99"), Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"1.0": 99, "+Inf": 100}}.Build()}.Build(),
				}}.Build(),
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{
			"g_avg": 20.0, // (10+30)/2
			"c_sum": 3.0,  // (1.0+2.0)
			"h_p99": 1.0,  // P99 in [0, 1.0]
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(2),
	}.Build()
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_PodScope(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu_global"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build(), Scope: proto.String("Global")}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("cpu_pod"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build(), Scope: proto.String("Pod")}.Build(),
			},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{
				pb.PodState_builder{Name: proto.String("pod1"), IsReady: proto.Bool(true)}.Build(),
				pb.PodState_builder{Name: proto.String("pod2"), IsReady: proto.Bool(true)}.Build(),
			},
		}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("pod1"), Samples: []*pb.MetricSample{
					pb.MetricSample_builder{Name: proto.String("cpu_global"), Value: proto.Float64(10)}.Build(),
					pb.MetricSample_builder{Name: proto.String("cpu_pod"), Value: proto.Float64(10)}.Build(),
				}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("pod2"), Samples: []*pb.MetricSample{
					pb.MetricSample_builder{Name: proto.String("cpu_global"), Value: proto.Float64(30)}.Build(),
					pb.MetricSample_builder{Name: proto.String("cpu_pod"), Value: proto.Float64(30)}.Build(),
				}}.Build(),
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{
			"cpu_global": 20.0, // Aggregated: (10 + 30) / 2
			// cpu_pod should NOT be in Values
		},
		PodMetrics: map[string]*pb.MetricValues{
			"pod1": pb.MetricValues_builder{Values: map[string]float64{"cpu_pod": 10.0}}.Build(),
			"pod2": pb.MetricValues_builder{Values: map[string]float64{"cpu_pod": 30.0}}.Build(),
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(2),
	}.Build()

	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics with Pod scope mismatch (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_PodContainerScope(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build(), Scope: proto.String("PodContainer")}.Build(),
			},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{
				pb.PodState_builder{Name: proto.String("pod1"), IsReady: proto.Bool(true)}.Build(),
				pb.PodState_builder{Name: proto.String("pod2"), IsReady: proto.Bool(true)}.Build(),
			},
		}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("pod1"), ContainerName: proto.String("app"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(10)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("pod1"), ContainerName: proto.String("sidecar"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(2)}.Build()}}.Build(),
				// pod2 reports at the pod level only.
				pb.MetricBatch_builder{PodName: proto.String("pod2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(30)}.Build()}}.Build(),
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{},
		PodMetrics: map[string]*pb.MetricValues{
			// pod1's container samples are only reported per container, not per-pod.
			// pod2's sample has no container, so it stays pod-level.
			"pod2": pb.MetricValues_builder{Values: map[string]float64{"cpu": 30.0}}.Build(),
		},
		PodContainerMetrics: map[string]*pb.ContainerMetrics{
			"pod1": pb.ContainerMetrics_builder{
				ContainerMetrics: map[string]*pb.MetricValues{
					"app":     pb.MetricValues_builder{Values: map[string]float64{"cpu": 10.0}}.Build(),
					"sidecar": pb.MetricValues_builder{Values: map[string]float64{"cpu": 2.0}}.Build(),
				},
			}.Build(),
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(2),
	}.Build()

	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics with PodContainer scope mismatch (-want +got):\n%s", diff)
	}
}

// TestGetControlMetrics_ContainerScope verifies that a "Container" scoped
// metric reports one value per container name, averaged over the pods, and
// leaves the per-pod views empty.
func TestGetControlMetrics_ContainerScope(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build(), Scope: proto.String("Container")}.Build(),
			},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{
				pb.PodState_builder{Name: proto.String("pod1"), IsReady: proto.Bool(true)}.Build(),
				pb.PodState_builder{Name: proto.String("pod2"), IsReady: proto.Bool(true)}.Build(),
			},
		}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("pod1"), ContainerName: proto.String("app"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(10)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("pod1"), ContainerName: proto.String("sidecar"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(2)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("pod2"), ContainerName: proto.String("app"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(30)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("pod2"), ContainerName: proto.String("sidecar"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(6)}.Build()}}.Build(),
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{},
		ContainerMetrics: pb.ContainerMetrics_builder{
			ContainerMetrics: map[string]*pb.MetricValues{
				// Averaged across both pods: app (10+30)/2, sidecar (2+6)/2.
				"app":     pb.MetricValues_builder{Values: map[string]float64{"cpu": 20.0}}.Build(),
				"sidecar": pb.MetricValues_builder{Values: map[string]float64{"cpu": 4.0}}.Build(),
			},
		}.Build(),
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(2),
	}.Build()

	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics with Container scope mismatch (-want +got):\n%s", diff)
	}
}

// TestGetControlMetrics_ContainerScopeAveragesOverReportingPodsOnly verifies
// that a container running in only some of the pods is averaged over just
// those pods, rather than being diluted by the pods that do not report it.
func TestGetControlMetrics_ContainerScopeAveragesOverReportingPodsOnly(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build(), Scope: proto.String("Container")}.Build(),
			},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{
				pb.PodState_builder{Name: proto.String("pod1"), IsReady: proto.Bool(true)}.Build(),
				pb.PodState_builder{Name: proto.String("pod2"), IsReady: proto.Bool(true)}.Build(),
			},
		}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("pod1"), ContainerName: proto.String("app"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(10)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("pod2"), ContainerName: proto.String("app"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(30)}.Build()}}.Build(),
				// Only pod1 runs the sidecar.
				pb.MetricBatch_builder{PodName: proto.String("pod1"), ContainerName: proto.String("sidecar"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(7)}.Build()}}.Build(),
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{},
		ContainerMetrics: pb.ContainerMetrics_builder{
			ContainerMetrics: map[string]*pb.MetricValues{
				"app": pb.MetricValues_builder{Values: map[string]float64{"cpu": 20.0}}.Build(),
				// Averaged over pod1 only, not halved by pod2.
				"sidecar": pb.MetricValues_builder{Values: map[string]float64{"cpu": 7.0}}.Build(),
			},
		}.Build(),
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(2),
	}.Build()

	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics with Container scope mismatch (-want +got):\n%s", diff)
	}
}

// TestGetControlMetrics_PodScopeOmitsContainerBreakdown verifies that a "Pod"
// scoped metric sums container-tagged samples into the pod value without
// exposing the per-container breakdown. That breakdown is reserved for the
// "PodContainer" scope.
func TestGetControlMetrics_PodScopeOmitsContainerBreakdown(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build(), Scope: proto.String("Pod")}.Build(),
			},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("pod1"), IsReady: proto.Bool(true)}.Build()},
		}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("pod1"), ContainerName: proto.String("app"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(10)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("pod1"), ContainerName: proto.String("sidecar"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(2)}.Build()}}.Build(),
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{},
		PodMetrics: map[string]*pb.MetricValues{
			// Container samples are summed, but not broken out individually.
			"pod1": pb.MetricValues_builder{Values: map[string]float64{"cpu": 12.0}}.Build(),
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()

	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics with Pod scope mismatch (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_PodEligibility(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(10000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:      id,
			Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m1"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Sum")}.Build()}.Build()},
		}.Build(),
	}.Build())

	start := timestamppb.New(time.Unix(1000, 0))
	at := func(offset time.Duration) *timestamppb.Timestamp {
		return timestamppb.New(time.Unix(1000, 0).Add(offset))
	}
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{
				pb.PodState_builder{Name: proto.String("pod-ready"), Phase: proto.String("Running"), StartTime: start, IsReady: proto.Bool(true), ReadyLastTransitionTime: at(10 * time.Second)}.Build(),
				// Was ready, and became unready later in its life (e.g. its
				// readiness probe fails under load): still eligible.
				pb.PodState_builder{Name: proto.String("pod-unready-later"), Phase: proto.String("Running"), StartTime: start, IsReady: proto.Bool(false), ReadyLastTransitionTime: at(time.Hour)}.Build(),
				// Its Ready condition has been false since it started.
				pb.PodState_builder{Name: proto.String("pod-never-ready"), Phase: proto.String("Running"), StartTime: start, IsReady: proto.Bool(false), ReadyLastTransitionTime: at(time.Second)}.Build(),
				pb.PodState_builder{Name: proto.String("pod-pending"), Phase: proto.String("Pending")}.Build(),
				pb.PodState_builder{Name: proto.String("pod-succeeded"), Phase: proto.String("Succeeded"), StartTime: start, IsReady: proto.Bool(false), ReadyLastTransitionTime: at(time.Hour)}.Build(),
			},
		}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("pod-ready"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m1"), Value: proto.Float64(10)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("pod-unready-later"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m1"), Value: proto.Float64(20)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("pod-never-ready"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m1"), Value: proto.Float64(100)}.Build()}}.Build(), // Should be ignored
				pb.MetricBatch_builder{PodName: proto.String("pod-pending"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m1"), Value: proto.Float64(200)}.Build()}}.Build(),     // Should be ignored
				pb.MetricBatch_builder{PodName: proto.String("pod-succeeded"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m1"), Value: proto.Float64(300)}.Build()}}.Build(),   // Should be ignored
				pb.MetricBatch_builder{PodName: proto.String("pod-unknown"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m1"), Value: proto.Float64(50)}.Build()}}.Build(),      // Should be ignored
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values:        map[string]float64{"m1": 30},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(2),
	}.Build()
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

// Terminating pods are not eligible, even while they are still ready.
func TestGetControlMetrics_TerminatingPod(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(10000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:      id,
			Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m1"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Sum")}.Build()}.Build()},
		}.Build(),
	}.Build())

	start := timestamppb.New(time.Unix(1000, 0))
	ready := timestamppb.New(time.Unix(1010, 0))
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{
				pb.PodState_builder{Name: proto.String("pod-ready"), Phase: proto.String("Running"), StartTime: start, IsReady: proto.Bool(true), ReadyLastTransitionTime: ready}.Build(),
				pb.PodState_builder{Name: proto.String("pod-terminating"), Phase: proto.String("Running"), StartTime: start, IsReady: proto.Bool(true), ReadyLastTransitionTime: ready, DeletionTimestamp: timestamppb.New(time.Unix(5000, 0))}.Build(),
			},
		}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("pod-ready"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m1"), Value: proto.Float64(10)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("pod-terminating"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m1"), Value: proto.Float64(100)}.Build()}}.Build(), // Should be ignored
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values:        map[string]float64{"m1": 10},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_PolicyUpdate(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	stored := mustUpdatePolicy(t, client, pb.Policy_builder{
		Id:      id,
		Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m1"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{Id: id, Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build()}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m1"), Value: proto.Float64(10)}.Build()}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())
	wantCM1 := pb.ControlMetrics_builder{
		Values:        map[string]float64{"m1": 10},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM1, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("Initial ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	// Update Policy: Remove m1, add m2
	mustUpdatePolicy(t, client, pb.Policy_builder{
		Id:      id,
		Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m2"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
		Etag:    proto.String(stored.GetEtag()),
	}.Build())

	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m2"), Value: proto.Float64(20)}.Build()}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM2 := pb.ControlMetrics_builder{
		Values:        map[string]float64{"m2": 20},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM2, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("Updated ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_SlidingWindow(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{
					Name: proto.String("m_instant"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build(),
				}.Build(),
				pb.MetricDefinition_builder{
					Name: proto.String("c_avg"), Rate: pb.Rate_builder{Window: durationpb.New(time.Minute), Aggregation: proto.String("Avg")}.Build(),
				}.Build(),
			},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{Id: id, Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build()}.Build())

	// T0
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{
			pb.MetricSample_builder{Name: proto.String("m_instant"), Value: proto.Float64(10)}.Build(),
			pb.MetricSample_builder{Name: proto.String("c_avg"), Value: proto.Float64(0)}.Build(),
		}}.Build()}}.Build()},
	}.Build())

	// T1 (30s later)
	clk.Advance(30 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{
			pb.MetricSample_builder{Name: proto.String("m_instant"), Value: proto.Float64(20)}.Build(),
			pb.MetricSample_builder{Name: proto.String("c_avg"), Value: proto.Float64(10)}.Build(), // Rate = 10/30 = 0.333
		}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())
	wantCM1 := pb.ControlMetrics_builder{
		Values: map[string]float64{
			"m_instant": 20.0,
			"c_avg":     0.333,
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM1, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch at T1 (-want +got):\n%s", diff)
	}

	// T2 (90s after T0): 10 should be gone from window
	clk.Advance(60 * time.Second)
	ts = clk.Now().Unix()
	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())
	wantCM2 := pb.ControlMetrics_builder{
		Values: map[string]float64{
			"m_instant": 20.0,
			"c_avg":     0.333,
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM2, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch at T2 (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_DecayingHistogramWindow(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{
					Name: proto.String("m_max"), DecayingDistribution: pb.DecayingDistribution_builder{HalfLife: durationpb.New(time.Minute), BucketSize: proto.String("0.1"), Percentile: proto.String("p100")}.Build(),
				}.Build(),
				pb.MetricDefinition_builder{
					Name: proto.String("c_max"), DecayingDistribution: pb.DecayingDistribution_builder{HalfLife: durationpb.New(time.Minute), BucketSize: proto.String("0.1"), Percentile: proto.String("p100"), Rate: durationpb.New(time.Minute)}.Build(),
				}.Build(),
			},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{Id: id, Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build()}.Build())

	// T0
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{
			pb.MetricSample_builder{Name: proto.String("m_max"), Value: proto.Float64(1.0)}.Build(),
			pb.MetricSample_builder{Name: proto.String("c_max"), Value: proto.Float64(0)}.Build(),
		}}.Build()}}.Build()},
	}.Build())

	// T1 (30s later)
	clk.Advance(30 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{
			pb.MetricSample_builder{Name: proto.String("c_max"), Value: proto.Float64(30)}.Build(), // Rate = 1.0
		}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{
			"m_max": 1.1,
			"c_max": 1.1,
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	// Using EquateApprox for bucket upper bounds (0.1 bucket size)
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.11)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

// --- UpdateRecommenderState ---

func TestUpdateRecommenderState_InvalidID(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: &pb.PolicyId{},
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestUpdateRecommenderState_NotFound(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id:              pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("missing")}.Build(),
		RecommenderName: proto.String("r1"),
		Recommendation:  pb.Recommendation_builder{Replicas: proto.Int32(1)}.Build(),
	}.Build())
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", err)
	}
}

func TestUpdateRecommenderState_NotDefined(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{Policy: pb.Policy_builder{Id: id}.Build()}.Build())

	_, err := client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id:              id,
		RecommenderName: proto.String("undefined"),
		Recommendation:  pb.Recommendation_builder{Replicas: proto.Int32(1)}.Build(),
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestUpdateRecommenderState_EmptyClears(t *testing.T) {
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:      id,
			Scaling: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("r1"), Recommender: proto.String("Linear"), Type: proto.String("Linear"), Mode: proto.String("Active")}.Build()},
		}.Build(),
	}.Build())

	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("r1"), Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(5), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: id}.Build())
	wantResp1 := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(5),
		}.Build(),
		Explanation:    []*pb.RecommenderStatus{pb.RecommenderStatus_builder{Name: proto.String("r1"), Type: proto.String("Linear"), Phase: proto.String("Scaling"), Mode: proto.String("Active"), Replicas: proto.Int32(5), IsActive: proto.Bool(true)}.Build()},
		MetricStatuses: []*pb.MetricStatus{},
	}.Build()
	// Note: Enrichment logic adds LastUpdated, which we might want to ignore or match in cmp.Diff
	opts := []cmp.Option{
		protocmp.Transform(), protocmp.IgnoreDefaultScalars(),
		protocmp.IgnoreFields(&pb.RecommenderStatus{}, "last_updated"),
	}

	if diff := cmp.Diff(wantResp1, resp, opts...); diff != "" {
		t.Errorf("Initial GetRecommendation mismatch (-want +got):\n%s", diff)
	}

	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("r1"), Recommendation: nil,
	}.Build())

	memStore.CalculateAll()
	resp, _ = client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: id}.Build())
	if diff := cmp.Diff(&pb.GetRecommendationResponse{}, resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("Expected empty response after clearing state (-want +got):\n%s", diff)
	}
}

func TestUpdateRecommenderState_VerticalResources(t *testing.T) {
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Scaling: []*pb.RecommenderDefinition{
				pb.RecommenderDefinition_builder{Name: proto.String("r1"), Recommender: proto.String("AddonResizer"), Type: proto.String("AddonResizer"), Mode: proto.String("Active")}.Build(),
			},
		}.Build(),
	}.Build())

	recommendation := pb.Recommendation_builder{
		IsActive: proto.Bool(true),
		WorkloadResources: []*pb.ContainerResource{
			pb.ContainerResource_builder{
				Requests: map[string]string{"cpu": "100m", "memory": "200Mi"},
			}.Build(),
		},
		PodContainerResources: []*pb.PodContainerResource{
			pb.PodContainerResource_builder{PodName: proto.String("pod1"), ContainerResources: pb.ContainerResource_builder{Requests: map[string]string{"cpu": "200m"}}.Build()}.Build(),
			pb.PodContainerResource_builder{PodName: proto.String("pod2"), ContainerResources: pb.ContainerResource_builder{Limits: map[string]string{"memory": "1Gi"}}.Build()}.Build(),
		},
	}.Build()

	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("r1"), Recommendation: recommendation,
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: id}.Build())

	wantResp := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			WorkloadResources:     recommendation.GetWorkloadResources(),
			PodContainerResources: recommendation.GetPodContainerResources(),
		}.Build(),
		Explanation: []*pb.RecommenderStatus{
			pb.RecommenderStatus_builder{
				Name:              proto.String("r1"),
				Type:              proto.String("AddonResizer"),
				Phase:             proto.String("Scaling"),
				Mode:              proto.String("Active"),
				IsActive:          proto.Bool(true),
				WorkloadResources: recommendation.GetWorkloadResources(),
				PodResources:      recommendation.GetPodContainerResources(),
			}.Build(),
		},
		MetricStatuses: []*pb.MetricStatus{},
	}.Build()

	opts := []cmp.Option{
		protocmp.Transform(), protocmp.IgnoreDefaultScalars(),
		protocmp.IgnoreFields(&pb.RecommenderStatus{}, "last_updated"),
	}

	if diff := cmp.Diff(wantResp, resp, opts...); diff != "" {
		t.Errorf("Vertical resources mismatch (-want +got):\n%s", diff)
	}
}

func TestUpdateRecommenderState_VerticalResources_PerContainer(t *testing.T) {
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Scaling: []*pb.RecommenderDefinition{
				pb.RecommenderDefinition_builder{Name: proto.String("app-sizer"), Recommender: proto.String("AddonResizer"), Type: proto.String("AddonResizer"), Mode: proto.String("Active")}.Build(),
				pb.RecommenderDefinition_builder{Name: proto.String("sidecar-sizer"), Recommender: proto.String("AddonResizer"), Type: proto.String("AddonResizer"), Mode: proto.String("Active")}.Build(),
			},
		}.Build(),
	}.Build())

	appVote := pb.Recommendation_builder{
		IsActive: proto.Bool(true),
		WorkloadResources: []*pb.ContainerResource{
			pb.ContainerResource_builder{
				ContainerName: proto.String("app"),
				Requests:      map[string]string{"cpu": "500m", "memory": "1Gi"},
			}.Build(),
		},
	}.Build()
	sidecarVote := pb.Recommendation_builder{
		IsActive: proto.Bool(true),
		WorkloadResources: []*pb.ContainerResource{
			pb.ContainerResource_builder{
				ContainerName: proto.String("sidecar"),
				Requests:      map[string]string{"cpu": "50m"},
				Limits:        map[string]string{"memory": "128Mi"},
			}.Build(),
		},
	}.Build()

	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("app-sizer"), Recommendation: appVote,
	}.Build())
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("sidecar-sizer"), Recommendation: sidecarVote,
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: id}.Build())

	wantResp := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			WorkloadResources: append([]*pb.ContainerResource{}, append(appVote.GetWorkloadResources(), sidecarVote.GetWorkloadResources()...)...),
		}.Build(),
		Explanation: []*pb.RecommenderStatus{
			pb.RecommenderStatus_builder{
				Name:              proto.String("app-sizer"),
				Type:              proto.String("AddonResizer"),
				Phase:             proto.String("Scaling"),
				Mode:              proto.String("Active"),
				IsActive:          proto.Bool(true),
				WorkloadResources: appVote.GetWorkloadResources(),
			}.Build(),
			pb.RecommenderStatus_builder{
				Name:              proto.String("sidecar-sizer"),
				Type:              proto.String("AddonResizer"),
				Phase:             proto.String("Scaling"),
				Mode:              proto.String("Active"),
				IsActive:          proto.Bool(true),
				WorkloadResources: sidecarVote.GetWorkloadResources(),
			}.Build(),
		},
		MetricStatuses: []*pb.MetricStatus{},
	}.Build()

	opts := []cmp.Option{
		protocmp.Transform(), protocmp.IgnoreDefaultScalars(),
		protocmp.IgnoreFields(&pb.RecommenderStatus{}, "last_updated"),
	}

	if diff := cmp.Diff(wantResp, resp, opts...); diff != "" {
		t.Errorf("Per-container vertical resources mismatch (-want +got):\n%s", diff)
	}
}

func TestUpdateRecommenderState_Validation(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Scaling: []*pb.RecommenderDefinition{
				pb.RecommenderDefinition_builder{Name: proto.String("r1"), Recommender: proto.String("Linear"), Type: proto.String("Linear"), Mode: proto.String("Active")}.Build(),
			},
		}.Build(),
	}.Build())

	tests := []struct {
		name           string
		recommendation *pb.Recommendation
	}{
		{"Negative Replicas", pb.Recommendation_builder{Replicas: proto.Int32(-1)}.Build()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
				Id:              id,
				RecommenderName: proto.String("r1"),
				Recommendation:  tc.recommendation,
			}.Build())
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("Expected InvalidArgument for %s, got %v", tc.name, err)
			}
		})
	}
}

// --- GetRecommendation ---

func TestGetRecommendation_InvalidID(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: &pb.PolicyId{}}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestGetRecommendation_NotFound(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("missing")}.Build(),
	}.Build())
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", err)
	}
}

func TestGetRecommendation_NoRecommenders(t *testing.T) {
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{Policy: pb.Policy_builder{Id: id}.Build()}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: id}.Build())
	if diff := cmp.Diff(&pb.GetRecommendationResponse{}, resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("Expected empty response for policy with no recommenders (-want +got):\n%s", diff)
	}
}

func TestGetRecommendation_NeverCalled(t *testing.T) {
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id:      id,
			Scaling: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("r1"), Recommender: proto.String("Linear"), Mode: proto.String("Active")}.Build()},
		}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: id}.Build())
	if diff := cmp.Diff(&pb.GetRecommendationResponse{}, resp, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("Expected empty response before any recommender state update (-want +got):\n%s", diff)
	}
}

func TestGetRecommendation_Aggregation(t *testing.T) {
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Activation: []*pb.RecommenderDefinition{
				pb.RecommenderDefinition_builder{Name: proto.String("act1"), Recommender: proto.String("Threshold"), Type: proto.String("Threshold"), Mode: proto.String("Active")}.Build(),
				pb.RecommenderDefinition_builder{Name: proto.String("act2"), Recommender: proto.String("Threshold"), Type: proto.String("Threshold"), Mode: proto.String("Active")}.Build(),
			},
			Scaling: []*pb.RecommenderDefinition{
				pb.RecommenderDefinition_builder{Name: proto.String("scale1"), Recommender: proto.String("Linear"), Type: proto.String("Linear"), Mode: proto.String("Active")}.Build(),
				pb.RecommenderDefinition_builder{Name: proto.String("scale2"), Recommender: proto.String("Linear"), Type: proto.String("Linear"), Mode: proto.String("Active")}.Build(),
				pb.RecommenderDefinition_builder{Name: proto.String("vpa1"), Recommender: proto.String("vpa"), Type: proto.String("VPA"), Mode: proto.String("Active")}.Build(),
			},
		}.Build(),
	}.Build())

	// Scenario: act1=true, act2=false -> Active (OR)
	//           scale1=10, scale2=20 -> 20 (MAX)
	//					 mem=256Mi, cpu=25m
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("act1"), Recommendation: pb.Recommendation_builder{IsActive: proto.Bool(true)}.Build(),
	}.Build())
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("act2"), Recommendation: pb.Recommendation_builder{IsActive: proto.Bool(false)}.Build(),
	}.Build())
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("scale1"), Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(10), IsActive: proto.Bool(true)}.Build(),
	}.Build())
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("scale2"), Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(20), IsActive: proto.Bool(true)}.Build(),
	}.Build())
	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("vpa1"), Recommendation: pb.Recommendation_builder{
			IsActive: proto.Bool(true),
			WorkloadResources: []*pb.ContainerResource{
				pb.ContainerResource_builder{
					Requests: map[string]string{
						"cpu":    "25m",
						"memory": "265Mi",
					},
					Limits: map[string]string{
						"cpu":    "25m",
						"memory": "265Mi",
					},
				}.Build(),
			},
			Message: proto.String("Recommendation generated successfully."),
		}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: id}.Build())

	wantResp := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(20),
			WorkloadResources: []*pb.ContainerResource{
				pb.ContainerResource_builder{
					Requests: map[string]string{"cpu": "25m", "memory": "265Mi"},
					Limits:   map[string]string{"cpu": "25m", "memory": "265Mi"},
				}.Build(),
			},
		}.Build(),
		Explanation: []*pb.RecommenderStatus{
			pb.RecommenderStatus_builder{Name: proto.String("scale1"), Type: proto.String("Linear"), Phase: proto.String("Scaling"), Mode: proto.String("Active"), Replicas: proto.Int32(10), IsActive: proto.Bool(true)}.Build(),
			pb.RecommenderStatus_builder{Name: proto.String("scale2"), Type: proto.String("Linear"), Phase: proto.String("Scaling"), Mode: proto.String("Active"), Replicas: proto.Int32(20), IsActive: proto.Bool(true)}.Build(),
			pb.RecommenderStatus_builder{Name: proto.String("act1"), Type: proto.String("Threshold"), Phase: proto.String("Activation"), Mode: proto.String("Active"), IsActive: proto.Bool(true)}.Build(),
			pb.RecommenderStatus_builder{Name: proto.String("act2"), Type: proto.String("Threshold"), Phase: proto.String("Activation"), Mode: proto.String("Active"), IsActive: proto.Bool(false)}.Build(),
			pb.RecommenderStatus_builder{Name: proto.String("vpa1"), Type: proto.String("VPA"), Phase: proto.String("Scaling"), Mode: proto.String("Active"), IsActive: proto.Bool(true), WorkloadResources: []*pb.ContainerResource{
				pb.ContainerResource_builder{
					Requests: map[string]string{"cpu": "25m", "memory": "265Mi"},
					Limits:   map[string]string{"cpu": "25m", "memory": "265Mi"},
				}.Build(),
			},
				Message: proto.String("Recommendation generated successfully.")}.Build(),
		},
		MetricStatuses: []*pb.MetricStatus{},
	}.Build()
	// Sort for comparison
	opts := []cmp.Option{
		protocmp.Transform(), protocmp.IgnoreDefaultScalars(),
		protocmp.SortRepeated(func(a, b *pb.RecommenderStatus) bool { return a.GetName() < b.GetName() }),
		protocmp.IgnoreFields(&pb.RecommenderStatus{}, "last_updated"),
	}
	if diff := cmp.Diff(wantResp, resp, opts...); diff != "" {
		t.Errorf("GetRecommendation aggregation mismatch (-want +got):\n%s", diff)
	}
}

func TestGetRecommendation_MetricStatuses(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("m1"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("m2"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
			},
			Scaling: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("r1"), Recommender: proto.String("Linear"), Type: proto.String("Linear"), Mode: proto.String("Active")}.Build()},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{Id: id, Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build()}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m1"), Value: proto.Float64(10)}.Build()}}.Build()}}.Build()},
	}.Build())
	// m2 has no data

	client.UpdateRecommenderState(ctx, pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("r1"), Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(5), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, pb.GetRecommendationRequest_builder{Id: id}.Build())

	wantResp := pb.GetRecommendationResponse_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(5),
		}.Build(),
		Explanation: []*pb.RecommenderStatus{pb.RecommenderStatus_builder{Name: proto.String("r1"), Type: proto.String("Linear"), Phase: proto.String("Scaling"), Mode: proto.String("Active"), Replicas: proto.Int32(5), IsActive: proto.Bool(true)}.Build()},
		MetricStatuses: []*pb.MetricStatus{
			pb.MetricStatus_builder{Name: proto.String("m1"), Value: proto.Float64(10), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
			pb.MetricStatus_builder{Name: proto.String("m2"), Error: proto.String("No data available"), Timestamp: timestamppb.New(time.Unix(ts, 0))}.Build(),
		},
	}.Build()
	// Sort for comparison
	opts := []cmp.Option{
		protocmp.Transform(), protocmp.IgnoreDefaultScalars(),
		protocmp.SortRepeated(func(a, b *pb.MetricStatus) bool { return a.GetName() < b.GetName() }),
		protocmp.IgnoreFields(&pb.RecommenderStatus{}, "last_updated"),
	}
	if diff := cmp.Diff(wantResp, resp, opts...); diff != "" {
		t.Errorf("GetRecommendation response mismatch (-want +got):\n%s", diff)
	}
}

// --- IngestMetrics ---

func TestIngestMetrics_InvalidCluster(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{ClusterName: proto.String(""), Timestamp: timestamppb.Now()}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestIngestMetrics_MissingTimestamp(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"),
		Policies:    []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()},
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestIngestMetrics_NotFound(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"),
		Timestamp:   timestamppb.Now(),
		Policies:    []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("missing")}.Build()},
	}.Build())
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", err)
	}
}

func TestIngestMetrics_NotDefinedMetric(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{Policy: pb.Policy_builder{Id: id}.Build()}.Build())

	_, err := client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"),
		Timestamp:   timestamppb.Now(),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("undefined")}.Build()}}.Build()},
		}.Build()},
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument for undefined metric, got %v", err)
	}
}

func TestIngestMetrics_Global(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("g_sum"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Sum")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("c_sum"), Rate: pb.Rate_builder{Window: durationpb.New(time.Minute), Aggregation: proto.String("Sum")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("h_p50"), Distribution: pb.Distribution_builder{Percentile: proto.String("p50"), Aggregation: proto.String("Max")}.Build()}.Build(),
			},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{Id: id, Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build()}.Build())

	ts := clk.Now().Unix()
	// T0 for counter
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String(""), Samples: []*pb.MetricSample{
			pb.MetricSample_builder{Name: proto.String("c_sum"), Value: proto.Float64(0)}.Build(),
			pb.MetricSample_builder{Name: proto.String("h_p50"), Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"1.0": 0, "+Inf": 0}}.Build()}.Build(),
		}}.Build()}}.Build()},
	}.Build())

	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String(""), Samples: []*pb.MetricSample{
			pb.MetricSample_builder{Name: proto.String("g_sum"), Value: proto.Float64(100)}.Build(),
			pb.MetricSample_builder{Name: proto.String("c_sum"), Value: proto.Float64(10)}.Build(),                                                                     // Rate = 1.0
			pb.MetricSample_builder{Name: proto.String("h_p50"), Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"1.0": 100, "+Inf": 100}}.Build()}.Build(), // P50 = 0.5
		}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{
			"g_sum": 100,
			"c_sum": 1.0,
			"h_p50": 0.5,
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_AggregatedDecayingHistogram(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	stored := mustUpdatePolicy(t, client, pb.Policy_builder{
		Id: id,
		Metrics: []*pb.MetricDefinition{
			pb.MetricDefinition_builder{
				Name: proto.String("m_hist"), DecayingDistribution: pb.DecayingDistribution_builder{
					HalfLife:   durationpb.New(time.Hour),
					BucketSize: proto.String("1.0"),
					Percentile: proto.String("p50"),
				}.Build(),
			}.Build(),
		},
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{
				pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(),
				pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build(),
			},
		}.Build(),
	}.Build())

	// Ingest 10 from p1 and 20 from p2
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m_hist"), Value: proto.Float64(10)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("p2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m_hist"), Value: proto.Float64(20)}.Build()}}.Build(),
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	// Aggregate samples: [10, 20]. Weight total = 2.0. Target p50 Rank = 1.0.
	// Bucket 10 has weight 1.0. It reaches target.
	// Upper bound of bucket 10 is 11.0.
	if val := cm.GetValues()["m_hist"]; val != 11.0 {
		t.Errorf("Aggregated Histogram: Expected 11.0 (upper bound of bucket 10), got %f", val)
	}

	// Now check p95 (should reach bucket 20)
	stored = mustUpdatePolicy(t, client, pb.Policy_builder{
		Id: id,
		Metrics: []*pb.MetricDefinition{
			pb.MetricDefinition_builder{
				Name: proto.String("m_hist"), DecayingDistribution: pb.DecayingDistribution_builder{
					HalfLife:   durationpb.New(time.Hour),
					BucketSize: proto.String("1.0"),
					Percentile: proto.String("p95"),
				}.Build(),
			}.Build(),
		},
		Etag: proto.String(stored.GetEtag()),
	}.Build())
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m_hist"), Value: proto.Float64(10)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("p2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m_hist"), Value: proto.Float64(20)}.Build()}}.Build(),
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	// Target p95 Rank = 2.0 * 0.95 = 1.9.
	// Bucket 10 reaches 1.0.
	// Bucket 20 reaches 2.0. (Target met)
	if val := cm.GetValues()["m_hist"]; val != 21.0 {
		t.Errorf("Aggregated Histogram (P95): Expected 21.0 (upper bound of bucket 20), got %f", val)
	}

	// Verify time decay on aggregated histogram. The definition is left as is
	// (changing it would reset the state) and both pods keep reporting (a
	// series silent for too long is garbage-collected).
	// Advance by 5 hours (five half-lives): old samples weigh 1/32 each.
	clk.Advance(5 * time.Hour)
	ts = clk.Now().Unix()

	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m_hist"), Value: proto.Float64(1)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("p2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m_hist"), Value: proto.Float64(1)}.Build()}}.Build(),
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	// Weights: bucket 1 = 2.0, bucket 10 = 1/32, bucket 20 = 1/32.
	// Total = 2.0625. Target p95 Rank = 1.959. Bucket 1 reaches 2.0: target
	// met, upper bound 2.0. Without decay the total would be 4.0 and the
	// p95 (rank 3.8) would still land on bucket 20.
	if val := cm.GetValues()["m_hist"]; val != 2.0 {
		t.Errorf("Aggregated Histogram (Decay): Expected 2.0, got %f", val)
	}
}

// TestGetControlMetrics_DecayingHistogramPodChurn verifies that the memory of
// a decaying distribution does not outlive the pod that reported it: once a
// pod leaves the workload, its series stops taking part in the result.
func TestGetControlMetrics_DecayingHistogramPodChurn(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{
					Name: proto.String("cpu"), DecayingDistribution: pb.DecayingDistribution_builder{HalfLife: durationpb.New(time.Minute), BucketSize: proto.String("0.1"), Percentile: proto.String("p100")}.Build(),
				}.Build(),
			},
		}.Build(),
	}.Build())

	// Step 1: Add pod-1 and ingest high value.
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("pod-1"), IsReady: proto.Bool(true)}.Build()},
		}.Build(),
	}.Build())

	ts0 := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts0, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod-1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(1.0)}.Build()}}.Build()},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm1, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM1 := pb.ControlMetrics_builder{
		Values:        map[string]float64{"cpu": 1.1}, // p100 of bucket 1.0 (size 0.1)
		Timestamp:     timestamppb.New(time.Unix(ts0, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM1, cm1, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("Step 1 mismatch (-want +got):\n%s", diff)
	}

	// Step 2: Update workload (delete pod-1, add pod-2) and ingest low value.
	// pod-1 left the workload: its high value is not carried over to pod-2.
	clk.Advance(10 * time.Second)
	ts1 := clk.Now().Unix()
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("pod-2"), IsReady: proto.Bool(true)}.Build()},
		}.Build(),
	}.Build())

	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts1, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod-2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(0.1)}.Build()}}.Build()},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm2, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM2 := pb.ControlMetrics_builder{
		Values:        map[string]float64{"cpu": 1.1}, // Persistent from pod-1
		Timestamp:     timestamppb.New(time.Unix(ts1, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM2, cm2, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("Step 2 mismatch (-want +got):\n%s", diff)
	}

	// Step 3: Advance time significantly to decay pod-1's value.
	// We re-ingest pod-2's value to keep it fresh and prevent it from decaying away.
	clk.Advance(30 * time.Minute)
	ts2 := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts2, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod-2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("cpu"), Value: proto.Float64(0.1)}.Build()}}.Build()},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm3, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM3 := pb.ControlMetrics_builder{
		Values:        map[string]float64{"cpu": 0.2}, // Now pod-1 has decayed, pod-2 is max
		Timestamp:     timestamppb.New(time.Unix(ts2, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM3, cm3, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("Step 3 mismatch (-want +got):\n%s", diff)
	}
}

// --- Intent-Based Metrics ---

func TestIntent_GaugeAggregation(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("g_avg"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("g_sum"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Sum")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("g_max"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Max")}.Build()}.Build(),
			},
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{
				pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(),
				pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build(),
			},
		}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{
					pb.MetricSample_builder{Name: proto.String("g_avg"), Value: proto.Float64(10)}.Build(),
					pb.MetricSample_builder{Name: proto.String("g_sum"), Value: proto.Float64(10)}.Build(),
					pb.MetricSample_builder{Name: proto.String("g_max"), Value: proto.Float64(10)}.Build(),
				}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("p2"), Samples: []*pb.MetricSample{
					pb.MetricSample_builder{Name: proto.String("g_avg"), Value: proto.Float64(30)}.Build(),
					pb.MetricSample_builder{Name: proto.String("g_sum"), Value: proto.Float64(20)}.Build(),
					pb.MetricSample_builder{Name: proto.String("g_max"), Value: proto.Float64(40)}.Build(),
				}}.Build(),
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{
			"g_avg": 20.0, // (10+30)/2
			"g_sum": 30.0, // 10+20
			"g_max": 40.0, // max(10, 40)
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(2),
	}.Build()
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestIntent_RateCalculation(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("rps"), Rate: pb.Rate_builder{Window: durationpb.New(time.Minute), Aggregation: proto.String("Sum")}.Build()}.Build(),
			},
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	// T0
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("rps"), Value: proto.Float64(100)}.Build()}}.Build()},
		}.Build()},
	}.Build())

	// T1 (10s later)
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("rps"), Value: proto.Float64(110)}.Build()}}.Build()},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	// Rate = (110 - 100) / 10s = 1.0
	wantCM := pb.ControlMetrics_builder{
		Values:        map[string]float64{"rps": 1.0},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestIntent_DistributionPercentile(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("latency"), Distribution: pb.Distribution_builder{Percentile: proto.String("p99"), Aggregation: proto.String("Max")}.Build()}.Build(),
			},
		}.Build(),
	}.Build())

	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	// T0 for counter-based buckets
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("latency"), Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"1.0": 0, "+Inf": 0}}.Build()}.Build()}}.Build()},
		}.Build()},
	}.Build())

	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("p1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("latency"), Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"1.0": 99, "+Inf": 100}}.Build()}.Build()}}.Build()},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values:        map[string]float64{"latency": 1.0},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestIntent_DecayingDistributionPersistence(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("peak_cpu"), DecayingDistribution: pb.DecayingDistribution_builder{HalfLife: durationpb.New(time.Minute), BucketSize: proto.String("0.1"), Percentile: proto.String("p100")}.Build()}.Build(),
			},
		}.Build(),
	}.Build())

	// Step 1: Add pod-1 and ingest high value
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("pod-1"), IsReady: proto.Bool(true)}.Build()},
		}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod-1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("peak_cpu"), Value: proto.Float64(1.5)}.Build()}}.Build()},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm1, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())
	if val := cm1.GetValues()["peak_cpu"]; val < 1.5 || val > 1.6 {
		t.Errorf("Expected peak_cpu around 1.6 (upper bound), got %f", val)
	}

	// Step 2: Replace pod-1 with pod-2 (lower value)
	// The high value should persist
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("pod-2"), IsReady: proto.Bool(true)}.Build()},
		}.Build(),
	}.Build())
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod-2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("peak_cpu"), Value: proto.Float64(0.5)}.Build()}}.Build()},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm2, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())
	if val := cm2.GetValues()["peak_cpu"]; val < 1.5 || val > 1.6 {
		t.Errorf("Expected peak_cpu to persist around 1.6, got %f", val)
	}
}

func TestIntent_Validation(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()

	tests := []struct {
		name   string
		metric *pb.MetricDefinition
	}{
		{
			name: "Multiple Intents",
			metric: pb.MetricDefinition_builder{
				Name:  proto.String("m1"),
				Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build(),
				Rate:  pb.Rate_builder{Window: durationpb.New(time.Minute)}.Build(),
			}.Build(),
		},
		{
			name: "Rate Missing Window",
			metric: pb.MetricDefinition_builder{
				Name: proto.String("m1"),
				Rate: pb.Rate_builder{Aggregation: proto.String("Sum")}.Build(),
			}.Build(),
		},
		{
			name: "Distribution Missing Percentile",
			metric: pb.MetricDefinition_builder{
				Name:         proto.String("m1"),
				Distribution: pb.Distribution_builder{Aggregation: proto.String("Max")}.Build(),
			}.Build(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
				Policy: pb.Policy_builder{
					Id:      id,
					Metrics: []*pb.MetricDefinition{tc.metric},
				}.Build(),
			}.Build())
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("Expected InvalidArgument for %s, got %v", tc.name, err)
			}
		})
	}
}

func TestIntent_PodScope_Gauge(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("g_avg"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build(), Scope: proto.String("Pod")}.Build(),
			},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       id,
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("pod1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("pod2"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{
				pb.MetricBatch_builder{PodName: proto.String("pod1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("g_avg"), Value: proto.Float64(10)}.Build()}}.Build(),
				pb.MetricBatch_builder{PodName: proto.String("pod2"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("g_avg"), Value: proto.Float64(30)}.Build()}}.Build(),
			},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{},
		PodMetrics: map[string]*pb.MetricValues{
			"pod1": pb.MetricValues_builder{Values: map[string]float64{"g_avg": 10.0}}.Build(),
			"pod2": pb.MetricValues_builder{Values: map[string]float64{"g_avg": 30.0}}.Build(),
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(2),
	}.Build()
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestIntent_PodScope_Rate(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("c_rate"), Rate: pb.Rate_builder{Window: durationpb.New(time.Minute), Aggregation: proto.String("Avg")}.Build(), Scope: proto.String("Pod")}.Build(),
			},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       id,
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("pod1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("c_rate"), Value: proto.Float64(0)}.Build()}}.Build()}}.Build()},
	}.Build())

	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("c_rate"), Value: proto.Float64(10)}.Build()}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{},
		PodMetrics: map[string]*pb.MetricValues{
			"pod1": pb.MetricValues_builder{Values: map[string]float64{"c_rate": 1.0}}.Build(), // Rate = 10/10s = 1.0
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestIntent_PodScope_Distribution(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("h_p99"), Distribution: pb.Distribution_builder{Percentile: proto.String("p99"), Aggregation: proto.String("Max")}.Build(), Scope: proto.String("Pod")}.Build(),
			},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       id,
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("pod1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("h_p99"), Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"1.0": 0, "+Inf": 0}}.Build()}.Build()}}.Build()}}.Build()},
	}.Build())

	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{Namespace: proto.String("ns"), Name: proto.String("p1"), Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("h_p99"), Histogram: pb.Histogram_builder{Buckets: map[string]uint64{"1.0": 99, "+Inf": 100}}.Build()}.Build()}}.Build()}}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{},
		PodMetrics: map[string]*pb.MetricValues{
			"pod1": pb.MetricValues_builder{Values: map[string]float64{"h_p99": 1.0}}.Build(),
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestIntent_PodScope_DecayingDistribution(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := pb.PolicyId_builder{ClusterName: proto.String("c1"), Namespace: proto.String("ns"), Name: proto.String("p1")}.Build()
	client.CreatePolicy(ctx, pb.CreatePolicyRequest_builder{
		Policy: pb.Policy_builder{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("decay_p100"), DecayingDistribution: pb.DecayingDistribution_builder{HalfLife: durationpb.New(time.Minute), BucketSize: proto.String("0.1"), Percentile: proto.String("p100")}.Build(), Scope: proto.String("Pod")}.Build(),
			},
		}.Build(),
	}.Build())
	client.UpdateWorkload(ctx, pb.UpdateWorkloadRequest_builder{
		Id:       id,
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("pod1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("c1"), Timestamp: timestamppb.New(time.Unix(ts, 0)),
		Policies: []*pb.PolicyBatch{pb.PolicyBatch_builder{
			Namespace: proto.String("ns"), Name: proto.String("p1"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod1"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("decay_p100"), Value: proto.Float64(1.0)}.Build()}}.Build()},
		}.Build()},
	}.Build())

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, pb.GetControlMetricsRequest_builder{Id: id}.Build())

	wantCM := pb.ControlMetrics_builder{
		Values: map[string]float64{},
		PodMetrics: map[string]*pb.MetricValues{
			"pod1": pb.MetricValues_builder{Values: map[string]float64{"decay_p100": 1.1}}.Build(), // Upper bound of bucket 1.0 (size 0.1)
		},
		Timestamp:     timestamppb.New(time.Unix(ts, 0)),
		ReadyReplicas: proto.Int32(1),
	}.Build()
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), protocmp.IgnoreDefaultScalars(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}
