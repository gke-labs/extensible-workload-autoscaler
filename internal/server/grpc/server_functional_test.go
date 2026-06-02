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
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	servergrpc "github.com/gke-labs/extensible-workload-autoscaler/internal/server/grpc"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store"
)

const bufSizeFunctional = 1024 * 1024

func setupFunctionalGRPCServer(t *testing.T, c clock.Clock) (*store.MemoryStore, pb.XASControlPlaneClient, func()) {
	lis := bufconn.Listen(bufSizeFunctional)
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

// --- UpdatePolicy ---

func TestUpdatePolicy_InvalidID(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	tests := []struct {
		name string
		id   *pb.PolicyId
	}{
		{"Missing Cluster", &pb.PolicyId{Namespace: "ns", Name: "name"}},
		{"Missing Namespace", &pb.PolicyId{ClusterName: "c1", Name: "name"}},
		{"Missing Name", &pb.PolicyId{ClusterName: "c1", Namespace: "ns"}},
		{"Empty ID", &pb.PolicyId{}},
		{"Nil ID", nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
				Policy: &pb.Policy{Id: tc.id},
			})
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

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	req := &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{
					Name: "m1",
				},
			},
		},
	}

	_, err := client.UpdatePolicy(ctx, req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument for missing intent, got %v", err)
	}
}

func TestUpdatePolicy_CrossCluster(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id1 := &pb.PolicyId{ClusterName: "cluster-1", Namespace: "default", Name: "policy"}
	id2 := &pb.PolicyId{ClusterName: "cluster-2", Namespace: "default", Name: "policy"}

	p1 := &pb.Policy{Id: id1, MinReplicas: 1}
	p2 := &pb.Policy{Id: id2, MinReplicas: 5}

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{Policy: p1})
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{Policy: p2})

	resp1, _ := client.ListPolicies(ctx, &pb.ListPoliciesRequest{ClusterName: "cluster-1"})
	if diff := cmp.Diff(&pb.ListPoliciesResponse{Policies: []*pb.Policy{p1}}, resp1, protocmp.Transform()); diff != "" {
		t.Errorf("Cluster 1 mismatch (-want +got):\n%s", diff)
	}

	resp2, _ := client.ListPolicies(ctx, &pb.ListPoliciesRequest{ClusterName: "cluster-2"})
	if diff := cmp.Diff(&pb.ListPoliciesResponse{Policies: []*pb.Policy{p2}}, resp2, protocmp.Transform()); diff != "" {
		t.Errorf("Cluster 2 mismatch (-want +got):\n%s", diff)
	}
}

func TestUpdatePolicy_FullUpdate(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	p1 := &pb.Policy{
		Id:          id,
		MinReplicas: 1,
		MaxReplicas: 10,
		Workload:    &pb.WorkloadRef{Name: "app1"},
		Metrics:     []*pb.MetricDefinition{{Name: "cpu", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
	}

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{Policy: p1})

	p2 := &pb.Policy{
		Id:          id,
		MinReplicas: 2,
		MaxReplicas: 20,
		Workload:    &pb.WorkloadRef{Name: "app2"},
		Metrics:     []*pb.MetricDefinition{{Name: "cpu", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
	}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{Policy: p2})

	resp, _ := client.ListPolicies(ctx, &pb.ListPoliciesRequest{ClusterName: "c1"})
	if diff := cmp.Diff(&pb.ListPoliciesResponse{Policies: []*pb.Policy{p2}}, resp, protocmp.Transform()); diff != "" {
		t.Errorf("Policy update mismatch (-want +got):\n%s", diff)
	}
}

// --- DeletePolicy ---

func TestDeletePolicy_InvalidID(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.DeletePolicy(ctx, &pb.DeletePolicyRequest{Id: &pb.PolicyId{}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestDeletePolicy_Effective(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{Policy: &pb.Policy{Id: id}})

	client.DeletePolicy(ctx, &pb.DeletePolicyRequest{Id: id})

	resp, _ := client.ListPolicies(ctx, &pb.ListPoliciesRequest{ClusterName: "c1"})
	if diff := cmp.Diff(&pb.ListPoliciesResponse{}, resp, protocmp.Transform()); diff != "" {
		t.Errorf("Expected empty response after delete (-want +got):\n%s", diff)
	}
}

func TestDeletePolicy_Idempotent(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	_, err := client.DeletePolicy(ctx, &pb.DeletePolicyRequest{Id: id})
	if err != nil {
		t.Errorf("First delete failed: %v", err)
	}
	_, err = client.DeletePolicy(ctx, &pb.DeletePolicyRequest{Id: id})
	if err != nil {
		t.Errorf("Second delete failed: %v", err)
	}
}

// --- ListPolicies ---

func TestListPolicies_InvalidCluster(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.ListPolicies(ctx, &pb.ListPoliciesRequest{ClusterName: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestListPolicies_Filtering(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	p1 := &pb.Policy{Id: &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}}
	p2 := &pb.Policy{Id: &pb.PolicyId{ClusterName: "c2", Namespace: "ns", Name: "p2"}}

	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{Policy: p1})
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{Policy: p2})

	resp, _ := client.ListPolicies(ctx, &pb.ListPoliciesRequest{ClusterName: "c1"})
	if diff := cmp.Diff(&pb.ListPoliciesResponse{Policies: []*pb.Policy{p1}}, resp, protocmp.Transform()); diff != "" {
		t.Errorf("ListPolicies filtering mismatch (-want +got):\n%s", diff)
	}
}

func TestListPolicies_Empty(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	resp, _ := client.ListPolicies(ctx, &pb.ListPoliciesRequest{ClusterName: "non-existent"})
	if diff := cmp.Diff(&pb.ListPoliciesResponse{}, resp, protocmp.Transform()); diff != "" {
		t.Errorf("Expected empty response for non-existent cluster (-want +got):\n%s", diff)
	}
}

// --- UpdateWorkload ---

func TestUpdateWorkload_InvalidID(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: ""},
		Workload: &pb.Workload{},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestUpdateWorkload_NotFound(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "non-existent"},
		Workload: &pb.Workload{},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", err)
	}
}

func TestUpdateWorkload_InvalidPodState(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{Policy: &pb.Policy{Id: id}})

	_, err := client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: id,
		Workload: &pb.Workload{
			Pods: []*pb.PodState{{Name: ""}}, // Missing Name
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument for missing pod name, got %v", err)
	}
}

// --- GetControlMetrics ---

func TestGetControlMetrics_InvalidID(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: &pb.PolicyId{}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestGetControlMetrics_NotFound(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{
		Id: &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "non-existent"},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", err)
	}
}

func TestGetControlMetrics_Aggregation(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{Name: "g_avg", Gauge: &pb.Gauge{Aggregation: "Avg"}},
				{Name: "c_sum", Rate: &pb.Rate{Window: "1m", Aggregation: "Sum"}},
				{Name: "h_p99", Distribution: &pb.Distribution{Percentile: "p99", Aggregation: "Max"}},
			},
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: id,
		Workload: &pb.Workload{
			Pods: []*pb.PodState{
				{Name: "p1", IsReady: true},
				{Name: "p2", IsReady: true},
			},
		},
	})

	ts := clk.Now().Unix()
	// T0 for counter
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{
				{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "c_sum", Value: 0}, {Name: "h_p99", HistogramBuckets: map[string]uint64{"1.0": 0, "+Inf": 0}}}},
				{EntityKey: "p2", Samples: []*pb.MetricSample{{Name: "c_sum", Value: 0}, {Name: "h_p99", HistogramBuckets: map[string]uint64{"1.0": 0, "+Inf": 0}}}},
			},
		}},
	})

	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{
				{EntityKey: "p1", Samples: []*pb.MetricSample{
					{Name: "g_avg", Value: 10},
					{Name: "c_sum", Value: 10}, // Diff=10, Rate=1.0
					{Name: "h_p99", HistogramBuckets: map[string]uint64{"1.0": 99, "+Inf": 100}},
				}},
				{EntityKey: "p2", Samples: []*pb.MetricSample{
					{Name: "g_avg", Value: 30},
					{Name: "c_sum", Value: 20}, // Diff=20, Rate=2.0
					{Name: "h_p99", HistogramBuckets: map[string]uint64{"1.0": 99, "+Inf": 100}},
				}},
			},
		}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM := &pb.ControlMetrics{
		Values: map[string]float64{
			"g_avg": 20.0, // (10+30)/2
			"c_sum": 3.0,  // (1.0+2.0)
			"h_p99": 1.0,  // P99 in [0, 1.0]
		},
		Timestamp:     ts,
		ReadyReplicas: 2,
	}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_PodScope(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{Name: "cpu_global", Gauge: &pb.Gauge{Aggregation: "Avg"}, Scope: "Global"},
				{Name: "cpu_pod", Gauge: &pb.Gauge{Aggregation: "Avg"}, Scope: "Pod"},
			},
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: id,
		Workload: &pb.Workload{
			Pods: []*pb.PodState{
				{Name: "pod1", IsReady: true},
				{Name: "pod2", IsReady: true},
			},
		},
	})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{
				{EntityKey: "pod1", Samples: []*pb.MetricSample{
					{Name: "cpu_global", Value: 10},
					{Name: "cpu_pod", Value: 10},
				}},
				{EntityKey: "pod2", Samples: []*pb.MetricSample{
					{Name: "cpu_global", Value: 30},
					{Name: "cpu_pod", Value: 30},
				}},
			},
		}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM := &pb.ControlMetrics{
		Values: map[string]float64{
			"cpu_global": 20.0, // Aggregated: (10 + 30) / 2
			// cpu_pod should NOT be in Values
		},
		PodMetrics: map[string]*pb.PodMetrics{
			"pod1": {Values: map[string]float64{"cpu_pod": 10.0}},
			"pod2": {Values: map[string]float64{"cpu_pod": 30.0}},
		},
		Timestamp:     ts,
		ReadyReplicas: 2,
	}

	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics with Pod scope mismatch (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_IgnoreNotReadyPods(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:      id,
			Metrics: []*pb.MetricDefinition{{Name: "m1", Gauge: &pb.Gauge{Aggregation: "Sum"}}},
		},
	})

	// Register one ready pod and one not ready pod
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: id,
		Workload: &pb.Workload{
			Pods: []*pb.PodState{
				{Name: "pod-ready", IsReady: true},
				{Name: "pod-not-ready", IsReady: false},
			},
		},
	})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{
				{EntityKey: "pod-ready", Samples: []*pb.MetricSample{{Name: "m1", Value: 10}}},
				{EntityKey: "pod-not-ready", Samples: []*pb.MetricSample{{Name: "m1", Value: 100}}}, // Should be ignored
				{EntityKey: "pod-unknown", Samples: []*pb.MetricSample{{Name: "m1", Value: 50}}},    // Should be ignored
			},
		}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM := &pb.ControlMetrics{
		Values:        map[string]float64{"m1": 10},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform()); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_PolicyUpdate(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:      id,
			Metrics: []*pb.MetricDefinition{{Name: "m1", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{Id: id, Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}}})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "m1", Value: 10}}}}}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})
	wantCM1 := &pb.ControlMetrics{
		Values:        map[string]float64{"m1": 10},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM1, cm, protocmp.Transform()); diff != "" {
		t.Errorf("Initial ControlMetrics mismatch (-want +got):\n%s", diff)
	}

	// Update Policy: Remove m1, add m2
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:      id,
			Metrics: []*pb.MetricDefinition{{Name: "m2", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
		},
	})

	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "m2", Value: 20}}}}}},
	})

	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM2 := &pb.ControlMetrics{
		Values:        map[string]float64{"m2": 20},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM2, cm, protocmp.Transform()); diff != "" {
		t.Errorf("Updated ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_SlidingWindow(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{
					Name: "m_instant", Gauge: &pb.Gauge{Aggregation: "Avg"},
				},
				{
					Name: "c_avg", Rate: &pb.Rate{Window: "1m", Aggregation: "Avg"},
				},
			},
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{Id: id, Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}}})

	// T0
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{
			{Name: "m_instant", Value: 10},
			{Name: "c_avg", Value: 0},
		}}}}},
	})

	// T1 (30s later)
	clk.Advance(30 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{
			{Name: "m_instant", Value: 20},
			{Name: "c_avg", Value: 10}, // Rate = 10/30 = 0.333
		}}}}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})
	wantCM1 := &pb.ControlMetrics{
		Values: map[string]float64{
			"m_instant": 20.0,
			"c_avg":     0.333,
		},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM1, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch at T1 (-want +got):\n%s", diff)
	}

	// T2 (90s after T0): 10 should be gone from window
	clk.Advance(60 * time.Second)
	ts = clk.Now().Unix()
	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})
	wantCM2 := &pb.ControlMetrics{
		Values: map[string]float64{
			"m_instant": 20.0,
			"c_avg":     0.333,
		},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM2, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch at T2 (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_DecayingHistogramWindow(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{
					Name: "m_max", DecayingDistribution: &pb.DecayingDistribution{HalfLife: "1m", BucketSize: "0.1", Percentile: "p100"},
				},
				{
					Name: "c_max", DecayingDistribution: &pb.DecayingDistribution{HalfLife: "1m", BucketSize: "0.1", Percentile: "p100", Rate: "1m"},
				},
			},
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{Id: id, Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}}})

	// T0
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{
			{Name: "m_max", Value: 1.0},
			{Name: "c_max", Value: 0},
		}}}}},
	})

	// T1 (30s later)
	clk.Advance(30 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{
			{Name: "c_max", Value: 30}, // Rate = 1.0
		}}}}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM := &pb.ControlMetrics{
		Values: map[string]float64{
			"m_max": 1.1,
			"c_max": 1.1,
		},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	// Using EquateApprox for bucket upper bounds (0.1 bucket size)
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.11)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

// --- UpdateRecommenderState ---

func TestUpdateRecommenderState_InvalidID(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestUpdateRecommenderState_NotFound(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id:              &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "missing"},
		RecommenderName: "r1",
		Vote:            &pb.RecommenderVote{DesiredReplicas: 1},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", err)
	}
}

func TestUpdateRecommenderState_NotDefined(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{Policy: &pb.Policy{Id: id}})

	_, err := client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id:              id,
		RecommenderName: "undefined",
		Vote:            &pb.RecommenderVote{DesiredReplicas: 1},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestUpdateRecommenderState_EmptyClears(t *testing.T) {
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:      id,
			Scaling: []*pb.RecommenderDefinition{{Name: "r1", Recommender: "Linear", Type: "Linear", Mode: "Active"}},
		},
	})

	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: id, RecommenderName: "r1", Vote: &pb.RecommenderVote{DesiredReplicas: 5, IsActive: true},
	})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: id})
	wantResp1 := &pb.GetRecommendationResponse{
		Recommendation: &pb.Recommendation{
			TargetReplicas: 5,
			Explanation:    []*pb.RecommenderStatus{{Name: "r1", Type: "Linear", Phase: "Scaling", Mode: "Active", DesiredReplicas: 5, IsActive: true}},
		},
		MetricStatuses: []*pb.MetricStatus{},
	}
	// Note: Enrichment logic adds LastUpdated, which we might want to ignore or match in cmp.Diff
	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.IgnoreFields(&pb.RecommenderStatus{}, "last_updated"),
	}

	if diff := cmp.Diff(wantResp1, resp, opts...); diff != "" {
		t.Errorf("Initial GetRecommendation mismatch (-want +got):\n%s", diff)
	}

	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: id, RecommenderName: "r1", Vote: nil,
	})

	memStore.CalculateAll()
	resp, _ = client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: id})
	if diff := cmp.Diff(&pb.GetRecommendationResponse{}, resp, protocmp.Transform()); diff != "" {
		t.Errorf("Expected empty response after clearing state (-want +got):\n%s", diff)
	}
}

func TestUpdateRecommenderState_VerticalResources(t *testing.T) {
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Scaling: []*pb.RecommenderDefinition{
				{Name: "r1", Recommender: "AddonResizer", Type: "AddonResizer", Mode: "Active"},
			},
		},
	})

	vote := &pb.RecommenderVote{
		IsActive:        true,
		DesiredReplicas: 0, // AddonResizer might not vote on replicas
		WorkloadResources: &pb.ResourceRecommendation{
			Requests: map[string]string{"cpu": "100m", "memory": "200Mi"},
		},
		PodResources: []*pb.PodResourceRecommendation{
			{PodName: "pod1", Requests: map[string]string{"cpu": "200m"}},
			{PodName: "pod2", Limits: map[string]string{"memory": "1Gi"}},
		},
	}

	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: id, RecommenderName: "r1", Vote: vote,
	})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: id})

	wantResp := &pb.GetRecommendationResponse{
		Recommendation: &pb.Recommendation{
			TargetReplicas: 0,
			Explanation: []*pb.RecommenderStatus{
				{
					Name:              "r1",
					Type:              "AddonResizer",
					Phase:             "Scaling",
					Mode:              "Active",
					IsActive:          true,
					DesiredReplicas:   0,
					WorkloadResources: vote.WorkloadResources,
					PodResources:      vote.PodResources,
				},
			},
		},
		MetricStatuses: []*pb.MetricStatus{},
	}

	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.IgnoreFields(&pb.RecommenderStatus{}, "last_updated"),
	}

	if diff := cmp.Diff(wantResp, resp, opts...); diff != "" {
		t.Errorf("Vertical resources mismatch (-want +got):\n%s", diff)
	}
}

func TestUpdateRecommenderState_Validation(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Scaling: []*pb.RecommenderDefinition{
				{Name: "r1", Recommender: "Linear", Type: "Linear", Mode: "Active"},
			},
		},
	})

	tests := []struct {
		name string
		vote *pb.RecommenderVote
	}{
		{"Negative Replicas", &pb.RecommenderVote{DesiredReplicas: -1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
				Id:              id,
				RecommenderName: "r1",
				Vote:            tc.vote,
			})
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

	_, err := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: &pb.PolicyId{}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestGetRecommendation_NotFound(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{
		Id: &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "missing"},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", err)
	}
}

func TestGetRecommendation_NoRecommenders(t *testing.T) {
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{Policy: &pb.Policy{Id: id}})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: id})
	if diff := cmp.Diff(&pb.GetRecommendationResponse{}, resp, protocmp.Transform()); diff != "" {
		t.Errorf("Expected empty response for policy with no recommenders (-want +got):\n%s", diff)
	}
}

func TestGetRecommendation_NeverCalled(t *testing.T) {
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id:      id,
			Scaling: []*pb.RecommenderDefinition{{Name: "r1", Recommender: "Linear", Mode: "Active"}},
		},
	})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: id})
	if diff := cmp.Diff(&pb.GetRecommendationResponse{}, resp, protocmp.Transform()); diff != "" {
		t.Errorf("Expected empty response before any recommender state update (-want +got):\n%s", diff)
	}
}

func TestGetRecommendation_Aggregation(t *testing.T) {
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Activation: []*pb.RecommenderDefinition{
				{Name: "act1", Recommender: "Threshold", Type: "Threshold", Mode: "Active"},
				{Name: "act2", Recommender: "Threshold", Type: "Threshold", Mode: "Active"},
			},
			Scaling: []*pb.RecommenderDefinition{
				{Name: "scale1", Recommender: "Linear", Type: "Linear", Mode: "Active"},
				{Name: "scale2", Recommender: "Linear", Type: "Linear", Mode: "Active"},
			},
		},
	})

	// Scenario: act1=true, act2=false -> Active (OR)
	//           scale1=10, scale2=20 -> 20 (MAX)
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: id, RecommenderName: "act1", Vote: &pb.RecommenderVote{IsActive: true},
	})
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: id, RecommenderName: "act2", Vote: &pb.RecommenderVote{IsActive: false},
	})
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: id, RecommenderName: "scale1", Vote: &pb.RecommenderVote{DesiredReplicas: 10, IsActive: true},
	})
	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: id, RecommenderName: "scale2", Vote: &pb.RecommenderVote{DesiredReplicas: 20, IsActive: true},
	})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: id})

	wantResp := &pb.GetRecommendationResponse{
		Recommendation: &pb.Recommendation{
			TargetReplicas: 20,
			Explanation: []*pb.RecommenderStatus{
				{Name: "scale1", Type: "Linear", Phase: "Scaling", Mode: "Active", DesiredReplicas: 10, IsActive: true},
				{Name: "scale2", Type: "Linear", Phase: "Scaling", Mode: "Active", DesiredReplicas: 20, IsActive: true},
				{Name: "act1", Type: "Threshold", Phase: "Activation", Mode: "Active", IsActive: true},
				{Name: "act2", Type: "Threshold", Phase: "Activation", Mode: "Active", IsActive: false},
			},
		},
		MetricStatuses: []*pb.MetricStatus{},
	}
	// Sort for comparison
	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.SortRepeated(func(a, b *pb.RecommenderStatus) bool { return a.Name < b.Name }),
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

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{Name: "m1", Gauge: &pb.Gauge{Aggregation: "Avg"}},
				{Name: "m2", Gauge: &pb.Gauge{Aggregation: "Avg"}},
			},
			Scaling: []*pb.RecommenderDefinition{{Name: "r1", Recommender: "Linear", Type: "Linear", Mode: "Active"}},
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{Id: id, Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}}})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "m1", Value: 10}}}}}},
	})
	// m2 has no data

	client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
		Id: id, RecommenderName: "r1", Vote: &pb.RecommenderVote{DesiredReplicas: 5, IsActive: true},
	})

	memStore.CalculateAll()
	resp, _ := client.GetRecommendation(ctx, &pb.GetRecommendationRequest{Id: id})

	wantResp := &pb.GetRecommendationResponse{
		Recommendation: &pb.Recommendation{
			TargetReplicas: 5,
			Explanation:    []*pb.RecommenderStatus{{Name: "r1", Type: "Linear", Phase: "Scaling", Mode: "Active", DesiredReplicas: 5, IsActive: true}},
		},
		MetricStatuses: []*pb.MetricStatus{
			{Name: "m1", Value: 10, Timestamp: ts},
			{Name: "m2", Error: "No data available", Timestamp: ts},
		},
	}
	// Sort for comparison
	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.SortRepeated(func(a, b *pb.MetricStatus) bool { return a.Name < b.Name }),
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

	_, err := client.IngestMetrics(ctx, &pb.IngestMetricsRequest{ClusterName: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestIngestMetrics_NotFound(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	_, err := client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1",
		Policies:    []*pb.PolicyBatch{{Namespace: "ns", Name: "missing"}},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", err)
	}
}

func TestIngestMetrics_NotDefinedMetric(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{Policy: &pb.Policy{Id: id}})

	_, err := client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1",
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "undefined"}}}},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument for undefined metric, got %v", err)
	}
}

func TestIngestMetrics_Global(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{Name: "g_sum", Gauge: &pb.Gauge{Aggregation: "Sum"}},
				{Name: "c_sum", Rate: &pb.Rate{Window: "1m", Aggregation: "Sum"}},
				{Name: "h_p50", Distribution: &pb.Distribution{Percentile: "p50", Aggregation: "Max"}},
			},
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{Id: id, Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}}})

	ts := clk.Now().Unix()
	// T0 for counter
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "", Samples: []*pb.MetricSample{
			{Name: "c_sum", Value: 0},
			{Name: "h_p50", HistogramBuckets: map[string]uint64{"1.0": 0, "+Inf": 0}},
		}}}}},
	})

	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "", Samples: []*pb.MetricSample{
			{Name: "g_sum", Value: 100},
			{Name: "c_sum", Value: 10}, // Rate = 1.0
			{Name: "h_p50", HistogramBuckets: map[string]uint64{"1.0": 100, "+Inf": 100}}, // P50 = 0.5
		}}}}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM := &pb.ControlMetrics{
		Values: map[string]float64{
			"g_sum": 100,
			"c_sum": 1.0,
			"h_p50": 0.5,
		},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestGetControlMetrics_AggregatedDecayingHistogram(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{
					Name: "m_hist", DecayingDistribution: &pb.DecayingDistribution{
						HalfLife:   "1h",
						BucketSize: "1.0",
						Percentile: "p50",
					},
				},
			},
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: id,
		Workload: &pb.Workload{
			Pods: []*pb.PodState{
				{Name: "p1", IsReady: true},
				{Name: "p2", IsReady: true},
			},
		},
	})

	// Ingest 10 from p1 and 20 from p2
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{
				{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "m_hist", Value: 10}}},
				{EntityKey: "p2", Samples: []*pb.MetricSample{{Name: "m_hist", Value: 20}}},
			},
		}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	// Aggregate samples: [10, 20]. Weight total = 2.0. Target p50 Rank = 1.0.
	// Bucket 10 has weight 1.0. It reaches target.
	// Upper bound of bucket 10 is 11.0.
	if val := cm.Values["m_hist"]; val != 11.0 {
		t.Errorf("Aggregated Histogram: Expected 11.0 (upper bound of bucket 10), got %f", val)
	}

	// Now check p95 (should reach bucket 20)
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{
					Name: "m_hist", DecayingDistribution: &pb.DecayingDistribution{
						HalfLife:   "1h",
						BucketSize: "1.0",
						Percentile: "p95",
					},
				},
			},
		},
	})

	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	// Target p95 Rank = 2.0 * 0.95 = 1.9.
	// Bucket 10 reaches 1.0.
	// Bucket 20 reaches 2.0. (Target met)
	if val := cm.Values["m_hist"]; val != 21.0 {
		t.Errorf("Aggregated Histogram (P95): Expected 21.0 (upper bound of bucket 20), got %f", val)
	}

	// Verify time decay on aggregated histogram
	// Advance by 1 hour (one half-life)
	clk.Advance(1 * time.Hour)
	ts = clk.Now().Unix()

	// Update policy back to p50 for decay check
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{
					Name: "m_hist", DecayingDistribution: &pb.DecayingDistribution{
						HalfLife:   "1h",
						BucketSize: "1.0",
						Percentile: "p50",
					},
				},
			},
		},
	})

	// Ingest a new large sample (weight 1.0, vs old samples now weight 0.5 each -> total 1.0)
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{
				{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "m_hist", Value: 100}}},
			},
		}},
	})

	memStore.CalculateAll()
	cm, _ = client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	// New total weight = 1.0 (new) + 0.5 (old 10) + 0.5 (old 20) = 2.0.
	// Target p50 Rank = 1.0.
	// Bucket 10 has weight 0.5.
	// Bucket 20 has weight 0.5. (Cumulative = 1.0). Target met.
	if val := cm.Values["m_hist"]; val != 21.0 {
		t.Errorf("Aggregated Histogram (Decay): Expected 21.0, got %f", val)
	}
}

func TestGetControlMetrics_DecayingHistogramCrossPod(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{
					Name: "cpu", DecayingDistribution: &pb.DecayingDistribution{HalfLife: "1m", BucketSize: "0.1", Percentile: "p100"},
				},
			},
		},
	})

	// Step 1: Add pod-1 and ingest high value.
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: id,
		Workload: &pb.Workload{
			Pods: []*pb.PodState{{Name: "pod-1", IsReady: true}},
		},
	})

	ts0 := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts0,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{{EntityKey: "pod-1", Samples: []*pb.MetricSample{{Name: "cpu", Value: 1.0}}}},
		}},
	})

	memStore.CalculateAll()
	cm1, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM1 := &pb.ControlMetrics{
		Values:        map[string]float64{"cpu": 1.1}, // p100 of bucket 1.0 (size 0.1)
		Timestamp:     ts0,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM1, cm1, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("Step 1 mismatch (-want +got):\n%s", diff)
	}

	// Step 2: Update workload (delete pod-1, add pod-2) and ingest low value.
	// The high value from pod-1 should persist in the global decaying histogram.
	clk.Advance(10 * time.Second)
	ts1 := clk.Now().Unix()
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: id,
		Workload: &pb.Workload{
			Pods: []*pb.PodState{{Name: "pod-2", IsReady: true}},
		},
	})

	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts1,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{{EntityKey: "pod-2", Samples: []*pb.MetricSample{{Name: "cpu", Value: 0.1}}}},
		}},
	})

	memStore.CalculateAll()
	cm2, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM2 := &pb.ControlMetrics{
		Values:        map[string]float64{"cpu": 1.1}, // Persistent from pod-1
		Timestamp:     ts1,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM2, cm2, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("Step 2 mismatch (-want +got):\n%s", diff)
	}

	// Step 3: Advance time significantly to decay pod-1's value.
	// We re-ingest pod-2's value to keep it fresh and prevent it from decaying away.
	clk.Advance(30 * time.Minute)
	ts2 := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts2,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{{EntityKey: "pod-2", Samples: []*pb.MetricSample{{Name: "cpu", Value: 0.1}}}},
		}},
	})

	memStore.CalculateAll()
	cm3, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM3 := &pb.ControlMetrics{
		Values:        map[string]float64{"cpu": 0.2}, // Now pod-1 has decayed, pod-2 is max
		Timestamp:     ts2,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM3, cm3, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("Step 3 mismatch (-want +got):\n%s", diff)
	}
}

// --- Intent-Based Metrics ---

func TestIntent_GaugeAggregation(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{Name: "g_avg", Gauge: &pb.Gauge{Aggregation: "Avg"}},
				{Name: "g_sum", Gauge: &pb.Gauge{Aggregation: "Sum"}},
				{Name: "g_max", Gauge: &pb.Gauge{Aggregation: "Max"}},
			},
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: id,
		Workload: &pb.Workload{
			Pods: []*pb.PodState{
				{Name: "p1", IsReady: true},
				{Name: "p2", IsReady: true},
			},
		},
	})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{
				{EntityKey: "p1", Samples: []*pb.MetricSample{
					{Name: "g_avg", Value: 10},
					{Name: "g_sum", Value: 10},
					{Name: "g_max", Value: 10},
				}},
				{EntityKey: "p2", Samples: []*pb.MetricSample{
					{Name: "g_avg", Value: 30},
					{Name: "g_sum", Value: 20},
					{Name: "g_max", Value: 40},
				}},
			},
		}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM := &pb.ControlMetrics{
		Values: map[string]float64{
			"g_avg": 20.0, // (10+30)/2
			"g_sum": 30.0, // 10+20
			"g_max": 40.0, // max(10, 40)
		},
		Timestamp:     ts,
		ReadyReplicas: 2,
	}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestIntent_RateCalculation(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{Name: "rps", Rate: &pb.Rate{Window: "1m", Aggregation: "Sum"}},
			},
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: id,
		Workload: &pb.Workload{
			Pods: []*pb.PodState{{Name: "p1", IsReady: true}}},
	})

	// T0
	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "rps", Value: 100}}}},
		}},
	})

	// T1 (10s later)
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "rps", Value: 110}}}},
		}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	// Rate = (110 - 100) / 10s = 1.0
	wantCM := &pb.ControlMetrics{
		Values:        map[string]float64{"rps": 1.0},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestIntent_DistributionPercentile(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{Name: "latency", Distribution: &pb.Distribution{Percentile: "p99", Aggregation: "Max"}},
			},
		},
	})

	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: id,
		Workload: &pb.Workload{
			Pods: []*pb.PodState{{Name: "p1", IsReady: true}}},
	})

	ts := clk.Now().Unix()
	// T0 for counter-based buckets
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "latency", HistogramBuckets: map[string]uint64{"1.0": 0, "+Inf": 0}}}}},
		}},
	})

	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{{EntityKey: "p1", Samples: []*pb.MetricSample{{Name: "latency", HistogramBuckets: map[string]uint64{"1.0": 99, "+Inf": 100}}}}},
		}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM := &pb.ControlMetrics{
		Values:        map[string]float64{"latency": 1.0},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestIntent_DecayingDistributionPersistence(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{Name: "peak_cpu", DecayingDistribution: &pb.DecayingDistribution{HalfLife: "1m", BucketSize: "0.1", Percentile: "p100"}},
			},
		},
	})

	// Step 1: Add pod-1 and ingest high value
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: id,
		Workload: &pb.Workload{
			Pods: []*pb.PodState{{Name: "pod-1", IsReady: true}},
		},
	})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{{EntityKey: "pod-1", Samples: []*pb.MetricSample{{Name: "peak_cpu", Value: 1.5}}}},
		}},
	})

	memStore.CalculateAll()
	cm1, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})
	if val := cm1.Values["peak_cpu"]; val < 1.5 || val > 1.6 {
		t.Errorf("Expected peak_cpu around 1.6 (upper bound), got %f", val)
	}

	// Step 2: Replace pod-1 with pod-2 (lower value)
	// The high value should persist
	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id: id,
		Workload: &pb.Workload{
			Pods: []*pb.PodState{{Name: "pod-2", IsReady: true}},
		},
	})
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{{EntityKey: "pod-2", Samples: []*pb.MetricSample{{Name: "peak_cpu", Value: 0.5}}}},
		}},
	})

	memStore.CalculateAll()
	cm2, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})
	if val := cm2.Values["peak_cpu"]; val < 1.5 || val > 1.6 {
		t.Errorf("Expected peak_cpu to persist around 1.6, got %f", val)
	}
}

func TestIntent_Validation(t *testing.T) {
	_, client, cleanup := setupFunctionalGRPCServer(t, clock.RealClock{})
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}

	tests := []struct {
		name   string
		metric *pb.MetricDefinition
	}{
		{
			name: "Multiple Intents",
			metric: &pb.MetricDefinition{
				Name:  "m1",
				Gauge: &pb.Gauge{Aggregation: "Avg"},
				Rate:  &pb.Rate{Window: "1m"},
			},
		},
		{
			name: "Rate Missing Window",
			metric: &pb.MetricDefinition{
				Name: "m1",
				Rate: &pb.Rate{Aggregation: "Sum"},
			},
		},
		{
			name: "Distribution Missing Percentile",
			metric: &pb.MetricDefinition{
				Name:         "m1",
				Distribution: &pb.Distribution{Aggregation: "Max"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
				Policy: &pb.Policy{
					Id:      id,
					Metrics: []*pb.MetricDefinition{tc.metric},
				},
			})
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

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{Name: "g_avg", Gauge: &pb.Gauge{Aggregation: "Avg"}, Scope: "Pod"},
			},
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       id,
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "pod1", IsReady: true}, {Name: "pod2", IsReady: true}}},
	})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{
				{EntityKey: "pod1", Samples: []*pb.MetricSample{{Name: "g_avg", Value: 10}}},
				{EntityKey: "pod2", Samples: []*pb.MetricSample{{Name: "g_avg", Value: 30}}},
			},
		}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM := &pb.ControlMetrics{
		Values: map[string]float64{},
		PodMetrics: map[string]*pb.PodMetrics{
			"pod1": {Values: map[string]float64{"g_avg": 10.0}},
			"pod2": {Values: map[string]float64{"g_avg": 30.0}},
		},
		Timestamp:     ts,
		ReadyReplicas: 2,
	}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestIntent_PodScope_Rate(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{Name: "c_rate", Rate: &pb.Rate{Window: "1m", Aggregation: "Avg"}, Scope: "Pod"},
			},
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       id,
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "pod1", IsReady: true}}},
	})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "pod1", Samples: []*pb.MetricSample{{Name: "c_rate", Value: 0}}}}}},
	})

	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "pod1", Samples: []*pb.MetricSample{{Name: "c_rate", Value: 10}}}}}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM := &pb.ControlMetrics{
		Values: map[string]float64{},
		PodMetrics: map[string]*pb.PodMetrics{
			"pod1": {Values: map[string]float64{"c_rate": 1.0}}, // Rate = 10/10s = 1.0
		},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestIntent_PodScope_Distribution(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{Name: "h_p99", Distribution: &pb.Distribution{Percentile: "p99", Aggregation: "Max"}, Scope: "Pod"},
			},
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       id,
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "pod1", IsReady: true}}},
	})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "pod1", Samples: []*pb.MetricSample{{Name: "h_p99", HistogramBuckets: map[string]uint64{"1.0": 0, "+Inf": 0}}}}}}},
	})

	clk.Advance(10 * time.Second)
	ts = clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{Namespace: "ns", Name: "p1", Batches: []*pb.MetricBatch{{EntityKey: "pod1", Samples: []*pb.MetricSample{{Name: "h_p99", HistogramBuckets: map[string]uint64{"1.0": 99, "+Inf": 100}}}}}}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM := &pb.ControlMetrics{
		Values: map[string]float64{},
		PodMetrics: map[string]*pb.PodMetrics{
			"pod1": {Values: map[string]float64{"h_p99": 1.0}},
		},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}

func TestIntent_PodScope_DecayingDistribution(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	memStore, client, cleanup := setupFunctionalGRPCServer(t, clk)
	defer cleanup()
	ctx := context.Background()

	id := &pb.PolicyId{ClusterName: "c1", Namespace: "ns", Name: "p1"}
	client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{
		Policy: &pb.Policy{
			Id: id,
			Metrics: []*pb.MetricDefinition{
				{Name: "decay_p100", DecayingDistribution: &pb.DecayingDistribution{HalfLife: "1m", BucketSize: "0.1", Percentile: "p100"}, Scope: "Pod"},
			},
		},
	})
	client.UpdateWorkload(ctx, &pb.UpdateWorkloadRequest{
		Id:       id,
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "pod1", IsReady: true}}},
	})

	ts := clk.Now().Unix()
	client.IngestMetrics(ctx, &pb.IngestMetricsRequest{
		ClusterName: "c1", Timestamp: ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: "ns", Name: "p1",
			Batches: []*pb.MetricBatch{{EntityKey: "pod1", Samples: []*pb.MetricSample{{Name: "decay_p100", Value: 1.0}}}},
		}},
	})

	memStore.CalculateAll()
	cm, _ := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{Id: id})

	wantCM := &pb.ControlMetrics{
		Values: map[string]float64{},
		PodMetrics: map[string]*pb.PodMetrics{
			"pod1": {Values: map[string]float64{"decay_p100": 1.1}}, // Upper bound of bucket 1.0 (size 0.1)
		},
		Timestamp:     ts,
		ReadyReplicas: 1,
	}
	if diff := cmp.Diff(wantCM, cm, protocmp.Transform(), cmpopts.EquateApprox(0, 0.01)); diff != "" {
		t.Errorf("ControlMetrics mismatch (-want +got):\n%s", diff)
	}
}
