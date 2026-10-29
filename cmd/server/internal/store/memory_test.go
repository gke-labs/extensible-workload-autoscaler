package store

import (
	"encoding/json"
	"errors"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
)

func TestMetricCalculations(t *testing.T) {
	tests := []struct {
		name     string
		metrics  []*pb.MetricDefinition
		ingest   func(s *MemoryStore, clk *clock.FakeClock, ns, pol string)
		workload []*pb.PodState
		want     map[string]float64
	}{
		{
			name: "Counters: Rate Calculation (Avg)",
			metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("requests"), Rate: pb.Rate_builder{Window: durationpb.New(time.Minute), Aggregation: proto.String("Avg")}.Build()}.Build(),
			},
			workload: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build()},
			ingest: func(s *MemoryStore, clk *clock.FakeClock, ns, pol string) {
				// T0
				ingest(s, clk.Now().Unix(), ns, pol, "p1", "requests", 100)
				ingest(s, clk.Now().Unix(), ns, pol, "p2", "requests", 200)
				// T1 (+10s)
				clk.Advance(10 * time.Second)
				// p1: +20 in 10s = 2.0 req/s
				// p2: +40 in 10s = 4.0 req/s
				ingest(s, clk.Now().Unix(), ns, pol, "p1", "requests", 120)
				ingest(s, clk.Now().Unix(), ns, pol, "p2", "requests", 240)
			},
			// Avg(2.0, 4.0) = 3.0
			want: map[string]float64{"requests": 3.0},
		},
		{
			name: "Counters: Rate Calculation (Sum)",
			metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("requests"), Rate: pb.Rate_builder{Window: durationpb.New(time.Minute), Aggregation: proto.String("Sum")}.Build()}.Build(),
			},
			workload: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build()},
			ingest: func(s *MemoryStore, clk *clock.FakeClock, ns, pol string) {
				ingest(s, clk.Now().Unix(), ns, pol, "p1", "requests", 100)
				ingest(s, clk.Now().Unix(), ns, pol, "p2", "requests", 200)
				clk.Advance(10 * time.Second)
				ingest(s, clk.Now().Unix(), ns, pol, "p1", "requests", 120)
				ingest(s, clk.Now().Unix(), ns, pol, "p2", "requests", 240)
			},
			// Sum(2.0, 4.0) = 6.0
			want: map[string]float64{"requests": 6.0},
		},
		{
			name: "Gauges: Max Aggregation",
			metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("queue_depth"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Max")}.Build()}.Build(),
			},
			workload: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build()},
			ingest: func(s *MemoryStore, clk *clock.FakeClock, ns, pol string) {
				ingest(s, clk.Now().Unix(), ns, pol, "p1", "queue_depth", 10)
				ingest(s, clk.Now().Unix(), ns, pol, "p2", "queue_depth", 50)
			},
			// Max(10, 50) = 50
			want: map[string]float64{"queue_depth": 50},
		},
		{
			name: "Gauges: Min Aggregation",
			metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("latency"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Min")}.Build()}.Build(),
			},
			workload: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build()},
			ingest: func(s *MemoryStore, clk *clock.FakeClock, ns, pol string) {
				ingest(s, clk.Now().Unix(), ns, pol, "p1", "latency", 100)
				ingest(s, clk.Now().Unix(), ns, pol, "p2", "latency", 50)
			},
			// Min(100, 50) = 50
			want: map[string]float64{"latency": 50},
		},
		{
			name: "Histogram: Percentile (P90) Global",
			metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{
					Name: proto.String("hist"), Distribution: pb.Distribution_builder{Percentile: proto.String("p90")}.Build(),
				}.Build(),
			},
			workload: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build()},
			ingest: func(s *MemoryStore, clk *clock.FakeClock, ns, pol string) {
				// T0
				b0 := map[string]uint64{"0.1": 0, "0.5": 0, "+Inf": 0}
				ingestHist(s, clk.Now().Unix(), ns, pol, "p1", "hist", b0)
				ingestHist(s, clk.Now().Unix(), ns, pol, "p2", "hist", b0)

				// T1 (+10s)
				clk.Advance(10 * time.Second)
				// P1: 10 requests. 8 in 0.1, 2 in 0.5.
				b1 := map[string]uint64{"0.1": 8, "0.5": 10, "+Inf": 10}
				// P2: 10 requests. 2 in 0.1, 8 in 0.5.
				b2 := map[string]uint64{"0.1": 2, "0.5": 10, "+Inf": 10}

				ingestHist(s, clk.Now().Unix(), ns, pol, "p1", "hist", b1)
				ingestHist(s, clk.Now().Unix(), ns, pol, "p2", "hist", b2)
			},
			// Aggregated Rates (over 10s):
			// 0.1 bucket: (8 + 2) = 10 count -> 1.0 rate
			// 0.5 bucket: (10 + 10) = 20 count -> 2.0 rate
			// +Inf bucket: 20 count -> 2.0 rate
			//
			// Distribution:
			// LE 0.1: 1.0/s
			// LE 0.5: 2.0/s (Includes 0.1) -> 1.0/s in range (0.1, 0.5]
			// Total: 2.0/s
			// Target Rank: 0.9 * 2.0 = 1.8
			//
			// Bucket 0.1 has 1.0. (< 1.8)
			// Bucket 0.5 has 2.0. (>= 1.8)
			//
			// Interpolate in (0.1, 0.5]:
			// PrevLe=0.1, PrevCount=1.0.
			// NextLe=0.5, NextCount=2.0.
			// CountDiff = 1.0.
			// RankDiff = 1.8 - 1.0 = 0.8.
			// Fraction = 0.8 / 1.0 = 0.8.
			// Width = 0.4.
			// Result = 0.1 + (0.4 * 0.8) = 0.1 + 0.32 = 0.42.
			want: map[string]float64{"hist": 0.42},
		},
		{
			name: "Filter: By Label (DEPRECATED - now Gauge)",
			metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{
					Name: proto.String("filtered_gauge"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Sum")}.Build(),
				}.Build(),
			},
			workload: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()},
			ingest: func(s *MemoryStore, clk *clock.FakeClock, ns, pol string) {
				ingest(s, clk.Now().Unix(), ns, pol, "p1", "filtered_gauge", 100)
			},
			want: map[string]float64{"filtered_gauge": 100},
		},
		{
			name: "Counters: Rapid Updates (Same Timestamp)",
			metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("reqs"), Rate: pb.Rate_builder{Window: durationpb.New(time.Minute), Aggregation: proto.String("Sum")}.Build()}.Build(),
			},
			workload: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()},
			ingest: func(s *MemoryStore, clk *clock.FakeClock, ns, pol string) {
				t0 := clk.Now().Unix()
				ingest(s, t0, ns, pol, "p1", "reqs", 100)
				ingest(s, t0, ns, pol, "p1", "reqs", 150) // Same timestamp, should update LastRaw.Value to 150

				clk.Advance(10 * time.Second)
				t1 := clk.Now().Unix()
				ingest(s, t1, ns, pol, "p1", "reqs", 250) // diff = 250 - 150 = 100. rate = 100/10 = 10
			},
			want: map[string]float64{"reqs": 10.0},
		},
		{
			name: "Readiness: Ignore NotReady Pods",
			metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
			},
			// p2 is NotReady
			workload: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(false)}.Build()},
			ingest: func(s *MemoryStore, clk *clock.FakeClock, ns, pol string) {
				ingest(s, clk.Now().Unix(), ns, pol, "p1", "cpu", 10)
				ingest(s, clk.Now().Unix(), ns, pol, "p2", "cpu", 1000) // Should be ignored
			},
			// Avg(10) = 10. (Not 505)
			want: map[string]float64{"cpu": 10.0},
		},
		{
			name: "Window: Stale gauge is held",
			metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
			},
			workload: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()},
			ingest: func(s *MemoryStore, clk *clock.FakeClock, ns, pol string) {
				// A gauge that stopped reporting 70s ago is still reported
				// until its series is garbage collected.
				oldTime := clk.Now().Add(-70 * time.Second).Unix()
				ingest(s, oldTime, ns, pol, "p1", "cpu", 100)
			},
			want: map[string]float64{"cpu": 100.0},
		},
		{
			name: "Window: Data Expiry",
			metrics: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
			},
			workload: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()},
			ingest: func(s *MemoryStore, clk *clock.FakeClock, ns, pol string) {
				// The series stopped reporting longer than seriesRetention
				// ago: it is garbage collected.
				oldTime := clk.Now().Add(-seriesRetention - time.Minute).Unix()
				ingest(s, oldTime, ns, pol, "p1", "cpu", 100)
			},
			// No series left -> No metric calculated
			want: map[string]float64{}, // Empty
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Unix(1000, 0)
			clk := &clock.FakeClock{CurrentTime: start}
			s := NewMemoryStoreWithClock(clk)

			policyName := "test-policy"
			ns := "default"

			policy := pb.Policy_builder{
				Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
				Metrics:  tc.metrics,
				Workload: pb.WorkloadRef_builder{Name: proto.String("app")}.Build(),
			}.Build()
			s.UpdatePolicy("default", true, policy)

			if tc.workload != nil {
				s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
					Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build(),
					Workload: pb.Workload_builder{Pods: tc.workload}.Build(),
				}.Build())
			}

			tc.ingest(s, clk, ns, policyName)

			s.CalculateAll()

			cm := policyControlMetrics(s, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(policyName)}.Build())
			got := make(map[string]float64)
			if cm != nil {
				got = cm.GetValues()
			}

			// For missing keys in want (empty map), got should be empty or nil
			if len(tc.want) == 0 && len(got) > 0 {
				t.Errorf("Expected no metrics, got %v", got)
			}

			if diff := cmp.Diff(tc.want, got, cmpopts.EquateApprox(0, 0.0001)); diff != "" {
				t.Errorf("Metric calculation mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// --- Helpers ---

// policyControlMetrics returns the policy-wide control metrics of a policy,
// nil if it has not been calculated.
func policyControlMetrics(s *MemoryStore, id *pb.PolicyId) *pb.ControlMetrics {
	res, _ := s.GetControlMetrics(id)
	return res.GetControlMetrics()
}

func ingest(s *MemoryStore, ts int64, ns, pol, pod, metric string, val float64) {
	ingestWithLabels(s, ts, ns, pol, pod, metric, val, nil)
}

func ingestWithLabels(s *MemoryStore, ts int64, ns, pol, pod, metric string, val float64, labels map[string]string) {
	s.AddBatch(pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		PolicyMetrics: []*pb.PolicyMetricBatch{pb.PolicyMetricBatch_builder{
			Namespace: proto.String(ns), Name: proto.String(pol),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String(pod),
				Samples: []*pb.MetricSample{pb.MetricSample_builder{
					Name:      proto.String(metric),
					Value:     proto.Float64(val),
					Labels:    labels,
					Timestamp: timestamppb.New(time.Unix(ts, 0)),
				}.Build()},
			}.Build()},
		}.Build()},
	}.Build())
}

func ingestHist(s *MemoryStore, ts int64, ns, pol, pod, metric string, buckets map[string]uint64) {
	s.AddBatch(pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		PolicyMetrics: []*pb.PolicyMetricBatch{pb.PolicyMetricBatch_builder{
			Namespace: proto.String(ns), Name: proto.String(pol),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String(pod),
				Samples: []*pb.MetricSample{pb.MetricSample_builder{
					Name:      proto.String(metric),
					Histogram: pb.Histogram_builder{Buckets: buckets}.Build(),
					Timestamp: timestamppb.New(time.Unix(ts, 0)),
				}.Build()},
			}.Build()},
		}.Build()},
	}.Build())
}

func TestRecommendationArbitration(t *testing.T) {
	// Verify Max(Scaling) and OR(Activation) logic
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := NewMemoryStoreWithClock(clk)

	policy := pb.Policy_builder{
		Id:          pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String("arb-pol")}.Build(),
		MinReplicas: proto.Int32(1),
		MaxReplicas: proto.Int32(10),
		Scaling: []*pb.RecommenderDefinition{
			pb.RecommenderDefinition_builder{Name: proto.String("r1"), Recommender: proto.String("Linear"), Type: proto.String("Linear")}.Build(),
			pb.RecommenderDefinition_builder{Name: proto.String("r2"), Recommender: proto.String("Linear"), Type: proto.String("Linear")}.Build(),
		},
		Activation: []*pb.RecommenderDefinition{
			pb.RecommenderDefinition_builder{Name: proto.String("a1"), Recommender: proto.String("Threshold"), Type: proto.String("Threshold")}.Build(),
		},
	}.Build()
	s.UpdatePolicy("default", true, policy)

	// Case 1: All Active. R1=3, R2=5. Result=5.
	s.UpdateRecommenderState(pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String("arb-pol")}.Build(), RecommenderName: proto.String("r1"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(3), IsActive: proto.Bool(true)}.Build(),
	}.Build())
	s.UpdateRecommenderState(pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String("arb-pol")}.Build(), RecommenderName: proto.String("r2"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(5), IsActive: proto.Bool(true)}.Build(),
	}.Build())
	s.UpdateRecommenderState(pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String("arb-pol")}.Build(), RecommenderName: proto.String("a1"),
		Recommendation: pb.Recommendation_builder{IsActive: proto.Bool(true)}.Build(),
	}.Build())

	s.CalculateAll()
	rec, _ := s.GetArbitratedRecommendation(pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String("arb-pol")}.Build())
	if rec.GetRecommendation().GetReplicas() != 5 {
		t.Errorf("Case 1: Want 5, Got %d", rec.GetRecommendation().GetReplicas())
	}

	// Case 2: Inactive (Scale to Zero). a1=False.
	s.UpdateRecommenderState(pb.UpdateRecommenderStateRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String("arb-pol")}.Build(), RecommenderName: proto.String("a1"),
		Recommendation: pb.Recommendation_builder{IsActive: proto.Bool(false)}.Build(),
	}.Build())
	// Force window expiry (default 300s window in store logic)
	clk.Advance(301 * time.Second)
	// Must trigger calculation to update LastActive logic inside store
	// (Note: Store updates LastActive on *every* CalculateAll call if any recommender says active)
	// Here a1 says inactive.
	s.CalculateAll()

	rec, _ = s.GetArbitratedRecommendation(pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String("arb-pol")}.Build())
	if rec.GetRecommendation().GetReplicas() != 0 {
		t.Errorf("Case 2: Want 0 (Inactive), Got %d", rec.GetRecommendation().GetReplicas())
	}
}

func TestDump(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := NewMemoryStoreWithClock(clk)

	// 1. Policy
	policy := pb.Policy_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String("dump-pol")}.Build(),
		Metrics: []*pb.MetricDefinition{
			pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
		},
		Scaling: []*pb.RecommenderDefinition{
			pb.RecommenderDefinition_builder{Name: proto.String("cpu-rec"), Recommender: proto.String("Linear"), Type: proto.String("Linear"), Params: map[string]string{"metric": "cpu", "target": "0.5"}}.Build(),
		},
		Workload:    pb.WorkloadRef_builder{Name: proto.String("app")}.Build(),
		MinReplicas: proto.Int32(1),
		MaxReplicas: proto.Int32(10),
	}.Build()
	stored := mustUpdatePolicy(t, s, "default", policy)
	if stored.GetEtag() == "" {
		t.Fatal("UpdatePolicy() returned a policy without an ETag")
	}

	// 2. Workload
	s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String("dump-pol")}.Build(),
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{
				pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(),
			},
		}.Build(),
	}.Build())

	// 3. Ingest (Series)
	s.AddBatch(pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(1000, 0)),
		PolicyMetrics: []*pb.PolicyMetricBatch{pb.PolicyMetricBatch_builder{
			Namespace: proto.String("default"), Name: proto.String("dump-pol"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("p1"),
				Samples: []*pb.MetricSample{pb.MetricSample_builder{
					Name: proto.String("cpu"), Value: proto.Float64(1.0), Timestamp: timestamppb.New(time.Unix(1000, 0)),
				}.Build()},
			}.Build()},
		}.Build()},
	}.Build())

	// 4. Calculate (ControlMetrics, RecommenderStatuses, arbitrated Recommendation)
	s.UpdateRecommenderState(pb.UpdateRecommenderStateRequest_builder{
		Id:              pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("default"), Name: proto.String("dump-pol")}.Build(),
		RecommenderName: proto.String("cpu-rec"),
		Recommendation: pb.Recommendation_builder{
			Replicas: proto.Int32(2),
			IsActive: proto.Bool(true),
		}.Build(),
	}.Build())

	s.CalculateAll()

	dump := s.Dump()
	data, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal dump: %v", err)
	}

	expected := `{
  "default/default/dump-pol": {
    "Policy": {
      "id": {
        "cluster_name": "default",
        "namespace": "default",
        "name": "dump-pol"
      },
      "workload": {
        "name": "app"
      },
      "min_replicas": 1,
      "max_replicas": 10,
      "metrics": [
        {
          "name": "cpu",
          "gauge": {
            "aggregation": "Avg"
          }
        }
      ],
      "scaling": [
        {
          "recommender": "Linear",
          "name": "cpu-rec",
          "params": {
            "metric": "cpu",
            "target": "0.5"
          },
          "type": "Linear"
        }
      ],
      "etag": "ETAG"
    },
    "Workload": {
      "p1": {
        "name": "p1",
        "is_ready": true
      }
    },
    "Metrics": {
      "Series": {
        "cpu": {
          "Series": {
            "p1||0000000000000000": {
              "PodName": "p1",
              "ContainerName": "",
              "ResourceName": "",
              "Labels": null,
              "LastRaw": {
                "Timestamp": "1970-01-01T00:16:40Z",
                "Value": 1,
                "Distribution": null,
                "DecayingHistogram": null
              },
              "LastRawWindow": 0,
              "LastIngested": "1970-01-01T00:16:40Z"
            }
          },
          "ResourceName": ""
        }
      }
    },
    "ArbitratedRecommendation": {
      "replicas": 2
    },
    "Explanation": [
      {
        "is_active": true,
        "message": "",
        "last_updated": "1970-01-01T00:16:40Z",
        "phase": "Scaling",
        "mode": "",
        "name": "cpu-rec",
        "type": "Linear",
        "replicas": 2
      }
    ],
    "LastActive": "1970-01-01T00:16:40Z",
    "RecommenderStatuses": {
      "cpu-rec": {
        "is_active": true,
        "message": "",
        "last_updated": "1970-01-01T00:16:40Z",
        "phase": "Scaling",
        "mode": "",
        "name": "cpu-rec",
        "type": "Linear",
        "replicas": 2
      }
    },
    "ControlMetrics": {
      "timestamp": "1970-01-01T00:16:40Z",
      "ready_replicas": 1,
      "control_metrics": {
        "values": {
          "cpu": 1
        }
      }
    }
  }
}`
	expected = strings.Replace(expected, `"etag": "ETAG"`, `"etag": "`+stored.GetEtag()+`"`, 1)

	if string(data) != expected {
		t.Errorf("Dump JSON mismatch.\nWant:\n%s\nGot:\n%s", expected, string(data))
	}
}

func TestWindowedMetrics(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := NewMemoryStoreWithClock(clk)
	ns, pol := "default", "window-pol"

	// 1. Policy with Windowed Metrics
	policy := pb.Policy_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build(),
		Metrics: []*pb.MetricDefinition{
			pb.MetricDefinition_builder{
				Name: proto.String("cpu_hist"), DecayingDistribution: pb.DecayingDistribution_builder{
					HalfLife:   durationpb.New(24 * time.Hour),
					BucketSize: proto.String("0.1"),
					Percentile: proto.String("p100"),
				}.Build(),
			}.Build(),
			pb.MetricDefinition_builder{
				Name: proto.String("cpu_slide"), Gauge: pb.Gauge_builder{
					Aggregation: proto.String("Avg"),
				}.Build(),
			}.Build(),
		},
		Workload: pb.WorkloadRef_builder{Name: proto.String("app")}.Build(),
	}.Build()
	s.UpdatePolicy("default", true, policy)

	s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	// 2. Ingest Data (Spiky)
	// T0: Low usage
	ingest(s, clk.Now().Unix(), ns, pol, "p1", "cpu_hist", 0.1)
	ingest(s, clk.Now().Unix(), ns, pol, "p1", "cpu_slide", 0.1)

	// T1: Spike (1m later)
	clk.Advance(1 * time.Minute)
	ingest(s, clk.Now().Unix(), ns, pol, "p1", "cpu_hist", 1.0)
	ingest(s, clk.Now().Unix(), ns, pol, "p1", "cpu_slide", 1.0)

	// T2: Back to normal (2m later)
	clk.Advance(1 * time.Minute)
	ingest(s, clk.Now().Unix(), ns, pol, "p1", "cpu_hist", 0.1)
	ingest(s, clk.Now().Unix(), ns, pol, "p1", "cpu_slide", 0.1)

	s.CalculateAll()
	cm := policyControlMetrics(s, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build())

	// Histogram should remember the 1.0 spike (approx 1.0 bucket upper bound -> 1.1)
	// Sliding Window (Max) should see 1.0.
	gotHist := cm.GetValues()["cpu_hist"]
	gotSlide := cm.GetValues()["cpu_slide"]

	// Decaying Histogram p100 should capture the max bucket.
	// 1.0 falls into bucket [1.0, 1.1) or similar depending on implementation.
	// Let's just check it's >= 1.0
	if gotHist < 1.0 {
		t.Errorf("Histogram: Want >= 1.0, Got %f", gotHist)
	}

	// Gauge is now instantaneous, latest value is 0.1
	if gotSlide != 0.1 {
		t.Errorf("Gauge: Want 0.1, Got %f", gotSlide)
	}

	// T3: 6m later. Latest is 0.2.
	clk.Advance(6 * time.Minute)
	// Ingest new low value to trigger update
	ingest(s, clk.Now().Unix(), ns, pol, "p1", "cpu_slide", 0.2)

	s.CalculateAll()
	cm = policyControlMetrics(s, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build())

	// Gauge should see 0.2.
	if cm.GetValues()["cpu_slide"] != 0.2 {
		t.Errorf("Gauge: Want 0.2, Got %f", cm.GetValues()["cpu_slide"])
	}
}

func TestAggregatedDecayingHistogram(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := NewMemoryStoreWithClock(clk)
	ns, pol := "default", "agg-hist-pol"

	// Policy with DecayingHistogram
	policy := pb.Policy_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build(),
		Metrics: []*pb.MetricDefinition{
			pb.MetricDefinition_builder{
				Name: proto.String("cpu"), DecayingDistribution: pb.DecayingDistribution_builder{
					HalfLife:   durationpb.New(24 * time.Hour),
					BucketSize: proto.String("0.1"),
					Percentile: proto.String("p95"),
				}.Build(),
			}.Build(),
		},
		Workload: pb.WorkloadRef_builder{Name: proto.String("app")}.Build(),
	}.Build()
	s.UpdatePolicy("default", true, policy)

	s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	// Ingest Data:
	// Pod 1 always sees 0.5
	// Pod 2 always sees 1.5
	// Current behavior (per-pod p95s):
	// p95(p1) = 0.6 (approx), p95(p2) = 1.6 (approx)
	// Avg(0.6, 1.6) = 1.1
	//
	// Desired behavior (workload-level p95):
	// All samples: [0.5, 1.5, 0.5, 1.5, ...]
	// p95 of these samples should be 1.6 (approx) because 50% are 1.5.

	ingest(s, clk.Now().Unix(), ns, pol, "p1", "cpu", 0.5)
	ingest(s, clk.Now().Unix(), ns, pol, "p2", "cpu", 1.5)

	s.CalculateAll()
	cm := policyControlMetrics(s, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build())
	val := cm.GetValues()["cpu"]

	// If it's 1.1, it's per-pod aggregation (Avg of p95s).
	// If it's >= 1.5, it's workload-level aggregation.
	if val < 1.5 {
		t.Errorf("Aggregated Histogram: Want >= 1.5, Got %f", val)
	}
}

func TestDeletePolicy(t *testing.T) {
	s := NewMemoryStore()
	ns, name := "default", "pol"
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(name)}.Build()

	// 1. Setup state
	s.UpdatePolicy("default", true, pb.Policy_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(name)}.Build(),
	}.Build())
	s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{Id: id, Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1")}.Build()}}.Build()}.Build())
	s.UpdateRecommenderState(pb.UpdateRecommenderStateRequest_builder{Id: id, RecommenderName: proto.String("r1"), Recommendation: pb.Recommendation_builder{IsActive: proto.Bool(true)}.Build()}.Build())

	// 2. Delete
	s.DeletePolicy(id)

	// 3. Verify
	if s.GetPolicy(id) != nil {
		t.Error("Policy still exists after deletion")
	}

	// Internal maps should be empty (or at least key should be missing)
	dump := s.Dump().(map[policyID]*PolicyState)
	if _, exists := dump[policyID{cluster: "default", ns: "default", name: "pol"}]; exists {
		t.Error("PolicyState still exists after deletion")
	}
}

func TestMultiTenantIsolation(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := NewMemoryStoreWithClock(clk)
	ns, name := "default", "common-policy"

	// 1. Setup Policies for Cluster A and Cluster B
	polA := pb.Policy_builder{
		Id:      pb.PolicyId_builder{ClusterName: proto.String("cluster-A"), Namespace: proto.String(ns), Name: proto.String(name)}.Build(),
		Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
	}.Build()
	s.UpdatePolicy("cluster-A", true, polA)

	polB := pb.Policy_builder{
		Id:      pb.PolicyId_builder{ClusterName: proto.String("cluster-B"), Namespace: proto.String(ns), Name: proto.String(name)}.Build(),
		Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
	}.Build()
	s.UpdatePolicy("cluster-B", true, polB)

	// 2. Setup Workloads
	s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("cluster-A"), Namespace: proto.String(ns), Name: proto.String(name)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("pod-a"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())
	s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("cluster-B"), Namespace: proto.String(ns), Name: proto.String(name)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("pod-b"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	// 3. Ingest Data (Value 100 for A, 200 for B)
	s.AddBatch(pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("cluster-A"), Timestamp: timestamppb.New(time.Unix(1000, 0)),
		PolicyMetrics: []*pb.PolicyMetricBatch{pb.PolicyMetricBatch_builder{
			Namespace: proto.String(ns), Name: proto.String(name),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod-a"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m"), Value: proto.Float64(100), Timestamp: timestamppb.New(time.Unix(1000, 0))}.Build()}}.Build()},
		}.Build()},
	}.Build())
	s.AddBatch(pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("cluster-B"), Timestamp: timestamppb.New(time.Unix(1000, 0)),
		PolicyMetrics: []*pb.PolicyMetricBatch{pb.PolicyMetricBatch_builder{
			Namespace: proto.String(ns), Name: proto.String(name),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{PodName: proto.String("pod-b"), Samples: []*pb.MetricSample{pb.MetricSample_builder{Name: proto.String("m"), Value: proto.Float64(200), Timestamp: timestamppb.New(time.Unix(1000, 0))}.Build()}}.Build()},
		}.Build()},
	}.Build())

	s.CalculateAll()

	// 4. Verify Isolation
	resA, okA := s.GetControlMetrics(pb.PolicyId_builder{ClusterName: proto.String("cluster-A"), Namespace: proto.String(ns), Name: proto.String(name)}.Build())
	if !okA || resA.GetControlMetrics().GetValues()["m"] != 100 {
		t.Errorf("Cluster A: Want 100, Got %v", resA)
	}

	resB, okB := s.GetControlMetrics(pb.PolicyId_builder{ClusterName: proto.String("cluster-B"), Namespace: proto.String(ns), Name: proto.String(name)}.Build())
	if !okB || resB.GetControlMetrics().GetValues()["m"] != 200 {
		t.Errorf("Cluster B: Want 200, Got %v", resB)
	}

	// 5. Verify ListPolicies Filtering
	listA := s.ListPolicies("cluster-A")
	if len(listA) != 1 {
		t.Errorf("ListPolicies(cluster-A): Want 1, Got %d", len(listA))
	}
	listB := s.ListPolicies("cluster-B")
	if len(listB) != 1 {
		t.Errorf("ListPolicies(cluster-B): Want 1, Got %d", len(listB))
	}
	listAll := s.ListPolicies("") // All
	if len(listAll) != 2 {
		t.Errorf("ListPolicies(''): Want 2, Got %d", len(listAll))
	}
}

func TestOrphanedMetricCleanup(t *testing.T) {
	// A fake clock keeps the ingested samples within the GC window, so that
	// only the orphan cleanup removes metrics.
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	s := NewMemoryStoreWithClock(clk)
	ns, name := "default", "pol"

	// 1. Setup policy with 2 metrics
	pol := pb.Policy_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(name)}.Build(),
		Metrics: []*pb.MetricDefinition{
			pb.MetricDefinition_builder{Name: proto.String("m1"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
			pb.MetricDefinition_builder{Name: proto.String("m2"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
		},
	}.Build()
	stored := mustUpdatePolicy(t, s, "default", pol)

	// 2. Ingest data for both
	reportReadyPods(t, s, ns, name, "p1")
	ingest(s, clk.Now().Unix(), ns, name, "p1", "m1", 10)
	ingest(s, clk.Now().Unix(), ns, name, "p1", "m2", 20)

	// 3. Update policy: remove m1
	pol.SetMetrics([]*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("m2"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()})
	pol.SetEtag(stored.GetEtag())
	mustUpdatePolicy(t, s, "default", pol)

	// 4. Calculate
	s.CalculateAll()

	// 5. Verify m1 is gone from series map
	dump := s.Dump().(map[policyID]*PolicyState)
	policySeries := dump[policyID{cluster: "default", ns: "default", name: "pol"}].Metrics.Series

	if _, exists := policySeries[metricID{name: "m1"}]; exists {
		t.Error("Orphaned metric m1 still exists in series map")
	}
	if _, exists := policySeries[metricID{name: "m2"}]; !exists {
		t.Error("Active metric m2 was incorrectly deleted")
	}
}

func TestRemoveRecommender(t *testing.T) {
	s := NewMemoryStore()
	ns, name := "default", "pol"
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(name)}.Build()

	// 1. Setup policy with 1 recommender
	pol := pb.Policy_builder{
		Id:          id,
		MinReplicas: proto.Int32(1),
		MaxReplicas: proto.Int32(10),
		Scaling: []*pb.RecommenderDefinition{
			pb.RecommenderDefinition_builder{Name: proto.String("r1"), Recommender: proto.String("Linear"), Type: proto.String("Linear"), Params: map[string]string{"target": "10"}}.Build(),
		},
	}.Build()
	stored := mustUpdatePolicy(t, s, "default", pol)

	// 2. Simulate R1 recommendation = 10
	s.UpdateRecommenderState(pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("r1"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(10), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	s.CalculateAll()
	rec, _ := s.GetArbitratedRecommendation(id)
	if rec.GetRecommendation().GetReplicas() != 10 {
		t.Errorf("Initial: Want 10, Got %d", rec.GetRecommendation().GetReplicas())
	}

	// 3. Update Policy: Remove R1
	pol.SetScaling([]*pb.RecommenderDefinition{}) // Empty list
	pol.SetEtag(stored.GetEtag())
	mustUpdatePolicy(t, s, "default", pol)

	s.CalculateAll()
	rec, ok := s.GetArbitratedRecommendation(id)
	if !ok || rec.HasRecommendation() {
		t.Errorf("After removal: Want nil recommendation, Got %v (ok=%v)", rec.GetRecommendation(), ok)
	}

	// 4. Simulate Zombie R1 recommendation = 100
	s.UpdateRecommenderState(pb.UpdateRecommenderStateRequest_builder{
		Id: id, RecommenderName: proto.String("r1"),
		Recommendation: pb.Recommendation_builder{Replicas: proto.Int32(100), IsActive: proto.Bool(true)}.Build(),
	}.Build())

	s.CalculateAll()
	rec, ok = s.GetArbitratedRecommendation(id)
	if !ok || rec.HasRecommendation() {
		t.Errorf("After zombie recommendation: Want nil recommendation, Got %v (ok=%v)", rec.GetRecommendation(), ok)
	}

	// 5. Verify cleanup of internal RecommenderStatuses map
	dump := s.Dump().(map[policyID]*PolicyState)
	if _, exists := dump[policyID{cluster: "default", ns: "default", name: "pol"}].RecommenderStatuses["r1"]; exists {
		t.Error("Orphaned status for r1 still exists in RecommenderStatuses map")
	}
}

func TestPodScopedDecayingHistogram(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := NewMemoryStoreWithClock(clk)
	ns, pol := "default", "pod-hist-pol"

	// Policy with Pod-Scoped DecayingHistogram
	policy := pb.Policy_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build(),
		Metrics: []*pb.MetricDefinition{
			pb.MetricDefinition_builder{
				Name:  proto.String("cpu"),
				Scope: proto.String("Pod"),
				DecayingDistribution: pb.DecayingDistribution_builder{
					HalfLife:   durationpb.New(24 * time.Hour),
					BucketSize: proto.String("0.1"),
					Percentile: proto.String("p95"),
				}.Build(),
			}.Build(),
		},
		Workload: pb.WorkloadRef_builder{Name: proto.String("app")}.Build(),
	}.Build()
	s.UpdatePolicy("default", true, policy)

	s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	ingest(s, clk.Now().Unix(), ns, pol, "p1", "cpu", 0.5)
	ingest(s, clk.Now().Unix(), ns, pol, "p2", "cpu", 1.5)

	s.CalculateAll()
	cm := policyControlMetrics(s, pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build())

	if cm.GetValues()["cpu"] != 0 {
		t.Errorf("Pod scoped metric should not have a global value, got %f", cm.GetValues()["cpu"])
	}

	if cm.GetPodMetrics() == nil {
		t.Fatalf("PodMetrics is nil")
	}

	if p1Val := cm.GetPodMetrics()["p1"].GetValues()["cpu"]; p1Val < 0.5 {
		t.Errorf("Pod 1 scoped metric: Want >= 0.5, Got %f", p1Val)
	}

	if p2Val := cm.GetPodMetrics()["p2"].GetValues()["cpu"]; p2Val < 1.5 {
		t.Errorf("Pod 2 scoped metric: Want >= 1.5, Got %f", p2Val)
	}
}

func TestContainerResourceRequestWeighting(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	// The pods started long enough ago to be past their CPU initialization
	// period, so that every cpu sample counts.
	podStart := timestamppb.New(start.Add(-time.Hour))
	s := NewMemoryStoreWithClock(clk)
	ns, pol := "default", "weighted-pol"
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build()

	policy := pb.Policy_builder{
		Id: id,
		Metrics: []*pb.MetricDefinition{
			pb.MetricDefinition_builder{
				// Pod scope: container samples are weighted by their requests.
				Name:  proto.String("cpu_util"),
				Scope: proto.String("Pod"),
				Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build(),
			}.Build(),
			pb.MetricDefinition_builder{
				// PodContainer scope: raw per-container values, no weighting.
				Name:  proto.String("cpu_raw"),
				Scope: proto.String("PodContainer"),
				Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build(),
			}.Build(),
		},
		Workload: pb.WorkloadRef_builder{Name: proto.String("app")}.Build(),
	}.Build()
	s.UpdatePolicy("default", true, policy)

	s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{Pods: []*pb.PodState{
			pb.PodState_builder{
				Name:      proto.String("p1"),
				IsReady:   proto.Bool(true),
				StartTime: podStart,
				Containers: []*pb.ContainerState{
					pb.ContainerState_builder{Name: proto.String("c1"), Requests: map[string]string{"cpu": "100m"}}.Build(),
					pb.ContainerState_builder{Name: proto.String("c2"), Requests: map[string]string{"cpu": "300m"}}.Build(),
				},
			}.Build(),
			pb.PodState_builder{
				Name:      proto.String("p2"),
				IsReady:   proto.Bool(true),
				StartTime: podStart,
				Containers: []*pb.ContainerState{
					pb.ContainerState_builder{Name: proto.String("c1"), Requests: map[string]string{"cpu": "200m"}}.Build(),
					// c2 has no cpu request: it is left out of the weighted pod
					// value, but reported as is at PodContainer scope.
					pb.ContainerState_builder{Name: proto.String("c2"), Requests: map[string]string{"memory": "100Mi"}}.Build(),
				},
			}.Build(),
			pb.PodState_builder{
				// No container declares a cpu request, as with pods sized by
				// pod-level resources only.
				Name:      proto.String("p3"),
				IsReady:   proto.Bool(true),
				StartTime: podStart,
				Containers: []*pb.ContainerState{
					pb.ContainerState_builder{Name: proto.String("c1")}.Build(),
					pb.ContainerState_builder{Name: proto.String("c2")}.Build(),
				},
			}.Build(),
		}}.Build(),
	}.Build())

	ts := clk.Now().Unix()
	for _, m := range []string{"cpu_util", "cpu_raw"} {
		ingestResource(s, ts, ns, pol, "p1", "c1", m, "cpu", 0.5)
		ingestResource(s, ts, ns, pol, "p1", "c2", m, "cpu", 0.1)
		ingestResource(s, ts, ns, pol, "p2", "c1", m, "cpu", 0.4)
		ingestResource(s, ts, ns, pol, "p2", "c2", m, "cpu", 0.9)
		ingestResource(s, ts, ns, pol, "p3", "c1", m, "cpu", 0.3)
		ingestResource(s, ts, ns, pol, "p3", "c2", m, "cpu", 0.6)
	}

	s.CalculateAll()
	res, ok := s.GetControlMetrics(id)
	if !ok {
		t.Fatalf("No control metrics")
	}
	cm := res.GetControlMetrics()

	// p1: 0.5*(100/400) + 0.1*(300/400) = 0.2
	if got, want := cm.GetPodMetrics()["p1"].GetValues()["cpu_util"], 0.2; math.Abs(got-want) > 1e-9 {
		t.Errorf("Pod p1 weighted value: Want %f, Got %f", want, got)
	}
	// p2: c2 is left out of the weighted value, so c1 carries the full weight: 0.4*(200/200) = 0.4
	if got, want := cm.GetPodMetrics()["p2"].GetValues()["cpu_util"], 0.4; math.Abs(got-want) > 1e-9 {
		t.Errorf("Pod p2 weighted value: Want %f, Got %f", want, got)
	}
	// p3: no container is weightable, so there is no pod value.
	if _, exists := cm.GetPodMetrics()["p3"]; exists {
		t.Errorf("Pod p3 has no cpu request, it should have no weighted pod value")
	}

	// At PodContainer scope every container reports its raw (unweighted)
	// value, whether or not it declares a request.
	for _, tc := range []struct {
		pod, container string
		want           float64
	}{
		{"p1", "c2", 0.1},
		{"p2", "c2", 0.9},
		{"p3", "c1", 0.3},
		{"p3", "c2", 0.6},
	} {
		pcm, ok := cm.GetPodContainerMetrics()[tc.pod]
		if !ok || pcm.GetContainerMetrics()[tc.container] == nil {
			t.Errorf("Container %s/%s missing from the breakdown", tc.pod, tc.container)
			continue
		}
		if got := pcm.GetContainerMetrics()[tc.container].GetValues()["cpu_raw"]; math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("Container %s/%s value: Want %f, Got %f", tc.pod, tc.container, tc.want, got)
		}
	}
}

func TestVerticalResourceArbitration(t *testing.T) {
	clk := &clock.FakeClock{CurrentTime: time.Unix(1000, 0)}
	s := NewMemoryStoreWithClock(clk)

	pol := "vpa-pol"
	ns := "default"
	policy := pb.Policy_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build(),
		Scaling: []*pb.RecommenderDefinition{
			pb.RecommenderDefinition_builder{Name: proto.String("vpa-1"), Recommender: proto.String("PerPodVertical"), Mode: proto.String("Active")}.Build(),
			pb.RecommenderDefinition_builder{Name: proto.String("vpa-2"), Recommender: proto.String("PerPodVertical"), Mode: proto.String("Active")}.Build(),
			pb.RecommenderDefinition_builder{Name: proto.String("dry-1"), Recommender: proto.String("PerPodVertical"), Mode: proto.String("DryRun")}.Build(),
		},
		Workload: pb.WorkloadRef_builder{Name: proto.String("app")}.Build(),
	}.Build()
	s.UpdatePolicy("default", true, policy)

	s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build(),
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	// vpa-1 recommends 500m CPU for p1
	s.UpdateRecommenderState(pb.UpdateRecommenderStateRequest_builder{
		Id:              pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build(),
		RecommenderName: proto.String("vpa-1"),
		Recommendation: pb.Recommendation_builder{
			IsActive: proto.Bool(true),
			PodContainerResources: []*pb.PodContainerResource{
				pb.PodContainerResource_builder{PodName: proto.String("p1"), ContainerResources: pb.ContainerResource_builder{ContainerName: proto.String("main"), Requests: map[string]string{"cpu": "500m"}}.Build()}.Build(),
			},
		}.Build(),
	}.Build())

	// vpa-2 recommends 800m CPU for p1 (higher)
	s.UpdateRecommenderState(pb.UpdateRecommenderStateRequest_builder{
		Id:              pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build(),
		RecommenderName: proto.String("vpa-2"),
		Recommendation: pb.Recommendation_builder{
			IsActive: proto.Bool(true),
			PodContainerResources: []*pb.PodContainerResource{
				pb.PodContainerResource_builder{PodName: proto.String("p1"), ContainerResources: pb.ContainerResource_builder{ContainerName: proto.String("main"), Requests: map[string]string{"cpu": "800m"}}.Build()}.Build(),
			},
		}.Build(),
	}.Build())

	// dry-1 recommends 2000m CPU for p1 (DryRun, should be ignored for arbitration)
	s.UpdateRecommenderState(pb.UpdateRecommenderStateRequest_builder{
		Id:              pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build(),
		RecommenderName: proto.String("dry-1"),
		Recommendation: pb.Recommendation_builder{
			IsActive: proto.Bool(true),
			PodContainerResources: []*pb.PodContainerResource{
				pb.PodContainerResource_builder{PodName: proto.String("p1"), ContainerResources: pb.ContainerResource_builder{ContainerName: proto.String("main"), Requests: map[string]string{"cpu": "2000m"}}.Build()}.Build(),
			},
		}.Build(),
	}.Build())

	s.CalculateAll()

	resp, ok := s.GetArbitratedRecommendation(pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build())
	if !ok || !resp.HasRecommendation() {
		t.Fatalf("Expected recommendation, got nil")
	}

	rec := resp.GetRecommendation()
	if len(rec.GetPodContainerResources()) != 1 {
		t.Fatalf("Want 1 arbitrated pod resource, got %d", len(rec.GetPodContainerResources()))
	}

	gotPod := rec.GetPodContainerResources()[0]
	if gotPod.GetPodName() != "p1" || gotPod.GetContainerResources().GetContainerName() != "main" {
		t.Errorf("Unexpected pod/container: %s/%s", gotPod.GetPodName(), gotPod.GetContainerResources().GetContainerName())
	}

	if gotCpu := gotPod.GetContainerResources().GetRequests()["cpu"]; gotCpu != "800m" {
		t.Errorf("Arbitration want max 800m CPU (ignoring dry-run 2000m), got %s", gotCpu)
	}
}

func ingestResource(s *MemoryStore, ts int64, ns, pol, pod, container, metric, resourceName string, val float64) {
	s.AddBatch(pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		PolicyMetrics: []*pb.PolicyMetricBatch{pb.PolicyMetricBatch_builder{
			Namespace: proto.String(ns), Name: proto.String(pol),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName:       proto.String(pod),
				ContainerName: proto.String(container),
				Samples: []*pb.MetricSample{pb.MetricSample_builder{
					Name:         proto.String(metric),
					ResourceName: proto.String(resourceName),
					Value:        proto.Float64(val),
					Timestamp:    timestamppb.New(time.Unix(ts, 0)),
				}.Build()},
			}.Build()},
		}.Build()},
	}.Build())
}

// ingestOwned ingests a sample of a metric owned by a recommender.
func ingestOwned(s *MemoryStore, ts int64, ns, pol, pod, owner, metric string, val float64) {
	s.AddBatch(pb.IngestMetricsRequest_builder{
		ClusterName: proto.String("default"),
		Timestamp:   timestamppb.New(time.Unix(ts, 0)),
		PolicyMetrics: []*pb.PolicyMetricBatch{pb.PolicyMetricBatch_builder{
			Namespace: proto.String(ns), Name: proto.String(pol),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String(pod),
				Samples: []*pb.MetricSample{pb.MetricSample_builder{
					Name:            proto.String(metric),
					RecommenderName: proto.String(owner),
					Value:           proto.Float64(val),
					Timestamp:       timestamppb.New(time.Unix(ts, 0)),
				}.Build()},
			}.Build()},
		}.Build()},
	}.Build())
}

// TestRecommenderOwnedMetrics checks that metrics owned by a recommender are
// aggregated like policy-wide ones, but reported apart, keyed by their owner.
// A metric is identified by the <name, recommender> pair, so the same name can
// be used by several owners without them colliding.
func TestRecommenderOwnedMetrics(t *testing.T) {
	s := NewMemoryStore()
	ns, name := "default", "pol"
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(name)}.Build()

	pol := pb.Policy_builder{
		Id: id,
		Metrics: []*pb.MetricDefinition{
			pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
		},
		// "other" owns no metric.
		Scaling: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("vpa")}.Build(), pb.RecommenderDefinition_builder{Name: proto.String("hpa")}.Build(), pb.RecommenderDefinition_builder{Name: proto.String("other")}.Build()},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("vpa"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Max")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("memory"), RecommenderName: proto.String("vpa"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
			}}.Build(),
			"hpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("hpa"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
			}}.Build(),
		},
	}.Build()
	s.UpdatePolicy("default", true, pol)
	s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
		Id:       id,
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build(), pb.PodState_builder{Name: proto.String("p2"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	now := time.Now().Unix()
	ingest(s, now, ns, name, "p1", "cpu", 1)
	ingest(s, now, ns, name, "p2", "cpu", 3)
	ingestOwned(s, now, ns, name, "p1", "vpa", "cpu", 10)
	ingestOwned(s, now, ns, name, "p2", "vpa", "cpu", 30)
	ingestOwned(s, now, ns, name, "p1", "vpa", "memory", 100)
	ingestOwned(s, now, ns, name, "p1", "hpa", "cpu", 7)

	s.CalculateAll()

	res, ok := s.GetControlMetrics(id)
	if !ok {
		t.Fatal("GetControlMetrics() not found")
	}
	// The workload state is reported once, for all the owners.
	if res.GetReadyReplicas() != 2 {
		t.Errorf("GetControlMetrics() ready replicas = %d, want 2", res.GetReadyReplicas())
	}
	// Policy-wide metrics are unaffected by the owned ones: Avg(1, 3).
	if diff := cmp.Diff(map[string]float64{"cpu": 2}, res.GetControlMetrics().GetValues()); diff != "" {
		t.Errorf("GetControlMetrics() policy-wide values mismatch (-want +got):\n%s", diff)
	}

	// Each owner gets its metrics, aggregated its own way. A recommender
	// owning no metric has no entry.
	wantOwned := map[string]map[string]float64{
		// Max(10, 30), plus its own metric.
		"vpa": {"cpu": 30, "memory": 100},
		"hpa": {"cpu": 7},
	}
	rcm := res.GetRecommenderControlMetrics()
	gotOwned := make(map[string]map[string]float64, len(rcm))
	for owner, ocm := range rcm {
		gotOwned[owner] = ocm.GetValues()
	}
	if diff := cmp.Diff(wantOwned, gotOwned); diff != "" {
		t.Errorf("GetControlMetrics() owned values mismatch (-want +got):\n%s", diff)
	}
}

// TestRecommenderOwnedMetricsCleanup checks that the series of a metric are
// dropped once its owner stops declaring it.
func TestRecommenderOwnedMetricsCleanup(t *testing.T) {
	s := NewMemoryStore()
	ns, name := "default", "pol"
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(name)}.Build()

	pol := pb.Policy_builder{
		Id: id,
		Metrics: []*pb.MetricDefinition{
			pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
		},
		Scaling: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("vpa")}.Build()},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String("vpa"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
			}}.Build(),
		},
	}.Build()
	stored := mustUpdatePolicy(t, s, "default", pol)

	now := time.Now().Unix()
	reportReadyPods(t, s, ns, name, "p1")
	ingest(s, now, ns, name, "p1", "cpu", 1)
	ingestOwned(s, now, ns, name, "p1", "vpa", "cpu", 10)

	dump := s.Dump().(map[policyID]*PolicyState)
	if _, ok := dump[policyID{cluster: "default", ns: "default", name: "pol"}].Metrics.Series[metricID{name: "cpu", recommenderName: "vpa"}]; !ok {
		t.Fatal("Owned metric series was not tracked under its metric key")
	}

	// The recommender no longer owns any metric.
	pol.SetRecommenderMetrics(nil)
	pol.SetEtag(stored.GetEtag())
	mustUpdatePolicy(t, s, "default", pol)
	s.CalculateAll()

	series := s.Dump().(map[policyID]*PolicyState)[policyID{cluster: "default", ns: "default", name: "pol"}].Metrics.Series
	if _, ok := series[metricID{name: "cpu", recommenderName: "vpa"}]; ok {
		t.Error("Orphaned owned metric still exists in series map")
	}
	if _, ok := series[metricID{name: "cpu"}]; !ok {
		t.Error("Policy-wide metric was incorrectly deleted")
	}
}

// mustUpdatePolicy writes p to the store and fails the test on error. It
// returns the stored policy, which carries the new ETag.
// reportReadyPods reports the named pods of a policy as ready, so that their
// samples are ingested.
func reportReadyPods(t *testing.T, s *MemoryStore, ns, pol string, pods ...string) {
	t.Helper()
	states := make([]*pb.PodState, 0, len(pods))
	for _, name := range pods {
		states = append(states, pb.PodState_builder{Name: proto.String(name), IsReady: proto.Bool(true)}.Build())
	}
	if err := s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
		Id:       pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(pol)}.Build(),
		Workload: pb.Workload_builder{Pods: states}.Build(),
	}.Build()); err != nil {
		t.Fatalf("UpdateWorkload() = %v", err)
	}
}

func mustUpdatePolicy(t *testing.T, s *MemoryStore, clusterName string, p *pb.Policy) *pb.Policy {
	t.Helper()
	stored, err := s.UpdatePolicy(clusterName, true, p)
	if err != nil {
		t.Fatalf("UpdatePolicy(%s) error = %v", p.GetId().GetName(), err)
	}
	return stored
}

func TestCreatePolicy(t *testing.T) {
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("ns"), Name: proto.String("pol")}.Build()
	newPolicy := func(maxReplicas int32, etag string) *pb.Policy {
		return pb.Policy_builder{Id: id, MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(maxReplicas), Etag: proto.String(etag)}.Build()
	}

	tests := []struct {
		name     string
		existing bool
		etag     string
		wantErr  error
	}{
		{
			name: "new policy, empty etag: created",
		},
		{
			name:    "new policy, etag given: rejected",
			etag:    "some-etag",
			wantErr: ErrUnknownEtag,
		},
		{
			name:     "existing policy: rejected",
			existing: true,
			wantErr:  ErrPolicyAlreadyExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemoryStore()
			var before *pb.Policy
			if tt.existing {
				var err error
				before, err = s.CreatePolicy("default", newPolicy(10, ""))
				if err != nil {
					t.Fatalf("setup CreatePolicy() error = %v", err)
				}
			}

			got, err := s.CreatePolicy("default", newPolicy(20, tt.etag))
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("CreatePolicy() error = %v, want %v", err, tt.wantErr)
				}
				if got != nil {
					t.Errorf("CreatePolicy() returned %v on error, want nil", got)
				}
				after := s.GetPolicy(id)
				if (after != nil) != tt.existing || !proto.Equal(after, before) {
					t.Errorf("store changed on rejection: got %v, want %v", after, before)
				}
				return
			}

			if err != nil {
				t.Fatalf("CreatePolicy() error = %v", err)
			}
			if got.GetMaxReplicas() != 20 {
				t.Errorf("stored MaxReplicas = %d, want 20", got.GetMaxReplicas())
			}
			if got.GetEtag() == "" {
				t.Error("CreatePolicy() returned an empty ETag")
			}
		})
	}
}

func TestUpdatePolicyEtag(t *testing.T) {
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("ns"), Name: proto.String("pol")}.Build()
	newPolicy := func(maxReplicas int32, etag string) *pb.Policy {
		return pb.Policy_builder{Id: id, MinReplicas: proto.Int32(1), MaxReplicas: proto.Int32(maxReplicas), Etag: proto.String(etag)}.Build()
	}

	tests := []struct {
		name         string
		existing     bool // create the policy first
		allowMissing bool
		// etag returns the ETag to send, given the stored one ("" if none).
		etag    func(stored string) string
		wantErr error
	}{
		{
			name:         "new policy, allowMissing=false: rejected",
			allowMissing: false,
			etag:         func(string) string { return "" },
			wantErr:      ErrUnknownPolicy,
		},
		{
			name:         "new policy, allowMissing=true, empty etag: created",
			allowMissing: true,
			etag:         func(string) string { return "" },
		},
		{
			name:         "new policy, allowMissing=true, etag given: rejected",
			allowMissing: true,
			etag:         func(string) string { return "some-etag" },
			wantErr:      ErrUnknownEtag,
		},
		{
			name:     "existing policy, empty etag: overwritten",
			existing: true,
			etag:     func(string) string { return "" },
		},
		{
			name:     "existing policy, matching etag: updated",
			existing: true,
			etag:     func(stored string) string { return stored },
		},
		{
			name:     "existing policy, stale etag: rejected",
			existing: true,
			etag:     func(stored string) string { return stored + "-stale" },
			wantErr:  ErrStaleEtag,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemoryStore()
			var before *pb.Policy
			if tt.existing {
				before = mustUpdatePolicy(t, s, "default", newPolicy(10, ""))
			}

			got, err := s.UpdatePolicy("default", tt.allowMissing, newPolicy(20, tt.etag(before.GetEtag())))

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("UpdatePolicy() error = %v, want %v", err, tt.wantErr)
				}
				if got != nil {
					t.Errorf("UpdatePolicy() returned %v on error, want nil", got)
				}
				// A rejected write must leave the store as it was.
				after := s.GetPolicy(id)
				if (after != nil) != tt.existing || !proto.Equal(after, before) {
					t.Errorf("store changed on rejection: got %v, want %v", after, before)
				}
				return
			}

			if err != nil {
				t.Fatalf("UpdatePolicy() error = %v", err)
			}
			if got.GetMaxReplicas() != 20 {
				t.Errorf("stored MaxReplicas = %d, want 20", got.GetMaxReplicas())
			}
			if got.GetEtag() == "" {
				t.Error("UpdatePolicy() returned an empty ETag")
			}
			if got.GetEtag() == before.GetEtag() {
				t.Errorf("ETag did not change after a content change: %q", got.GetEtag())
			}
		})
	}
}

func TestUpdatePolicyDoesNotAliasCaller(t *testing.T) {
	s := NewMemoryStore()
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("ns"), Name: proto.String("pol")}.Build()
	p := pb.Policy_builder{Id: id, MaxReplicas: proto.Int32(10)}.Build()

	mustUpdatePolicy(t, s, "default", p)

	if p.GetEtag() != "" {
		t.Errorf("UpdatePolicy() set the ETag on the caller's policy: %q", p.GetEtag())
	}
	// Changing the caller's object afterwards must not change the store.
	p.SetMaxReplicas(99)
	stored := s.GetPolicy(id)
	if stored.GetMaxReplicas() != 10 {
		t.Errorf("stored MaxReplicas = %d after caller mutation, want 10", stored.GetMaxReplicas())
	}
}

func TestListPoliciesReturnsEtag(t *testing.T) {
	s := NewMemoryStore()
	stored := mustUpdatePolicy(t, s, "default", pb.Policy_builder{
		Id: pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("ns"), Name: proto.String("pol")}.Build(),
	}.Build())

	list := s.ListPolicies("default")
	if len(list) != 1 {
		t.Fatalf("ListPolicies() returned %d policies, want 1", len(list))
	}
	if got := list[0].GetEtag(); got == "" || got != stored.GetEtag() {
		t.Errorf("ListPolicies() ETag = %q, want %q", got, stored.GetEtag())
	}
}

func TestUpdatePolicyConcurrentSameEtag(t *testing.T) {
	s := NewMemoryStore()
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("ns"), Name: proto.String("pol")}.Build()
	initial := mustUpdatePolicy(t, s, "default", pb.Policy_builder{Id: id, MaxReplicas: proto.Int32(1)}.Build())

	// All writers read the same version, then race to write different content.
	const writers = 20
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func(i int) {
			_, err := s.UpdatePolicy("default", false, pb.Policy_builder{
				Id:          id,
				MaxReplicas: proto.Int32(int32(100 + i)),
				Etag:        proto.String(initial.GetEtag()),
			}.Build())
			errs <- err
		}(i)
	}

	wins := 0
	for i := 0; i < writers; i++ {
		err := <-errs
		switch {
		case err == nil:
			wins++
		case !errors.Is(err, ErrStaleEtag):
			t.Errorf("unexpected error: %v", err)
		}
	}
	if wins != 1 {
		t.Errorf("%d writers succeeded with the same ETag, want exactly 1", wins)
	}
}

// TestGetWorkload checks that the reported pods are returned sorted by name,
// as copies the caller can't use to change the store.
func TestGetWorkload(t *testing.T) {
	s := NewMemoryStore()
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String("prod"), Name: proto.String("web")}.Build()
	if _, ok := s.GetWorkload(id); ok {
		t.Fatal("GetWorkload(unknown policy) found, want not found")
	}
	if _, err := s.UpdatePolicy("default", true, pb.Policy_builder{Id: id}.Build()); err != nil {
		t.Fatalf("UpdatePolicy() error = %v", err)
	}
	if err := s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{Id: id, Workload: pb.Workload_builder{Pods: []*pb.PodState{
		pb.PodState_builder{Name: proto.String("b"), Containers: []*pb.ContainerState{pb.ContainerState_builder{Name: proto.String("app"), Requests: map[string]string{"cpu": "200m"}}.Build()}}.Build(),
		pb.PodState_builder{Name: proto.String("a"), Containers: []*pb.ContainerState{pb.ContainerState_builder{Name: proto.String("app"), Requests: map[string]string{"cpu": "100m"}}.Build()}}.Build(),
	}}.Build()}.Build()); err != nil {
		t.Fatalf("UpdateWorkload() error = %v", err)
	}

	got, ok := s.GetWorkload(id)
	if !ok {
		t.Fatal("GetWorkload() not found")
	}
	var names []string
	for _, p := range got.GetPods() {
		names = append(names, p.GetName())
	}
	if diff := cmp.Diff([]string{"a", "b"}, names); diff != "" {
		t.Errorf("GetWorkload() pods mismatch (-want +got):\n%s", diff)
	}

	got.GetPods()[0].GetContainers()[0].GetRequests()["cpu"] = "1"
	again, _ := s.GetWorkload(id)
	if v := again.GetPods()[0].GetContainers()[0].GetRequests()["cpu"]; v != "100m" {
		t.Errorf("stored request changed through a returned workload: got %q, want 100m", v)
	}
}

// TestRemovedRecommenderOwnedMetricsGC checks that the metrics owned by a
// recommender removed from the policy are dropped, with their series, while
// those of the remaining recommenders are kept.
func TestRemovedRecommenderOwnedMetricsGC(t *testing.T) {
	s := NewMemoryStore()
	ns, name := "default", "pol"
	key := policyID{cluster: "default", ns: ns, name: name}
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(name)}.Build()
	owned := func(owner string) *pb.MetricDefinitionList {
		return pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
			pb.MetricDefinition_builder{Name: proto.String("cpu"), RecommenderName: proto.String(owner), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build(),
		}}.Build()
	}

	pol := pb.Policy_builder{
		Id:         id,
		Scaling:    []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("vpa")}.Build()},
		Activation: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("act")}.Build()},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"vpa": owned("vpa"),
			"act": owned("act"),
		},
	}.Build()
	stored := mustUpdatePolicy(t, s, "default", pol)
	if got := slices.Sorted(maps.Keys(stored.GetRecommenderMetrics())); !slices.Equal(got, []string{"act", "vpa"}) {
		t.Fatalf("recommender_metrics = %v, want [act vpa]", got)
	}

	now := time.Now().Unix()
	reportReadyPods(t, s, ns, name, "p1")
	ingestOwned(s, now, ns, name, "p1", "vpa", "cpu", 10)
	ingestOwned(s, now, ns, name, "p1", "act", "cpu", 1)

	// The controller removes vpa, but still sends the entry it copied from
	// the server.
	pol = proto.Clone(stored).(*pb.Policy)
	pol.SetScaling(nil)
	stored = mustUpdatePolicy(t, s, "default", pol)

	if got := slices.Sorted(maps.Keys(stored.GetRecommenderMetrics())); !slices.Equal(got, []string{"act"}) {
		t.Errorf("recommender_metrics after removing vpa = %v, want [act]", got)
	}
	series := s.Dump().(map[policyID]*PolicyState)[key].Metrics.Series
	if _, ok := series[metricID{name: "cpu", recommenderName: "vpa"}]; ok {
		t.Error("series of the removed recommender's metric still exists")
	}
	if _, ok := series[metricID{name: "cpu", recommenderName: "act"}]; !ok {
		t.Error("series of the remaining Activation recommender's metric was deleted")
	}

	// The returned ETag matches the stored, cleaned-up policy.
	want, err := policy.CreateEtag(stored)
	if err != nil {
		t.Fatalf("CreateEtag() error = %v", err)
	}
	if stored.GetEtag() != want {
		t.Errorf("returned etag = %q, want the etag of the stored policy %q", stored.GetEtag(), want)
	}
	got := s.GetPolicy(id)
	if got.GetEtag() != stored.GetEtag() {
		t.Errorf("stored etag = %q, want the returned one %q", got.GetEtag(), stored.GetEtag())
	}
	// A follow-up update with the returned ETag succeeds.
	next := proto.Clone(stored).(*pb.Policy)
	next.SetMaxReplicas(7)
	if _, err := s.UpdatePolicy("default", false, next); err != nil {
		t.Errorf("UpdatePolicy() with the returned etag error = %v", err)
	}
}

// TestOwnedMetricStatuses checks that owned metrics are reported in the
// metric statuses as <recommender>/<metric>, after the policy-wide ones.
func TestOwnedMetricStatuses(t *testing.T) {
	s := NewMemoryStore()
	ns, name := "default", "pol"
	id := pb.PolicyId_builder{ClusterName: proto.String("default"), Namespace: proto.String(ns), Name: proto.String(name)}.Build()
	pol := pb.Policy_builder{
		Id:      id,
		Metrics: []*pb.MetricDefinition{pb.MetricDefinition_builder{Name: proto.String("cpu"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Avg")}.Build()}.Build()},
		Scaling: []*pb.RecommenderDefinition{pb.RecommenderDefinition_builder{Name: proto.String("vpa")}.Build()},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{Name: proto.String("cpu-target"), RecommenderName: proto.String("vpa"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Max")}.Build()}.Build(),
				pb.MetricDefinition_builder{Name: proto.String("memory-target"), RecommenderName: proto.String("vpa"), Gauge: pb.Gauge_builder{Aggregation: proto.String("Max")}.Build()}.Build(),
			}}.Build(),
		},
	}.Build()
	mustUpdatePolicy(t, s, "default", pol)
	s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
		Id:       id,
		Workload: pb.Workload_builder{Pods: []*pb.PodState{pb.PodState_builder{Name: proto.String("p1"), IsReady: proto.Bool(true)}.Build()}}.Build(),
	}.Build())

	now := time.Now().Unix()
	ingest(s, now, ns, name, "p1", "cpu", 2)
	ingestOwned(s, now, ns, name, "p1", "vpa", "cpu-target", 20)
	s.CalculateAll()

	resp, ok := s.GetArbitratedRecommendation(id)
	if !ok {
		t.Fatal("GetArbitratedRecommendation() not found")
	}
	type st struct {
		Name  string
		Value float64
		Error string
	}
	var got []st
	for _, ms := range resp.GetMetricStatuses() {
		got = append(got, st{ms.GetName(), ms.GetValue(), ms.GetError()})
	}
	want := []st{
		{Name: "cpu", Value: 2},
		{Name: "vpa/cpu-target", Value: 20},
		{Name: "vpa/memory-target", Error: "No data available"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("MetricStatuses mismatch (-want +got):\n%s", diff)
	}
}
