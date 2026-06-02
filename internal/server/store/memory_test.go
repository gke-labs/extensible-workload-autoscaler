package store_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store"
)

func TestMetricCalculations(t *testing.T) {
	tests := []struct {
		name     string
		metrics  []*pb.MetricDefinition
		ingest   func(s *store.MemoryStore, clk *clock.FakeClock, ns, pol string)
		workload []*pb.PodState
		want     map[string]float64
	}{
		{
			name: "Counters: Rate Calculation (Avg)",
			metrics: []*pb.MetricDefinition{
				{Name: "requests", Rate: &pb.Rate{Window: "1m", Aggregation: "Avg"}},
			},
			workload: []*pb.PodState{{Name: "p1", IsReady: true}, {Name: "p2", IsReady: true}},
			ingest: func(s *store.MemoryStore, clk *clock.FakeClock, ns, pol string) {
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
				{Name: "requests", Rate: &pb.Rate{Window: "1m", Aggregation: "Sum"}},
			},
			workload: []*pb.PodState{{Name: "p1", IsReady: true}, {Name: "p2", IsReady: true}},
			ingest: func(s *store.MemoryStore, clk *clock.FakeClock, ns, pol string) {
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
				{Name: "queue_depth", Gauge: &pb.Gauge{Aggregation: "Max"}},
			},
			workload: []*pb.PodState{{Name: "p1", IsReady: true}, {Name: "p2", IsReady: true}},
			ingest: func(s *store.MemoryStore, clk *clock.FakeClock, ns, pol string) {
				ingest(s, clk.Now().Unix(), ns, pol, "p1", "queue_depth", 10)
				ingest(s, clk.Now().Unix(), ns, pol, "p2", "queue_depth", 50)
			},
			// Max(10, 50) = 50
			want: map[string]float64{"queue_depth": 50},
		},
		{
			name: "Gauges: Min Aggregation",
			metrics: []*pb.MetricDefinition{
				{Name: "latency", Gauge: &pb.Gauge{Aggregation: "Min"}},
			},
			workload: []*pb.PodState{{Name: "p1", IsReady: true}, {Name: "p2", IsReady: true}},
			ingest: func(s *store.MemoryStore, clk *clock.FakeClock, ns, pol string) {
				ingest(s, clk.Now().Unix(), ns, pol, "p1", "latency", 100)
				ingest(s, clk.Now().Unix(), ns, pol, "p2", "latency", 50)
			},
			// Min(100, 50) = 50
			want: map[string]float64{"latency": 50},
		},
		{
			name: "Histogram: Percentile (P90) Global",
			metrics: []*pb.MetricDefinition{
				{
					Name: "hist", Distribution: &pb.Distribution{Percentile: "p90"},
				},
			},
			workload: []*pb.PodState{{Name: "p1", IsReady: true}, {Name: "p2", IsReady: true}},
			ingest: func(s *store.MemoryStore, clk *clock.FakeClock, ns, pol string) {
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
				{
					Name: "filtered_gauge", Gauge: &pb.Gauge{Aggregation: "Sum"},
				},
			},
			workload: []*pb.PodState{{Name: "p1", IsReady: true}},
			ingest: func(s *store.MemoryStore, clk *clock.FakeClock, ns, pol string) {
				ingest(s, clk.Now().Unix(), ns, pol, "p1", "filtered_gauge", 100)
			},
			want: map[string]float64{"filtered_gauge": 100},
		},
		{
			name: "Counters: Rapid Updates (Same Timestamp)",
			metrics: []*pb.MetricDefinition{
				{Name: "reqs", Rate: &pb.Rate{Window: "1m", Aggregation: "Sum"}},
			},
			workload: []*pb.PodState{{Name: "p1", IsReady: true}},
			ingest: func(s *store.MemoryStore, clk *clock.FakeClock, ns, pol string) {
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
			name: "Counters: Rapid Updates (Same Timestamp)",
			metrics: []*pb.MetricDefinition{
				{Name: "reqs", Rate: &pb.Rate{Window: "1m", Aggregation: "Sum"}},
			},
			workload: []*pb.PodState{{Name: "p1", IsReady: true}},
			ingest: func(s *store.MemoryStore, clk *clock.FakeClock, ns, pol string) {
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
				{Name: "cpu", Gauge: &pb.Gauge{Aggregation: "Avg"}},
			},
			// p2 is NotReady
			workload: []*pb.PodState{{Name: "p1", IsReady: true}, {Name: "p2", IsReady: false}},
			ingest: func(s *store.MemoryStore, clk *clock.FakeClock, ns, pol string) {
				ingest(s, clk.Now().Unix(), ns, pol, "p1", "cpu", 10)
				ingest(s, clk.Now().Unix(), ns, pol, "p2", "cpu", 1000) // Should be ignored
			},
			// Avg(10) = 10. (Not 505)
			want: map[string]float64{"cpu": 10.0},
		},
		{
			name: "Window: Data Expiry",
			metrics: []*pb.MetricDefinition{
				{Name: "cpu", Gauge: &pb.Gauge{Aggregation: "Avg"}},
			},
			workload: []*pb.PodState{{Name: "p1", IsReady: true}},
			ingest: func(s *store.MemoryStore, clk *clock.FakeClock, ns, pol string) {
				// Ingest old data (70s ago). Cutoff is 60s.
				oldTime := clk.Now().Add(-70 * time.Second).Unix()
				ingest(s, oldTime, ns, pol, "p1", "cpu", 100)
			},
			// No data in window -> No metric calculated
			want: map[string]float64{}, // Empty
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Unix(1000, 0)
			clk := &clock.FakeClock{CurrentTime: start}
			s := store.NewMemoryStoreWithClock(clk)

			policyName := "test-policy"
			ns := "default"

			policy := &pb.Policy{
				Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
				Metrics:  tc.metrics,
				Workload: &pb.WorkloadRef{Name: "app"},
			}
			s.SetPolicy("default", policy)

			if tc.workload != nil {
				s.UpdateWorkload(&pb.UpdateWorkloadRequest{
					Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName},
					Workload: &pb.Workload{Pods: tc.workload},
				})
			}

			tc.ingest(s, clk, ns, policyName)

			s.CalculateAll()

			cm, _ := s.GetControlMetrics(&pb.PolicyId{ClusterName: "default", Namespace: ns, Name: policyName})
			got := make(map[string]float64)
			if cm != nil {
				got = cm.Values
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

func ingest(s *store.MemoryStore, ts int64, ns, pol, pod, metric string, val float64) {
	ingestWithLabels(s, ts, ns, pol, pod, metric, val, nil)
}

func ingestWithLabels(s *store.MemoryStore, ts int64, ns, pol, pod, metric string, val float64, labels map[string]string) {
	s.AddBatch(&pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: ns, Name: pol,
			Batches: []*pb.MetricBatch{{
				EntityKey: pod,
				Samples: []*pb.MetricSample{{
					Name:      metric,
					Value:     val,
					Labels:    labels,
					Timestamp: ts,
				}},
			}},
		}},
	})
}

func ingestHist(s *store.MemoryStore, ts int64, ns, pol, pod, metric string, buckets map[string]uint64) {
	s.AddBatch(&pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   ts,
		Policies: []*pb.PolicyBatch{{
			Namespace: ns, Name: pol,
			Batches: []*pb.MetricBatch{{
				EntityKey: pod,
				Samples: []*pb.MetricSample{{
					Name:             metric,
					HistogramBuckets: buckets,
					Timestamp:        ts,
				}},
			}},
		}},
	})
}

func TestRecommendationArbitration(t *testing.T) {
	// Verify Max(Scaling) and OR(Activation) logic
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := store.NewMemoryStoreWithClock(clk)

	policy := &pb.Policy{
		Id:          &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: "arb-pol"},
		MinReplicas: 1,
		MaxReplicas: 10,
		Scaling: []*pb.RecommenderDefinition{
			{Name: "r1", Recommender: "Linear", Type: "Linear"},
			{Name: "r2", Recommender: "Linear", Type: "Linear"},
		},
		Activation: []*pb.RecommenderDefinition{
			{Name: "a1", Recommender: "Threshold", Type: "Threshold"},
		},
	}
	s.SetPolicy("default", policy)

	// Case 1: All Active. R1=3, R2=5. Result=5.
	s.UpdateRecommenderState(&pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: "arb-pol"}, RecommenderName: "r1",
		Vote: &pb.RecommenderVote{DesiredReplicas: 3, IsActive: true},
	})
	s.UpdateRecommenderState(&pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: "arb-pol"}, RecommenderName: "r2",
		Vote: &pb.RecommenderVote{DesiredReplicas: 5, IsActive: true},
	})
	s.UpdateRecommenderState(&pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: "arb-pol"}, RecommenderName: "a1",
		Vote: &pb.RecommenderVote{IsActive: true},
	})

	s.CalculateAll()
	rec, _ := s.GetRecommendation(&pb.PolicyId{ClusterName: "default", Namespace: "default", Name: "arb-pol"})
	if rec.Recommendation.TargetReplicas != 5 {
		t.Errorf("Case 1: Want 5, Got %d", rec.Recommendation.TargetReplicas)
	}

	// Case 2: Inactive (Scale to Zero). a1=False.
	s.UpdateRecommenderState(&pb.UpdateRecommenderStateRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: "arb-pol"}, RecommenderName: "a1",
		Vote: &pb.RecommenderVote{IsActive: false},
	})
	// Force window expiry (default 300s window in store logic)
	clk.Advance(301 * time.Second)
	// Must trigger calculation to update LastActive logic inside store
	// (Note: Store updates LastActive on *every* CalculateAll call if any recommender says active)
	// Here a1 says inactive.
	s.CalculateAll()

	rec, _ = s.GetRecommendation(&pb.PolicyId{ClusterName: "default", Namespace: "default", Name: "arb-pol"})
	if rec.Recommendation.TargetReplicas != 0 {
		t.Errorf("Case 2: Want 0 (Inactive), Got %d", rec.Recommendation.TargetReplicas)
	}
}

func TestDump(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := store.NewMemoryStoreWithClock(clk)

	// 1. Policy
	policy := &pb.Policy{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: "dump-pol"},
		Metrics: []*pb.MetricDefinition{
			{Name: "cpu", Gauge: &pb.Gauge{Aggregation: "Avg"}},
		},
		Scaling: []*pb.RecommenderDefinition{
			{Name: "cpu-rec", Recommender: "Linear", Type: "Linear", Params: map[string]string{"metric": "cpu", "target": "0.5"}},
		},
		Workload:    &pb.WorkloadRef{Name: "app"},
		MinReplicas: 1,
		MaxReplicas: 10,
	}
	s.SetPolicy("default", policy)

	// 2. Workload
	s.UpdateWorkload(&pb.UpdateWorkloadRequest{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: "dump-pol"},
		Workload: &pb.Workload{
			Pods: []*pb.PodState{
				{Name: "p1", IsReady: true},
			},
		},
	})

	// 3. Ingest (Series)
	s.AddBatch(&pb.IngestMetricsRequest{
		ClusterName: "default",
		Timestamp:   1000,
		Policies: []*pb.PolicyBatch{{
			Namespace: "default", Name: "dump-pol",
			Batches: []*pb.MetricBatch{{
				EntityKey: "p1",
				Samples: []*pb.MetricSample{{
					Name: "cpu", Value: 1.0, Timestamp: 1000,
				}},
			}},
		}},
	})

	// 4. Calculate (ControlMetrics, Decisions, Recommendations)
	s.UpdateRecommenderState(&pb.UpdateRecommenderStateRequest{
		Id:              &pb.PolicyId{ClusterName: "default", Namespace: "default", Name: "dump-pol"},
		RecommenderName: "cpu-rec",
		Vote: &pb.RecommenderVote{
			DesiredReplicas: 2,
			IsActive:        true,
		},
	})

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
      ]
    },
    "Workload": {
      "p1": {
        "name": "p1",
        "is_ready": true
      }
    },
    "Series": {
      "cpu": {
        "p1|": {
          "PodName": "p1",
          "Labels": null,
          "LastRaw": {
            "Timestamp": 1000,
            "Value": 1,
            "CumulativeBuckets": null
          },
          "ControlMetric": {
            "Timestamp": 1000,
            "Value": 1,
            "Labels": null,
            "Buckets": null
          },
          "Window": null,
          "DecayingHistogram": null
        }
      }
    },
    "GlobalHistograms": {},
    "Recommendation": {
      "target_replicas": 2,
      "explanation": [
        {
          "desired_replicas": 2,
          "is_active": true,
          "last_updated": {
            "seconds": 1000
          },
          "phase": "Scaling",
          "name": "cpu-rec",
          "type": "Linear"
        }
      ]
    },
    "LastActive": 1000,
    "Decisions": {
      "cpu-rec": {
        "desired_replicas": 2,
        "is_active": true,
        "last_updated": {
          "seconds": 1000
        },
        "phase": "Scaling",
        "name": "cpu-rec",
        "type": "Linear"
      }
    },
    "ControlMetrics": {
      "values": {
        "cpu": 1
      },
      "timestamp": 1000,
      "ready_replicas": 1
    }
  }
}`

	if string(data) != expected {
		t.Errorf("Dump JSON mismatch.\nWant:\n%s\nGot:\n%s", expected, string(data))
	}
}

func TestWindowedMetrics(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := store.NewMemoryStoreWithClock(clk)
	ns, pol := "default", "window-pol"

	// 1. Policy with Windowed Metrics
	policy := &pb.Policy{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: pol},
		Metrics: []*pb.MetricDefinition{
			{
				Name: "cpu_hist", DecayingDistribution: &pb.DecayingDistribution{
					HalfLife:   "24h",
					BucketSize: "0.1",
					Percentile: "p100",
				},
			},
			{
				Name: "cpu_slide", Gauge: &pb.Gauge{
					Aggregation: "Avg",
				},
			},
		},
		Workload: &pb.WorkloadRef{Name: "app"},
	}
	s.SetPolicy("default", policy)

	s.UpdateWorkload(&pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: pol},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}}},
	})

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
	cm, _ := s.GetControlMetrics(&pb.PolicyId{ClusterName: "default", Namespace: ns, Name: pol})

	// Histogram should remember the 1.0 spike (approx 1.0 bucket upper bound -> 1.1)
	// Sliding Window (Max) should see 1.0.
	gotHist := cm.Values["cpu_hist"]
	gotSlide := cm.Values["cpu_slide"]

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
	cm, _ = s.GetControlMetrics(&pb.PolicyId{ClusterName: "default", Namespace: ns, Name: pol})

	// Gauge should see 0.2.
	if cm.Values["cpu_slide"] != 0.2 {
		t.Errorf("Gauge: Want 0.2, Got %f", cm.Values["cpu_slide"])
	}
}

func TestAggregatedDecayingHistogram(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := store.NewMemoryStoreWithClock(clk)
	ns, pol := "default", "agg-hist-pol"

	// Policy with DecayingHistogram
	policy := &pb.Policy{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: pol},
		Metrics: []*pb.MetricDefinition{
			{
				Name: "cpu", DecayingDistribution: &pb.DecayingDistribution{
					HalfLife:   "24h",
					BucketSize: "0.1",
					Percentile: "p95",
				},
			},
		},
		Workload: &pb.WorkloadRef{Name: "app"},
	}
	s.SetPolicy("default", policy)

	s.UpdateWorkload(&pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: pol},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}, {Name: "p2", IsReady: true}}},
	})

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
	cm, _ := s.GetControlMetrics(&pb.PolicyId{ClusterName: "default", Namespace: ns, Name: pol})
	val := cm.Values["cpu"]

	// If it's 1.1, it's per-pod aggregation (Avg of p95s).
	// If it's >= 1.5, it's workload-level aggregation.
	if val < 1.5 {
		t.Errorf("Aggregated Histogram: Want >= 1.5, Got %f", val)
	}
}

func TestDeletePolicy(t *testing.T) {
	s := store.NewMemoryStore()
	ns, name := "default", "pol"
	id := &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: name}

	// 1. Setup state
	s.SetPolicy("default", &pb.Policy{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: name},
	})
	s.UpdateWorkload(&pb.UpdateWorkloadRequest{Id: id, Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1"}}}})
	s.UpdateRecommenderState(&pb.UpdateRecommenderStateRequest{Id: id, RecommenderName: "r1", Vote: &pb.RecommenderVote{IsActive: true}})

	// 2. Delete
	s.DeletePolicy(id)

	// 3. Verify
	if _, ok := s.GetPolicy(id); ok {
		t.Error("Policy still exists after deletion")
	}

	// Internal maps should be empty (or at least key should be missing)
	dump := s.Dump().(map[string]*store.PolicyState)
	if _, exists := dump["default/default/pol"]; exists {
		t.Error("PolicyState still exists after deletion")
	}
}

func TestMultiTenantIsolation(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := store.NewMemoryStoreWithClock(clk)
	ns, name := "default", "common-policy"

	// 1. Setup Policies for Cluster A and Cluster B
	polA := &pb.Policy{
		Id:      &pb.PolicyId{ClusterName: "cluster-A", Namespace: ns, Name: name},
		Metrics: []*pb.MetricDefinition{{Name: "m", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
	}
	s.SetPolicy("cluster-A", polA)

	polB := &pb.Policy{
		Id:      &pb.PolicyId{ClusterName: "cluster-B", Namespace: ns, Name: name},
		Metrics: []*pb.MetricDefinition{{Name: "m", Gauge: &pb.Gauge{Aggregation: "Avg"}}},
	}
	s.SetPolicy("cluster-B", polB)

	// 2. Setup Workloads
	s.UpdateWorkload(&pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "cluster-A", Namespace: ns, Name: name},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "pod-a", IsReady: true}}},
	})
	s.UpdateWorkload(&pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "cluster-B", Namespace: ns, Name: name},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "pod-b", IsReady: true}}},
	})

	// 3. Ingest Data (Value 100 for A, 200 for B)
	s.AddBatch(&pb.IngestMetricsRequest{
		ClusterName: "cluster-A", Timestamp: 1000,
		Policies: []*pb.PolicyBatch{{
			Namespace: ns, Name: name,
			Batches: []*pb.MetricBatch{{EntityKey: "pod-a", Samples: []*pb.MetricSample{{Name: "m", Value: 100, Timestamp: 1000}}}},
		}},
	})
	s.AddBatch(&pb.IngestMetricsRequest{
		ClusterName: "cluster-B", Timestamp: 1000,
		Policies: []*pb.PolicyBatch{{
			Namespace: ns, Name: name,
			Batches: []*pb.MetricBatch{{EntityKey: "pod-b", Samples: []*pb.MetricSample{{Name: "m", Value: 200, Timestamp: 1000}}}},
		}},
	})

	s.CalculateAll()

	// 4. Verify Isolation
	cmA, okA := s.GetControlMetrics(&pb.PolicyId{ClusterName: "cluster-A", Namespace: ns, Name: name})
	if !okA || cmA.Values["m"] != 100 {
		t.Errorf("Cluster A: Want 100, Got %v", cmA)
	}

	cmB, okB := s.GetControlMetrics(&pb.PolicyId{ClusterName: "cluster-B", Namespace: ns, Name: name})
	if !okB || cmB.Values["m"] != 200 {
		t.Errorf("Cluster B: Want 200, Got %v", cmB)
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
	s := store.NewMemoryStore()
	ns, name := "default", "pol"

	// 1. Setup policy with 2 metrics
	pol := &pb.Policy{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: name},
		Metrics: []*pb.MetricDefinition{
			{Name: "m1", Gauge: &pb.Gauge{Aggregation: "Avg"}},
			{Name: "m2", Gauge: &pb.Gauge{Aggregation: "Avg"}},
		},
	}
	s.SetPolicy("default", pol)

	// 2. Ingest data for both
	ingest(s, 1000, ns, name, "p1", "m1", 10)
	ingest(s, 1000, ns, name, "p1", "m2", 20)

	// 3. Update policy: remove m1
	pol.Metrics = []*pb.MetricDefinition{{Name: "m2", Gauge: &pb.Gauge{Aggregation: "Avg"}}}
	s.SetPolicy("default", pol)

	// 4. Calculate
	s.CalculateAll()

	// 5. Verify m1 is gone from series map
	dump := s.Dump().(map[string]*store.PolicyState)
	policySeries := dump["default/default/pol"].Series

	if _, exists := policySeries["m1"]; exists {
		t.Error("Orphaned metric m1 still exists in series map")
	}
	if _, exists := policySeries["m2"]; !exists {
		t.Error("Active metric m2 was incorrectly deleted")
	}
}

func TestRemoveRecommender(t *testing.T) {
	s := store.NewMemoryStore()
	ns, name := "default", "pol"
	id := &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: name}

	// 1. Setup policy with 1 recommender
	pol := &pb.Policy{
		Id:          id,
		MinReplicas: 1,
		MaxReplicas: 10,
		Scaling: []*pb.RecommenderDefinition{
			{Name: "r1", Recommender: "Linear", Type: "Linear", Params: map[string]string{"target": "10"}},
		},
	}
	s.SetPolicy("default", pol)

	// 2. Simulate R1 vote = 10
	s.UpdateRecommenderState(&pb.UpdateRecommenderStateRequest{
		Id: id, RecommenderName: "r1",
		Vote: &pb.RecommenderVote{DesiredReplicas: 10, IsActive: true},
	})

	s.CalculateAll()
	rec, _ := s.GetRecommendation(id)
	if rec.Recommendation.TargetReplicas != 10 {
		t.Errorf("Initial: Want 10, Got %d", rec.Recommendation.TargetReplicas)
	}

	// 3. Update Policy: Remove R1
	pol.Scaling = []*pb.RecommenderDefinition{} // Empty list
	s.SetPolicy("default", pol)

	s.CalculateAll()
	rec, ok := s.GetRecommendation(id)
	if !ok || rec.Recommendation != nil {
		t.Errorf("After removal: Want nil recommendation, Got %v (ok=%v)", rec.Recommendation, ok)
	}

	// 4. Simulate Zombie R1 vote = 100
	s.UpdateRecommenderState(&pb.UpdateRecommenderStateRequest{
		Id: id, RecommenderName: "r1",
		Vote: &pb.RecommenderVote{DesiredReplicas: 100, IsActive: true},
	})

	s.CalculateAll()
	rec, ok = s.GetRecommendation(id)
	if !ok || rec.Recommendation != nil {
		t.Errorf("After zombie vote: Want nil recommendation, Got %v (ok=%v)", rec.Recommendation, ok)
	}

	// 5. Verify cleanup of internal Decisions map
	dump := s.Dump().(map[string]*store.PolicyState)
	if _, exists := dump["default/default/pol"].Decisions["r1"]; exists {
		t.Error("Orphaned decision r1 still exists in Decisions map")
	}
}

func TestPodScopedDecayingHistogram(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := store.NewMemoryStoreWithClock(clk)
	ns, pol := "default", "pod-hist-pol"

	// Policy with Pod-Scoped DecayingHistogram
	policy := &pb.Policy{
		Id: &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: pol},
		Metrics: []*pb.MetricDefinition{
			{
				Name:  "cpu",
				Scope: "Pod",
				DecayingDistribution: &pb.DecayingDistribution{
					HalfLife:   "24h",
					BucketSize: "0.1",
					Percentile: "p95",
				},
			},
		},
		Workload: &pb.WorkloadRef{Name: "app"},
	}
	s.SetPolicy("default", policy)

	s.UpdateWorkload(&pb.UpdateWorkloadRequest{
		Id:       &pb.PolicyId{ClusterName: "default", Namespace: ns, Name: pol},
		Workload: &pb.Workload{Pods: []*pb.PodState{{Name: "p1", IsReady: true}, {Name: "p2", IsReady: true}}},
	})

	ingest(s, clk.Now().Unix(), ns, pol, "p1", "cpu", 0.5)
	ingest(s, clk.Now().Unix(), ns, pol, "p2", "cpu", 1.5)

	s.CalculateAll()
	cm, _ := s.GetControlMetrics(&pb.PolicyId{ClusterName: "default", Namespace: ns, Name: pol})

	if cm.Values["cpu"] != 0 {
		t.Errorf("Pod scoped metric should not have a global value, got %f", cm.Values["cpu"])
	}

	if cm.PodMetrics == nil {
		t.Fatalf("PodMetrics is nil")
	}

	if p1Val := cm.PodMetrics["p1"].Values["cpu"]; p1Val < 0.5 {
		t.Errorf("Pod 1 scoped metric: Want >= 0.5, Got %f", p1Val)
	}

	if p2Val := cm.PodMetrics["p2"].Values["cpu"]; p2Val < 1.5 {
		t.Errorf("Pod 2 scoped metric: Want >= 1.5, Got %f", p2Val)
	}
}
