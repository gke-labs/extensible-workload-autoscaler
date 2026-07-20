package main

import (
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	requestCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total requests",
	})

	requestActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "http_requests_active",
		Help: "Active requests",
	})

	requestDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "Request duration",
		Buckets: []float64{0.1, 0.2, 0.5, 1.0},
	})

	memoryAllocated = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sample_app_memory_allocated_bytes",
		Help: "Current memory allocated by the /alloc endpoint",
	})

	sawtooth = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "sawtooth",
			Help: "A metric going from 0 to 100 every minute",
		},
		func() float64 {
			return float64(time.Now().Second()) * (100.0 / 60.0)
		},
	)

	sinewave = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "sinewave",
			Help: "A sinewave metric going from 0 to 100 with a period of 1 minute",
		},
		func() float64 {
			return (math.Sin(float64(time.Now().Second())*(2*math.Pi/60.0)) + 1) * 50.0
		},
	)

	// Global variable to prevent compiler optimization from freeing memory
	memoryHold [][]byte
)

func init() {
	prometheus.MustRegister(requestCount)
	prometheus.MustRegister(requestActive)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(memoryAllocated)
	prometheus.MustRegister(sawtooth)
}

func main() {
	// 1. App Endpoint
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		requestActive.Inc()
		defer requestActive.Dec()

		timer := prometheus.NewTimer(requestDuration)
		defer timer.ObserveDuration()

		requestCount.Inc()

		// Simulate latency (0-500ms)
		//		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

		w.Write([]byte("Hello from XAS Sample App"))
	})

	// 1. App Endpoint
	http.HandleFunc("/latency", func(w http.ResponseWriter, r *http.Request) {
		requestActive.Inc()
		defer requestActive.Dec()

		timer := prometheus.NewTimer(requestDuration)
		defer timer.ObserveDuration()

		requestCount.Inc()

		// Simulate latency (0-500ms)
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

		w.Write([]byte("Hello from XAS Sample App"))
	})

	// 2. CPU Burn Endpoint
	http.HandleFunc("/burn", func(w http.ResponseWriter, r *http.Request) {
		requestActive.Inc()
		defer requestActive.Dec()

		timer := prometheus.NewTimer(requestDuration)
		defer timer.ObserveDuration()

		requestCount.Inc()
		x := 0.0
		for i := 0; i < 500_000_000; i++ {
			x += math.Sqrt(float64(i))
		}
		w.Write([]byte(fmt.Sprintf("Burned CPU: %f", x)))
	})

	// 3. Memory Allocation Endpoint
	http.HandleFunc("/alloc", func(w http.ResponseWriter, r *http.Request) {
		sizeStr := r.URL.Query().Get("size")
		sizeMB, err := strconv.Atoi(sizeStr)
		if err != nil {
			http.Error(w, "Invalid size parameter. Use /alloc?size=100 (in MB)", http.StatusBadRequest)
			return
		}

		// Free previous allocation and request GC
		memoryHold = nil
		runtime.GC()

		if sizeMB > 0 {
			memoryHold = make([][]byte, sizeMB)
			for i := 0; i < sizeMB; i++ {
				memoryHold[i] = make([]byte, 1024*1024)
				// Fill memory to ensure it's actually resident (paged in)
				for j := 0; j < len(memoryHold[i]); j += 4096 {
					memoryHold[i][j] = 1
				}
			}
		}

		memoryAllocated.Set(float64(sizeMB * 1024 * 1024))
		fmt.Fprintf(w, "Allocated %d MB\n", sizeMB)
	})

	// 4. Metrics Endpoint
	http.Handle("/metrics", promhttp.Handler())

	fmt.Println("Starting sample app on :8080")
	http.ListenAndServe(":8080", nil)
}
