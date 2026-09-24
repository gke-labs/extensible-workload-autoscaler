package main

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"net/http"
	"os"
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

func startCpuBurnWorkers(count int) {
	for i := 0; i < count; i++ {
		go func(workerID int) {
			for {
				x := 0.0
				for j := 0; j < 50_000_000; j++ {
					x += math.Sqrt(float64(j))
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}
}

func allocateMemory(sizeMB int) {
	memoryHold = nil
	runtime.GC()

	if sizeMB > 0 {
		memoryHold = make([][]byte, sizeMB)
		for i := 0; i < sizeMB; i++ {
			memoryHold[i] = make([]byte, 1024*1024)
			for j := 0; j < len(memoryHold[i]); j += 4096 {
				memoryHold[i][j] = 1
			}
		}
	}

	memoryAllocated.Set(float64(sizeMB * 1024 * 1024))
}

func main() {
	// Auto load generation from environment variables
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName = os.Getenv("HOSTNAME")
	}

	if cpuStr := os.Getenv("CPU_LOAD_WORKERS"); cpuStr != "" {
		if workers, err := strconv.Atoi(cpuStr); err == nil && workers > 0 {
			fmt.Printf("Starting %d background CPU burn workers\n", workers)
			startCpuBurnWorkers(workers)
		}
	}

	if memStr := os.Getenv("MEMORY_ALLOC_MB"); memStr != "" {
		if sizeMB, err := strconv.Atoi(memStr); err == nil && sizeMB > 0 {
			fmt.Printf("Allocating %d MB resident memory at startup\n", sizeMB)
			allocateMemory(sizeMB)
		}
	}

	if os.Getenv("AUTO_PER_POD_LOAD") == "true" && podName != "" {
		h := fnv.New32a()
		h.Write([]byte(podName))
		hashVal := h.Sum32()

		workers := int(hashVal % 3) // 0, 1, or 2 workers
		memMB := int(((hashVal/3)%4 + 1) * 50)
		fmt.Printf("AUTO_PER_POD_LOAD active for pod %s: %d CPU workers, %d MB memory\n", podName, workers, memMB)
		if workers > 0 {
			startCpuBurnWorkers(workers)
		}
		allocateMemory(memMB)
	}

	// 1. App Endpoint
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		requestActive.Inc()
		defer requestActive.Dec()

		timer := prometheus.NewTimer(requestDuration)
		defer timer.ObserveDuration()

		requestCount.Inc()

		w.Write([]byte("Hello from XAS Sample App"))
	})

	// Latency Endpoint
	http.HandleFunc("/latency", func(w http.ResponseWriter, r *http.Request) {
		requestActive.Inc()
		defer requestActive.Dec()

		timer := prometheus.NewTimer(requestDuration)
		defer timer.ObserveDuration()

		requestCount.Inc()

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

		allocateMemory(sizeMB)
		fmt.Fprintf(w, "Allocated %d MB\n", sizeMB)
	})

	// 4. Metrics Endpoint
	http.Handle("/metrics", promhttp.Handler())

	fmt.Println("Starting sample app on :8080")
	http.ListenAndServe(":8080", nil)
}
