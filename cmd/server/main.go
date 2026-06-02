package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/logging"
	servergrpc "github.com/gke-labs/extensible-workload-autoscaler/internal/server/grpc"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/ui"
)

func main() {
	var debug bool
	flag.BoolVar(&debug, "debug", false, "Enable debug logging")
	flag.Parse()

	logging.Setup(debug)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metricsStore := store.NewMemoryStore()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				metricsStore.CalculateAll()
			case <-ctx.Done():
				return
			}
		}
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "50051"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		slog.Error("failed to listen", "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterXASControlPlaneServer(grpcServer, servergrpc.NewServer(metricsStore, clock.RealClock{}))
	reflection.Register(grpcServer)

	slog.Info("XAS Control Plane gRPC starting", "port", port)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server stopped", "error", err)
		}
	}()

	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "8080"
	}

	mux := http.NewServeMux()
	ui.RegisterHandlers(mux, metricsStore)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/storez", func(w http.ResponseWriter, r *http.Request) {
		state := metricsStore.Dump()
		w.Header().Set("Content-Type", "application/json")
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(state)
	})

	httpServer := &http.Server{
		Addr:    ":" + metricsPort,
		Handler: mux,
	}

	slog.Info("XAS Metrics starting", "port", metricsPort)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Metrics server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("Shutting down...")

	grpcServer.GracefulStop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}
	slog.Info("Shutdown complete.")
}
