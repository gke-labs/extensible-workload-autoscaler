package watchclient

import (
	"context"

	"log/slog"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/watch"
	"google.golang.org/grpc"
)

// ControlMetricsClient implements a client watching ControlMetrics updates.
// Not thread-safe.
type ControlMetricsClient struct {
	connection *watch.Connection[pb.ControlMetricsEvent]
}

// NewControlMetricsClient creates a new client using a given UAS connection.
func NewControlMetricsClient(c *grpc.ClientConn, clusterName string) *ControlMetricsClient {
	return &ControlMetricsClient{
		connection: watch.NewConnection(c, clusterName, openControlMetricsStream),
	}
}

// Run starts this client that listens for control metrics updates and pushes
// them to a channel. It reconnects on failure.
func (c *ControlMetricsClient) Run(ctx context.Context, ch chan *pb.ControlMetricsEvent) {
	slog.Info("ControlMetrics watch client started")
	defer slog.Info("ControlMetrics watch client stopped")

	for {
		resp, err := c.connection.Recv(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.Error("Error receiving event from xAS Server stream", "error", err)
			continue
		}
		ch <- resp
	}
}

// openStream uses a gRPC connection to create a server-streaming connection
// used to receive control metrics for the given cluster.
func openControlMetricsStream(ctx context.Context, grpcClient *grpc.ClientConn, clusterName string) (pb.XASServer_WatchControlMetricsClient, error) {
	serverClient := pb.NewXASServerClient(grpcClient)
	req := pb.WatchControlMetricsRequest_builder{
		ClusterName: new(clusterName),
	}.Build()
	return serverClient.WatchControlMetrics(ctx, req)
}
