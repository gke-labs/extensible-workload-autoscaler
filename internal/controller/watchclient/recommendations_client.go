package watchclient

import (
	"context"

	"log/slog"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/pkg/client/watch"
	"google.golang.org/grpc"
)

// RecommendationsClient implements a client watching Recommendation updates.
// Not thread-safe.
type RecommendationsClient struct {
	connection *watch.Connection[pb.ArbitratedRecommendationsEvent]
}

// NewRecommendationsClient creates a new client using a given xAS Server connection.
func NewRecommendationsClient(c *grpc.ClientConn, clusterName string) *RecommendationsClient {
	return &RecommendationsClient{
		connection: watch.NewConnection(c, clusterName, openRecommendationsStream),
	}
}

// Run starts this client that listens for recommendation updates and pushes
// them to a channel. It reconnects on failure.
func (c *RecommendationsClient) Run(ctx context.Context, ch chan *pb.ArbitratedRecommendationsEvent) {
	slog.Info("Recommendations watch client started")
	defer slog.Info("Recommendations watch client stopped")

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

// openRecommendationsStream uses a gRPC connection to create a server-streaming
// connection used to receive recommendations for the given cluster.
func openRecommendationsStream(ctx context.Context, grpcClient *grpc.ClientConn, clusterName string) (pb.XASServer_WatchArbitratedRecommendationsClient, error) {
	serverClient := pb.NewXASServerClient(grpcClient)
	req := pb.WatchArbitratedRecommendationsRequest_builder{
		ClusterName: new(clusterName),
	}.Build()
	return serverClient.WatchArbitratedRecommendations(ctx, req)
}
