package watchclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"k8s.io/apimachinery/pkg/util/wait"
)

type connection[T any] struct {
	grpcClient         *grpc.ClientConn
	clusterName        string
	createStreamClient clientCreate[T]

	// Stream and its context cancel function. Can be nil if the streamClient
	// connection is not established.
	streamClient grpc.ServerStreamingClient[T]
	streamCancel context.CancelFunc
}

// clientCreate creates a streaming client receiving messages of type T.
type clientCreate[T any] func(ctx context.Context, grpcClient *grpc.ClientConn, clusterName string) (grpc.ServerStreamingClient[T], error)

// newConnection handles a streaming connection to xAS Server. It handles
// reconnections in case of failure.
func newConnection[T any](c *grpc.ClientConn, clusterName string, openStream clientCreate[T]) *connection[T] {
	return &connection[T]{
		grpcClient:         c,
		clusterName:        clusterName,
		createStreamClient: openStream,
	}
}

// Recv receives a *T from a stream. This function creates a stream if one is
// not set up yet.
//
// If any error occurs when receiving the request, this function closes the
// stream and returns an error. This function can then be called again to
// recreate the stream connection.
func (c *connection[T]) Recv(ctx context.Context) (*T, error) {
	if c.streamClient == nil {
		if err := c.initializeStream(ctx); err != nil {
			return nil, fmt.Errorf("error creating UAS streaming client: %w", err)
		}
	}
	resp, err := c.streamClient.Recv()
	if err != nil {
		defer c.terminateStream()
		if err == io.EOF {
			return nil, errors.New("stream has been gracefully closed by server")
		}
		return nil, fmt.Errorf("error reading stream: %w", err)
	} else {
		return resp, nil
	}
}

// initializeStream creates a new steam. Once created, it can be closed using
// [terminateStream].
//
// This function keeps retrying to create the client until it succeeds or the
// given context is canceled (in which case it returns an error).
func (c *connection[T]) initializeStream(ctx context.Context) error {
	backoff := wait.Backoff{
		Steps:    math.MaxInt, // Only stop retrying when ctx is canceled
		Duration: 10 * time.Millisecond,
		Factor:   2.0,
		Jitter:   0.1,
		Cap:      10 * time.Minute, // Retry at least every 10m
	}

	streamctx, streamcancel := context.WithCancel(ctx)

	createStream := func(ctx context.Context) (done bool, err error) {
		if err := waitForConnection(ctx, c.grpcClient); err != nil {
			return false, fmt.Errorf("could not get connection established: %w", err)
		}
		s, err := c.createStreamClient(streamctx, c.grpcClient, c.clusterName)
		if err != nil {
			slog.Warn("Error creating xAS Server streaming client; retrying", "error", err)
			return false, nil
		}
		c.streamClient = s
		c.streamCancel = streamcancel
		return true, nil
	}

	if err := wait.ExponentialBackoffWithContext(ctx, backoff, createStream); err != nil {
		streamcancel()
		return err
	}
	return nil
}

func (c *connection[T]) terminateStream() {
	c.streamClient = nil
	c.streamCancel()
	c.streamCancel = nil
}

// waitForConnection blocks until the connection of the given gRPC client has no
// error. Returns an error if the provided context is canceled.
func waitForConnection(ctx context.Context, grpcClient *grpc.ClientConn) error {
	state := grpcClient.GetState()
	for state != connectivity.Ready && state != connectivity.Idle {
		slog.Info("Waiting for connection to xAS Server to be ready", "state", state)
		if !grpcClient.WaitForStateChange(ctx, state) {
			return ctx.Err() // Context expired
		}
		state = grpcClient.GetState()
	}
	return nil
}
