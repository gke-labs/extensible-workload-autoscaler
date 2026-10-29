package core_cluster_metrics_provider

import (
	"context"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/client-go/kubernetes"
	customclient "k8s.io/metrics/pkg/client/custom_metrics"
	externalclient "k8s.io/metrics/pkg/client/external_metrics"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

type CoreClusterMetricsProvider struct {
	kubeClient            kubernetes.Interface
	externalMetricsClient externalclient.ExternalMetricsClient
	customMetricsClient   customclient.CustomMetricsClient
	providerLister        listers.MetricProviderClassLister

	grpcConn   *grpc.ClientConn
	grpcClient pb.XASServerClient

	clusterName string
}

func NewCoreClusterMetricsProvider(
	kubeClient kubernetes.Interface,
	externalMetricsClient externalclient.ExternalMetricsClient,
	customMetricsClient customclient.CustomMetricsClient,
	providerLister listers.MetricProviderClassLister,
	serverAddress, clusterName string,
) *CoreClusterMetricsProvider {
	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("Cannot connect to xAS server", "error", err)
		os.Exit(1)
	}
	client := pb.NewXASServerClient(conn)

	return &CoreClusterMetricsProvider{
		kubeClient:            kubeClient,
		externalMetricsClient: externalMetricsClient,
		customMetricsClient:   customMetricsClient,
		providerLister:        providerLister,
		grpcConn:              conn,
		grpcClient:            client,
		clusterName:           clusterName,
	}
}

func (p *CoreClusterMetricsProvider) Run(ctx context.Context) {
	defer p.grpcConn.Close()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.scrapeAndSend()
		case <-ctx.Done():
			return
		}
	}
}

func (p *CoreClusterMetricsProvider) scrapeAndSend() {
	slog.Debug("Cluster scrape cycle starting...")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := p.grpcClient.ListPolicies(ctx, pb.ListPoliciesRequest_builder{
		ClusterName: proto.String(p.clusterName),
	}.Build())
	if err != nil {
		slog.Error("Error listing policies", "error", err)
		return
	}

	var policyBatches []*pb.PolicyMetricBatch

	for _, pol := range resp.GetPolicies() {
		var policyMetrics []*pb.MetricBatch

		for _, m := range policy.MetricDefinitions(pol) {
			class, err := p.providerLister.Get(m.GetProvider())
			if err != nil {
				slog.Warn("MetricProviderClass not found for metric", "class", m.GetProvider(), "metric", m.GetName())
				continue
			}

			switch class.Spec.Type {
			case "ExternalMetrics":
				batches := p.processExternalMetric(pol.GetId().GetNamespace(), m, class)
				if len(batches) > 0 {
					policyMetrics = append(policyMetrics, batches...)
				}
			case "CustomMetrics":
				batches := p.processCustomMetric(pol.GetId().GetNamespace(), pol, m, class)
				if len(batches) > 0 {
					policyMetrics = append(policyMetrics, batches...)
				}
			}
		}

		if len(policyMetrics) > 0 {
			policyBatches = append(policyBatches, pb.PolicyMetricBatch_builder{
				Namespace: proto.String(pol.GetId().GetNamespace()),
				Name:      proto.String(pol.GetId().GetName()),
				Batches:   policyMetrics,
			}.Build())
		}
	}

	if len(policyBatches) > 0 {
		req := pb.IngestMetricsRequest_builder{
			ClusterName:   proto.String(p.clusterName),
			Timestamp:     timestamppb.Now(),
			PolicyMetrics: policyBatches,
		}.Build()
		p.sendBatch(req)
	}
}

func (p *CoreClusterMetricsProvider) sendBatch(req *pb.IngestMetricsRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	slog.Debug("Sending cluster metrics batch", "policies", len(req.GetPolicyMetrics()))
	resp, err := p.grpcClient.IngestMetrics(ctx, req)
	if err != nil {
		slog.Error("Failed to send cluster metrics batch", "error", err)
		return
	}
	slog.Info("Cluster metrics batch sent", "success", resp.GetSuccess())
}
