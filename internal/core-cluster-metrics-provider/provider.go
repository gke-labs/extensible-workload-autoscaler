package core_cluster_metrics_provider

import (
	"context"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/client-go/kubernetes"
	customclient "k8s.io/metrics/pkg/client/custom_metrics"
	externalclient "k8s.io/metrics/pkg/client/external_metrics"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

type CoreClusterMetricsProvider struct {
	kubeClient            kubernetes.Interface
	externalMetricsClient externalclient.ExternalMetricsClient
	customMetricsClient   customclient.CustomMetricsClient
	providerLister        listers.MetricProviderClassLister

	grpcConn   *grpc.ClientConn
	grpcClient pb.XASControlPlaneClient

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
	client := pb.NewXASControlPlaneClient(conn)

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

	resp, err := p.grpcClient.ListPolicies(ctx, &pb.ListPoliciesRequest{
		ClusterName: p.clusterName,
	})
	if err != nil {
		slog.Error("Error listing policies", "error", err)
		return
	}

	var policyBatches []*pb.PolicyBatch

	for _, pol := range resp.Policies {
		var policyMetrics []*pb.MetricBatch

		for _, m := range pol.Metrics {
			class, err := p.providerLister.Get(m.Provider)
			if err != nil {
				slog.Warn("MetricProviderClass not found for metric", "class", m.Provider, "metric", m.Name)
				continue
			}

			if class.Spec.Type == "ExternalMetrics" {
				batches := p.processExternalMetric(pol.Id.Namespace, m, class)
				if len(batches) > 0 {
					policyMetrics = append(policyMetrics, batches...)
				}
			} else if class.Spec.Type == "CustomMetrics" {
				batches := p.processCustomMetric(pol.Id.Namespace, pol, m, class)
				if len(batches) > 0 {
					policyMetrics = append(policyMetrics, batches...)
				}
			}
		}

		if len(policyMetrics) > 0 {
			policyBatches = append(policyBatches, &pb.PolicyBatch{
				Namespace: pol.Id.Namespace,
				Name:      pol.Id.Name,
				Batches:   policyMetrics,
			})
		}
	}

	if len(policyBatches) > 0 {
		req := &pb.IngestMetricsRequest{
			ClusterName: p.clusterName,
			Timestamp:   time.Now().Unix(),
			Policies:    policyBatches,
		}
		p.sendBatch(req)
	}
}

func (p *CoreClusterMetricsProvider) sendBatch(req *pb.IngestMetricsRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	slog.Debug("Sending cluster metrics batch", "policies", len(req.Policies))
	resp, err := p.grpcClient.IngestMetrics(ctx, req)
	if err != nil {
		slog.Error("Failed to send cluster metrics batch", "error", err)
		return
	}
	slog.Info("Cluster metrics batch sent", "success", resp.Success)
}
