package core_node_metrics_provider

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

type SampleState struct {
	Timestamp int64
	Value     float64
}

type CoreNodeMetricsProvider struct {
	kubeClient     kubernetes.Interface
	providerLister listers.MetricProviderClassLister

	grpcConn   *grpc.ClientConn
	grpcClient pb.XASControlPlaneClient

	clusterName   string
	nodeName      string
	hostIP        string
	httpClient    *http.Client // For other HTTP needs if any
	kubeletClient *http.Client
	token         string

	lastSamples map[string]SampleState
}

func NewCoreNodeMetricsProvider(kubeClient kubernetes.Interface, providerLister listers.MetricProviderClassLister, serverAddress, nodeName, hostIP, clusterName string) *CoreNodeMetricsProvider {
	token, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")

	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("did not connect", "error", err)
		os.Exit(1)
	}
	client := pb.NewXASControlPlaneClient(conn)

	return &CoreNodeMetricsProvider{
		kubeClient:     kubeClient,
		providerLister: providerLister,
		grpcConn:       conn,
		grpcClient:     client,
		clusterName:    clusterName,
		nodeName:       nodeName,
		hostIP:         hostIP,
		token:          string(token),
		httpClient:     &http.Client{Timeout: 5 * time.Second},
		kubeletClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
		lastSamples: make(map[string]SampleState),
	}
}

func (a *CoreNodeMetricsProvider) Run(ctx context.Context) {
	defer a.grpcConn.Close()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.scrapeAndSend()
		case <-ctx.Done():
			return
		}
	}
}

func (a *CoreNodeMetricsProvider) scrapeAndSend() {
	slog.Info("Scrape cycle starting...")

	// 1. Get Policies from Control Plane
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := a.grpcClient.ListPolicies(ctx, &pb.ListPoliciesRequest{
		ClusterName: a.clusterName,
	})
	if err != nil {
		slog.Error("Error listing policies", "error", err)
		return
	}

	slog.Info("Found policies", "count", len(resp.Policies))

	// 2. Discover needed Kubelet endpoints and group metrics by Policy
	kubeletConfigs := make(map[string]bool) // key: "port|path"
	policies := make(map[string]*groupedPolicy)

	for _, pol := range resp.Policies {
		var relevantMetrics []*pb.MetricDefinition
		for _, m := range pol.Metrics {
			class, err := a.providerLister.Get(m.Provider)
			if err != nil {
				slog.Warn("MetricProviderClass not found for metric", "class", m.Provider, "metric", m.Name)
				continue
			}

			if class.Spec.Type == "Kubelet" {
				relevantMetrics = append(relevantMetrics, m)
				// Record needed Kubelet endpoint
				port := m.Params["port"]
				if port == "" {
					port = class.Spec.Config["port"]
				}
				path := m.Params["path"]
				if path == "" {
					path = class.Spec.Config["path"]
				}
				key := fmt.Sprintf("%s|%s", port, path)
				kubeletConfigs[key] = true
			} else if class.Spec.Type == "Prometheus" {
				relevantMetrics = append(relevantMetrics, m)
			}
		}

		if len(relevantMetrics) > 0 {
			key := fmt.Sprintf("%s/%s", pol.Id.Namespace, pol.Id.Name)
			policies[key] = &groupedPolicy{
				Namespace: pol.Id.Namespace,
				Name:      pol.Id.Name,
				Selector:  pol.Selector,
				Metrics:   relevantMetrics,
			}
		}
	}

	// 3. Scrape all required Kubelet endpoints
	kubeletMetrics := make(map[string]map[string]*KubeletPodMetrics)
	for cfg := range kubeletConfigs {
		parts := strings.Split(cfg, "|")
		metrics := a.scrapeKubelet(parts[0], parts[1])
		if metrics != nil {
			kubeletMetrics[cfg] = metrics
		}
	}

	seenKeys := make(map[string]bool)

	// 4. Process each policy
	for _, pol := range policies {
		a.processGroupedPolicy(pol, kubeletMetrics, seenKeys)
	}

	for k := range a.lastSamples {
		if !seenKeys[k] {
			delete(a.lastSamples, k)
		}
	}
}

type groupedPolicy struct {
	Namespace string
	Name      string
	Selector  string
	Metrics   []*pb.MetricDefinition
}

func (a *CoreNodeMetricsProvider) processGroupedPolicy(policy *groupedPolicy, kubeletMetrics map[string]map[string]*KubeletPodMetrics, seenKeys map[string]bool) {
	if policy.Selector == "" {
		return
	}

	opts := metav1.ListOptions{
		LabelSelector: policy.Selector,
	}
	if a.nodeName != "" {
		opts.FieldSelector = fmt.Sprintf("spec.nodeName=%s", a.nodeName)
	}

	pods, err := a.kubeClient.CoreV1().Pods(policy.Namespace).List(context.TODO(), opts)
	if err != nil {
		slog.Error("Error listing pods", "error", err)
		return
	}
	slog.Debug("Found pods for policy", "policy", policy.Name, "count", len(pods.Items))

	var podMetrics []*pb.MetricBatch

	for _, pod := range pods.Items {
		if pod.Status.Phase != "Running" || !isPodReady(&pod) {
			continue
		}

		for _, m := range policy.Metrics {
			class, err := a.providerLister.Get(m.Provider)
			if err != nil {
				continue
			}

			var batches []*pb.MetricBatch

			switch class.Spec.Type {
			case "Kubelet":
				if kubeletMetrics != nil {
					batches = a.processKubeletMetric(pod, m, kubeletMetrics, seenKeys, class)
				}
			case "Prometheus":
				batches = a.processPrometheusMetric(pod, m, class)
			default:
				slog.Warn("Unknown provider type for metric", "type", class.Spec.Type, "metric", m.Name)
			}

			if len(batches) > 0 {
				podMetrics = append(podMetrics, batches...)
			}
		}
	}

	if len(podMetrics) > 0 {
		req := &pb.IngestMetricsRequest{
			Timestamp: time.Now().Unix(),
			Policies: []*pb.PolicyBatch{
				{
					Namespace: policy.Namespace,
					Name:      policy.Name,
					Batches:   podMetrics,
				},
			},
		}
		a.sendBatch(req)
	}
}

func (a *CoreNodeMetricsProvider) sendBatch(pbReq *pb.IngestMetricsRequest) {
	pbReq.ClusterName = a.clusterName

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	slog.Debug("Sending metrics batch", "policies", len(pbReq.Policies))
	resp, err := a.grpcClient.IngestMetrics(ctx, pbReq)
	if err != nil {
		slog.Error("Failed to send batch", "error", err)
		return
	}
	slog.Info("Batch sent", "success", resp.Success)
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
