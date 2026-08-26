package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	customclient "k8s.io/metrics/pkg/client/custom_metrics"
	externalclient "k8s.io/metrics/pkg/client/external_metrics"

	provider "github.com/gke-labs/extensible-workload-autoscaler/internal/core-cluster-metrics-provider"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/logging"
	clientset "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/clientset/versioned"
	informers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/informers/externalversions"
)

func main() {
	var serverAddress string
	var clusterName string
	var kubeconfig string
	var debug bool

	flag.StringVar(&serverAddress, "server-address", "xas-server:8080", "Address of the XAS Control Plane (host:port)")
	flag.StringVar(&clusterName, "cluster-name", "default", "Name of the cluster this provider is running in")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.BoolVar(&debug, "debug", false, "Enable debug logging")
	flag.Parse()

	logging.Setup(debug)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var cfg *rest.Config
	var err error
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		slog.Error("Error building kubeconfig", "error", err)
		os.Exit(1)
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Error("Error building kubernetes clientset", "error", err)
		os.Exit(1)
	}

	externalMetricsClient, err := externalclient.NewForConfig(cfg)
	if err != nil {
		slog.Error("Error building external metrics clientset", "error", err)
		os.Exit(1)
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		slog.Error("Error building discovery client", "error", err)
		os.Exit(1)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))

	customMetricsClient, err := customclient.NewForVersionForConfig(cfg, mapper, schema.GroupVersion{Group: "custom.metrics.k8s.io", Version: "v1beta2"})
	if err != nil {
		slog.Error("Error building custom metrics clientset", "error", err)
		os.Exit(1)
	}

	xasClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		slog.Error("Error building xas clientset", "error", err)
		os.Exit(1)
	}

	factory := informers.NewSharedInformerFactory(xasClient, time.Second*30)
	providerLister := factory.Xas().V1().MetricProviderClasses().Lister()

	p := provider.NewCoreClusterMetricsProvider(kubeClient, externalMetricsClient, customMetricsClient, providerLister, serverAddress, clusterName)

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	slog.Info("XAS Core Cluster Metrics Provider starting...")
	p.Run(ctx)
}
