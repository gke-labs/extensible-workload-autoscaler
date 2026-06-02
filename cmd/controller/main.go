package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/gke-labs/extensible-workload-autoscaler/internal/controller"
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
	flag.StringVar(&clusterName, "cluster-name", "default", "Name of the cluster this controller is running in")
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

	xasClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		slog.Error("Error building xas clientset", "error", err)
		os.Exit(1)
	}

	xasInformerFactory := informers.NewSharedInformerFactory(xasClient, time.Second*30)

	ctl := controller.NewController(
		kubeClient,
		xasClient,
		xasInformerFactory.Xas().V1().ScalingPolicies(),
		xasInformerFactory.Xas().V1().MetricProviderClasses(),
		xasInformerFactory.Xas().V1().RecommenderClasses(),
		serverAddress,
		clusterName,
	)

	xasInformerFactory.Start(ctx.Done())

	if err = ctl.Run(2, ctx); err != nil {
		slog.Error("Error running controller", "error", err)
		os.Exit(1)
	}
}
