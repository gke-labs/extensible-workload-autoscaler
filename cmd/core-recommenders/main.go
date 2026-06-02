package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	k8sinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/gke-labs/extensible-workload-autoscaler/internal/logging"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/recommenders/engine"
	clientset "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/clientset/versioned"
	informers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/informers/externalversions"
)

func main() {
	serverAddress := flag.String("server-address", "localhost:8080", "XAS Control Plane Address (host:port)")
	clusterName := flag.String("cluster-name", "default", "Name of the cluster")
	kubeconfig := flag.String("kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	logging.Setup(*debug)

	slog.Info("Starting Core Recommenders", "address", *serverAddress, "cluster", *clusterName)

	var cfg *rest.Config
	var err error
	if *kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		slog.Error("Error building kubeconfig", "error", err)
		os.Exit(1)
	}

	xasClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		slog.Error("Error building xas clientset", "error", err)
		os.Exit(1)
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Error("Error building kubernetes clientset", "error", err)
		os.Exit(1)
	}

	factory := informers.NewSharedInformerFactory(xasClient, time.Second*30)
	recommenderLister := factory.Xas().V1().RecommenderClasses().Lister()

	kubeFactory := k8sinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	nodeLister := kubeFactory.Core().V1().Nodes().Lister()

	eng := engine.NewEngine(recommenderLister, nodeLister, *serverAddress, *clusterName)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	kubeFactory.Start(ctx.Done())
	kubeFactory.WaitForCacheSync(ctx.Done())

	eng.Run(ctx)
	slog.Info("Shutting down...")
}
