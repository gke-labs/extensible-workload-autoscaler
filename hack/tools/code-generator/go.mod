module github.com/gke-labs/extensible-workload-autoscaler/hack/tools/code-generator

go 1.26.1

require (
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	golang.org/x/mod v0.32.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	golang.org/x/tools v0.41.0 // indirect
	k8s.io/code-generator v0.36.1 // indirect
	k8s.io/gengo/v2 v2.0.0-20250922181213-ec3ebc5fd46b // indirect
	k8s.io/klog/v2 v2.140.0 // indirect
)

tool (
	k8s.io/code-generator/cmd/client-gen
	k8s.io/code-generator/cmd/deepcopy-gen
	k8s.io/code-generator/cmd/informer-gen
	k8s.io/code-generator/cmd/lister-gen
)
