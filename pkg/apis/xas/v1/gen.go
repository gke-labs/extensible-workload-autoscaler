/*
Copyright 2024 The xAS Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

//go:generate bash ../../../../hack/run-tool.sh deepcopy-gen --output-file deepcopy_generated.go --go-header-file ../../../../hack/boilerplate.go.txt github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1
//go:generate bash ../../../../hack/run-tool.sh controller-gen crd paths="./..." output:crd:dir="../../../../deploy/crd"
//go:generate bash ../../../../hack/run-tool.sh client-gen --clientset-name versioned --input-base "" --input github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1 --output-pkg github.com/gke-labs/extensible-workload-autoscaler/pkg/client/clientset --output-dir ../../../../pkg/client/clientset --go-header-file ../../../../hack/boilerplate.go.txt
//go:generate bash ../../../../hack/run-tool.sh lister-gen --output-pkg github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers --output-dir ../../../../pkg/client/listers --go-header-file ../../../../hack/boilerplate.go.txt github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1
//go:generate bash ../../../../hack/run-tool.sh informer-gen --versioned-clientset-package github.com/gke-labs/extensible-workload-autoscaler/pkg/client/clientset/versioned --listers-package github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers --output-pkg github.com/gke-labs/extensible-workload-autoscaler/pkg/client/informers --output-dir ../../../../pkg/client/informers --go-header-file ../../../../hack/boilerplate.go.txt github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1
