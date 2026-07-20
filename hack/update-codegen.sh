#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
MODULE="github.com/gke-labs/extensible-workload-autoscaler"
PKG="${MODULE}/pkg/apis/xas/v1"

cd "${ROOT}"

echo "Generating clientset..."
hack/run-tool.sh client-gen \
  --clientset-name versioned \
  --input-base "" \
  --input "${PKG}" \
  --output-pkg "${MODULE}/pkg/client/clientset" \
  --output-dir "${ROOT}/pkg/client/clientset" \
  --go-header-file hack/boilerplate.go.txt

echo "Generating listers..."
hack/run-tool.sh lister-gen \
  --output-pkg "${MODULE}/pkg/client/listers" \
  --output-dir "${ROOT}/pkg/client/listers" \
  --go-header-file hack/boilerplate.go.txt \
  "${PKG}"

echo "Generating informers..."
hack/run-tool.sh informer-gen \
  --versioned-clientset-package "${MODULE}/pkg/client/clientset/versioned" \
  --listers-package "${MODULE}/pkg/client/listers" \
  --output-pkg "${MODULE}/pkg/client/informers" \
  --output-dir "${ROOT}/pkg/client/informers" \
  --go-header-file hack/boilerplate.go.txt \
  "${PKG}"

echo "Generating deepcopy..."
hack/run-tool.sh deepcopy-gen \
  --output-file deepcopy_generated.go \
  --go-header-file hack/boilerplate.go.txt \
  "${PKG}"

echo "Generating CRDs..."
hack/run-tool.sh controller-gen crd paths="${ROOT}"/pkg/apis/xas/v1/... output:crd:dir="${ROOT}"/deploy/crd

echo "Generating protobuf..."
# Resolve plugin paths using 'go tool -n'
PROTOC_GEN_GO=$(go tool -n google.golang.org/protobuf/cmd/protoc-gen-go)
PROTOC_GEN_GO_GRPC=$(go tool -n google.golang.org/grpc/cmd/protoc-gen-go-grpc)

protoc \
    --plugin="protoc-gen-go=${PROTOC_GEN_GO}" \
    --plugin="protoc-gen-go-grpc=${PROTOC_GEN_GO_GRPC}" \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    api/proto/v1alpha/xas.proto

echo "Done."
