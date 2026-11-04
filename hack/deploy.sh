#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail

# Builds and deploys xAS components to a Kubernetes cluster.
# Builds sample applications.
#
# Usage: deploy.sh [tag]
#
#        tag: The xAS docker image tag (defaults to "latest").

TAG=${1:-"latest"}

ROOT="$(git rev-parse --show-toplevel)"
CLUSTER_NAME="${CLUSTER_NAME:-xas-e2e}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
SAMPLES_IMAGE_NAME="xas-sample:latest"


# Configure ko repository destination
export KO_DOCKER_REPO="kind.local"

echo "=========================================================="
echo "Building and pushing Sample App Docker image"
echo "=========================================================="
make -C "${ROOT}/samples/sample-app/" docker-build IMAGE_TAG="latest"

echo "Loading Sample App into Kind cluster '$CLUSTER_NAME'..."
kind load docker-image "${SAMPLES_IMAGE_NAME}" --name "$CLUSTER_NAME"


echo "=========================================================="
echo "Deploying xAS manifests to kind cluster '${CLUSTER_NAME}' using ko"
echo "=========================================================="

kubectl --context="${KUBE_CONTEXT}" apply -f "${ROOT}"/manifests/crd/

# For ko resolve to push images to the KinD image cache.
# See https://ko.build/configuration/#local-publishing-options
export KIND_CLUSTER_NAME="${CLUSTER_NAME}"

# Build system binaries and resolve manifest references using ko in one unified step.
# A single yaml file must not contain more than 4 ko:// images. Otherwise it will hang.
"${ROOT}"/hack/run-tool.sh ko resolve -f "${ROOT}"/manifests/xas-server.yaml | kubectl --context="${KUBE_CONTEXT}" apply -f -
"${ROOT}"/hack/run-tool.sh ko resolve -f "${ROOT}"/manifests/xas-core-node-metrics-provider.yaml | kubectl --context="${KUBE_CONTEXT}" apply -f -
"${ROOT}"/hack/run-tool.sh ko resolve -f "${ROOT}"/manifests/xas-core-recommenders.yaml | kubectl --context="${KUBE_CONTEXT}" apply -f -
"${ROOT}"/hack/run-tool.sh ko resolve -f "${ROOT}"/manifests/xas-core-cluster-metrics-provider.yaml | kubectl --context="${KUBE_CONTEXT}" apply -f -

echo "=========================================================="
echo "Restarting xAS workloads..."
echo "=========================================================="

# Force restart workloads to pick up the newly built images
kubectl --context="${KUBE_CONTEXT}" rollout restart deployment -n xas-system xas-server xas-controller xas-core-recommenders xas-core-cluster-metrics-provider || true
kubectl --context="${KUBE_CONTEXT}" rollout restart daemonset -n xas-system xas-core-node-metrics-provider || true


echo "=========================================================="
echo "Deployment Complete!"
echo "=========================================================="
echo ""
echo "Sample apps are located in samples/."
echo "To deploy the 'kubelet-cpu' sample only: kubectl apply -f samples/manifests/kubelet-cpu.yaml"
echo "To deploy all samples: kubectl apply -f samples/manifests"
