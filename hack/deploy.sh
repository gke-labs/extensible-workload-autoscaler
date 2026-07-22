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
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-xas-e2e}"
SAMPLES_IMAGE_NAME="xas-sample:latest"


# Configure ko repository destination
export KO_DOCKER_REPO="kind.local"

echo "=========================================================="
echo "Building and pushing Sample App Docker image"
echo "=========================================================="
make -C "${ROOT}/test/samples/sample-app/" docker-build IMAGE_TAG="latest"

echo "Loading Sample App into Kind cluster '$KIND_CLUSTER_NAME'..."
kind load docker-image "${SAMPLES_IMAGE_NAME}" --name "$KIND_CLUSTER_NAME"


echo "=========================================================="
echo "Deploying xAS manifests to kind cluster '${KIND_CLUSTER_NAME}' using ko"
echo "=========================================================="

kubectl apply -f "${ROOT}"/deploy/crd/

export KIND_CLUSTER_NAME # For ko resolve to push images to the KinD image cache.

# Build system binaries and resolve manifest references using ko in one unified step
"${ROOT}"/hack/run-tool.sh ko resolve -f "${ROOT}"/deploy/install.yaml | envsubst | kubectl apply -f -

echo "=========================================================="
echo "Restarting xAS workloads..."
echo "=========================================================="

# Force restart workloads to pick up the newly built images
kubectl rollout restart deployment -n xas-system xas-server xas-controller xas-core-recommenders || true
kubectl rollout restart daemonset -n xas-system xas-core-node-metrics-provider || true


echo "=========================================================="
echo "Deployment Complete!"
echo "=========================================================="
echo ""
echo "Sample apps are located in test/samples."
echo "To deploy the 'kubelet-cpu' sample only: kubectl apply -f test/samples/manifests/kubelet-cpu.yaml"
echo "To deploy all samples: kubectl apply -f test/samples/manifests"
