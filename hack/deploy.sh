#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail

# Builds and deploys xAS components to a Kubernetes cluster.
# Builds sample applications.
#
# Usage: deploy.sh <kind|gke> [tag]
#
#        tag: The xAS docker image tag (defaults to "latest").

TARGET=${1:-""}
TAG=${2:-"latest"}

ROOT="$(git rev-parse --show-toplevel)"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-xas-e2e}"
SAMPLES_IMAGE_NAME="xas-sample:latest"
DOCKER_REGISTRY=${DOCKER_REGISTRY:-""} # E.g. us-central1-docker.pkg.dev/my-project/xas-dev

if [[ -z "$TARGET" ]]; then
    echo "Usage: $0 <kind|gke> [tag]"
    exit 1
fi

if [[ "$TARGET" == "gke" ]]; then
    SAMPLE_IMAGE_REF="${DOCKER_REGISTRY}/${SAMPLES_IMAGE_NAME}"
fi

# Configure ko repository destination
if [[ "$TARGET" == "kind" ]]; then
    export KO_DOCKER_REPO="kind.local"
elif [[ "$TARGET" == "gke" ]]; then
    if [[ -z "$DOCKER_REGISTRY" ]]; then
        echo "Error: DOCKER_REGISTRY environment variable must be set when targeting GKE"
        exit 1
    fi
    export KO_DOCKER_REPO="$DOCKER_REGISTRY"
fi

echo "=========================================================="
echo "Building and pushing Sample App Docker image"
echo "=========================================================="
make -C "${ROOT}/test/samples/sample-app/" docker-build IMAGE_TAG="latest"

if [[ "$TARGET" == "kind" ]]; then
    echo "Loading Sample App into Kind cluster '$KIND_CLUSTER_NAME'..."
    kind load docker-image "${SAMPLES_IMAGE_NAME}" --name "$KIND_CLUSTER_NAME"
elif [[ "$TARGET" == "gke" ]]; then
    echo "Pushing Sample App image to Registry..."
    docker tag "${SAMPLES_IMAGE_NAME}" "${SAMPLE_IMAGE_REF}"
    docker push "${SAMPLE_IMAGE_REF}"
fi

echo "=========================================================="
echo "Deploying xAS manifests to ${TARGET} using ko"
echo "=========================================================="

kubectl apply -f "${ROOT}"/deploy/crd/

export KIND_CLUSTER_NAME # For ko resolve to push imaged in the KinD image cache.
# Build system binaries and resolve manifest references using ko in one unified step
"${ROOT}"/hack/run-tool.sh ko resolve -f "${ROOT}"/deploy/install.yaml | envsubst | kubectl apply -f -

if [[ "$TARGET" == "gke" ]]; then
    echo "Applying GMP PodMonitoring..."
    kubectl apply -f "${ROOT}"/deploy/gmp/podmonitoring.yaml || echo "Warning: GMP PodMonitoring skipped (CRD missing?)"
fi

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
echo "To deploy the 'kubelet-cpu' sample only: kubectl kustomize test/samples/overlays/${TARGET} | kubectl apply -f - --selector=sample=kubelet-cpu"
echo "To deploy all samples: kubectl apply -k test/samples/overlays/${TARGET}"
