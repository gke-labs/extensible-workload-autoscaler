#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail

# 1. Create Cluster
./hack/create-kind-cluster.sh

CLUSTER_NAME="xas-e2e"
IMAGE_TAG="e2e"

echo "=========================================================="
echo "Setting up XAS on Kind..."
echo "=========================================================="

# 2. Deploy System using shared script
./hack/deploy.sh kind "$IMAGE_TAG"

echo "=========================================================="
echo "Setup Complete!"
echo "You can check the status with:"
echo "kubectl get pods -n xas-system"
