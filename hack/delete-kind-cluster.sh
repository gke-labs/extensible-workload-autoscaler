#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

CLUSTER_NAME="xas-e2e"

echo "Checking for existing kind cluster '$CLUSTER_NAME'..."
if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
  echo "Deleting existing cluster '$CLUSTER_NAME'..."
  kind delete cluster --name "${CLUSTER_NAME}"
  echo "Cluster '$CLUSTER_NAME' deleted."
else
  echo "Cluster '$CLUSTER_NAME' not found."
fi
