#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail

# Deploys the Kubernetes Metrics Server to a KinD cluster.
#
# Usage: deploy-metrics-server.sh [version]
#
#        version: The metrics-server release version (defaults to "latest").

VERSION=${1:-"latest"}
CLUSTER_NAME="${CLUSTER_NAME:-xas-e2e}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"

if ! kubectl config get-contexts "${KUBE_CONTEXT}" >/dev/null 2>&1; then
  echo "Error: kubectl context '${KUBE_CONTEXT}' not found. Is KinD cluster '${CLUSTER_NAME}' running?" >&2
  exit 1
fi

if [ "${VERSION}" = "latest" ]; then
  MANIFEST_URL="https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml"
else
  MANIFEST_URL="https://github.com/kubernetes-sigs/metrics-server/releases/download/${VERSION}/components.yaml"
fi

echo "=========================================================="
echo "Deploying metrics-server (${VERSION}) to '${KUBE_CONTEXT}'"
echo "=========================================================="

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

curl -fsSL "${MANIFEST_URL}" -o "${TMP_DIR}/components.yaml"

# KinD uses self-signed kubelet certificates by default, so metrics-server
# needs --kubelet-insecure-tls to scrape kubelets. Patch locally via Kustomize
# before applying so only a single Deployment rollout is triggered.
cat > "${TMP_DIR}/kustomization.yaml" <<'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - components.yaml
patches:
  - target:
      group: apps
      version: v1
      kind: Deployment
      name: metrics-server
      namespace: kube-system
    patch: |-
      - op: add
        path: /spec/template/spec/containers/0/args/-
        value: --kubelet-insecure-tls
EOF

kubectl --context="${KUBE_CONTEXT}" apply --kustomize "${TMP_DIR}"

echo "=========================================================="
echo "Waiting for metrics-server rollout..."
echo "=========================================================="

kubectl --context="${KUBE_CONTEXT}" rollout status deployment metrics-server \
    -n kube-system --timeout=120s

echo "=========================================================="
echo "Metrics Server Deployment Complete!"
echo "=========================================================="
echo ""
echo "Verify with: kubectl top nodes"
echo "             kubectl top pods -A"
