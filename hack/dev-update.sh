#!/bin/bash
set -e

CLUSTER_NAME="xas-e2e"
IMAGE_TAG="e2e"

echo "Building binaries..."
make build
make extensions

echo "Building docker image..."
make docker-build IMAGE_TAG=$IMAGE_TAG
make docker-build-extensions IMAGE_TAG=$IMAGE_TAG

echo "Loading image into Kind cluster '$CLUSTER_NAME'..."
kind load docker-image xas:$IMAGE_TAG --name $CLUSTER_NAME
kind load docker-image xas-extensions:$IMAGE_TAG --name $CLUSTER_NAME

echo "Updating CRDs..."
kubectl apply -f deploy/crd/

echo "Deploying Network Provider..."
kubectl apply -f extensions/metric-provider/network/deploy/install.yaml

echo "Restarting components..."
kubectl rollout restart deployment/xas-controller -n xas-system
kubectl rollout restart deployment/xas-server -n xas-system
kubectl rollout restart deployment/xas-core-recommenders -n xas-system
kubectl rollout restart daemonset/xas-core-node-metrics-provider -n xas-system
kubectl rollout restart daemonset/network-provider -n xas-system

echo "Waiting for rollout..."
kubectl rollout status deployment/xas-controller -n xas-system
kubectl rollout status deployment/xas-server -n xas-system
kubectl rollout status deployment/xas-core-recommenders -n xas-system
kubectl rollout status daemonset/xas-core-node-metrics-provider -n xas-system
kubectl rollout status daemonset/network-provider -n xas-system

echo "Done! Components updated."
