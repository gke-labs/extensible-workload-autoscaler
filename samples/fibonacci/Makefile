REPO_ROOT ?= $(shell git rev-parse --show-toplevel 2>/dev/null || realpath ../..)

CLUSTER_NAME ?= xas-e2e
KUBE_CONTEXT := kind-$(CLUSTER_NAME)

NAMESPACE ?= default
FIBONACCI_DEPLOYMENT_NAME ?= fibonacci
FIBONACCI_IMAGE_NAME ?= fibonacci
FIBONACCI_IMAGE_TAG ?= latest
LOADGEN_DEPLOYMENT_NAME ?= load-generator
LOADGEN_IMAGE_NAME ?= loadgen
LOADGEN_IMAGE_TAG ?= latest

.DELETE_ON_ERROR:

.PHONY: docker-build
docker-build:
	cd fibonacci && docker build -t $(FIBONACCI_IMAGE_NAME):$(FIBONACCI_IMAGE_TAG) .
	cd load-generator && docker build -t $(LOADGEN_IMAGE_NAME):$(LOADGEN_IMAGE_TAG) .

.PHONY: kind-load
kind-load: ensure-kind-cluster docker-build
	kind load docker-image $(FIBONACCI_IMAGE_NAME):$(FIBONACCI_IMAGE_TAG) --name $(CLUSTER_NAME)
	kind load docker-image $(LOADGEN_IMAGE_NAME):$(LOADGEN_IMAGE_TAG) --name $(CLUSTER_NAME)

# Prerequisites
.PHONY: ensure-kind-cluster
ensure-kind-cluster:
	@if ! kind get clusters 2>/dev/null | grep -qxF "$(CLUSTER_NAME)"; then \
		CLUSTER_NAME=$(CLUSTER_NAME) $(REPO_ROOT)/hack/create-kind-cluster.sh; \
	fi

.PHONY: create-kind-cluster
create-kind-cluster:
	CLUSTER_NAME=$(CLUSTER_NAME) $(REPO_ROOT)/hack/create-kind-cluster.sh

.PHONY: delete-kind-cluster
delete-kind-cluster:
	CLUSTER_NAME=$(CLUSTER_NAME) $(REPO_ROOT)/hack/delete-kind-cluster.sh

# A dependency that forces a target's recipe to always run while still allowing
# Make to check the target file's timestamp afterward (unlike .PHONY, which
# would always force downstream dependents to rebuild). Also works with pattern
# rules, which cannot be declared .PHONY.
.PHONY: .FORCE_CHECK
.FORCE_CHECK:

# List of variables that can be substituted into the yaml.in files.
TEMPLATE_VARS := \
	FIBONACCI_DEPLOYMENT_NAME \
	FIBONACCI_IMAGE_NAME \
	FIBONACCI_IMAGE_TAG \
	LOADGEN_DEPLOYMENT_NAME \
	LOADGEN_IMAGE_NAME \
	LOADGEN_IMAGE_TAG

export $(TEMPLATE_VARS)

# Store the variable value (so that make can determine if the value has changed
# on subsequent runs).
.%.value: .FORCE_CHECK
	@echo "$($*)" | cmp --silent - $@ || echo "$($*)" > $@

# Rebuild the yaml files if the corresponding yaml.in file changed, or if any
# of the dependent variables changed.
.%.yaml: %.yaml.in $(TEMPLATE_VARS:%=.%.value)
	envsubst '$(patsubst %,$${%},$(TEMPLATE_VARS))' < $< > $@

YAML_FILES := .fibonacci.yaml .load_generator.yaml

.PHONY: deploy
deploy: kind-load $(YAML_FILES)
	kubectl --context "$(KUBE_CONTEXT)" create namespace $(NAMESPACE) \
		--dry-run=client -o yaml \
		| kubectl --context "$(KUBE_CONTEXT)" apply -f -
	kubectl --context "$(KUBE_CONTEXT)" apply -n $(NAMESPACE) $(YAML_FILES:%=-f %)
	kubectl --context "$(KUBE_CONTEXT)" rollout restart \
		deployment/$(FIBONACCI_DEPLOYMENT_NAME) \
		deployment/$(LOADGEN_DEPLOYMENT_NAME) -n $(NAMESPACE)

.PHONY: clean
clean:
	rm -f $(YAML_FILES)
	rm -f .*.value
