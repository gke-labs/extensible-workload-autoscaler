IMAGE_TAG ?= latest
IMAGE_NAME ?= xas
GO_VERSION := $(shell awk '$$1 == "go" {print $$2}' "$$(go env GOMOD)")
GEN_PATHS := pkg/client deploy/crd pkg/apis/xas/v1/deepcopy_generated.go api/proto/v1alpha

.PHONY: build
build:
	go build -o bin/core-node-metrics-provider ./cmd/core-node-metrics-provider
	go build -o bin/core-cluster-metrics-provider ./cmd/core-cluster-metrics-provider
	go build -o bin/server ./cmd/server
	go build -o bin/controller ./cmd/controller
	go build -o bin/core-recommenders ./cmd/core-recommenders

.PHONY: test
test:
	go test ./...

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: verify-fmt
verify-fmt:
	@unformatted=$$(GOTOOLCHAIN=go$(GO_VERSION) gofmt -l $$(find . -path ./vendor -prune -o -name '*.go' -print)); \
	if [ -n "$$unformatted" ]; then \
		echo "Error: The following files are not formatted:"; \
		echo "$$unformatted"; \
		echo "Run 'make fmt' to fix."; \
		exit 1; \
	fi

.PHONY: vet
vet:
	go vet ./...

.PHONY: docker-build
docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

.PHONY: codegen
codegen:
	go generate ./...

.PHONY: verify-codegen
verify-codegen: codegen
	@if ! git diff --quiet -- $(GEN_PATHS); then \
		echo "Error: Generated code is out of date."; \
		echo "The following files have changed:"; \
		git diff --name-only -- $(GEN_PATHS); \
		echo "Run 'make codegen' and commit the changes."; \
		exit 1; \
	fi

.PHONY: verify
verify: verify-fmt vet build test verify-codegen
