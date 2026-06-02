IMAGE_TAG ?= latest
IMAGE_NAME ?= xas

.PHONY: build
build:
	go build -o bin/core-node-metrics-provider ./cmd/core-node-metrics-provider
	go build -o bin/server ./cmd/server
	go build -o bin/controller ./cmd/controller
	go build -o bin/core-recommenders ./cmd/core-recommenders

.PHONY: test
test:
	go test ./...

.PHONY: docker-build
docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

.PHONY: codegen
codegen:
	./hack/update-codegen.sh

.PHONY: verify
verify:
	./hack/verify.sh
