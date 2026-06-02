IMAGE_TAG ?= latest
IMAGE_NAME ?=xas-sample

.PHONY: docker-build
docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .
