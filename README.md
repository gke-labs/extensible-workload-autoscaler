# The Kubernetes *Extensible Workload Autoscaler* (xAS)

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

## What is xAS?

> [!CAUTION]
> This is a very early prototype, and is intended mainly to use a tool to investigate
> API ideas and high-level architectures. It is not intended for production use. We
> are working with the sig-autoscaling community to begin work on a usable autoscaler
> based on the ideas explored in this repo.

xAS is the *Extensible Workload Autoscaler* for Kubernetes.

It is a Pod autoscaler that can be used together with, or as a replacement for,
standard Kubernetes
[horizontal](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/)
and
[vertical](https://kubernetes.io/docs/concepts/workloads/autoscaling/vertical-pod-autoscale/)
Pod autoscalers.

It offers several advantages, such as *extensibility* (enabling pluggable custom
metric providers) and *multi-dimensional scaling* (scaling both horizontally and
vertically).

## Why xAS?

Standard Kubernetes Pod autoscaling is reaching its natural limits. Horizontal
and Vertical Pod autoscalers have hardcoded logic best suited for scaling on
resources such as CPU and memory. Their only extension points are metric APIs,
but those are hard to use, limited to one metric provider per cluster, and in
general do not scale with cluster size.

The rise of new workload types, such as those for AI inference, has forced
developers to build fragmented, domain-specific autoscaling solutions from
scratch (see for example [1], [2]). Because standard Kubernetes lacks a
pluggable autoscaling framework, these custom engines must reinvent essential
infrastructure like metric collection and custom APIs. By building xAS as a
pluggable autoscaling platform, we aim to eliminate this fragmentation and
accelerate innovation, allowing developers to focus purely on custom scaling
heuristics.

For more details, see our
[positioning document](https://docs.google.com/document/d/1acUnlcyks4UvdlVnvwSu3RUoGlYElOKfF8dtRETK8oM).

[1]: https://github.com/llm-d-incubation/workload-variant-autoscaler
[2]: https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/v2

## Quickstart

### Prerequisites

1.  *KinD* development clusters

    xAS is in an early stage of development and only supports KinD clusters.
    Refer to the
    [KinD website](https://kind.sigs.k8s.io/#installation-and-usage) for
    installation instructions.

2.  *Protoc Protocol Buffer Compiler*

    Refer to https://grpc.io/docs/protoc-installation/ for installation
    instructions.

### Getting Started

To set up the development environment on a KinD cluster:

```sh
# Create a KinD cluster.
$ hack/create-kind-cluster.sh

# Build and deploy xAS.
$ hack/deploy.sh kind

# Deploy the samples.
$ kubectl apply -k test/samples/overlays/kind

# Observe a sample scaling policy.
$ kubectl describe scalingpolicy kubelet-cpu -n sample-kubelet-cpu

# Update codebase and CRDs.
# Re-run `hack/deploy.sh kind` to re-deploy to your KinD cluster.
```

To cleanup and delete the cluster, run `hack/delete-kind-cluster.sh`.

## How Does xAS Work?

See the [user guide](docs/user-guide.md) to learn how to use xAS.

The [architecture document](docs/architecture.md) explains how xAS is designed
and defines the key concepts.

## Contributing

This project is licensed under the [Apache 2.0 License](LICENSE).

Please check our [contribution guidelines](docs/contributing.md) to get started.
While we welcome all types of contributions, note that this project is in its
early stages, our current focus is on establishing the core architecture,
identifying suitable use cases, and refining our API definitions.

We follow
[Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).

## Disclaimer

This is not an officially supported Google product.

This project is not eligible for the Google Open Source Software Vulnerability
Rewards Program.
