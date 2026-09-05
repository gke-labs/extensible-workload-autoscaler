# XAS (Extensible Autoscaling System)

XAS is a decoupled, model-based autoscaling system for Kubernetes. It separates metric collection, decision making, and actuation into distinct microservices, allowing for greater flexibility and extensibility compared to the standard Horizontal Pod Autoscaler (HPA).

## Project Overview

The system architecture follows a Hub-and-Spoke model:

*   **Server (Control Plane):** The central brain. Aggregates metrics, stores history in memory, and makes scaling decisions.
*   **Controller (Actuator):** The Kubernetes Operator. Syncs `ScalingPolicy` CRDs to the Server and applies scaling recommendations to Deployments.
*   **Core Node Metrics Provider (Agent):** Runs as a DaemonSet. Scrapes metrics from Pods (Prometheus) and Nodes (Kubelet) and pushes them to the Server.
*   **Core Recommenders (Engine):** Runs as a Deployment. Executes advanced scaling algorithms (like Linear Regression) and pushes votes to the Server.

## Key Technologies

*   **Language:** Go (1.25+)
*   **Protocol:** gRPC (Internal communication)
*   **Kubernetes:** Custom Resource Definitions (CRDs) for configuration.
*   **Metrics:** Prometheus text format support.

## Building and Running

### Prerequisites
*   Go 1.25+
*   Docker

### Commands

*   **Build Binaries:**
    ```bash
    make build
    ```
    Builds all components into the `bin/` directory.

*   **Run Tests:**
    ```bash
    make test
    ```
    Runs unit tests for all packages.

*   **Build Docker Image:**
    ```bash
    make docker-build
    ```
    Builds the unified `xas:latest` image containing all binaries.

*   **Code Generation:**
    ```bash
    make codegen
    ```
    Regenerates Kubernetes clientsets, listers, informers, and Protobuf code. Run this after modifying `pkg/apis` or `.proto` files.

*   **Verification:**
    ```bash
    make verify
    ```
    Runs formatting, vet, and build checks.

### Local Deployment

## Deployment & Usage

See [Read-me file](README.md).

## Configuration (CRDs)

### ScalingPolicy
Defines *what* to scale and *how*.
*   **`metrics`**: List of inputs. References a `MetricProviderClass`.
*   **`scaling`**: List of algorithms to determine replica count. References a `RecommenderClass`.
*   **`activation`**: List of algorithms to determine if workload should be 0 or >0.

### MetricProviderClass
Defines a reusable source of metrics.
*   **`type`**: `Kubelet`, `Prometheus`.
*   **`config`**: Provider-specific settings (e.g., port, path).

### RecommenderClass
Defines a reusable scaling strategy.
*   **`type`**: `Linear`, `Threshold`, `Cron`, `ClusterProportional`.
*   **`config`**: Default parameters for the algorithm.

## Directory Structure

*   `api/proto`: gRPC service definitions.
*   `cmd/`: Main entrypoints for each component.
*   `deploy/`: Kubernetes manifests (CRDs, RBAC, Install).
*   `internal/`: Private implementation code.
    *   `server/`: Control Plane logic.
    *   `controller/`: K8s Operator logic.
    *   `core-node-metrics-provider/`: Scraper logic.
    *   `recommenders/`: Scaling algorithms.
*   `pkg/apis`: Kubernetes API types (CRDs).
*   `pkg/client`: Generated K8s clients.
*   `test/e2e`: End-to-end test scenarios and example policies.
