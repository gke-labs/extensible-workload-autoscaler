# Fibonacci Autoscaling Sample

This sample provides a CPU-bound workload (`fibonacci`) and a synthetic traffic
generator (`load-generator`) to evaluate Kubernetes **Horizontal Pod Autoscaler
(HPA)** scaling on a KinD cluster.

## How It Works

*   **`fibonacci` ([fibonacci/app.py](fibonacci/app.py)):** An HTTP server that
    recursively computes `fibonacci(30)` on every request, creating predictable
    CPU load per request. Each pod is configured with `200m` CPU requests and
    limits.
*   **`load-generator` ([load-generator/app.py](load-generator/app.py)):** Runs
    a 30-minute load cycle against `http://fibonacci`, classifies request
    latencies against `BASELINE_LATENCY` (`pass` <= 2x, `slow` 2x–5x, `failed`
    > 5x timeout or error), and exposes Prometheus metrics
    (`loadgen_queries_total` and `loadgen_target_qps`) on port `8080` (Service
    port `80`):
    *   **Phase 1 (15 mins):** Ramps up over 10 seconds to 25 QPS and sustains
        25 QPS.
    *   **Phase 2 (15 mins):** Linearly decays from 25 QPS down to 0 QPS.
*   **HPA Namespace:**
    *   `make deploy-hpa` deploys the workload and an HPA (`averageUtilization:
        60`, `minReplicas: 3`, `maxReplicas: 40`) into the **`fibonacci-hpa`**
        namespace.

---

## Prerequisites & Assumptions

### 1. Required Host Tools

*   `docker`
*   `kind`
*   `kubectl`
*   `make` and `envsubst` (provided by `gettext`)

### 2. Cluster & Component Prerequisites

The `make deploy-hpa` target will automatically create the KinD cluster and
install any missing prerequisite components:

*   **KinD Cluster:** Created automatically via `hack/create-kind-cluster.sh`
    named `xas-e2e` (or override `CLUSTER_NAME`).
*   **Kubernetes `metrics-server`:** Deployed automatically for `deploy-hpa` so
    HPA can read pod CPU utilization metrics.

If you prefer to provision everything upfront, you can run:

```bash
make prereqs
```

Or invoke any step individually:

```bash
# 1. Create the KinD cluster
make create-kind-cluster

# 2. Deploy metrics-server (required for HPA)
make deploy-metrics-server
```

### 3. Resource & Architectural Assumptions

*   **Host CPU Capacity (~9+ cores):** The deployment scales up to `40`
    replicas at `200m` CPU per pod (`8` cores), plus overhead for the load
    generator and Kubernetes control plane.
*   **Hourly Load Schedule:** By default,
    [`load-generator/app.py`](load-generator/app.py) triggers
    `run_load_cycle()` at the top of every hour (`:00`). *(For immediate or
    more frequent testing, edit `main()` in
    [`load-generator/app.py`](load-generator/app.py) before building.)*

---

## Usage

Run all commands below from `samples/fibonacci/`:

### Deploy with HPA

```bash
# Deploy workload + HPA to namespace 'fibonacci-hpa'
make deploy-hpa
```

### Deploy Workload Only

To deploy just the `fibonacci` and `load-generator` manifests without an
autoscaler (defaults to `NAMESPACE=default`):

```bash
make deploy

# Or to a custom namespace:
make deploy NAMESPACE=my-namespace
```

### Monitor Scaling Behaviour

```bash
# Watch HPA scaling in fibonacci-hpa
kubectl get hpa,deployment,pods -n fibonacci-hpa

# Inspect pod CPU usage
kubectl top pods -n fibonacci-hpa
```

### Configuration Variables

You can override any of the following variables when invoking `make` (e.g.,
`make deploy-hpa CLUSTER_NAME=my-cluster`):

| Variable                    | Default              | Description                                                           |
| :-------------------------- | :------------------- | :-------------------------------------------------------------------- |
| `CLUSTER_NAME`              | `xas-e2e`            | Name of the target KinD cluster for `kind load`                       |
| `NAMESPACE`                 | `default`            | Target namespace for `make deploy` (`deploy-hpa` uses `fibonacci-hpa`) |
| `FIBONACCI_DEPLOYMENT_NAME` | `fibonacci`          | Name of the Fibonacci `Deployment`, `Service`, and autoscaler target  |
| `FIBONACCI_IMAGE_NAME`      | `fibonacci`          | Docker image name for the Fibonacci server                            |
| `FIBONACCI_IMAGE_TAG`       | `latest`             | Docker image tag for the Fibonacci server                             |
| `LOADGEN_DEPLOYMENT_NAME`   | `load-generator`     | Name of the load generator `Deployment`                               |
| `LOADGEN_IMAGE_NAME`        | `loadgen`            | Docker image name for the load generator                              |
| `LOADGEN_IMAGE_TAG`         | `latest`             | Docker image tag for the load generator                               |
| `BASELINE_NAMESPACE`        | `fibonacci-baseline` | Ephemeral namespace used by `make baseline`                           |
| `BASELINE_DURATION`         | `30`                 | Duration in seconds for 1-QPS baseline latency measurement            |

*(Note: The `Makefile` caches variable values in hidden `.*.value` files and
automatically regenerates `.*.yaml` manifests whenever any template variable or
`*.yaml.in` file changes.)*

### Cleanup

```bash
# Remove generated YAML and cached variable files from local disk
make clean

# Delete the test namespace from the cluster
kubectl delete namespace fibonacci-hpa

# Optional: Tear down the KinD cluster when finished
make delete-kind-cluster
```
