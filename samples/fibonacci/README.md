# Fibonacci Autoscaling Sample

This sample provides a CPU-bound workload (`fibonacci`) and a synthetic traffic
generator (`load-generator`) for testing autoscaling on a KinD cluster.

## How It Works

*   **`fibonacci` ([fibonacci/app.py](fibonacci/app.py)):** An HTTP server that
    recursively computes `fibonacci(30)` on every request, creating predictable
    CPU load per request. Each pod is configured with `200m` CPU requests and
    limits.
*   **`load-generator` ([load-generator/app.py](load-generator/app.py)):** Runs
    a 30-minute load cycle against `http://fibonacci`:
    *   **Phase 1 (15 mins):** Ramps up over 10 seconds to 25 QPS and sustains
        25 QPS.
    *   **Phase 2 (15 mins):** Linearly decays from 25 QPS down to 0 QPS.

---

## Prerequisites & Assumptions

### 1. Required Host Tools

*   `docker`
*   `kind`
*   `kubectl`
*   `make` and `envsubst` (provided by `gettext`)

### 2. Cluster Prerequisites

The `make deploy` target will automatically create the KinD cluster if it does
not already exist:

*   **KinD Cluster:** Created automatically via `hack/create-kind-cluster.sh`
    named `xas-e2e` (or override `CLUSTER_NAME`).

You can also create the cluster explicitly:

```bash
make create-kind-cluster
```

### 3. Architectural Assumptions

*   **Hourly Load Schedule:** By default,
    [`load-generator/app.py`](load-generator/app.py) triggers
    `run_load_cycle()` at the top of every hour (`:00`). *(For immediate or
    more frequent testing, edit `main()` in
    [`load-generator/app.py`](load-generator/app.py) before building.)*

---

## Usage

Run all commands below from `samples/fibonacci/`:

### Deploy Workload

To deploy the `fibonacci` and `load-generator` manifests without an autoscaler
(defaults to `NAMESPACE=default`):

```bash
make deploy

# Or to a custom namespace:
make deploy NAMESPACE=my-namespace
```

### Configuration Variables

You can override any of the following variables when invoking `make` (e.g.,
`make deploy CLUSTER_NAME=my-cluster`):

| Variable                    | Default          | Description                                              |
| :-------------------------- | :--------------- | :------------------------------------------------------- |
| `CLUSTER_NAME`              | `xas-e2e`        | Name of the target KinD cluster for `kind load`          |
| `NAMESPACE`                 | `default`        | Target namespace for `make deploy`                       |
| `FIBONACCI_DEPLOYMENT_NAME` | `fibonacci`      | Name of the Fibonacci `Deployment` and `Service`         |
| `FIBONACCI_IMAGE_NAME`      | `fibonacci`      | Docker image name for the Fibonacci server               |
| `FIBONACCI_IMAGE_TAG`       | `latest`         | Docker image tag for the Fibonacci server                |
| `LOADGEN_DEPLOYMENT_NAME`   | `load-generator` | Name of the load generator `Deployment`                  |
| `LOADGEN_IMAGE_NAME`        | `loadgen`        | Docker image name for the load generator                 |
| `LOADGEN_IMAGE_TAG`         | `latest`         | Docker image tag for the load generator                  |

*(Note: The `Makefile` caches variable values in hidden `.*.value` files and
automatically regenerates `.*.yaml` manifests whenever any template variable or
`*.yaml.in` file changes.)*

### Cleanup

```bash
# Remove generated YAML and cached variable files from local disk
make clean

# Optional: Tear down the KinD cluster when finished
make delete-kind-cluster
```
