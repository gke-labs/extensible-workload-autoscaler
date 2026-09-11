# Requirements

This document captures the high-level requirements for xAS, organized by area.

Every requirement is assigned a priority. *Mandatory* ones make up the core feature set that xAS must deliver; lower priorities represent stretch, optional, and exploratory goals.

## Scaling types

| Requirement | Priority | Description |
| :--- | :--- | :--- |
| Horizontal | Mandatory | At least at parity with existing HPA. |
| Per-workload Vertical | Mandatory | At least at parity with existing VPA. Includes (event-based) immediate scale-up on OOM. |
| Multi-dimensional scaling | Mandatory | Combining HPA and VPA. Examples of existing solutions are [MPA][mpa], [IBM multi-dimensional autoscaler][ibm-mda], and [Borg Autopilot][borg-autopilot]. Two modes: horizontal-first scaling (i.e. HPA, then rightsizing) and vertical-first scaling (lower priority as it requires IPPR, preemption). |
| Per-Pod Vertical scaling | Mandatory | Typically useful to scale individual Pods in DaemonSets, and possibly batch [Jobs][k8s-jobs]. |

## Use-cases

| Requirement | Priority | Description |
| :--- | :--- | :--- |
| Compatibility with HPA/VPA | Mandatory | xAS should support the existing HPA and VPA APIs with near-perfect fidelity, allowing to migrate to this stack. |
| Cron recommender | High | Ensures recommenders can feed from something else than a metric provider. |
| Allow to pick pods to evict on scale-down | High | This may require some non-trivial interactions with the scheduler or cluster autoscaler. |
| Scale-to-zero | High | Full support, including (event-based) fast SFZ, buffering during STZ, selecting different metrics for STZ & SFZ. |
| llm-d autoscaler | Medium | Emulate llm-d's autoscaler. |
| Simple use-cases can be expressed using CEL expressions | Medium | CEL is a first-class citizen: metric aggregation or transformations, recommendations can be expressed in CEL. |
| Utilization-based load-balancing (UBB) | Medium | Both standard and per-pod. See the [documentation][ubb]. |
| Interface with various metrics sources and autoscalers | Medium | Integrate with various metric sources or existing autoscalers, e.g. Azure, Datadog,.... |
| Cluster-proportional autoscaler & bootstrapping | Low | Vertical and horizontal autoscaling based on cluster size. See [cluster-proportional-vertical-autoscaler][cpva], [cluster-proportional-autoscaler][cpa] and [addon-resizer][addon-resizer]. This is typically useful for autoscaling core K8s components, e.g. Kube-DNS. This may require starting xAS in a simplified mode (to ensure it doesn't require a dependency that it's itself autoscaling). |
| Event-based scaling | Low | Support events, e.g. fast scale-up following OOMs, or for SFZ (scaling from 0 to 1). |
| Predictive autoscaling | Low | It should be possible to make use of a timeseries prediction models. |

## Extensibility (custom metric providers & recommenders)

| Requirement | Priority | Description |
| :--- | :--- | :--- |
| New recommenders and metric sources can be plugged in | Mandatory | The plugin API is non-ambiguous, well documented, and designed to evolve if needed. Metrics sources can be configured to talk to multiple recommenders, such as [autoscaling.googleapis.com][autoscaling-api] when on GCP. |
| Plugins are easy to use | Medium | Using a plugin (understanding what parameters it takes, and finding its documentation, installing, removing) is easy and identical for all plugins. |
| Plugins are easy to implement | Low | There is a standard way to develop plugins, and in most cases the creation of new plugins can be delegated to AI. |

## Metrics & Aggregation

| Requirement | Priority | Description |
| :--- | :--- | :--- |
| Support all well-known metric types | Mandatory | E.g. Counters. Less-used types (e.g. summary) may not be supported. |
| Raw metrics | Mandatory | Reading metrics emitted in real-time by pods (e.g. Kubelet, custom /metrics endpoints). |
| Stored metrics | Mandatory | Use PromQL or similar queries to pull metrics from sources like Prometheus servers, or GCP Cloud Monitoring. |
| Aggregations over long time windows | Mandatory | Aggregate over e.g. a week. This is a building block for Vertical scaling. |
| External-raw metrics | Medium | This should support Inference Gateway Picker, and GPU and TPU metrics. |
| Support OTel protocol | Medium | OTel being push-based, allowing for event-based autoscaling, e.g. fast Scale-from-zero. |
| Metrics arithmetics | Medium | Simple algebraic expressions, divisions of 2 metrics. |
| Simple aggregations | Medium | Rates, filtering across timeseries, ... |

## Horizontals

| Requirement | Priority | Description |
| :--- | :--- | :--- |
| Scales to large clusters | High | Scales to >100k nodes, 200k pods, 10k HPAs. This may require users to increase controller CPU, memory, and/or replica count. |
| Reliability compatible with control plane components | Medium | It is both acceptable and desirable to support multiple reliability modes. Higher reliability is understood to be tied to higher cost (e.g. running multiple replicas, or persistent storage). |
| Observability | Medium | Relevant observability signals (number of pods, missing pods, scaling decisions, ...) can be found and possibly stored. |
| Explainability | Medium | It is easy to understand why an autoscaling decision was taken. |

<!-- Link definitions -->

[mpa]: https://github.com/kubernetes/autoscaler/blob/db5e83bc1afeaef6bfd3d69befef9c12ad469f94/multidimensional-pod-autoscaler/AEP.md#goals
[ibm-mda]: https://docs.google.com/document/d/1alCV1wxIjkn85rpOY8hRZBL86QIXZO9RNVtRcnykFgc/edit?usp=sharing
[borg-autopilot]: https://dl.acm.org/doi/pdf/10.1145/3342195.3387524
[k8s-jobs]: https://kubernetes.io/docs/concepts/workloads/controllers/job/
[ubb]: https://docs.cloud.google.com/kubernetes-engine/docs/concepts/about-ubb
[cpva]: https://github.com/kubernetes-sigs/cluster-proportional-vertical-autoscaler
[cpa]: https://github.com/kubernetes-sigs/cluster-proportional-autoscaler
[addon-resizer]: https://github.com/kubernetes/autoscaler/blob/master/addon-resizer/README.md