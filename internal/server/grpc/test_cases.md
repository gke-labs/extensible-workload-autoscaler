The following test cases are for the gRPC server implementation of the XASControlPlane service.

For each test case, a Go unit tests exists in server_functional_test.go. 
The test cases are grouped by the gRPC method they test.
For each test case, assertions must ALWAYS be on the whole returned gRPC response proto, using "cmp" package, not by checking individual fields.


# UpdatePolicy

* An attempt to call UpdatePolicy with a policy that has an invalid ID (missing cluster, namespace, name) is rejected.
* An attempt to call UpdatePolicy with a metric definition that combines `Type: Histogram` and a `Window` is rejected (invalid argument).
* Updating policies with same namespace and name across different clusters works as expected.
* Updating a policy updates all the specified fields. 

# DeletePolicy

* An attempt to call DeletePolicy with a policy that has an invalid ID (missing cluster, namespace, name) is rejected.
* Deleting effectively removes the policy from the control plane.
* Deleting a policy that does not exist is not rejected (deleting is idempotent).

# ListPolicies 

* An attempt to call ListPolicies with an invalid cluster name is rejected.
* Listing policies returns all policies for the specified cluster.
* Listing policies returns an empty list if no policies are found for the specified cluster.

# UpdateWorkload

* An attempt to call UpdateWorkload with an invalid policy ID is rejected.
* An attempt to call UpdateWorkload with an policy that doesnt exist returns not found error.
* An attemp to call UpdateWorkload with PodState which has missing name, node_name is rejected.

# GetControlMetrics 

* An attempt to call GetControlMetrics with an invalid policy ID is rejected.
* An attempt to call GetControlMetrics with an policy that doesnt exist returns not found error.
* A gauge metric is properly aggregated using the aggregation method specified in the metric definition.
* A counter metric is properly aggregated using the aggregation method specified in the metric definition.
* A histogram metric is properly aggregated using the aggregation method specified in the metric definition and the histogram configuration.
* Metrics from pods that are not ready (reported as `IsReady: false` in `UpdateWorkload`) are ignored during aggregation.
* Metrics from pods that are not present in the workload (not reported in `UpdateWorkload`) are ignored during aggregation.
* When a policy is updated and an existing metric is deleted, the control metrics are updated properly and the deleted metric is removed from the control metrics.
* When a policy is updated and a new metric is added, the control metrics are updated properly and the new metric is added to the control metrics.
* A gauge metric is properly computed when using sliding window.
* A counter metric is properly computed when using sliding window.
* A histogram metric is properly computed when using sliding window.
* A gauge metric is properly computed when using decaying histogram window.
* A counter metric is properly computed when using decaying histogram window.
* A histogram metric is properly computed when using decaying histogram window.

# UpdateRecommenderState

* An attempt to call UpdateRecommenderState with an invalid policy ID is rejected (invalid argument)
* An attempt to call UpdateRecommenderState with an policy that doesnt exist returns not found error.
* An attempt to call UpdateRecommenderState with a recommneder that is not defined in the policy is rejected (invalid argument).
* An attempt to call UpdateRecommenderState with an empty RecommenderStatus clears out the recommender state for that recommender / policy.
* An attempt to call UpdateRecommenderState with a RecommenderStatus with negative desired replicas is rejected (invalid argument).
* An attempt to call UpdateRecommenderState with a RecommenderStatus with a phase that is different than the phase of the recommender is rejected (invalid argument).
* An attempt to call UpdateRecommenderState with a RecommenderStatus with a mode that is different than the mode of the recommender is rejected (invalid argument).
* An attempt to call UpdateRecommenderState with a RecommenderStatus with a type that is different than the type of the recommender is rejected (invalid argument).
* An attempt to call UpdateRecommenderState with a RecommenderStatus with a name that is different than the name of the recommender is rejected (invalid argument).
* An attempt to update a recommender succeeds if the recommender is defined in the policy and the request values are valid.

# GetRecommendation

* An attemp to call GetRecommendation with an invalid policy ID is rejected (invalid argument)
* An attempt to call GetRecommendation with a policy that doesnt exist returns not found error.
* An attempt to call GetRecommendation with a policy that has no recommenders returns an empty recommendation.
* An attempt to call GetRecommendation with a policy where recommenders have never called UpdateRecommenderState returns an empty recommendation.
* GetRecommendation properly aggregates the recommendations from all recommenders voting on activation (if at least one recommneder votes active, it's active)
* GetRecommendation properly aggregates the recommendations from all recommenders voting on scaling (it takes the max of all active recommenders).
* GetRecommendation properly aggregates all the metrics for the policy in returns it in the metric_statuses field.

# IngestMetrics

* An attempt to call IngestMetrics with an invalid cluster name is rejected (invalid argument)
* An attempt to call IngestMetrics with a policy (cluster name, namespace, name) that doesnt exist returns not found error.
* An attempt to call IngestMetricsRequest with a metric which is not defined in the policy is rejected (invalid argument).
* Sending a new gauge pod metric properly updates the control metric reported by GetControlMetrics.
* Sending a new counter pod metric properly updates the control metric reported by GetControlMetrics.
* Sending a new histogram pod metric properly updates the control metric reported by GetControlMetrics.
* Sending a new gauge global metric properly updates the control metric reported by GetControlMetrics.
* Sending a new counter global metric properly updates the control metric reported by GetControlMetrics.
* Sending a new histogram global metric properly updates the control metric reported by GetControlMetrics.



# End to end workloads

The following tests are not testing a specific gRPC endpoint but rather a whole workflow that requires multiple gRPC calls and simulate a common use case.


## Decaying Histogram Across Pods

A ScalingPolicy has a metric to track a decaying histogram for the workload CPU usage. A pod is created and reports 500 millicores. Then the pod is deleted and a new pod comes up.
The new pod reports 100 millicores. The test should assert that the decaying histogram properly keeps the values from the old pod (and the new ones), simulate the time passing
and assert that the values decay properly.