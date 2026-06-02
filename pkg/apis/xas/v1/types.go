package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:subresource:status

// ScalingPolicy is the Schema for the scalingpolicies API
type ScalingPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ScalingPolicySpec   `json:"spec,omitempty"`
	Status ScalingPolicyStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ScalingPolicyList contains a list of ScalingPolicy
type ScalingPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ScalingPolicy `json:"items"`
}

// ScalingPolicySpec defines the desired state of ScalingPolicy
type ScalingPolicySpec struct {
	// ScaleTargetRef points to the target resource to scale
	ScaleTargetRef CrossVersionObjectReference `json:"scaleTargetRef"`

	// MinReplicas is the lower limit for the number of replicas
	MinReplicas *int32 `json:"minReplicas,omitempty"`

	// MaxReplicas is the upper limit for the number of replicas
	MaxReplicas int32 `json:"maxReplicas"`

	// Metrics defines how to process the collected raw data into useful control metrics
	Metrics []MetricDefinition `json:"metrics"`

	// Activation defines the list of recommenders that determine if the workload should be Active (>=MinReplicas) or Idle (0).
	// Logic: Active = Recommender1.Active || Recommender2.Active ...
	Activation []RecommenderDefinition `json:"activation,omitempty"`

	// Scaling defines the list of recommenders that determine the desired replica count.
	// Logic: Desired = Max(Recommender1.Replicas, Recommender2.Replicas ...)
	Scaling []RecommenderDefinition `json:"scaling,omitempty"`

	// ActuationPolicy defines how the controller should apply resource updates.
	ActuationPolicy *ActuationPolicy `json:"actuationPolicy,omitempty"`
}

type ActuationPolicy struct {
	// InPlaceFallback defines the action to take if an in-place pod resize fails or is infeasible.
	// Supported: "Ignore", "EvictOnFailure". Default: "Ignore".
	InPlaceFallback string `json:"inPlaceFallback,omitempty"`
}

type RecommenderDefinition struct {
	// Recommender points to a RecommenderClass by name.
	Recommender string `json:"recommender"`

	// Name is the unique identifier for this recommender instance within the policy.
	Name string `json:"name"`

	// Mode defines if this recommender is "Active" (enforced) or "DryRun" (observed only).
	// Default: "Active".
	Mode string `json:"mode,omitempty"`

	// Params overrides the configuration from the RecommenderClass.
	Params map[string]string `json:"params,omitempty"`
}

// CrossVersionObjectReference contains enough information to let you identify the referred resource.
// The target resource MUST implement the /scale subresource (e.g. Deployment, StatefulSet, ReplicaSet).
type CrossVersionObjectReference struct {
	// Kind of the referent; e.g. Deployment, StatefulSet
	Kind string `json:"kind"`

	// Name of the referent; e.g. my-app
	Name string `json:"name"`

	// APIVersion of the referent; e.g. apps/v1
	APIVersion string `json:"apiVersion,omitempty"`
}

// MetricDefinition defines how to transform raw scraped data into a single control metric value
type MetricDefinition struct {
	// Name is the unique identifier for this control metric (e.g. "total_rps")
	Name string `json:"name"`

	// Provider points to a MetricProviderClass by name.
	Provider string `json:"provider"`

	// Params overrides the configuration from the MetricProviderClass.
	Params map[string]string `json:"params,omitempty"`

	// Filter allows selecting specific samples based on labels.
	Filter map[string]string `json:"filter,omitempty"`

	// Gauge consumes raw scalar values and provides immediate spatial aggregation.
	Gauge *Gauge `json:"gauge,omitempty"`

	// Rate consumes raw counters and calculates Delta Value / Delta Time.
	Rate *Rate `json:"rate,omitempty"`

	// Distribution consumes native buckets and extracts a specific rank.
	Distribution *Distribution `json:"distribution,omitempty"`

	// DecayingDistribution builds a temporal distribution of scalar samples.
	DecayingDistribution *DecayingDistribution `json:"decayingDistribution,omitempty"`

	// Scope defines the granularity of the metric. "Global" (default) or "Pod".
	Scope string `json:"scope,omitempty"`
}

type Gauge struct {
	// Aggregation defines how to combine values from multiple pods.
	// Supported: "Avg", "Sum", "Max", "Min". Default: "Avg".
	Aggregation string `json:"aggregation,omitempty"`
}

type Rate struct {
	// Window defines the temporal period over which the rate is calculated.
	// Example: "1m", "5m".
	Window metav1.Duration `json:"window"`

	// Aggregation defines how to combine rates from multiple pods.
	// Supported: "Avg", "Sum", "Max", "Min". Default: "Sum".
	Aggregation string `json:"aggregation,omitempty"`
}

type Distribution struct {
	// Percentile to calculate (e.g. "p99", "p95").
	Percentile string `json:"percentile"`

	// Aggregation defines how to combine percentiles from multiple pods.
	// Supported: "Avg", "Sum", "Max", "Min". Default: "Max".
	Aggregation string `json:"aggregation,omitempty"`
}

type DecayingDistribution struct {
	// HalfLife is the duration after which a sample's weight is halved.
	// Example: "24h".
	HalfLife metav1.Duration `json:"halfLife"`

	// BucketSize defines the resolution of the histogram.
	// Example: "0.1" (for CPU cores) or "100Mi" (for Memory).
	BucketSize string `json:"bucketSize"`

	// Percentile to calculate (e.g. "p100", "p99").
	Percentile string `json:"percentile"`

	// Rate calculates a rate over the specified window before sampling.
	// If set, the input metric is treated as a Counter.
	Rate *metav1.Duration `json:"rate,omitempty"`
}

// ScalingPolicyStatus defines the observed state of ScalingPolicy
type ScalingPolicyStatus struct {
	// CurrentReplicas is the current number of replicas of the target workload.
	CurrentReplicas int32 `json:"currentReplicas"`

	// DesiredReplicas is the number of replicas calculated by the XAS control plane.
	DesiredReplicas int32 `json:"desiredReplicas"`

	// Selector is the label selector resolved from the target workload.
	// Used by Agents to identify pods.
	Selector string `json:"selector,omitempty"`

	// LastUpdated is the timestamp of the last recommendation calculation (ISO8601).
	LastUpdated string `json:"lastUpdated"` // ISO8601

	// Decisions lists the status of each recommender.
	Decisions []DecisionStatus `json:"decisions,omitempty"`

	// MetricStatuses lists the current value and status of each metric.
	MetricStatuses []MetricStatus `json:"metricStatuses,omitempty"`
}

type MetricStatus struct {
	Name        string `json:"name"`
	LastUpdated string `json:"lastUpdated"` // ISO8601
	Value       string `json:"value"`       // Float as string
	Error       string `json:"error,omitempty"`
}

type DecisionStatus struct {
	RecommenderName   string                      `json:"recommenderName"`
	Type              string                      `json:"type"`
	Phase             string                      `json:"phase"` // "Activation" or "Scaling"
	Mode              string                      `json:"mode,omitempty"`
	Active            bool                        `json:"active"`
	Replicas          int32                       `json:"replicas"`
	Reason            string                      `json:"reason,omitempty"`
	Error             string                      `json:"error,omitempty"`
	WorkloadResources *ResourceRecommendation     `json:"workloadResources,omitempty"`
	PodResources      []PodResourceRecommendation `json:"podResources,omitempty"`
}

type ResourceRecommendation struct {
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
}

type PodResourceRecommendation struct {
	PodName  string            `json:"podName"`
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:scope=Cluster

// MetricProviderClass defines a reusable configuration for a metric source.
type MetricProviderClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec MetricProviderClassSpec `json:"spec,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricProviderClassList contains a list of MetricProviderClass
type MetricProviderClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MetricProviderClass `json:"items"`
}

type MetricProviderClassSpec struct {
	// Type defines the provider implementation (e.g. "Prometheus", "Kubelet").
	Type string `json:"type"`

	// Config is a map of static configuration passed to the provider.
	Config map[string]string `json:"config,omitempty"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:scope=Cluster

// RecommenderClass defines a reusable configuration for a scaling algorithm.
type RecommenderClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec RecommenderClassSpec `json:"spec,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RecommenderClassList contains a list of RecommenderClass
type RecommenderClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RecommenderClass `json:"items"`
}

type RecommenderClassSpec struct {
	// Type defines the algorithm (e.g. "Linear", "Cron", "Threshold").
	Type string `json:"type"`

	// Config is a map of default configuration passed to the recommender.
	Config map[string]string `json:"config,omitempty"`
}
