package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ControlMetricValue = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "xas_control_metric_value",
			Help: "The aggregated value of a control metric defined in a ScalingPolicy",
		},
		[]string{
			"policy_cluster", "policy_namespace", "policy_name",
			"workload_group", "workload_version", "workload_kind", "workload_name",
			"control_metric_name",
		},
	)

	ReplicasRecommended = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "xas_replicas_recommended",
			Help: "The number of replicas recommended by the XAS control plane",
		},
		[]string{
			"policy_cluster", "policy_namespace", "policy_name",
			"workload_group", "workload_version", "workload_kind", "workload_name",
		},
	)

	WorkloadActive = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "xas_workload_active",
			Help: "Whether the workload is considered Active (1) or Idle (0) based on Activation triggers",
		},
		[]string{
			"policy_cluster", "policy_namespace", "policy_name",
			"workload_group", "workload_version", "workload_kind", "workload_name",
		},
	)
)

func RecordControlMetric(cluster, ns, pol, group, version, kind, wlName, controlMetric string, val float64) {
	ControlMetricValue.WithLabelValues(cluster, ns, pol, group, version, kind, wlName, controlMetric).Set(val)
}

func RecordRecommendation(cluster, ns, pol, group, version, kind, wlName string, replicas int32) {
	ReplicasRecommended.WithLabelValues(cluster, ns, pol, group, version, kind, wlName).Set(float64(replicas))
}

func RecordActive(cluster, ns, pol, group, version, kind, wlName string, active bool) {
	val := 0.0
	if active {
		val = 1.0
	}
	WorkloadActive.WithLabelValues(cluster, ns, pol, group, version, kind, wlName).Set(val)
}
