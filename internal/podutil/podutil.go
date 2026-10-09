// Package podutil decides whether the metrics of a pod should be considered
// for autoscaling, based on the pod's lifecycle.
package podutil

import (
	"time"

	corev1 "k8s.io/api/core/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

// InitialReadinessDelay is the period after a pod starts during which changes
// of its Ready condition are treated as part of its initialization. It matches
// the default of the HPA's --horizontal-pod-autoscaler-initial-readiness-delay.
const InitialReadinessDelay = 30 * time.Second

// Lifecycle holds the facts about a pod's lifecycle needed to decide whether
// its metrics are eligible.
type Lifecycle struct {
	// Phase is the pod phase.
	Phase corev1.PodPhase
	// StartTime is when the pod was acknowledged by the kubelet. Zero if unset.
	StartTime time.Time
	// Ready reports whether the pod's Ready condition is currently true.
	Ready bool
	// ReadyLastTransitionTime is when the pod's Ready condition last changed.
	// Zero if the pod has no Ready condition.
	ReadyLastTransitionTime time.Time
}

// IsEligible reports whether the metrics of a pod in the given lifecycle state
// should be considered.
//
// A ready pod is eligible. A pod that isn't ready is eligible only if it is
// running and has been ready before, i.e. it became unready later in its life
// (e.g. its readiness probe fails under load). Pods that are pending, have
// terminated, or have never been ready are not eligible.
//
// The Ready condition only records its last transition, so whether the pod has
// been ready before is inferred as in the HPA: a pod whose Ready condition last
// changed within InitialReadinessDelay of its start has never been ready.
func IsEligible(l Lifecycle) bool {
	if l.Ready {
		return true
	}
	if l.Phase != corev1.PodRunning || l.StartTime.IsZero() || l.ReadyLastTransitionTime.IsZero() {
		return false
	}
	return !l.ReadyLastTransitionTime.Before(l.StartTime.Add(InitialReadinessDelay))
}

// ReadyCondition returns the pod's Ready condition, or nil if it has none.
func ReadyCondition(pod *corev1.Pod) *corev1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

// FromPod returns the lifecycle state of a Kubernetes pod.
func FromPod(pod *corev1.Pod) Lifecycle {
	l := Lifecycle{Phase: pod.Status.Phase}
	if pod.Status.StartTime != nil {
		l.StartTime = pod.Status.StartTime.Time
	}
	if cond := ReadyCondition(pod); cond != nil {
		l.Ready = cond.Status == corev1.ConditionTrue
		l.ReadyLastTransitionTime = cond.LastTransitionTime.Time
	}
	return l
}

// FromPodState returns the lifecycle state of a pod as reported to the Server.
func FromPodState(p *pb.PodState) Lifecycle {
	l := Lifecycle{
		Phase: corev1.PodPhase(p.GetPhase()),
		Ready: p.GetIsReady(),
	}
	if p.GetStartTime() != nil {
		l.StartTime = p.GetStartTime().AsTime()
	}
	if p.GetReadyLastTransitionTime() != nil {
		l.ReadyLastTransitionTime = p.GetReadyLastTransitionTime().AsTime()
	}
	return l
}
