package podutil

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

func TestIsEligible(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		l    Lifecycle
		want bool
	}{
		{
			name: "ready",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, Ready: true, ReadyLastTransitionTime: start.Add(2 * time.Minute)},
			want: true,
		},
		{
			name: "ready without lifecycle facts",
			l:    Lifecycle{Ready: true},
			want: true,
		},
		{
			name: "starting up, Ready condition false since start",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, ReadyLastTransitionTime: start.Add(time.Second)},
			want: false,
		},
		{
			name: "unready later in its life",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, ReadyLastTransitionTime: start.Add(20 * time.Minute)},
			want: true,
		},
		{
			name: "Ready condition last changed exactly at the end of the initial readiness delay",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, ReadyLastTransitionTime: start.Add(InitialReadinessDelay)},
			want: true,
		},
		{
			name: "Ready condition last changed just before the end of the initial readiness delay",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, ReadyLastTransitionTime: start.Add(InitialReadinessDelay - time.Second)},
			want: false,
		},
		{
			name: "pending",
			l:    Lifecycle{Phase: corev1.PodPending},
			want: false,
		},
		{
			name: "running without start time",
			l:    Lifecycle{Phase: corev1.PodRunning, ReadyLastTransitionTime: start.Add(20 * time.Minute)},
			want: false,
		},
		{
			name: "running without Ready condition",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start},
			want: false,
		},
		{
			name: "succeeded",
			l:    Lifecycle{Phase: corev1.PodSucceeded, StartTime: start, ReadyLastTransitionTime: start.Add(20 * time.Minute)},
			want: false,
		},
		{
			name: "failed",
			l:    Lifecycle{Phase: corev1.PodFailed, StartTime: start, ReadyLastTransitionTime: start.Add(20 * time.Minute)},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsEligible(tc.l); got != tc.want {
				t.Errorf("IsEligible(%+v) = %v, want %v", tc.l, got, tc.want)
			}
		})
	}
}

func TestFromPod(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	transition := start.Add(time.Minute)
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			StartTime: &metav1.Time{Time: start},
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Time{Time: transition}},
			},
		},
	}
	want := Lifecycle{Phase: corev1.PodRunning, StartTime: start, Ready: true, ReadyLastTransitionTime: transition}
	if got := FromPod(pod); got != want {
		t.Errorf("FromPod() = %+v, want %+v", got, want)
	}

	if got := FromPod(&corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending}}); got != (Lifecycle{Phase: corev1.PodPending}) {
		t.Errorf("FromPod(pending) = %+v, want only the phase set", got)
	}
}

func TestFromPodState(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	transition := start.Add(time.Minute)
	p := &pb.PodState{
		Phase:                   "Running",
		IsReady:                 true,
		StartTime:               timestamppb.New(start),
		ReadyLastTransitionTime: timestamppb.New(transition),
	}
	want := Lifecycle{Phase: corev1.PodRunning, StartTime: start, Ready: true, ReadyLastTransitionTime: transition}
	if got := FromPodState(p); got != want {
		t.Errorf("FromPodState() = %+v, want %+v", got, want)
	}

	if got := FromPodState(&pb.PodState{}); got != (Lifecycle{}) {
		t.Errorf("FromPodState(empty) = %+v, want zero", got)
	}
}
