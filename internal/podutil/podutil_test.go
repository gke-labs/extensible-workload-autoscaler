package podutil

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
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
		{
			name: "terminating while still ready",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, Ready: true, ReadyLastTransitionTime: start.Add(2 * time.Minute), DeletionTimestamp: start.Add(time.Hour)},
			want: false,
		},
		{
			name: "terminating, unready later in its life",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, ReadyLastTransitionTime: start.Add(20 * time.Minute), DeletionTimestamp: start.Add(time.Hour)},
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

func TestIsCPUSampleEligible(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ready := start.Add(time.Minute)
	window := 30 * time.Second
	// Lifecycle of a pod that became ready one minute after it started.
	readyPod := Lifecycle{Phase: corev1.PodRunning, StartTime: start, Ready: true, ReadyLastTransitionTime: ready}
	unreadyPod := Lifecycle{Phase: corev1.PodRunning, StartTime: start, Ready: false, ReadyLastTransitionTime: ready}
	tests := []struct {
		name       string
		l          Lifecycle
		sampleTime time.Time
		now        time.Time
		want       bool
	}{
		{
			name:       "initializing: sample measured entirely after the pod became ready",
			l:          readyPod,
			sampleTime: ready.Add(window),
			now:        start.Add(2 * time.Minute),
			want:       true,
		},
		{
			name:       "initializing: sample measured partly before the pod became ready",
			l:          readyPod,
			sampleTime: ready.Add(window - time.Second),
			now:        start.Add(2 * time.Minute),
			want:       false,
		},
		{
			name:       "initializing: pod not ready",
			l:          unreadyPod,
			sampleTime: ready.Add(10 * time.Minute),
			now:        start.Add(2 * time.Minute),
			want:       false,
		},
		{
			name:       "past the initialization period: sample counts even if measured before readiness",
			l:          readyPod,
			sampleTime: ready.Add(window - time.Second),
			now:        start.Add(CPUInitializationPeriod),
			want:       true,
		},
		{
			name:       "past the initialization period: unready pod counts",
			l:          unreadyPod,
			sampleTime: ready.Add(10 * time.Minute),
			now:        start.Add(10 * time.Minute),
			want:       true,
		},
		{
			name:       "no start time: not acknowledged by the kubelet yet",
			l:          Lifecycle{Ready: true},
			sampleTime: ready,
			now:        ready,
			want:       false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsCPUSampleEligible(tc.l, tc.sampleTime, window, tc.now); got != tc.want {
				t.Errorf("IsCPUSampleEligible() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsPodEligible(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	workload := map[string]*pb.PodState{
		"ready":       pb.PodState_builder{Phase: proto.String("Running"), StartTime: timestamppb.New(start), IsReady: proto.Bool(true), ReadyLastTransitionTime: timestamppb.New(start.Add(time.Minute))}.Build(),
		"never-ready": pb.PodState_builder{Phase: proto.String("Running"), StartTime: timestamppb.New(start), ReadyLastTransitionTime: timestamppb.New(start.Add(time.Second))}.Build(),
		"no-start":    pb.PodState_builder{IsReady: proto.Bool(true)}.Build(),
	}
	grace := 10 * time.Minute
	defaults := (*pb.MetricDefinition_Eligibility)(nil)
	withGrace := pb.MetricDefinition_Eligibility_builder{StartupGracePeriod: durationpb.New(grace)}.Build()
	startupSamples := pb.MetricDefinition_Eligibility_builder{StartupSamples: proto.Bool(true)}.Build()
	tests := []struct {
		name        string
		pod         string
		eligibility *pb.MetricDefinition_Eligibility
		now         time.Time
		want        bool
	}{
		{name: "ready", pod: "ready", eligibility: defaults, want: true},
		{name: "never ready", pod: "never-ready", eligibility: defaults, want: false},
		{name: "unknown", pod: "unknown", eligibility: defaults, want: false},
		{name: "ready, within the grace period", pod: "ready", eligibility: withGrace, now: start.Add(grace - time.Second), want: false},
		{name: "ready, at the end of the grace period", pod: "ready", eligibility: withGrace, now: start.Add(grace), want: true},
		{name: "ready without a start time, with a grace period", pod: "no-start", eligibility: withGrace, now: start.Add(time.Hour), want: false},
		{name: "never ready, with a grace period", pod: "never-ready", eligibility: withGrace, now: start.Add(time.Hour), want: false},
		{name: "never ready, startup samples", pod: "never-ready", eligibility: startupSamples, want: true},
		{name: "unknown, startup samples", pod: "unknown", eligibility: startupSamples, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.now.IsZero() {
				tc.now = start.Add(time.Hour)
			}
			if got := IsPodEligible(workload, tc.pod, tc.eligibility, tc.now); got != tc.want {
				t.Errorf("IsPodEligible(%q, %v, %v) = %v, want %v", tc.pod, tc.eligibility, tc.now, got, tc.want)
			}
		})
	}
}
func TestReadyCondition(t *testing.T) {
	transition := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Time{Time: transition}},
			},
		},
	}
	cond := ReadyCondition(pod)
	if cond == nil || cond.Type != corev1.PodReady || !cond.LastTransitionTime.Time.Equal(transition) {
		t.Errorf("ReadyCondition() = %+v, want the Ready condition", cond)
	}

	if cond := ReadyCondition(&corev1.Pod{}); cond != nil {
		t.Errorf("ReadyCondition(no conditions) = %+v, want nil", cond)
	}
}

func TestFromPodState(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	transition := start.Add(time.Minute)
	p := pb.PodState_builder{
		Phase:                   proto.String("Running"),
		IsReady:                 proto.Bool(true),
		StartTime:               timestamppb.New(start),
		ReadyLastTransitionTime: timestamppb.New(transition),
		DeletionTimestamp:       timestamppb.New(transition.Add(time.Hour)),
	}.Build()
	want := Lifecycle{Phase: corev1.PodRunning, StartTime: start, Ready: true, ReadyLastTransitionTime: transition, DeletionTimestamp: transition.Add(time.Hour)}
	if got := FromPodState(p); got != want {
		t.Errorf("FromPodState() = %+v, want %+v", got, want)
	}

	if got := FromPodState(&pb.PodState{}); got != (Lifecycle{}) {
		t.Errorf("FromPodState(empty) = %+v, want zero", got)
	}
}
