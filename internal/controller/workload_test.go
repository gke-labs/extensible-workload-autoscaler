package controller

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

// TestBuildContainerStates checks that the requests and limits of every
// container are reported, so recommenders can keep the limit/request ratio.
func TestBuildContainerStates(t *testing.T) {
	pod := newPod(
		withContainer("app", rl("cpu", "100m", "memory", "128Mi"), rl("cpu", "500m", "memory", "512Mi")),
		withContainer("sidecar", rl("cpu", "10m"), nil),
		withContainer("empty", nil, nil),
	)
	want := []*pb.ContainerState{
		{Name: "app", Requests: map[string]string{"cpu": "100m", "memory": "128Mi"}, Limits: map[string]string{"cpu": "500m", "memory": "512Mi"}},
		{Name: "sidecar", Requests: map[string]string{"cpu": "10m"}},
		{Name: "empty"},
	}
	if diff := cmp.Diff(want, buildContainerStates(pod), protocmp.Transform()); diff != "" {
		t.Errorf("buildContainerStates() mismatch (-want +got):\n%s", diff)
	}
}

// TestBuildPodState checks that the pod lifecycle facts the Server needs to
// decide whether a pod's metrics count are reported.
func TestBuildPodState(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	transition := start.Add(time.Minute)

	tests := []struct {
		name   string
		status corev1.PodStatus
		want   *pb.PodState
	}{
		{
			name: "running and ready",
			status: corev1.PodStatus{
				Phase:     corev1.PodRunning,
				StartTime: &metav1.Time{Time: start},
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
					{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Time{Time: transition}},
				},
			},
			want: &pb.PodState{
				Phase:                   "Running",
				StartTime:               timestamppb.New(start),
				IsReady:                 true,
				ReadyLastTransitionTime: timestamppb.New(transition),
			},
		},
		{
			name: "running and not ready",
			status: corev1.PodStatus{
				Phase:     corev1.PodRunning,
				StartTime: &metav1.Time{Time: start},
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.Time{Time: transition}},
				},
			},
			want: &pb.PodState{
				Phase:                   "Running",
				StartTime:               timestamppb.New(start),
				ReadyLastTransitionTime: timestamppb.New(transition),
			},
		},
		{
			name:   "pending, not acknowledged by the kubelet yet",
			status: corev1.PodStatus{Phase: corev1.PodPending},
			want:   &pb.PodState{Phase: "Pending"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := newPod()
			pod.Status = tc.status
			tc.want.Name = pod.Name
			tc.want.NodeName = pod.Spec.NodeName
			tc.want.Containers = []*pb.ContainerState{}
			if diff := cmp.Diff(tc.want, buildPodState(pod), protocmp.Transform()); diff != "" {
				t.Errorf("buildPodState() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
