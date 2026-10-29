package internal

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
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
		pb.ContainerState_builder{Name: proto.String("app"), Requests: map[string]string{"cpu": "100m", "memory": "128Mi"}, Limits: map[string]string{"cpu": "500m", "memory": "512Mi"}}.Build(),
		pb.ContainerState_builder{Name: proto.String("sidecar"), Requests: map[string]string{"cpu": "10m"}}.Build(),
		pb.ContainerState_builder{Name: proto.String("empty")}.Build(),
	}
	if diff := cmp.Diff(want, buildContainerStates(pod), protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
		t.Errorf("buildContainerStates() mismatch (-want +got):\n%s", diff)
	}
}

// TestBuildPodState checks that the pod lifecycle facts the Server needs to
// decide whether a pod's metrics count are reported.
func TestBuildPodState(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	transition := start.Add(time.Minute)

	tests := []struct {
		name     string
		deletion *metav1.Time
		status   corev1.PodStatus
		want     *pb.PodState
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
			want: pb.PodState_builder{
				Phase:                   proto.String("Running"),
				StartTime:               timestamppb.New(start),
				IsReady:                 proto.Bool(true),
				ReadyLastTransitionTime: timestamppb.New(transition),
			}.Build(),
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
			want: pb.PodState_builder{
				Phase:                   proto.String("Running"),
				StartTime:               timestamppb.New(start),
				ReadyLastTransitionTime: timestamppb.New(transition),
			}.Build(),
		},
		{
			name:   "pending, not acknowledged by the kubelet yet",
			status: corev1.PodStatus{Phase: corev1.PodPending},
			want:   pb.PodState_builder{Phase: proto.String("Pending")}.Build(),
		},
		{
			name:     "terminating",
			deletion: &metav1.Time{Time: transition.Add(time.Hour)},
			status: corev1.PodStatus{
				Phase:     corev1.PodRunning,
				StartTime: &metav1.Time{Time: start},
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Time{Time: transition}},
				},
			},
			want: pb.PodState_builder{
				Phase:                   proto.String("Running"),
				StartTime:               timestamppb.New(start),
				IsReady:                 proto.Bool(true),
				ReadyLastTransitionTime: timestamppb.New(transition),
				DeletionTimestamp:       timestamppb.New(transition.Add(time.Hour)),
			}.Build(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := newPod()
			pod.Status = tc.status
			pod.DeletionTimestamp = tc.deletion
			tc.want.SetName(pod.Name)
			tc.want.SetNodeName(pod.Spec.NodeName)
			tc.want.SetContainers([]*pb.ContainerState{})
			if diff := cmp.Diff(tc.want, buildPodState(pod), protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
				t.Errorf("buildPodState() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
