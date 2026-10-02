package controller

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

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
