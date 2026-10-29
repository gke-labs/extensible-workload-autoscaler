package internal

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
)

type mockTransport struct {
	body   string
	verify func(*http.Request)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.verify != nil {
		m.verify(req)
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBufferString(m.body)),
		Header:     make(http.Header),
	}, nil
}

func TestProcessPrometheusMetric(t *testing.T) {

	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status:     corev1.PodStatus{PodIP: "1.2.3.4"},
	}

	tests := []struct {
		name     string
		mockBody string
		def      *pb.MetricDefinition
		class    *xasv1.MetricProviderClass
		verify   func(*http.Request)
		want     []*pb.MetricBatch
	}{
		{
			name: "Default Port and Path",
			mockBody: `
# TYPE http_requests_total counter
http_requests_total 10
`,
			def: pb.MetricDefinition_builder{
				Name:     proto.String("req_count"),
				Provider: proto.String("prometheus"),
				Params:   map[string]string{"metric": "http_requests_total"},
			}.Build(),
			verify: func(req *http.Request) {
				if req.URL.String() != "http://1.2.3.4:8080/metrics" {
					t.Errorf("expected http://1.2.3.4:8080/metrics, got %s", req.URL.String())
				}
			},
			want: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("pod-1"),
				Samples: []*pb.MetricSample{
					pb.MetricSample_builder{
						Name:   proto.String("req_count"),
						Labels: map[string]string{},
						Value:  proto.Float64(10),
					}.Build(),
				},
			}.Build()},
		},
		{
			name: "Custom Path from Definition",
			mockBody: `
# TYPE custom counter
custom 10
`,
			def: pb.MetricDefinition_builder{
				Name:     proto.String("custom"),
				Provider: proto.String("prometheus"),
				Params:   map[string]string{"metric": "custom", "path": "/custom_path"},
			}.Build(),
			verify: func(req *http.Request) {
				if req.URL.Path != "/custom_path" {
					t.Errorf("expected /custom_path, got %s", req.URL.Path)
				}
			},
			want: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("pod-1"),
				Samples: []*pb.MetricSample{
					pb.MetricSample_builder{
						Name:   proto.String("custom"),
						Labels: map[string]string{},
						Value:  proto.Float64(10),
					}.Build(),
				},
			}.Build()},
		},
		{
			name: "Custom Path from Class",
			mockBody: `
# TYPE class_path counter
class_path 10
`,
			def: pb.MetricDefinition_builder{
				Name:     proto.String("class_path"),
				Provider: proto.String("prometheus"),
				Params:   map[string]string{"metric": "class_path"},
			}.Build(),
			class: &xasv1.MetricProviderClass{
				Spec: xasv1.MetricProviderClassSpec{
					Config: map[string]string{"path": "/class_path"},
				},
			},
			verify: func(req *http.Request) {
				if req.URL.Path != "/class_path" {
					t.Errorf("expected /class_path, got %s", req.URL.Path)
				}
			},
			want: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("pod-1"),
				Samples: []*pb.MetricSample{
					pb.MetricSample_builder{
						Name:   proto.String("class_path"),
						Labels: map[string]string{},
						Value:  proto.Float64(10),
					}.Build(),
				},
			}.Build()},
		},
		{
			name: "Definition overrides Class",
			mockBody: `
# TYPE override counter
override 10
`,
			def: pb.MetricDefinition_builder{
				Name:     proto.String("override"),
				Provider: proto.String("prometheus"),
				Params:   map[string]string{"metric": "override", "path": "/def_path"},
			}.Build(),
			class: &xasv1.MetricProviderClass{
				Spec: xasv1.MetricProviderClassSpec{
					Config: map[string]string{"path": "/class_path"},
				},
			},
			verify: func(req *http.Request) {
				if req.URL.Path != "/def_path" {
					t.Errorf("expected /def_path, got %s", req.URL.Path)
				}
			},
			want: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("pod-1"),
				Samples: []*pb.MetricSample{
					pb.MetricSample_builder{
						Name:   proto.String("override"),
						Labels: map[string]string{},
						Value:  proto.Float64(10),
					}.Build(),
				},
			}.Build()},
		},
		{
			name: "Counter with Labels",
			mockBody: `# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027 1395066363000
http_requests_total{method="post",code="400"}    3 1395066363000
`,
			def: pb.MetricDefinition_builder{
				Name:     proto.String("req_count"),
				Provider: proto.String("prometheus"),
				Params:   map[string]string{"metric": "http_requests_total"},
			}.Build(),
			want: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("pod-1"),
				Samples: []*pb.MetricSample{
					pb.MetricSample_builder{
						Name:   proto.String("req_count"),
						Labels: map[string]string{"method": "post", "code": "200"},
						Value:  proto.Float64(1027),
					}.Build(),
					pb.MetricSample_builder{
						Name:   proto.String("req_count"),
						Labels: map[string]string{"method": "post", "code": "400"},
						Value:  proto.Float64(3),
					}.Build(),
				},
			}.Build()},
		},
		{
			name: "Gauge",
			mockBody: `# HELP queue_size Current size.
# TYPE queue_size gauge
queue_size 42
`,
			def: pb.MetricDefinition_builder{
				Name:     proto.String("q_size"),
				Provider: proto.String("prometheus"),
				Params:   map[string]string{"metric": "queue_size"},
			}.Build(),
			want: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("pod-1"),
				Samples: []*pb.MetricSample{
					pb.MetricSample_builder{
						Name:   proto.String("q_size"),
						Labels: map[string]string{},
						Value:  proto.Float64(42),
					}.Build(),
				},
			}.Build()},
		},
		{
			name: "Histogram (Buckets, Sum, Count)",
			mockBody: `# HELP lat Latency.
# TYPE lat histogram
lat_bucket{le="0.1"} 10
lat_bucket{le="+Inf"} 15
lat_sum 2.5
lat_count 15
`,
			def: pb.MetricDefinition_builder{
				Name:     proto.String("latency"),
				Provider: proto.String("prometheus"),
				Params:   map[string]string{"metric": "lat"},
			}.Build(),
			want: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("pod-1"),
				Samples: []*pb.MetricSample{
					pb.MetricSample_builder{
						Name:   proto.String("latency"),
						Labels: map[string]string{},
						Histogram: pb.Histogram_builder{
							Buckets: map[string]uint64{
								"0.1":  10,
								"+Inf": 15,
							},
							Sum:   proto.Float64(2.5),
							Count: proto.Uint64(15),
						}.Build(),
					}.Build(),
				},
			}.Build()},
		},
		{
			name: "Ignored Metrics (Not requested)",
			mockBody: `# TYPE other gauge
other 123
# TYPE requested gauge
requested 456
`,
			def: pb.MetricDefinition_builder{
				Name:     proto.String("my_metric"),
				Provider: proto.String("prometheus"),
				Params:   map[string]string{"metric": "requested"},
			}.Build(),
			want: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName: proto.String("pod-1"),
				Samples: []*pb.MetricSample{
					pb.MetricSample_builder{
						Name:   proto.String("my_metric"),
						Labels: map[string]string{},
						Value:  proto.Float64(456),
					}.Build(),
				},
			}.Build()},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := &CoreNodeMetricsProvider{
				httpClient: &http.Client{
					Transport: &mockTransport{body: tc.mockBody, verify: tc.verify},
				},
			}

			got := provider.processPrometheusMetric(pod, tc.def, tc.class)

			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), protocmp.IgnoreDefaultScalars()); diff != "" {
				t.Errorf("processPrometheusMetric mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
