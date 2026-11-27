package internal

import (
	"fmt"
	"net/http"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	corev1 "k8s.io/api/core/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
)

// An Exporter turns the metric families scraped from an endpoint into the
// metric batches of a metric, for a pod.
type Exporter interface {
	Export(families map[string]*dto.MetricFamily, pod corev1.Pod, m *pb.MetricDefinition, class *xasv1.MetricProviderClass) []*pb.MetricBatch
}

// unexpectedStatusError is returned by scrapeEndpoint for a response other
// than 200 OK.
type unexpectedStatusError struct {
	statusCode int
}

func (e *unexpectedStatusError) Error() string {
	return fmt.Sprintf("unexpected status code %d", e.statusCode)
}

// scrapeEndpoint fetches the metrics exposed at url and parses them, in the
// Prometheus text format. The token, if any, is sent as a Bearer token. If
// requireOK is set, a response other than 200 OK is an
// *unexpectedStatusError instead of being parsed.
func scrapeEndpoint(client *http.Client, url, token string, requireOK bool) (map[string]*dto.MetricFamily, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if requireOK && resp.StatusCode != http.StatusOK {
		return nil, &unexpectedStatusError{statusCode: resp.StatusCode}
	}

	parser := expfmt.NewTextParser(model.LegacyValidation)
	return parser.TextToMetricFamilies(resp.Body)
}
