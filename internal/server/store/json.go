package store

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// The store state is exported as JSON by Dump (e.g. on /storez). encoding/json
// does not support protobuf messages: well-known types are not rendered in
// their canonical form (e.g. RFC 3339 for a Timestamp), and messages generated
// with the Opaque API have no exported fields. The types below render the
// protobuf messages of the state with protojson instead.

// protoJSONOptions keeps the proto field names (snake_case), as read by the UI.
var protoJSONOptions = protojson.MarshalOptions{UseProtoNames: true}

// protoJSON renders a protobuf message with protojson. A nil message is
// rendered as null.
type protoJSON struct {
	m proto.Message
}

func (p protoJSON) MarshalJSON() ([]byte, error) {
	if p.m == nil || !p.m.ProtoReflect().IsValid() {
		return []byte("null"), nil
	}
	return protoJSONOptions.Marshal(p.m)
}

func protoJSONMap[M proto.Message](in map[string]M) map[string]protoJSON {
	if in == nil {
		return nil
	}
	out := make(map[string]protoJSON, len(in))
	for k, v := range in {
		out[k] = protoJSON{v}
	}
	return out
}

func protoJSONSlice[M proto.Message](in []M) []protoJSON {
	if in == nil {
		return nil
	}
	out := make([]protoJSON, len(in))
	for i, v := range in {
		out[i] = protoJSON{v}
	}
	return out
}

func (ps PolicyState) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Policy                    protoJSON
		Workload                  map[string]protoJSON
		Metrics                   *MetricStore
		Recommendation            protoJSON
		Explanation               []protoJSON
		LastActive                int64
		RecommenderStatuses       map[string]protoJSON
		ControlMetrics            protoJSON
		RecommenderControlMetrics map[string]protoJSON
	}{
		Policy:                    protoJSON{ps.Policy},
		Workload:                  protoJSONMap(ps.Workload),
		Metrics:                   ps.Metrics,
		Recommendation:            protoJSON{ps.Recommendation},
		Explanation:               protoJSONSlice(ps.Explanation),
		LastActive:                ps.LastActive,
		RecommenderStatuses:       protoJSONMap(ps.RecommenderStatuses),
		ControlMetrics:            protoJSON{ps.ControlMetrics},
		RecommenderControlMetrics: protoJSONMap(ps.RecommenderControlMetrics),
	})
}

func (s Sample) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Timestamp int64
		Value     float64
		Histogram protoJSON
	}{
		Timestamp: s.Timestamp,
		Value:     s.Value,
		Histogram: protoJSON{s.Histogram},
	})
}
