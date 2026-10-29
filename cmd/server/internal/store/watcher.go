package store

import (
	"maps"
	"slices"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
)

// ControlMetricEvent reports the control metrics of a policy after a
// recalculation, along with the number of ready replicas they were calculated
// against.
type ControlMetricEvent struct {
	// ID identifies the policy the event is about.
	ID *pb.PolicyId
	// ReadyReplicas is the number of ready replicas the control metrics were
	// calculated against.
	ReadyReplicas int32
	// ControlMetrics is the policy-wide control metrics.
	ControlMetrics *pb.ControlMetrics
	// RecommenderControlMetrics is the control metrics owned by each
	// recommender, keyed by recommender name.
	RecommenderControlMetrics map[string]*pb.ControlMetrics
}

// equal reports whether both events carry the exact same data.
func (e ControlMetricEvent) equal(o ControlMetricEvent) bool {
	return proto.Equal(e.ID, o.ID) &&
		e.ReadyReplicas == o.ReadyReplicas &&
		proto.Equal(e.ControlMetrics, o.ControlMetrics) &&
		maps.EqualFunc(e.RecommenderControlMetrics, o.RecommenderControlMetrics, func(a, b *pb.ControlMetrics) bool { return proto.Equal(a, b) })
}

// RecommendationEvent reports the arbitrated recommendation of a policy after
// an arbitration, along with its explanation and the status of the metrics it
// was calculated from: what GetArbitratedRecommendation returns.
type RecommendationEvent struct {
	// ID identifies the policy the event is about.
	ID *pb.PolicyId
	// Recommendation is the arbitrated recommendation, nil if none is
	// available yet.
	Recommendation *pb.Recommendation
	// Explanation is the status of the recommenders the recommendation was
	// arbitrated from.
	Explanation []*pb.RecommenderStatus
	// MetricStatuses is the status of the metrics the recommendation was
	// calculated from.
	MetricStatuses []*pb.MetricStatus
}

// equal reports whether both events carry the same data, ignoring when the
// recommender statuses were last updated and when the metric statuses were
// calculated: those timestamps move with every recommender update and every
// recalculation, and would otherwise make every event look new.
func (e RecommendationEvent) equal(o RecommendationEvent) bool {
	return proto.Equal(e.ID, o.ID) &&
		proto.Equal(e.Recommendation, o.Recommendation) &&
		slices.EqualFunc(e.Explanation, o.Explanation, recommenderStatusEqual) &&
		slices.EqualFunc(e.MetricStatuses, o.MetricStatuses, metricStatusEqual)
}

// recommenderStatusEqual reports whether two recommender statuses are equal,
// ignoring when they were last updated.
func recommenderStatusEqual(a, b *pb.RecommenderStatus) bool {
	if a == nil || b == nil {
		return a == b
	}
	a, b = proto.Clone(a).(*pb.RecommenderStatus), proto.Clone(b).(*pb.RecommenderStatus)
	a.ClearLastUpdated()
	b.ClearLastUpdated()
	return proto.Equal(a, b)
}

// metricStatusEqual reports whether two metric statuses are equal, ignoring
// when they were calculated.
func metricStatusEqual(a, b *pb.MetricStatus) bool {
	if a == nil || b == nil {
		return a == b
	}
	a, b = proto.Clone(a).(*pb.MetricStatus), proto.Clone(b).(*pb.MetricStatus)
	a.ClearTimestamp()
	b.ClearTimestamp()
	return proto.Equal(a, b)
}

// watcher is a registered Watch caller.
type watcher[E any] struct {
	ch chan E
}

const (
	// notifyInterval is the minimum sustained interval between two events of
	// a policy emitted to watchers.
	notifyInterval = time.Second
	// notifyBurst is the number of events that may be emitted back-to-back
	// before the rate limit kicks in.
	notifyBurst = 10
	// watcherChanCapacity is the number of events a watcher may lag behind
	// (beyond its initial snapshot) before it is forcibly unwatched.
	watcherChanCapacity = 1000
)

// Watchers manages the watchers of the events of type E emitted for the
// policies of a cluster.
type Watchers[E any] struct {
	mu          sync.RWMutex
	clusterName string
	// equal reports whether two events carry the same data (see Notify).
	equal func(E, E) bool
	// limiters rate-limits the notifications of each policy.
	limiters map[policyID]*rate.Limiter
	lastEvt  map[policyID]E
	watchers map[*watcher[E]]struct{}
}

// NewWatchers creates a new Watchers. equal reports whether two events carry
// the same data: Notify drops an event equal to the last one of its policy.
func NewWatchers[E any](clusterName string, equal func(E, E) bool) *Watchers[E] {
	return &Watchers[E]{
		clusterName: clusterName,
		equal:       equal,
		watchers:    make(map[*watcher[E]]struct{}),
		lastEvt:     make(map[policyID]E),
		limiters:    make(map[policyID]*rate.Limiter),
	}
}

// Watch registers a new watcher and returns the channel it receives
// events on, together with a function to unwatch. The channel is primed
// with the last event of every known policy, so the watcher starts from
// the current state. The channel is closed when the watcher unwatches,
// or by Notify if the watcher does not drain it fast enough (see
// watcherChanCapacity): a closed channel means events were lost and the
// watcher must re-watch to recover.
func (w *Watchers[E]) Watch() (<-chan E, func()) {
	w.mu.Lock()
	defer w.mu.Unlock()

	watcher := &watcher[E]{ch: make(chan E, watcherChanCapacity)}
	for _, evt := range w.lastEvt {
		watcher.ch <- evt
	}
	w.watchers[watcher] = struct{}{}

	unwatch := func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		// Notify may already have dropped (and closed) the watcher:
		// only close the channel if it is still registered.
		if _, ok := w.watchers[watcher]; ok {
			delete(w.watchers, watcher)
			close(watcher.ch)
		}
	}
	return watcher.ch, unwatch
}

// Throttled reports whether notifications for the policy are rate-limited.
// Each policy has its own limiter, so a chatty policy cannot starve the
// others.
func (w *Watchers[E]) Throttled(id policyID) bool {
	w.mu.RLock()
	limiter, ok := w.limiters[id]
	w.mu.RUnlock()
	if !ok {
		limiter = rate.NewLimiter(rate.Every(notifyInterval), notifyBurst)
		w.mu.Lock()
		w.limiters[id] = limiter
		w.mu.Unlock()
	}
	return !limiter.Allow()
}

// Notify notifies watchers of a new event of a policy, unless it is equal to
// the last event of that policy. Callers should first call Throttled to
// determine whether to call Notify.
func (w *Watchers[E]) Notify(id policyID, evt E) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if last, ok := w.lastEvt[id]; ok && w.equal(last, evt) {
		return // Ignore duplicated events
	}
	w.lastEvt[id] = evt

	for watcher := range w.watchers {
		select {
		case watcher.ch <- evt:
		default:
			// The watcher is not keeping up: unwatch.
			close(watcher.ch)
			delete(w.watchers, watcher)
		}
	}
}

// Forget drops the state kept for a policy: its last event, so that it is no
// longer replayed to new watchers, and its rate limiter. Call it when the
// policy is deleted.
func (w *Watchers[E]) Forget(id policyID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.lastEvt, id)
	delete(w.limiters, id)
}
