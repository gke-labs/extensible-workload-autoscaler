package store

import (
	"maps"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

// ControlMetricEvent reports the control metrics of a policy after a
// recalculation, along with the number of ready replicas they were calculated
// against.
type ControlMetricEvent struct {
	id            policyID
	readyReplicas int32
	cm            *pb.ControlMetrics
	rcm           map[string]*pb.ControlMetrics
}

// PolicyID returns the ID of the policy the event is about.
func (e ControlMetricEvent) PolicyID() *pb.PolicyId {
	return pb.PolicyId_builder{
		ClusterName: proto.String(e.id.cluster),
		Namespace:   proto.String(e.id.ns),
		Name:        proto.String(e.id.name),
	}.Build()
}

// ReadyReplicas returns the number of ready replicas the control metrics were
// calculated against.
func (e ControlMetricEvent) ReadyReplicas() int32 { return e.readyReplicas }

// ControlMetrics returns the policy-wide control metrics.
func (e ControlMetricEvent) ControlMetrics() *pb.ControlMetrics { return e.cm }

// RecommenderControlMetrics returns the control metrics owned by each
// recommender, keyed by recommender name.
func (e ControlMetricEvent) RecommenderControlMetrics() map[string]*pb.ControlMetrics { return e.rcm }

// equal reports whether both events carry the exact same data.
func (e ControlMetricEvent) equal(o ControlMetricEvent) bool {
	return e.id == o.id &&
		e.readyReplicas == o.readyReplicas &&
		proto.Equal(e.cm, o.cm) &&
		maps.EqualFunc(e.rcm, o.rcm, func(a, b *pb.ControlMetrics) bool { return proto.Equal(a, b) })
}

// watcher is a registered Watch caller.
type watcher struct {
	ch chan ControlMetricEvent
}

const (
	// notifyInterval is the minimum sustained interval between two control
	// metric events emitted to watchers.
	notifyInterval = time.Second
	// notifyBurst is the number of events that may be emitted back-to-back
	// before the rate limit kicks in.
	notifyBurst = 10
	// watcherChanCapacity is the number of events a watcher may lag behind
	// (beyond its initial snapshot) before it is forcibly unwatched.
	watcherChanCapacity = 10.000
)

// Watchers manages ControlMetricEvents watchers.
type Watchers struct {
	mu          sync.RWMutex
	clusterName string
	// limiters rate-limits the notifications of each policy.
	limiters map[policyID]*rate.Limiter
	lastEvt  map[policyID]ControlMetricEvent
	watchers map[*watcher]struct{}
}

// NewWatchers creates a new Watchers.
func NewWatchers(clusterName string) *Watchers {
	return &Watchers{
		clusterName: clusterName,
		watchers:    make(map[*watcher]struct{}),
		lastEvt:     make(map[policyID]ControlMetricEvent),
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
func (w *Watchers) Watch() (<-chan ControlMetricEvent, func()) {
	w.mu.Lock()
	defer w.mu.Unlock()

	watcher := &watcher{ch: make(chan ControlMetricEvent, watcherChanCapacity)}
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
func (w *Watchers) Throttled(id policyID) bool {
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

// Notify notifies watchers of a new control metric event.
// Callers should first call Throttled to determine whether to call Notify.
func (w *Watchers) Notify(id policyID, readyReplicas int32, cm *pb.ControlMetrics, rcm map[string]*pb.ControlMetrics) {
	evt := ControlMetricEvent{
		id:            id,
		readyReplicas: readyReplicas,
		cm:            cm,
		rcm:           rcm,
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if last, ok := w.lastEvt[id]; ok && last.equal(evt) {
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
func (w *Watchers) Forget(id policyID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.lastEvt, id)
	delete(w.limiters, id)
}
