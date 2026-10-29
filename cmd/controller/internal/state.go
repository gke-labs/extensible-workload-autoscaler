package internal

import (
	"sync"
)

// PolicyState holds the cached state of a policy's target workload.
type PolicyState struct {
	// selector is the label selector of the target workload's pods, as
	// reported by the /scale subresource (status.selector).
	selector string
}

// State holds the controller's cached state for all policies.
type State struct {
	mu           sync.RWMutex
	policyStates map[policyID]*PolicyState
}

// NewState creates a new State object.
func NewState() *State {
	return &State{
		policyStates: make(map[policyID]*PolicyState),
	}
}

// setSelector records the latest observed pod selector for the given policy's
// target workload, creating the PolicyState entry if it does not exist yet.
func (s *State) setSelector(id policyID, selector string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ps, ok := s.policyStates[id]
	if !ok {
		ps = &PolicyState{}
		s.policyStates[id] = ps
	}
	ps.selector = selector
}

// getSelector returns the cached pod selector for the given policy's target
// workload. The second return value is false if no selector is cached.
func (s *State) getSelector(id policyID) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ps, ok := s.policyStates[id]
	if !ok {
		return "", false
	}
	return ps.selector, true
}

// delete removes all cached state for the given policy.
func (s *State) delete(id policyID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.policyStates, id)
}
