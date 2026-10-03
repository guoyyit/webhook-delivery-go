package webhook

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Endpoint is a downstream HTTP receiver.
type Endpoint struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Secret    []byte    `json:"secret"`     // HMAC signing key, never logged
	AuthToken string    `json:"auth_token"` // bearer token, never logged
	CreatedAt time.Time `json:"created_at"`
}

// Subscription binds an endpoint to a set of event types.
// EventType "*" matches every event type.
type Subscription struct {
	ID         string    `json:"id"`
	EndpointID string    `json:"endpoint_id"`
	EventTypes []string  `json:"event_types"`
	CreatedAt  time.Time `json:"created_at"`
}

func (s Subscription) matches(eventType string) bool {
	for _, t := range s.EventTypes {
		if t == "*" || t == eventType {
			return true
		}
	}
	return false
}

// Registry stores endpoints and subscriptions and answers type matching.
// All operations are safe for concurrent use. Matching takes a snapshot under
// RLock so the result cannot be affected by registration order or by writers
// racing with the match; an unsubscribed id is simply absent from later
// snapshots and therefore receives no further deliveries.
type Registry struct {
	mu       sync.RWMutex
	ends     map[string]Endpoint
	subs     map[string]Subscription
	endIndex map[string]map[string]struct{} // endpointID -> set(subscriptionID)
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		ends:     map[string]Endpoint{},
		subs:     map[string]Subscription{},
		endIndex: map[string]map[string]struct{}{},
	}
}

// AddEndpoint registers (or replaces) an endpoint.
func (r *Registry) AddEndpoint(ep Endpoint) error {
	if ep.ID == "" {
		return errors.New("endpoint id is required")
	}
	if ep.URL == "" {
		return fmt.Errorf("endpoint %q url is required", ep.ID)
	}
	r.mu.Lock()
	r.ends[ep.ID] = ep
	if r.endIndex[ep.ID] == nil {
		r.endIndex[ep.ID] = map[string]struct{}{}
	}
	r.mu.Unlock()
	return nil
}

// Endpoint returns a registered endpoint.
func (r *Registry) Endpoint(id string) (Endpoint, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ep, ok := r.ends[id]
	return ep, ok
}

// PutSubscription registers or replaces a subscription.
func (r *Registry) PutSubscription(sub Subscription) error {
	if sub.ID == "" {
		return errors.New("subscription id is required")
	}
	if sub.EndpointID == "" {
		return fmt.Errorf("subscription %q endpoint id is required", sub.ID)
	}
	if len(sub.EventTypes) == 0 {
		return fmt.Errorf("subscription %q must subscribe to at least one event type", sub.ID)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.ends[sub.EndpointID]; !ok {
		return fmt.Errorf("subscription %q references unknown endpoint %q", sub.ID, sub.EndpointID)
	}
	// Replace semantics: drop any previous binding under the same id.
	if old, ok := r.subs[sub.ID]; ok {
		delete(r.endIndex[old.EndpointID], sub.ID)
	}
	r.subs[sub.ID] = sub
	if r.endIndex[sub.EndpointID] == nil {
		r.endIndex[sub.EndpointID] = map[string]struct{}{}
	}
	r.endIndex[sub.EndpointID][sub.ID] = struct{}{}
	return nil
}

// RemoveSubscription unsubscribes. After it returns the subscription matches
// nothing and receives no further deliveries.
func (r *Registry) RemoveSubscription(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.subs[id]; ok {
		delete(r.endIndex[old.EndpointID], id)
		delete(r.subs, id)
	}
}

// Matches returns every subscription interested in the event type.
// The returned slice is a stable snapshot (sorted by id) so matching results
// do not depend on registration/iteration order.
func (r *Registry) Matches(eventType string) []Subscription {
	r.mu.RLock()
	out := make([]Subscription, 0)
	for _, s := range r.subs {
		if s.matches(eventType) {
			out = append(out, s)
		}
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// subscriptionsForEndpoint returns the live subscription ids bound to an endpoint.
func (r *Registry) subscriptionsForEndpoint(endpointID string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	set := r.endIndex[endpointID]
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
