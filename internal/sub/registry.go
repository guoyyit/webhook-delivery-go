// Package sub maintains the subscription registry and performs event matching.
package sub

import (
	"sort"
	"sync"

	"github.com/guoyyit/webhook-delivery-go/internal/model"
)

// Registry is a concurrency-safe, in-memory subscription registry.
type Registry struct {
	mu            sync.RWMutex
	subscriptions map[string]*model.Subscription
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{subscriptions: make(map[string]*model.Subscription)}
}

// Add registers (or replaces) a subscription. The stored value is a copy, so
// later caller mutation cannot affect the registry.
func (r *Registry) Add(sub *model.Subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *sub
	if cp.EventTypes != nil {
		cp.EventTypes = append([]string(nil), sub.EventTypes...)
	}
	r.subscriptions[cp.ID] = &cp
}

// Remove deletes a subscription.
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.subscriptions, id)
}

// Get returns the subscription with the given id.
func (r *Registry) Get(id string) (*model.Subscription, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sub, ok := r.subscriptions[id]
	if !ok {
		return nil, false
	}
	cp := *sub
	return &cp, true
}

// List returns all subscriptions ordered by id.
func (r *Registry) List() []*model.Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*model.Subscription, 0, len(r.subscriptions))
	for _, sub := range r.subscriptions {
		cp := *sub
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Match returns the enabled subscriptions that accept the given event type.
// Results are ordered by subscription id so they are independent of
// registration order and concurrent mutation.
func (r *Registry) Match(eventType string) []*model.Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*model.Subscription, 0)
	for _, sub := range r.subscriptions {
		if sub.Disabled {
			continue
		}
		if !sub.Matches(eventType) {
			continue
		}
		cp := *sub
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
