// Package model defines the core domain types for the webhook delivery system.
package model

import "time"

// Event is an upstream business event that may be delivered to subscribers.
type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Payload   []byte    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
	Seq       int64     `json:"seq"`
}

// Subscription binds an endpoint URL to the event types it wants to receive.
type Subscription struct {
	ID         string    `json:"id"`
	URL        string    `json:"url"`
	EventTypes []string  `json:"event_types"`
	Secret     string    `json:"secret"`
	CreatedAt  time.Time `json:"created_at"`
	Disabled   bool      `json:"disabled"`
}

// Matches reports whether the subscription accepts events of the given type.
// An empty EventTypes list means the subscription accepts every event type.
func (s *Subscription) Matches(eventType string) bool {
	if len(s.EventTypes) == 0 {
		return true
	}
	for _, t := range s.EventTypes {
		if t == eventType {
			return true
		}
	}
	return false
}

// DeliveryStatus is the lifecycle state of a single event-to-subscription delivery.
type DeliveryStatus string

const (
	StatusPending      DeliveryStatus = "pending"
	StatusInProgress   DeliveryStatus = "in_progress"
	StatusSucceeded    DeliveryStatus = "succeeded"
	StatusFailed       DeliveryStatus = "failed"
	StatusDeadLettered DeliveryStatus = "dead_lettered"
)

// Delivery tracks the delivery of one event to one subscription endpoint.
type Delivery struct {
	ID               string         `json:"id"`
	EventID          string         `json:"event_id"`
	EventSeq         int64          `json:"event_seq"`
	EventType        string         `json:"event_type"`
	SubscriptionID   string         `json:"subscription_id"`
	EndpointURL      string         `json:"endpoint_url"`
	Status           DeliveryStatus `json:"status"`
	Attempts         int            `json:"attempts"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
	FirstAttemptAt   *time.Time     `json:"first_attempt_at,omitempty"`
	NextRetryAt      *time.Time     `json:"next_retry_at,omitempty"`
	LastError        string         `json:"last_error,omitempty"`
	DeadLetterReason string         `json:"dead_letter_reason,omitempty"`
}

// Terminal reports whether the delivery has reached a final state that must
// not be picked up by workers again.
func (d *Delivery) Terminal() bool {
	switch d.Status {
	case StatusSucceeded, StatusFailed, StatusDeadLettered:
		return true
	}
	return false
}
