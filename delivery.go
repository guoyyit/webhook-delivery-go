package webhook

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Status is the lifecycle state of one delivery to one endpoint.
type Status string

const (
	StatusPending   Status = "pending"   // queued, waiting for its turn/retry
	StatusInFlight  Status = "inflight"  // HTTP attempt in progress
	StatusSucceeded Status = "succeeded" // endpoint accepted (2xx)
	StatusDead      Status = "dead"      // terminal failure, moved to dead-letter
)

// Dead-letter reason codes. They make it possible to locate why a delivery stopped.
const (
	ReasonNonRetryable  = "non_retryable_status"
	ReasonMaxAttempts   = "max_attempts_exceeded"
	ReasonDeadline      = "delivery_deadline_exceeded"
	ReasonCircuitOpen   = "endpoint_circuit_open"
	ReasonQueueOverflow = "endpoint_queue_overflow"
	ReasonUnsubscribed  = "subscription_removed"
)

// Attempt records one HTTP delivery attempt for observability/troubleshooting.
type Attempt struct {
	Number     int       `json:"number"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	HTTPStatus int       `json:"http_status,omitempty"`
	Retryable  bool      `json:"retryable"`
	Error      string    `json:"error,omitempty"`
}

// Delivery is one (event, endpoint) fan-out unit with its full state machine.
type Delivery struct {
	ID             string     `json:"id"` // stable: see DeliveryID
	EventID        string     `json:"event_id"`
	EventType      string     `json:"event_type"`
	Seq            uint64     `json:"seq"` // process-wide monotonic; ties to endpoint ordering
	EndpointID     string     `json:"endpoint_id"`
	SubscriptionID string     `json:"subscription_id"`
	Payload        []byte     `json:"payload"`
	Status         Status     `json:"status"`
	Attempts       []Attempt  `json:"attempts"`
	CreatedAt      time.Time  `json:"created_at"`
	Deadline       time.Time  `json:"deadline"`
	NextAttemptAt  time.Time  `json:"next_attempt_at,omitempty"`
	SucceededAt    *time.Time `json:"succeeded_at,omitempty"`
	DeadReason     string     `json:"dead_reason,omitempty"`
}

// DeliveryID derives the stable identifier of one (event, endpoint/subscription)
// delivery. Re-publishing the same event, retries, restarts and dead-letter
// replay all produce the SAME id, so the downstream can dedup semantically:
// one delivery id == one business event for one endpoint.
func DeliveryID(eventID, endpointID string) string {
	sum := sha256.Sum256([]byte(eventID + "\x00" + endpointID))
	return "dlv_" + hex.EncodeToString(sum[:16])
}

// attemptsExhausted reports whether the retry budget ran out.
func (d *Delivery) attemptsExhausted(maxAttempts int) bool {
	return maxAttempts > 0 && len(d.Attempts) >= maxAttempts
}

// deadlinePassed reports whether the total delivery deadline elapsed.
// A zero deadline means "no overall limit".
func (d *Delivery) deadlinePassed(now time.Time) bool {
	return !d.Deadline.IsZero() && now.After(d.Deadline)
}

// cloneDelivery returns a deep copy so stored state cannot be mutated through
// references (payloads and attempt slices are copied).
func cloneDelivery(d *Delivery) *Delivery {
	if d == nil {
		return nil
	}
	c := *d
	if d.Payload != nil {
		c.Payload = append([]byte(nil), d.Payload...)
	}
	if d.Attempts != nil {
		c.Attempts = append([]Attempt(nil), d.Attempts...)
	}
	if d.SucceededAt != nil {
		t := *d.SucceededAt
		c.SucceededAt = &t
	}
	return &c
}
