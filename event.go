package webhook

import "time"

// Event is a business event produced upstream.
type Event struct {
	ID         string    // stable upstream event identifier
	Type       string    // event type, e.g. "order.created"
	OccurredAt time.Time // business time assigned by the producer
	Payload    []byte    // opaque JSON payload delivered verbatim
}
