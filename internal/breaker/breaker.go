// Package breaker implements a per-endpoint circuit breaker.
package breaker

import (
	"sync"
	"time"

	"github.com/guoyyit/webhook-delivery-go/internal/clock"
)

// State is the circuit breaker state.
type State int

// Circuit breaker states.
const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

// String returns the state name.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	}
	return "unknown"
}

// Config configures a circuit breaker.
type Config struct {
	// FailureThreshold is the number of consecutive failures that opens the breaker.
	FailureThreshold int
	// OpenInterval is how long the breaker stays open before probing recovery.
	OpenInterval time.Duration
	// Clock is the time source; defaults to the real clock.
	Clock clock.Clock
}

// DefaultConfig returns sensible breaker defaults.
func DefaultConfig() Config {
	return Config{
		FailureThreshold: 5,
		OpenInterval:     30 * time.Second,
	}
}

// Breaker is a circuit breaker for a single endpoint.
type Breaker struct {
	mu       sync.Mutex
	cfg      Config
	state    State
	failures int
	openedAt time.Time
	probing  bool
	now      func() time.Time
}

// New creates a breaker with the given config.
func New(cfg Config) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = DefaultConfig().FailureThreshold
	}
	if cfg.OpenInterval <= 0 {
		cfg.OpenInterval = DefaultConfig().OpenInterval
	}
	now := time.Now
	if cfg.Clock != nil {
		now = cfg.Clock.Now
	}
	return &Breaker{cfg: cfg, state: StateClosed, now: now}
}

// State returns the current state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Allow reports whether a request may proceed. In the open state it returns
// false until the open interval elapses, then it transitions to half-open and
// allows exactly one probe request.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		if b.now().Sub(b.openedAt) >= b.cfg.OpenInterval {
			b.state = StateHalfOpen
			b.probing = true
			return true
		}
		return false
	case StateHalfOpen:
		// Only a single probe is allowed; everyone else waits.
		return false
	}
	return false
}

// RecordSuccess records a successful request.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.probing = false
	b.state = StateClosed
}

// RecordFailure records a failed request.
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.state == StateHalfOpen {
		// The probe failed; reopen and wait out the interval again.
		b.state = StateOpen
		b.openedAt = b.now()
		b.probing = false
		return
	}
	if b.failures >= b.cfg.FailureThreshold {
		b.state = StateOpen
		b.openedAt = b.now()
	}
}

// Registry holds one breaker per endpoint.
type Registry struct {
	mu       sync.Mutex
	cfg      Config
	breakers map[string]*Breaker
}

// NewRegistry creates a registry.
func NewRegistry(cfg Config) *Registry {
	if cfg.Clock == nil {
		cfg.Clock = clock.NewReal()
	}
	return &Registry{cfg: cfg, breakers: make(map[string]*Breaker)}
}

// Get returns the breaker for the endpoint, creating one if needed.
func (r *Registry) Get(endpoint string) *Breaker {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.breakers[endpoint]
	if !ok {
		b = New(r.cfg)
		r.breakers[endpoint] = b
	}
	return b
}
