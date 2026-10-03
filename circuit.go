package webhook

import (
	"sync"
	"time"
)

// CircuitState labels the endpoint circuit-breaker state.
type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

// DefaultCircuitConfig are production defaults: 5 consecutive failures isolate
// the endpoint for 30s, then up to 1 probe may pass through.
func DefaultCircuitConfig() CircuitConfig {
	return CircuitConfig{
		FailureThreshold: 5,
		OpenTTL:          30 * time.Second,
		HalfOpenMax:      1,
	}
}

// CircuitConfig configures an endpoint circuit breaker.
type CircuitConfig struct {
	FailureThreshold int           // consecutive failures before opening
	OpenTTL          time.Duration // how long an endpoint stays isolated
	HalfOpenMax      int           // probe attempts allowed in half-open
}

// Circuit is a per-endpoint breaker. Only one request at a time is in flight
// for an endpoint (the worker serializes deliveries), so the half-open probe
// accounting needs no extra locking beyond mu.
type Circuit struct {
	clock    Clock
	mu       sync.Mutex
	cfg      CircuitConfig
	failures int
	state    CircuitState
	openedAt time.Time
	halfUsed int
}

// NewCircuit builds a breaker. Zero config values fall back to defaults.
func NewCircuit(cfg CircuitConfig, clock Clock) *Circuit {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = DefaultCircuitConfig().FailureThreshold
	}
	if cfg.OpenTTL <= 0 {
		cfg.OpenTTL = DefaultCircuitConfig().OpenTTL
	}
	if cfg.HalfOpenMax <= 0 {
		cfg.HalfOpenMax = DefaultCircuitConfig().HalfOpenMax
	}
	if clock == nil {
		clock = RealClock
	}
	return &Circuit{cfg: cfg, clock: clock, state: CircuitClosed}
}

// transitionLocked applies the open -> half-open expiry transition.
func (c *Circuit) transitionLocked() {
	if c.state == CircuitOpen && !c.clock.Now().Before(c.openedAt.Add(c.cfg.OpenTTL)) {
		c.state = CircuitHalfOpen
		c.halfUsed = 0
	}
}

// Allow reports whether a delivery may proceed. In half-open it consumes one
// of the limited probe slots; once probes are in flight/full it denies.
func (c *Circuit) Allow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.transitionLocked()
	switch c.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		return false
	default: // half-open: admit a bounded number of probes
		if c.halfUsed < c.cfg.HalfOpenMax {
			c.halfUsed++
			return true
		}
		return false
	}
}

// NoteResult feeds an attempt result back into the breaker:
//   - success closes the breaker immediately;
//   - a retryable failure in half-open re-opens it (probe failed);
//   - consecutive retryable failures in closed open it at the threshold.
//
// Non-retryable (terminal, e.g. 4xx) failures do not count: the endpoint
// answered correctly, the delivery itself is dead.
func (c *Circuit) NoteResult(retryableFailure bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.transitionLocked()
	if !retryableFailure {
		c.state = CircuitClosed
		c.failures = 0
		c.halfUsed = 0
		return
	}
	switch c.state {
	case CircuitHalfOpen:
		// Probe failed: isolate again with a fresh TTL.
		c.state = CircuitOpen
		c.openedAt = c.clock.Now()
		c.failures = c.cfg.FailureThreshold
	case CircuitClosed:
		c.failures++
		if c.failures >= c.cfg.FailureThreshold {
			c.state = CircuitOpen
			c.openedAt = c.clock.Now()
		}
	case CircuitOpen:
		// Results arriving while open (e.g. a just-finished attempt that
		// observed the old state) refresh the isolation window.
		c.openedAt = c.clock.Now()
	}
}

// ForceOpen isolates an endpoint immediately (used by tests / admin).
func (c *Circuit) ForceOpen() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = CircuitOpen
	c.openedAt = c.clock.Now()
	c.failures = c.cfg.FailureThreshold
}

// State returns the current state, applying TTL expiry first.
func (c *Circuit) State() CircuitState {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.transitionLocked()
	return c.state
}

// openedAtOf is a test/observability helper.
func (c *Circuit) openedAtOf() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.openedAt
}
