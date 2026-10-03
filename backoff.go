package webhook

import (
	"math"
	"math/rand"
	"time"
)

// BackoffConfig configures exponential backoff with full jitter.
type BackoffConfig struct {
	Base       time.Duration // delay before the first retry
	Max        time.Duration // hard ceiling for any computed delay
	Multiplier float64       // growth factor per attempt, >= 1
	Jitter     float64       // fraction of the computed delay used as random jitter, 0..1
}

// DefaultBackoffConfig returns sane production defaults.
func DefaultBackoffConfig() BackoffConfig {
	return BackoffConfig{
		Base:       500 * time.Millisecond,
		Max:        60 * time.Second,
		Multiplier: 2,
		Jitter:     0.2,
	}
}

// Backoffer computes the wait before the next attempt.
type Backoffer struct {
	cfg BackoffConfig
	rng *rand.Rand
}

// NewBackoffer builds a backoffer. A nil/zero config falls back to defaults.
// The random source is injectable so jitter is reproducible in tests.
func NewBackoffer(cfg BackoffConfig) *Backoffer {
	if cfg.Base <= 0 {
		cfg.Base = DefaultBackoffConfig().Base
	}
	if cfg.Max <= 0 {
		cfg.Max = DefaultBackoffConfig().Max
	}
	if cfg.Multiplier < 1 {
		cfg.Multiplier = DefaultBackoffConfig().Multiplier
	}
	if cfg.Jitter < 0 {
		cfg.Jitter = 0
	}
	if cfg.Jitter > 1 {
		cfg.Jitter = 1
	}
	if cfg.Max < cfg.Base {
		cfg.Max = cfg.Base
	}
	return &Backoffer{cfg: cfg, rng: rand.New(rand.NewSource(1))}
}

// Next returns the delay before the retry following the given attempt number
// (the first attempt is 1; the first retry asks for attempt=1).
func (b *Backoffer) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exp := float64(b.cfg.Base) * math.Pow(b.cfg.Multiplier, float64(attempt-1))
	if exp > float64(b.cfg.Max) {
		exp = float64(b.cfg.Max)
	}
	if exp < 0 { // overflow guard
		exp = float64(b.cfg.Max)
	}
	delay := time.Duration(exp)
	if b.cfg.Jitter > 0 {
		spread := float64(delay) * b.cfg.Jitter
		// Symmetric jitter in [-spread, +spread); the average delay is unchanged.
		delta := time.Duration(b.rng.Float64()*2*spread - spread)
		delay += delta
		if delay < 0 {
			delay = 0
		}
	}
	if delay > b.cfg.Max {
		delay = b.cfg.Max
	}
	return delay
}
