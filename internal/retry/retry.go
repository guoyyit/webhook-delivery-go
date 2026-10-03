// Package retry implements configurable backoff with jitter and failure classification.
package retry

import (
	"math"
	"math/rand"
	"net/http"
	"time"
)

// Policy controls retry behavior for a delivery.
type Policy struct {
	// InitialInterval is the delay before the first retry.
	InitialInterval time.Duration
	// MaxInterval caps the delay between retries.
	MaxInterval time.Duration
	// Multiplier scales the interval after each attempt.
	Multiplier float64
	// MaxRetries is the maximum number of retry attempts (0 = no retries).
	MaxRetries int
	// MaxElapsedTime caps the total time spent retrying since the first attempt.
	MaxElapsedTime time.Duration
	// Jitter is the fraction of the computed interval randomized away.
	Jitter float64
}

// DefaultPolicy returns a conservative retry policy.
func DefaultPolicy() Policy {
	return Policy{
		InitialInterval: time.Second,
		MaxInterval:     time.Minute,
		Multiplier:      2,
		MaxRetries:      5,
		MaxElapsedTime:  5 * time.Minute,
		Jitter:          0.2,
	}
}

// Delay returns the backoff delay for the given retry attempt number
// (attempt is 1 for the first retry). The second result reports whether
// another retry is allowed.
func (p Policy) Delay(attempt int) (time.Duration, bool) {
	if attempt <= 0 {
		attempt = 1
	}
	if p.MaxRetries >= 0 && attempt > p.MaxRetries {
		return 0, false
	}
	if p.InitialInterval <= 0 {
		return 0, true
	}
	mult := math.Pow(p.Multiplier, float64(attempt-1))
	interval := float64(p.InitialInterval) * mult
	if p.MaxInterval > 0 && interval > float64(p.MaxInterval) {
		interval = float64(p.MaxInterval)
	}
	if p.Jitter > 0 {
		// Randomize in [interval*(1-jitter), interval*(1+jitter)).
		delta := interval * p.Jitter
		interval += (rand.Float64()*2 - 1) * delta
	}
	if interval < 0 {
		interval = 0
	}
	return time.Duration(interval), true
}

// RetryableStatus reports whether an HTTP status code warrants a retry.
func RetryableStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout, // 408
		http.StatusTooManyRequests,     // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return true
	}
	return false
}

// Classify returns true when the error/status pair is retryable. A status of
// 0 means no response was received (network/timeout error), which is retryable.
func Classify(err error, statusCode int) bool {
	if err != nil {
		return true
	}
	if statusCode == 0 {
		return true
	}
	return RetryableStatus(statusCode)
}
