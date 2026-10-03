// Package clock abstracts time so delivery logic can be tested deterministically.
package clock

import "time"

// Clock is the minimal time abstraction used by the delivery engine.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// After waits for the duration to elapse and then sends the current time
	// on the returned channel. It must never block the caller.
	After(d time.Duration) <-chan time.Time
}

// RealClock is the production Clock backed by the real system clock.
type RealClock struct{}

// NewReal returns a RealClock.
func NewReal() RealClock { return RealClock{} }

// Now implements Clock.
func (RealClock) Now() time.Time { return time.Now() }

// After implements Clock.
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
