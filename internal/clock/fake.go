package clock

import (
	"sync"
	"time"
)

// FakeClock is a manually advanced clock for deterministic tests. Timers
// created with After only fire when Advance moves the clock past their
// deadline, so tests never depend on wall-clock scheduling.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
}

// NewFakeClock returns a fake clock starting at t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

// Now implements Clock.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After implements Clock. A non-positive duration fires immediately.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.timers = append(c.timers, &fakeTimer{deadline: c.now.Add(d), ch: ch})
	return ch
}

// Advance moves the clock forward by d and fires all due timers.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	c.fireLocked()
}

// Set moves the clock to an explicit time (timers are never moved backwards).
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.After(c.now) {
		c.now = t
		c.fireLocked()
	}
}

func (c *FakeClock) fireLocked() {
	due := make([]*fakeTimer, 0, len(c.timers))
	for _, t := range c.timers {
		if !t.deadline.After(c.now) {
			select {
			case t.ch <- c.now:
			default:
			}
		} else {
			due = append(due, t)
		}
	}
	c.timers = due
}
