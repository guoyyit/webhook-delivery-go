package webhook

import (
	"sort"
	"sync"
	"time"
)

// Clock abstracts time so retry/backoff and circuit breaking are testable.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// realClock is the production wall-clock implementation.
type realClock struct{}

// RealClock is the default Clock backed by the time package.
var RealClock Clock = realClock{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// fakeTimer is a manually controlled timer used by FakeClock.
type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
	fired    bool
}

// FakeClock is a deterministic clock whose time only moves on Advance.
// Timers created through After fire (in deadline order) while time advances.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock returns a fake clock pinned at start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	t := &fakeTimer{
		deadline: c.Now().Add(d),
		ch:       make(chan time.Time, 1),
	}
	c.mu.Lock()
	c.timers = append(c.timers, t)
	c.mu.Unlock()
	return t.ch
}

// Advance moves the clock forward and fires every timer whose deadline has
// been reached. Timers with zero/negative duration fire immediately.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	due := make([]*fakeTimer, 0)
	remaining := make([]*fakeTimer, 0, len(c.timers))
	for _, t := range c.timers {
		if !t.deadline.After(c.now) {
			due = append(due, t)
		} else {
			remaining = append(remaining, t)
		}
	}
	c.timers = remaining
	c.mu.Unlock()

	// Fire in deadline order so dependent timers behave deterministically.
	sort.Slice(due, func(i, j int) bool { return due[i].deadline.Before(due[j].deadline) })
	for _, t := range due {
		t.fired = true
		select {
		case t.ch <- t.deadline:
		default:
		}
	}
}
