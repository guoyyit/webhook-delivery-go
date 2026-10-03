package breaker

import (
	"testing"
	"time"

	"github.com/guoyyit/webhook-delivery-go/internal/clock"
)

func TestBreakerOpensAndProbeRecovers(t *testing.T) {
	fc := clock.NewFakeClock(time.Unix(1000, 0))
	b := New(Config{FailureThreshold: 3, OpenInterval: time.Minute, Clock: fc})

	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("request %d should be allowed while closed", i+1)
		}
		b.RecordFailure()
	}
	if b.State() != StateOpen {
		t.Fatalf("expected open, got %s", b.State())
	}
	if b.Allow() {
		t.Fatal("open breaker must not allow requests")
	}

	// Before the interval elapses, still blocked.
	fc.Advance(30 * time.Second)
	if b.Allow() {
		t.Fatal("breaker should remain open before interval elapses")
	}

	// At the interval boundary, a single probe is allowed.
	fc.Advance(31 * time.Second)
	if !b.Allow() {
		t.Fatal("probe should be allowed after interval")
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("expected half_open, got %s", b.State())
	}
	if b.Allow() {
		t.Fatal("only one probe may run in half_open")
	}

	// Probe succeeds -> closed again.
	b.RecordSuccess()
	if b.State() != StateClosed {
		t.Fatalf("expected closed after probe success, got %s", b.State())
	}
	if !b.Allow() {
		t.Fatal("closed breaker should allow requests")
	}
}

func TestBreakerProbeFailureReopens(t *testing.T) {
	fc := clock.NewFakeClock(time.Unix(1000, 0))
	b := New(Config{FailureThreshold: 1, OpenInterval: time.Minute, Clock: fc})
	b.Allow()
	b.RecordFailure()
	if b.State() != StateOpen {
		t.Fatalf("expected open, got %s", b.State())
	}
	fc.Advance(time.Minute + time.Second)
	b.Allow() // probe
	b.RecordFailure()
	if b.State() != StateOpen {
		t.Fatalf("probe failure should reopen, got %s", b.State())
	}
	if b.Allow() {
		t.Fatal("reopened breaker must block requests")
	}
}

func TestBreakerSuccessResetsFailures(t *testing.T) {
	fc := clock.NewFakeClock(time.Unix(1000, 0))
	b := New(Config{FailureThreshold: 2, OpenInterval: time.Minute, Clock: fc})
	b.Allow()
	b.RecordFailure()
	b.Allow()
	b.RecordSuccess()
	b.Allow()
	b.RecordFailure()
	if b.State() != StateClosed {
		t.Fatalf("success should reset failure count, got %s", b.State())
	}
}

func TestRegistryReturnsSameBreakerPerEndpoint(t *testing.T) {
	r := NewRegistry(DefaultConfig())
	a := r.Get("https://a.example")
	b := r.Get("https://b.example")
	if a == b {
		t.Fatal("different endpoints need different breakers")
	}
	if r.Get("https://a.example") != a {
		t.Fatal("same endpoint must return the same breaker")
	}
}
