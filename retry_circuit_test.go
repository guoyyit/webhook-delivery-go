package webhook

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestBackoffDeterministic(t *testing.T) {
	b := NewBackoffer(BackoffConfig{
		Base: 100 * time.Millisecond, Max: 10 * time.Second, Multiplier: 2, Jitter: 0,
	})
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}
	for attempt, w := range want {
		if got := b.Next(attempt + 1); got != w {
			t.Fatalf("attempt %d delay=%s want %s", attempt+1, got, w)
		}
	}
	// Cap at Max.
	capped := NewBackoffer(BackoffConfig{Base: 100 * time.Millisecond, Max: 500 * time.Millisecond, Multiplier: 10, Jitter: 0})
	if got := capped.Next(5); got != 500*time.Millisecond {
		t.Fatalf("cap: got %s want 500ms", got)
	}
}

// TestRetryThenSuccess: first two attempts 503, third 200. With a fake clock
// the retry waits are advanced deterministically; order/state stays consistent.
func TestRetryThenSuccess(t *testing.T) {
	clock := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	sender := newFakeSender()
	cfg := testCfg(clock, sender, "")
	cfg.MaxAttempts = 5
	cfg.Backoff = BackoffConfig{Base: 100 * time.Millisecond, Max: time.Second, Multiplier: 2, Jitter: 0}
	d := mustStart(t, cfg)
	const url = "http://retry"
	must(t, d.AddEndpoint(Endpoint{ID: "r", URL: url, Secret: []byte("k")}))
	must(t, d.PutSubscription(Subscription{ID: "rs", EndpointID: "r", EventTypes: []string{"t"}}))

	sender.setScript(url, func(nth int) (fakeResp, error) {
		if nth < 3 {
			return fakeResp{status: 503}, nil
		}
		return fakeResp{status: 200}, nil
	})

	if _, err := d.Publish(Event{ID: "ev", Type: "t", Payload: []byte("{}")}); err != nil {
		t.Fatal(err)
	}

	// Attempt 1 fails; advance through the two backoff windows (100ms, 200ms).
	sender.waitFor(t, url, 1, time.Second)
	for sender.totalCalls() < 3 {
		calls := len(sender.callsTo(url))
		switch calls {
		case 1:
			clock.Advance(100 * time.Millisecond)
		case 2:
			clock.Advance(200 * time.Millisecond)
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}

	dl := waitForStatus(t, d, DeliveryID("ev", "r"), StatusSucceeded, 2*time.Second)
	if len(dl.Attempts) != 3 {
		t.Fatalf("want 3 attempts, got %d", len(dl.Attempts))
	}
	if dl.Attempts[0].HTTPStatus != 503 || dl.Attempts[2].HTTPStatus != 200 {
		t.Fatalf("attempt trace wrong: %+v", dl.Attempts)
	}
	// Same delivery id across every retry.
	for _, c := range sender.callsTo(url) {
		if c.req.Header.Get(HeaderDeliveryID) != dl.ID {
			t.Fatal("delivery id must remain stable across retries")
		}
	}
}

// TestNonRetryableIsTerminal: 404 goes straight to dead with no retry.
func TestNonRetryableIsTerminal(t *testing.T) {
	sender := newFakeSender()
	d := mustStart(t, testCfg(RealClock, sender, ""))
	const url = "http://404"
	must(t, d.AddEndpoint(Endpoint{ID: "x", URL: url, Secret: []byte("k")}))
	must(t, d.PutSubscription(Subscription{ID: "xs", EndpointID: "x", EventTypes: []string{"t"}}))
	sender.setScript(url, func(int) (fakeResp, error) { return fakeResp{status: 404}, nil })

	_, _ = d.Publish(Event{ID: "ev", Type: "t", Payload: nil})
	dl := waitForStatus(t, d, DeliveryID("ev", "x"), StatusDead, time.Second)
	if dl.DeadReason != ReasonNonRetryable {
		t.Fatalf("reason=%s want %s", dl.DeadReason, ReasonNonRetryable)
	}
	if len(dl.Attempts) != 1 {
		t.Fatalf("non-retryable must attempt exactly once, got %d", len(dl.Attempts))
	}
}

// TestMaxAttemptsDeadLetters: every attempt 500 -> dead after MaxAttempts.
func TestMaxAttemptsDeadLetters(t *testing.T) {
	clock := NewFakeClock(time.Now())
	sender := newFakeSender()
	cfg := testCfg(clock, sender, "")
	cfg.MaxAttempts = 3
	cfg.Backoff = BackoffConfig{Base: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2, Jitter: 0}
	d := mustStart(t, cfg)
	const url = "http://500"
	must(t, d.AddEndpoint(Endpoint{ID: "x", URL: url, Secret: []byte("k")}))
	must(t, d.PutSubscription(Subscription{ID: "xs", EndpointID: "x", EventTypes: []string{"t"}}))
	sender.setScript(url, func(int) (fakeResp, error) { return fakeResp{status: 500}, nil })

	_, _ = d.Publish(Event{ID: "ev", Type: "t", Payload: nil})

	// Drain retries under the fake clock.
	deadline := time.Now().Add(3 * time.Second)
	for {
		calls := len(sender.callsTo(url))
		if calls >= 3 {
			break
		}
		clock.Advance(10 * time.Millisecond)
		time.Sleep(2 * time.Millisecond)
		if time.Now().After(deadline) {
			t.Fatalf("only %d attempts happened", calls)
		}
	}
	dl := waitForStatus(t, d, DeliveryID("ev", "x"), StatusDead, 2*time.Second)
	if dl.DeadReason != ReasonMaxAttempts {
		t.Fatalf("reason=%s", dl.DeadReason)
	}
	if len(dl.Attempts) != 3 {
		t.Fatalf("attempts=%d want 3", len(dl.Attempts))
	}
	dls := d.DeadLetters()
	if len(dls) != 1 {
		t.Fatalf("dead letters=%d want 1", len(dls))
	}
}

// TestCircuitOpensAndProbesRecovery: after threshold consecutive failures the
// circuit opens; later events are dead-lettered (not silently dropped); after
// the open TTL a successful probe closes it and delivery resumes.
func TestCircuitOpensAndProbesRecovery(t *testing.T) {
	clock := NewFakeClock(time.Now())
	sender := newFakeSender()
	cfg := testCfg(clock, sender, "")
	cfg.MaxAttempts = 1 // failures become terminal immediately so we isolate fast
	cfg.Circuit = CircuitConfig{FailureThreshold: 3, OpenTTL: 200 * time.Millisecond, HalfOpenMax: 1}
	d := mustStart(t, cfg)
	const url = "http://flaky"
	must(t, d.AddEndpoint(Endpoint{ID: "f", URL: url, Secret: []byte("k")}))
	must(t, d.PutSubscription(Subscription{ID: "fs", EndpointID: "f", EventTypes: []string{"t"}}))

	var healthy int32 // 0 = failing
	sender.setScript(url, func(int) (fakeResp, error) {
		if atomic.LoadInt32(&healthy) == 1 {
			return fakeResp{status: 200}, nil
		}
		return fakeResp{status: 500}, nil
	})

	// Three failing events trip the breaker.
	for i := 0; i < 3; i++ {
		_, _ = d.Publish(Event{ID: "bad" + string(rune('a'+i)), Type: "t", Payload: nil})
	}
	if !eventuallyTrue(2*time.Second, func() bool {
		return d.Circuit("f").State() == CircuitOpen
	}) {
		t.Fatal("circuit did not open")
	}

	// While open, new events are refused at the breaker and dead-lettered with
	// a circuit reason — never dropped silently.
	_, _ = d.Publish(Event{ID: "while-open", Type: "t", Payload: nil})
	dl := waitForStatus(t, d, DeliveryID("while-open", "f"), StatusDead, time.Second)
	if dl.DeadReason != ReasonCircuitOpen {
		t.Fatalf("want circuit-open dead letter, got %s", dl.DeadReason)
	}
	if n := len(sender.callsTo(url)); n != 3 {
		t.Fatalf("no attempt should be made while open, calls=%d", n)
	}

	// Endpoint recovers. Advance past the open TTL and publish a fresh event:
	// it becomes the limited half-open probe. Its success closes the breaker.
	atomic.StoreInt32(&healthy, 1)
	clock.Advance(250 * time.Millisecond)
	_, _ = d.Publish(Event{ID: "probe", Type: "t", Payload: nil})
	recovered := eventuallyTrue(2*time.Second, func() bool {
		return d.Circuit("f").State() == CircuitClosed
	})
	if !recovered {
		t.Fatalf("circuit did not re-close after a successful probe, state=%s", d.Circuit("f").State())
	}
	waitForStatus(t, d, DeliveryID("probe", "f"), StatusSucceeded, time.Second)

	// After recovery, a brand-new event flows normally.
	_, _ = d.Publish(Event{ID: "after", Type: "t", Payload: nil})
	waitForStatus(t, d, DeliveryID("after", "f"), StatusSucceeded, time.Second)
}

// TestNetworkErrorIsRetryable: transport errors count as transient.
func TestNetworkErrorIsRetryable(t *testing.T) {
	sender := newFakeSender()
	cfg := testCfg(RealClock, sender, "")
	cfg.MaxAttempts = 2
	cfg.Backoff = BackoffConfig{Base: time.Millisecond, Max: 2 * time.Millisecond, Jitter: 0}
	d := mustStart(t, cfg)
	const url = "http://net"
	must(t, d.AddEndpoint(Endpoint{ID: "n", URL: url, Secret: []byte("k")}))
	must(t, d.PutSubscription(Subscription{ID: "ns", EndpointID: "n", EventTypes: []string{"t"}}))
	var calls int32
	sender.setScript(url, func(nth int) (fakeResp, error) {
		atomic.AddInt32(&calls, 1)
		if nth == 1 {
			return fakeResp{}, errors.New("connection reset by peer")
		}
		return fakeResp{status: 200}, nil
	})
	_, _ = d.Publish(Event{ID: "ev", Type: "t", Payload: nil})
	waitForStatus(t, d, DeliveryID("ev", "n"), StatusSucceeded, 2*time.Second)
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("want 2 attempts, got %d", calls)
	}
}
