package webhook

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestPerEndpointOrderingAndFanOut verifies:
//   - one endpoint receives events strictly in publish (Seq) order;
//   - two endpoints both receive every shared event;
//   - header id/seq metadata is present and consistent.
func TestPerEndpointOrderingAndFanOut(t *testing.T) {
	sender := newFakeSender()
	d := mustStart(t, testCfg(RealClock, sender, ""))
	const slowURL, fastURL = "http://slow", "http://fast"
	must(t, d.AddEndpoint(Endpoint{ID: "slow", URL: slowURL, Secret: []byte("sk")}))
	must(t, d.AddEndpoint(Endpoint{ID: "fast", URL: fastURL, Secret: []byte("fk")}))
	must(t, d.PutSubscription(Subscription{ID: "ss", EndpointID: "slow", EventTypes: []string{"t"}}))
	must(t, d.PutSubscription(Subscription{ID: "fs", EndpointID: "fast", EventTypes: []string{"t"}}))

	const n = 20
	for i := 0; i < n; i++ {
		if _, err := d.Publish(Event{ID: fmt.Sprintf("ev-%d", i), Type: "t",
			OccurredAt: time.Now(), Payload: []byte(fmt.Sprintf(`{"i":%d}`, i))}); err != nil {
			t.Fatal(err)
		}
	}

	gotSlow := sender.waitFor(t, slowURL, n, 2*time.Second)
	gotFast := sender.waitFor(t, fastURL, n, 2*time.Second)

	checkOrder := func(calls []*recordedRequest, name string) {
		for i, c := range calls {
			if c.req.Header.Get(HeaderEventID) != fmt.Sprintf("ev-%d", i) {
				t.Fatalf("%s position %d got event %s", name, i, c.req.Header.Get(HeaderEventID))
			}
			if c.req.Header.Get(HeaderDeliveryID) == "" || c.req.Header.Get(HeaderSignature) == "" {
				t.Fatalf("%s missing signature/id headers", name)
			}
		}
	}
	checkOrder(gotSlow, "slow")
	checkOrder(gotFast, "fast")

	// Every delivery ends succeeded.
	for i := 0; i < n; i++ {
		waitForStatus(t, d, DeliveryID(fmt.Sprintf("ev-%d", i), "slow"), StatusSucceeded, time.Second)
		waitForStatus(t, d, DeliveryID(fmt.Sprintf("ev-%d", i), "fast"), StatusSucceeded, time.Second)
	}
}

// TestSlowEndpointDoesNotBlockFastEndpoint: slow endpoint's first request is
// held behind a gate, while the fast endpoint keeps draining its own queue.
func TestSlowEndpointDoesNotBlockFastEndpoint(t *testing.T) {
	sender := newFakeSender()
	d := mustStart(t, testCfg(RealClock, sender, ""))
	const slowURL, fastURL = "http://slow", "http://fast"
	must(t, d.AddEndpoint(Endpoint{ID: "slow", URL: slowURL, Secret: []byte("k")}))
	must(t, d.AddEndpoint(Endpoint{ID: "fast", URL: fastURL, Secret: []byte("k")}))
	must(t, d.PutSubscription(Subscription{ID: "ss", EndpointID: "slow", EventTypes: []string{"t"}}))
	must(t, d.PutSubscription(Subscription{ID: "fs", EndpointID: "fast", EventTypes: []string{"t"}}))

	gate := make(chan struct{})
	sender.setGate(slowURL, gate)

	// Publish: the slow event gets stuck in the gate; the fast events must
	// still flow because endpoints run independent workers.
	if _, err := d.Publish(Event{ID: "blocked", Type: "t", Payload: []byte("0")}); err != nil {
		t.Fatal(err)
	}
	// Give the slow worker a moment to enter the gate.
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if _, err := d.Publish(Event{ID: fmt.Sprintf("free-%d", i), Type: "t", Payload: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}

	// The blocked event matches BOTH subscriptions, so fast sees blocked +
	// free-0..4 = 6 deliveries while slow remains stuck on its first attempt.
	calls := sender.waitFor(t, fastURL, 6, 2*time.Second)
	if len(calls) != 6 {
		t.Fatalf("fast endpoint should process 6 while slow is blocked, got %d", len(calls))
	}
	if calls[0].req.Header.Get(HeaderEventID) != "blocked" {
		t.Fatalf("fast endpoint must still receive blocked first, got %q", calls[0].req.Header.Get(HeaderEventID))
	}
	if len(sender.callsTo(slowURL)) != 1 {
		t.Fatalf("slow endpoint should have exactly 1 in-flight attempt, got %d", len(sender.callsTo(slowURL)))
	}
	close(gate)
	waitForStatus(t, d, DeliveryID("blocked", "slow"), StatusSucceeded, 2*time.Second)
}

// TestPublishNeverBlocksProducer: with a tiny queue and an entirely wedged
// endpoint, Publish must return promptly; overflowing events land in the DLQ
// with ReasonQueueOverflow instead of stalling.
func TestPublishNeverBlocksProducer(t *testing.T) {
	sender := newFakeSender()
	cfg := testCfg(RealClock, sender, "")
	cfg.QueueSize = 2
	d := mustStart(t, cfg)
	const url = "http://wedged"
	must(t, d.AddEndpoint(Endpoint{ID: "w", URL: url, Secret: []byte("k")}))
	must(t, d.PutSubscription(Subscription{ID: "ws", EndpointID: "w", EventTypes: []string{"t"}}))

	// Wedge the endpoint: block forever (test never closes the gate).
	sender.setGate(url, make(chan struct{}))

	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			if _, err := d.Publish(Event{ID: fmt.Sprintf("e%d", i), Type: "t", Payload: []byte("x")}); err != nil {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a wedged endpoint")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Publish took too long under backpressure: %s", elapsed)
	}

	if !eventuallyTrue(2*time.Second, func() bool {
		for _, dl := range d.DeadLetters() {
			if dl.DeadReason == ReasonQueueOverflow {
				return true
			}
		}
		return false
	}) {
		t.Fatal("expected at least one queue-overflow dead letter")
	}
}

// TestConcurrentPublishDedup: concurrent publishes of the SAME event must
// collapse to one delivery per endpoint with the stable delivery id.
func TestConcurrentPublishDedup(t *testing.T) {
	sender := newFakeSender()
	d := mustStart(t, testCfg(RealClock, sender, ""))
	const url = "http://ep"
	must(t, d.AddEndpoint(Endpoint{ID: "ep", URL: url, Secret: []byte("k")}))
	must(t, d.PutSubscription(Subscription{ID: "s", EndpointID: "ep", EventTypes: []string{"t"}}))

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = d.Publish(Event{ID: "same-event", Type: "t", Payload: []byte("{}")})
		}()
	}
	wg.Wait()

	sender.waitFor(t, url, 1, 2*time.Second)
	time.Sleep(50 * time.Millisecond)
	if calls := sender.callsTo(url); len(calls) != 1 {
		t.Fatalf("expected exactly 1 delivery for duplicate concurrent publishes, got %d", len(calls))
	}
	dl, ok := d.Delivery(DeliveryID("same-event", "ep"))
	if !ok || dl.Status != StatusSucceeded {
		t.Fatalf("expected single succeeded delivery, got %+v", dl)
	}
}

// ensure unused imports stay referenced if the file is trimmed later.
var _ = errors.New
var _ = http.MethodPost
