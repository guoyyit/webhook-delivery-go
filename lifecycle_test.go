package webhook

import (
	"context"
	"testing"
	"time"
)

// TestShutdownRollsBackInFlight: an attempt blocked when shutdown is canceled
// must be persisted back to pending (not left inflight, not marked failed), so
// a restart can resume it without ambiguity.
func TestShutdownRollsBackInFlight(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/store.json"
	sender := newFakeSender()
	cfg := testCfg(RealClock, sender, path)
	cfg.HTTPTimeout = time.Hour // don't let the per-attempt timeout interfere
	d, err := NewDispatcher(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	const url = "http://blocked"
	must(t, d.AddEndpoint(Endpoint{ID: "b", URL: url, Secret: []byte("k")}))
	must(t, d.PutSubscription(Subscription{ID: "bs", EndpointID: "b", EventTypes: []string{"t"}}))

	gate := make(chan struct{})
	sender.setGate(url, gate)

	id := DeliveryID("ev", "b")
	if _, err := d.Publish(Event{ID: "ev", Type: "t", Payload: nil}); err != nil {
		t.Fatal(err)
	}
	sender.waitFor(t, url, 1, 2*time.Second) // attempt held in gate
	if dl, _ := d.Delivery(id); dl.Status != StatusInFlight {
		t.Fatalf("want inflight, got %s", dl.Status)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := d.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	cancel()

	// Store on disk must show pending (resumable), never inflight.
	st2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	dl, ok := st2.Get(id)
	if !ok {
		t.Fatal("delivery missing from reopened store")
	}
	if dl.Status != StatusPending {
		t.Fatalf("want pending after shutdown rollback, got %s", dl.Status)
	}
	close(gate)
	_ = st2.Close()
}

// TestUnsubscribeStopsQueuedDelivery: an event queued behind another one must
// not be delivered once its subscription is removed; it is dead-lettered with
// an explicit reason rather than dropped.
func TestUnsubscribeStopsQueuedDelivery(t *testing.T) {
	sender := newFakeSender()
	d := mustStart(t, testCfg(RealClock, sender, ""))
	const url = "http://q"
	must(t, d.AddEndpoint(Endpoint{ID: "q", URL: url, Secret: []byte("k")}))
	must(t, d.PutSubscription(Subscription{ID: "qs", EndpointID: "q", EventTypes: []string{"t"}}))

	// Hold the first event so the second stays queued.
	gate := make(chan struct{})
	sender.setGate(url, gate)
	if _, err := d.Publish(Event{ID: "first", Type: "t", Payload: nil}); err != nil {
		t.Fatal(err)
	}
	sender.waitFor(t, url, 1, 2*time.Second)

	if _, err := d.Publish(Event{ID: "second", Type: "t", Payload: nil}); err != nil {
		t.Fatal(err)
	}
	// Unsubscribe while "second" is still queued.
	d.RemoveSubscription("qs")
	close(gate)

	// "second" must become dead with the unsubscribe reason and must never
	// have reached the endpoint.
	dl := waitForStatus(t, d, DeliveryID("second", "q"), StatusDead, 2*time.Second)
	if dl.DeadReason != ReasonUnsubscribed {
		t.Fatalf("want %s, got %s", ReasonUnsubscribed, dl.DeadReason)
	}
	for _, c := range sender.callsTo(url) {
		if c.req.Header.Get(HeaderEventID) == "second" {
			t.Fatal("unsubscribed event was delivered")
		}
	}
	// "first" was already in flight and accepted; it may finish normally.
	waitForStatus(t, d, DeliveryID("first", "q"), StatusSucceeded, 2*time.Second)
}

// TestReplayDeadLetterResendsWithSameID: an explicit replay requeues a dead
// delivery; the endpoint sees the same stable delivery id again.
func TestReplayDeadLetterResendsWithSameID(t *testing.T) {
	sender := newFakeSender()
	cfg := testCfg(RealClock, sender, "")
	d := mustStart(t, cfg)
	const url = "http://dl"
	must(t, d.AddEndpoint(Endpoint{ID: "dl", URL: url, Secret: []byte("k")}))
	must(t, d.PutSubscription(Subscription{ID: "dls", EndpointID: "dl", EventTypes: []string{"t"}}))

	sender.setScript(url, func(nth int) (fakeResp, error) {
		if nth == 1 {
			return fakeResp{status: 404}, nil
		}
		return fakeResp{status: 200}, nil
	})
	_, _ = d.Publish(Event{ID: "ev", Type: "t", Payload: nil})
	id := DeliveryID("ev", "dl")
	waitForStatus(t, d, id, StatusDead, time.Second)

	// A plain re-publish must NOT resurrect a terminal delivery.
	_, _ = d.Publish(Event{ID: "ev", Type: "t", Payload: nil})
	time.Sleep(30 * time.Millisecond)
	if n := len(sender.callsTo(url)); n != 1 {
		t.Fatalf("republish of a dead event must not resend, calls=%d", n)
	}

	// Explicit replay with a bounded history does resend with the same id.
	rd, err := Replay(d, id)
	if err != nil {
		t.Fatal(err)
	}
	if rd.ID != id {
		t.Fatal("replay must preserve the stable delivery id")
	}
	calls := sender.waitFor(t, url, 2, 2*time.Second)
	if calls[1].req.Header.Get(HeaderDeliveryID) != id {
		t.Fatal("replayed request must carry the same delivery id")
	}
	waitForStatus(t, d, id, StatusSucceeded, time.Second)
}
