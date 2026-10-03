package webhook

import (
	"errors"
	"testing"
	"time"
)

// TestTroubleshootingTrace: for one event you must be able to find every
// per-endpoint delivery, its current step, per-attempt HTTP status/error and
// the terminal dead-letter reason — i.e. locate which event is stuck where.
func TestTroubleshootingTrace(t *testing.T) {
	sender := newFakeSender()
	d := mustStart(t, testCfg(RealClock, sender, ""))
	const goodURL, badURL = "http://good", "http://bad"
	must(t, d.AddEndpoint(Endpoint{ID: "good", URL: goodURL, Secret: []byte("k")}))
	must(t, d.AddEndpoint(Endpoint{ID: "bad", URL: badURL, Secret: []byte("k")}))
	must(t, d.PutSubscription(Subscription{ID: "g", EndpointID: "good", EventTypes: []string{"t"}}))
	must(t, d.PutSubscription(Subscription{ID: "b", EndpointID: "bad", EventTypes: []string{"t"}}))

	sender.setScript(badURL, func(int) (fakeResp, error) {
		return fakeResp{}, errors.New("dial tcp: connection refused")
	})

	_, _ = d.Publish(Event{ID: "ev-trace", Type: "t", Payload: []byte("{}")})
	waitForStatus(t, d, DeliveryID("ev-trace", "good"), StatusSucceeded, 2*time.Second)
	waitForStatus(t, d, DeliveryID("ev-trace", "bad"), StatusDead, 2*time.Second)

	ds := d.DeliveryByEvent("ev-trace")
	if len(ds) != 2 {
		t.Fatalf("want 2 deliveries for the event, got %d", len(ds))
	}
	var bad *Delivery
	for _, dl := range ds {
		if dl.EndpointID == "bad" {
			bad = dl
		}
	}
	if bad == nil {
		t.Fatal("missing failing-endpoint delivery")
	}
	if bad.DeadReason != ReasonMaxAttempts {
		t.Fatalf("reason=%s", bad.DeadReason)
	}
	if len(bad.Attempts) == 0 || bad.Attempts[0].Error == "" {
		t.Fatalf("attempt trace must record the transport error: %+v", bad.Attempts)
	}
	if !bad.Attempts[0].Retryable {
		t.Fatal("connection error must be flagged retryable")
	}
	for _, a := range bad.Attempts {
		if a.StartedAt.IsZero() || a.FinishedAt.IsZero() || !a.FinishedAt.After(a.StartedAt) {
			t.Fatalf("attempt timing not recorded: %+v", a)
		}
	}
	if dls := d.DeadLetters(); len(dls) != 1 || dls[0].EventID != "ev-trace" {
		t.Fatalf("dead-letter query must locate the event: %+v", dls)
	}
}
