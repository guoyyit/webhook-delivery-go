package webhook

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// TestSecretsNeverLogged: force retry+deadletter paths while logging into a
// buffer; the endpoint secret and bearer token must never appear.
func TestSecretsNeverLogged(t *testing.T) {
	var buf bytes.Buffer
	sender := newFakeSender()
	cfg := testCfg(RealClock, sender, "")
	cfg.Log = NewLoggerTo(&buf)
	cfg.MaxAttempts = 1
	d := mustStart(t, cfg)
	const url = "http://secret-ep"
	must(t, d.AddEndpoint(Endpoint{ID: "s", URL: url, Secret: []byte("supersecret-key"), AuthToken: "tok-abcdef123456"}))
	must(t, d.PutSubscription(Subscription{ID: "ss", EndpointID: "s", EventTypes: []string{"t"}}))
	sender.setScript(url, func(int) (fakeResp, error) { return fakeResp{status: 500}, nil })

	_, _ = d.Publish(Event{ID: "ev", Type: "t", Payload: nil})
	waitForStatus(t, d, DeliveryID("ev", "s"), StatusDead, 2*time.Second)

	out := buf.String()
	if strings.Contains(out, "supersecret-key") {
		t.Fatal("endpoint secret leaked into logs")
	}
	if strings.Contains(out, "tok-abcdef123456") {
		t.Fatal("auth token leaked into logs")
	}
}
