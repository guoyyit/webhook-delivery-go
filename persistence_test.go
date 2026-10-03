package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitUntil polls until fn true or timeout.
func waitUntil(timeout time.Duration, fn func() bool) bool {
	return eventuallyTrue(timeout, fn)
}

// TestRestartResumesPending: make the endpoint fail first attempts, shut down
// while a delivery is pending a retry, then restart with the same store file
// and a healthy endpoint; the delivery must resume and succeed exactly once.
func TestRestartResumesPending(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")

	var (
		mu       sync.Mutex
		failLeft = 1
		received []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		received = append(received, r.Header.Get(HeaderDeliveryID))
		if failLeft > 0 {
			failLeft--
			w.WriteHeader(503)
			return
		}
		_, _ = w.Write(b)
	}))
	defer srv.Close()

	secret := []byte("restart-secret")
	cfg := testCfg(RealClock, NewHTTPSender(), path)
	cfg.Backoff = BackoffConfig{Base: 200 * time.Millisecond, Max: time.Second, Multiplier: 2, Jitter: 0}
	cfg.HTTPTimeout = 2 * time.Second

	// First life: publish; the first attempt fails and the delivery is parked
	// for retry (or in flight) when we shut down cleanly.
	d1, err := NewDispatcher(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := d1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	must(t, d1.AddEndpoint(Endpoint{ID: "ep", URL: srv.URL, Secret: secret}))
	must(t, d1.PutSubscription(Subscription{ID: "s", EndpointID: "ep", EventTypes: []string{"t"}}))
	id := DeliveryID("ev1", "ep")
	if _, err := d1.Publish(Event{ID: "ev1", Type: "t", Payload: []byte(`{"hello":"world"}`)}); err != nil {
		t.Fatal(err)
	}
	// Wait until the failed first attempt was recorded.
	if !waitUntil(2*time.Second, func() bool {
		dl, ok := d1.Delivery(id)
		return ok && len(dl.Attempts) == 1
	}) {
		t.Fatal("first attempt not recorded before shutdown")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := d1.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	cancel()

	// The store file must exist and be valid JSON (atomic persistence).
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("store file missing: %v", err)
	}
	raw, _ := os.ReadFile(path)
	if !bytes.Contains(raw, []byte(id)) {
		t.Fatal("persisted file does not contain the delivery id")
	}

	// Second life: restart with the same file; backoff windows pass, endpoint
	// is now healthy, delivery resumes to success.
	d2, err := NewDispatcher(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := d2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	must(t, d2.AddEndpoint(Endpoint{ID: "ep", URL: srv.URL, Secret: secret}))
	if err := d2.PutSubscription(Subscription{ID: "s", EndpointID: "ep", EventTypes: []string{"t"}}); err != nil {
		t.Fatal(err)
	}
	dl := waitForStatus(t, d2, id, StatusSucceeded, 3*time.Second)
	if dl.SucceededAt == nil {
		t.Fatal("succeeded timestamp missing")
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	_ = d2.Shutdown(ctx2)

	mu.Lock()
	if len(received) != 2 {
		t.Fatalf("want exactly 2 HTTP deliveries (1 fail + 1 success), got %d: %v", len(received), received)
	}
	if received[0] != id || received[1] != id {
		t.Fatalf("delivery id changed across restart: %v", received)
	}
	mu.Unlock()
}

// TestConfirmedNotRedeliveredAfterRestart: a succeeded delivery must not be
// sent again when the process restarts.
func TestConfirmedNotRedeliveredAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	cfg := testCfg(RealClock, NewHTTPSender(), path)
	for i := 0; i < 2; i++ {
		d, _ := NewDispatcher(cfg)
		if err := d.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		must(t, d.AddEndpoint(Endpoint{ID: "ep", URL: srv.URL, Secret: []byte("k")}))
		must(t, d.PutSubscription(Subscription{ID: "s", EndpointID: "ep", EventTypes: []string{"t"}}))
		if i == 0 {
			if _, err := d.Publish(Event{ID: "once", Type: "t", Payload: nil}); err != nil {
				t.Fatal(err)
			}
			waitForStatus(t, d, DeliveryID("once", "ep"), StatusSucceeded, 2*time.Second)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = d.Shutdown(ctx)
		cancel()
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("succeeded delivery was sent %d times across restart", got)
	}
}

// TestCorruptStoreRejected: a half-written/garbage file must fail loudly
// instead of silently starting from a blank slate (no state clobbering).
func TestCorruptStoreRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testCfg(RealClock, NewHTTPSender(), path)
	if _, err := NewDispatcher(cfg); err == nil {
		t.Fatal("corrupt store should produce an error")
	}
}

// TestReplayBoundaryAndDedup: replay only works on retained dead letters;
// truncated ones return ErrNotFound.
func TestReplayBoundaryAndDedup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")
	st, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	mk := func(id string, seq uint64, status Status) *Delivery {
		return &Delivery{ID: id, EventID: "e" + id, Seq: seq, EndpointID: "ep",
			SubscriptionID: "s", Status: status, CreatedAt: now, Payload: []byte("x")}
	}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("d%d", i)
		if err := st.DeadLetter(mk(id, uint64(i), StatusPending), ReasonMaxAttempts); err != nil {
			t.Fatal(err)
		}
	}
	if len(st.DeadLetters()) != 5 {
		t.Fatal("want 5 dead letters")
	}
	st.TruncateHistory(2)
	if _, err := st.ReplayDeadLetter("d0"); err != ErrNotFound {
		t.Fatalf("truncate boundary: got %v want ErrNotFound", err)
	}
	rd, err := st.ReplayDeadLetter("d4")
	if err != nil {
		t.Fatalf("replay retained: %v", err)
	}
	if rd.Status != StatusPending || rd.ID != "d4" {
		t.Fatalf("replay state wrong: %+v", rd)
	}
	// Replaying a non-dead delivery is rejected.
	if _, err := st.ReplayDeadLetter("d4"); err != ErrNotDeadLetter {
		t.Fatalf("double replay: got %v want ErrNotDeadLetter", err)
	}
}

// TestEndToEndSignedLoopback exercises the real HTTP client against a local
// receiver that verifies the signature and enforces timestamp freshness.
func TestEndToEndSignedLoopback(t *testing.T) {
	secret := []byte("loopback-secret")
	signer := NewSigner()

	var got atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		err := signer.Verify(secret, body,
			r.Header.Get(HeaderTimestamp), r.Header.Get(HeaderSignature),
			func() int64 { return time.Now().Unix() },
			int64((5 * time.Minute).Seconds()))
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var pl map[string]any
		if err := json.Unmarshal(body, &pl); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		got.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	d := mustStart(t, testCfg(RealClock, NewHTTPSender(), ""))
	must(t, d.AddEndpoint(Endpoint{ID: "ep", URL: srv.URL, Secret: secret, AuthToken: "tok-xyz"}))
	must(t, d.PutSubscription(Subscription{ID: "s", EndpointID: "ep", EventTypes: []string{"t"}}))
	if _, err := d.Publish(Event{ID: "ev", Type: "t", Payload: []byte(`{"v":1}`)}); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, d, DeliveryID("ev", "ep"), StatusSucceeded, 2*time.Second)
	if got.Load() != 1 {
		t.Fatal("receiver did not accept the signed delivery")
	}
}

func TestRedaction(t *testing.T) {
	if Redact([]byte("")) != "<empty>" {
		t.Fatal("empty secret redaction")
	}
	if Redact([]byte("ab")) != "****" {
		t.Fatal("short secret should be fully masked")
	}
	if Redact([]byte("supersecret")) != "****cret" {
		t.Fatalf("long secret redaction got %s", Redact([]byte("supersecret")))
	}
}
