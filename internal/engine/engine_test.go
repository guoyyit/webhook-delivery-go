package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/webhook-delivery-go/internal/breaker"
	"github.com/guoyyit/webhook-delivery-go/internal/clock"
	"github.com/guoyyit/webhook-delivery-go/internal/model"
	"github.com/guoyyit/webhook-delivery-go/internal/retry"
	"github.com/guoyyit/webhook-delivery-go/internal/sign"
	"github.com/guoyyit/webhook-delivery-go/internal/store"
)

// --- test harness ---

type testEnv struct {
	t   *testing.T
	eng *Engine
	fc  *clock.FakeClock
}

func newEnv(t *testing.T, fc *clock.FakeClock, cfgFn func(*Config)) *testEnv {
	t.Helper()
	cfg := Config{
		StoreDir:       t.TempDir(),
		RequestTimeout: 5 * time.Second,
		Retry:          retry.DefaultPolicy(),
		Breaker:        breaker.DefaultConfig(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if fc != nil {
		cfg.Clock = fc
		old := store.Now
		store.Now = fc.Now
		t.Cleanup(func() { store.Now = old })
	}
	if cfgFn != nil {
		cfgFn(&cfg)
	}
	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	return &testEnv{t: t, eng: eng, fc: fc}
}

func (e *testEnv) subscribe(t *testing.T, id, url string, types ...string) *model.Subscription {
	t.Helper()
	sub := &model.Subscription{ID: id, URL: url, EventTypes: types, Secret: "whsec_" + id}
	if err := e.eng.Subscribe(context.Background(), sub); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	return sub
}

func (e *testEnv) ingest(t *testing.T, id, typ string, seq int) *model.Event {
	t.Helper()
	ev := &model.Event{ID: id, Type: typ, Payload: []byte(fmt.Sprintf(`{"seq":%d}`, seq))}
	if _, _, err := e.eng.Ingest(context.Background(), ev); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return ev
}

func waitDelivery(t *testing.T, eng *Engine, id string, timeout time.Duration,
	pred func(*model.Delivery) bool) *model.Delivery {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if d, ok := eng.GetDelivery(id); ok && pred(d) {
			return d
		}
		time.Sleep(5 * time.Millisecond)
	}
	d, _ := eng.GetDelivery(id)
	t.Fatalf("timeout waiting for delivery %s (last state: %+v)", id, d)
	return nil
}

func waitNoDelivery(t *testing.T, eng *Engine, id string, dur time.Duration) {
	t.Helper()
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		if d, ok := eng.GetDelivery(id); ok && d.Terminal() {
			t.Fatalf("delivery %s reached terminal state unexpectedly: %s", id, d.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// mockEndpoint records requests and returns configurable statuses.
type mockEndpoint struct {
	mu       sync.Mutex
	received []receivedRequest
	handler  func(body []byte, hdr http.Header) int
}

type receivedRequest struct {
	body       string
	eventID    string
	deliveryID string
}

func (m *mockEndpoint) requests() []receivedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]receivedRequest, len(m.received))
	copy(out, m.received)
	return out
}

func (m *mockEndpoint) serve() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.received = append(m.received, receivedRequest{
			body:       string(body),
			eventID:    r.Header.Get(sign.HeaderEventID),
			deliveryID: r.Header.Get(sign.HeaderDeliveryID),
		})
		m.mu.Unlock()
		status := http.StatusOK
		if m.handler != nil {
			status = m.handler(body, r.Header)
		}
		w.WriteHeader(status)
	}))
}

// --- tests ---

func TestFanoutAndTypeFiltering(t *testing.T) {
	env := newEnv(t, nil, nil)

	a1 := &mockEndpoint{}
	a2 := &mockEndpoint{}
	b := &mockEndpoint{}
	sa := a1.serve()
	sb := a2.serve()
	sc := b.serve()
	defer sa.Close()
	defer sb.Close()
	defer sc.Close()

	env.subscribe(t, "sub-a1", sa.URL, "order.created")
	env.subscribe(t, "sub-a2", sb.URL, "order.created")
	env.subscribe(t, "sub-b", sc.URL, "order.cancelled")

	for i := 0; i < 3; i++ {
		env.ingest(t, fmt.Sprintf("evt-a-%d", i), "order.created", i)
	}
	env.ingest(t, "evt-b-0", "order.cancelled", 0)

	for i := 0; i < 3; i++ {
		waitDelivery(t, env.eng, store.DeliveryID(fmt.Sprintf("evt-a-%d", i), "sub-a1"), 2*time.Second,
			func(d *model.Delivery) bool { return d.Status == model.StatusSucceeded })
		waitDelivery(t, env.eng, store.DeliveryID(fmt.Sprintf("evt-a-%d", i), "sub-a2"), 2*time.Second,
			func(d *model.Delivery) bool { return d.Status == model.StatusSucceeded })
	}
	if len(a1.requests()) != 3 {
		t.Fatalf("a1 got %d requests, want 3", len(a1.requests()))
	}
	if len(a2.requests()) != 3 {
		t.Fatalf("a2 got %d requests, want 3", len(a2.requests()))
	}
	waitDelivery(t, env.eng, store.DeliveryID("evt-b-0", "sub-b"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusSucceeded })
	if len(b.requests()) != 1 {
		t.Fatalf("b got %d requests, want 1 (only its subscribed type)", len(b.requests()))
	}
}

func TestPerEndpointOrderingAndBackpressure(t *testing.T) {
	env := newEnv(t, nil, func(c *Config) {
		c.RequestTimeout = 5 * time.Second
	})

	fast := &mockEndpoint{}
	slow := &mockEndpoint{handler: func(body []byte, hdr http.Header) int {
		time.Sleep(300 * time.Millisecond)
		return http.StatusOK
	}}
	fs := fast.serve()
	ss := slow.serve()
	defer fs.Close()
	defer ss.Close()

	env.subscribe(t, "fast", fs.URL, "x")
	env.subscribe(t, "slow", ss.URL, "x")

	const n = 10
	start := time.Now()
	for i := 0; i < n; i++ {
		env.ingest(t, fmt.Sprintf("evt-%02d", i), "x", i)
	}
	ingestElapsed := time.Since(start)
	if ingestElapsed > 2*time.Second {
		t.Fatalf("ingestion blocked on slow endpoints: took %s", ingestElapsed)
	}

	// Fast endpoint must receive everything even while the slow one is backed up.
	waitFor(t, 2*time.Second, func() bool { return len(fast.requests()) == n })
	// Slow endpoint still receives everything, in production order.
	waitFor(t, 8*time.Second, func() bool { return len(slow.requests()) == n })
	got := slow.requests()
	for i := 0; i < n; i++ {
		want := fmt.Sprintf(`{"seq":%d}`, i)
		if got[i].body != want {
			t.Fatalf("slow endpoint order broken at %d: got %s want %s", i, got[i].body, want)
		}
	}
}

func TestRetryWithBackoff(t *testing.T) {
	fc := clock.NewFakeClock(time.Unix(1000, 0))
	env := newEnv(t, fc, func(c *Config) {
		c.Retry = retry.Policy{
			InitialInterval: time.Second,
			MaxInterval:     time.Minute,
			Multiplier:      2,
			MaxRetries:      5,
			MaxElapsedTime:  0,
			Jitter:          0,
		}
	})

	var attemptsMu sync.Mutex
	attempts := 0
	ep := &mockEndpoint{handler: func(body []byte, hdr http.Header) int {
		attemptsMu.Lock()
		attempts++
		n := attempts
		attemptsMu.Unlock()
		if n < 3 {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	}}
	srv := ep.serve()
	defer srv.Close()
	env.subscribe(t, "sub-retry", srv.URL, "x")
	env.ingest(t, "evt-retry", "x", 0)

	delID := store.DeliveryID("evt-retry", "sub-retry")
	waitDelivery(t, env.eng, delID, 2*time.Second, func(d *model.Delivery) bool {
		return d.Status == model.StatusPending && d.Attempts == 1 && d.NextRetryAt != nil
	})
	fc.Advance(2 * time.Second)
	waitDelivery(t, env.eng, delID, 2*time.Second, func(d *model.Delivery) bool {
		return d.Status == model.StatusPending && d.Attempts == 2 && d.NextRetryAt != nil
	})
	fc.Advance(3 * time.Second)
	d := waitDelivery(t, env.eng, delID, 2*time.Second, func(d *model.Delivery) bool {
		return d.Status == model.StatusSucceeded
	})
	if d.Attempts != 2 {
		t.Fatalf("expected 2 failed attempts before success, got %d", d.Attempts)
	}
	if len(ep.requests()) != 3 {
		t.Fatalf("endpoint saw %d requests, want 3", len(ep.requests()))
	}
}

func TestNonRetryableGoesTerminal(t *testing.T) {
	env := newEnv(t, nil, nil)
	ep := &mockEndpoint{handler: func(body []byte, hdr http.Header) int {
		return http.StatusBadRequest
	}}
	srv := ep.serve()
	defer srv.Close()
	env.subscribe(t, "sub-400", srv.URL, "x")
	env.ingest(t, "evt-400", "x", 0)

	d := waitDelivery(t, env.eng, store.DeliveryID("evt-400", "sub-400"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusFailed })
	if d.Attempts != 1 {
		t.Fatalf("non-retryable failure should attempt exactly once, got %d", d.Attempts)
	}
	if len(ep.requests()) != 1 {
		t.Fatalf("endpoint saw %d requests, want 1", len(ep.requests()))
	}
}

func TestMaxElapsedTimeDeadLetters(t *testing.T) {
	fc := clock.NewFakeClock(time.Unix(1000, 0))
	env := newEnv(t, fc, func(c *Config) {
		c.Retry = retry.Policy{
			InitialInterval: 10 * time.Second,
			MaxInterval:     10 * time.Second,
			Multiplier:      2,
			MaxRetries:      10,
			MaxElapsedTime:  5 * time.Second,
			Jitter:          0,
		}
	})
	ep := &mockEndpoint{handler: func(body []byte, hdr http.Header) int {
		return http.StatusServiceUnavailable
	}}
	srv := ep.serve()
	defer srv.Close()
	env.subscribe(t, "sub-elapsed", srv.URL, "x")
	env.ingest(t, "evt-elapsed", "x", 0)

	delID := store.DeliveryID("evt-elapsed", "sub-elapsed")
	waitDelivery(t, env.eng, delID, 2*time.Second, func(d *model.Delivery) bool {
		return d.Status == model.StatusPending && d.Attempts == 1
	})
	fc.Advance(11 * time.Second)
	d := waitDelivery(t, env.eng, delID, 2*time.Second, func(d *model.Delivery) bool {
		return d.Status == model.StatusDeadLettered
	})
	if d.DeadLetterReason != DeadLetterMaxElapsed {
		t.Fatalf("expected max_elapsed reason, got %q", d.DeadLetterReason)
	}
}

func TestCircuitBreakerOpensAndProbeRecovers(t *testing.T) {
	fc := clock.NewFakeClock(time.Unix(1000, 0))
	env := newEnv(t, fc, func(c *Config) {
		c.Retry = retry.Policy{
			InitialInterval: time.Second,
			MaxInterval:     time.Second,
			Multiplier:      2,
			MaxRetries:      0, // fail fast: one attempt per event
			MaxElapsedTime:  0,
			Jitter:          0,
		}
		c.Breaker = breaker.Config{FailureThreshold: 2, OpenInterval: time.Minute, Clock: fc}
	})

	var mu sync.Mutex
	n := 0
	ep := &mockEndpoint{handler: func(body []byte, hdr http.Header) int {
		mu.Lock()
		n++
		calls := n
		mu.Unlock()
		if calls <= 2 {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	}}
	srv := ep.serve()
	defer srv.Close()
	env.subscribe(t, "sub-cb", srv.URL, "x")

	env.ingest(t, "evt-cb-1", "x", 0)
	waitDelivery(t, env.eng, store.DeliveryID("evt-cb-1", "sub-cb"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusDeadLettered })

	env.ingest(t, "evt-cb-2", "x", 1)
	waitDelivery(t, env.eng, store.DeliveryID("evt-cb-2", "sub-cb"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusDeadLettered })

	// Breaker is now open; this event must be dead-lettered without calling the endpoint.
	env.ingest(t, "evt-cb-3", "x", 2)
	d3 := waitDelivery(t, env.eng, store.DeliveryID("evt-cb-3", "sub-cb"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusDeadLettered })
	if d3.DeadLetterReason != DeadLetterCircuitOpen {
		t.Fatalf("expected circuit_open, got %q", d3.DeadLetterReason)
	}

	// Advance past the open interval: a probe succeeds and the breaker closes.
	fc.Advance(61 * time.Second)
	env.ingest(t, "evt-cb-4", "x", 3)
	waitDelivery(t, env.eng, store.DeliveryID("evt-cb-4", "sub-cb"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusSucceeded })

	if len(ep.requests()) != 3 {
		t.Fatalf("endpoint saw %d requests (want 3: evt1, evt2, probe evt4)", len(ep.requests()))
	}
}

func TestReplayBoundaries(t *testing.T) {
	fc := clock.NewFakeClock(time.Unix(1000, 0))
	env := newEnv(t, fc, func(c *Config) {
		c.Retry = retry.Policy{MaxRetries: 0, Jitter: 0, InitialInterval: time.Second, MaxInterval: time.Second}
		c.Breaker = breaker.Config{FailureThreshold: 100, OpenInterval: time.Hour, Clock: fc}
	})

	var healthy bool
	ep := &mockEndpoint{handler: func(body []byte, hdr http.Header) int {
		if healthy {
			return http.StatusOK
		}
		return http.StatusServiceUnavailable
	}}
	srv := ep.serve()
	defer srv.Close()
	env.subscribe(t, "sub-rp", srv.URL, "x")

	base := fc.Now()
	env.ingest(t, "evt-rp-1", "x", 0) // t=1000
	fc.Advance(time.Hour)
	env.ingest(t, "evt-rp-2", "x", 1) // t=1000+1h
	fc.Advance(time.Hour)
	env.ingest(t, "evt-rp-3", "x", 2) // t=1000+2h

	for _, id := range []string{"evt-rp-1", "evt-rp-2", "evt-rp-3"} {
		waitDelivery(t, env.eng, store.DeliveryID(id, "sub-rp"), 2*time.Second,
			func(d *model.Delivery) bool { return d.Status == model.StatusDeadLettered })
	}

	healthy = true
	from := base.Add(30 * time.Minute)
	to := base.Add(90 * time.Minute)
	n, err := env.eng.Replay(context.Background(), from, to)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("replayed %d deliveries, want 1 (only evt-rp-2 in [+30m,+90m))", n)
	}
	waitDelivery(t, env.eng, store.DeliveryID("evt-rp-2", "sub-rp"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusSucceeded })
	d1, _ := env.eng.GetDelivery(store.DeliveryID("evt-rp-1", "sub-rp"))
	d3, _ := env.eng.GetDelivery(store.DeliveryID("evt-rp-3", "sub-rp"))
	if d1.Status != model.StatusDeadLettered || d3.Status != model.StatusDeadLettered {
		t.Fatal("replay must not touch events outside the [from,to) boundary")
	}
}

func TestIngestIdempotentNoDuplicateDelivery(t *testing.T) {
	env := newEnv(t, nil, nil)
	ep := &mockEndpoint{}
	srv := ep.serve()
	defer srv.Close()
	env.subscribe(t, "sub-idem", srv.URL, "x")

	env.ingest(t, "evt-once", "x", 0)
	env.ingest(t, "evt-once", "x", 0) // producer retries with same event id

	waitDelivery(t, env.eng, store.DeliveryID("evt-once", "sub-idem"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusSucceeded })
	time.Sleep(100 * time.Millisecond)
	if len(ep.requests()) != 1 {
		t.Fatalf("endpoint saw %d requests for one event, want 1", len(ep.requests()))
	}
}

func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	fc := clock.NewFakeClock(time.Unix(1000, 0))
	store.Now = fc.Now
	t.Cleanup(func() { store.Now = time.Now })

	newEngine := func() *Engine {
		cfg := Config{
			StoreDir:       dir,
			RequestTimeout: 5 * time.Second,
			Retry: retry.Policy{
				InitialInterval: time.Hour,
				MaxInterval:     time.Hour,
				Multiplier:      1,
				MaxRetries:      10,
				MaxElapsedTime:  0,
				Jitter:          0,
			},
			Breaker: breaker.Config{FailureThreshold: 5, OpenInterval: time.Hour, Clock: fc},
			Clock:   fc,
			Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		eng, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := eng.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return eng
	}

	// First engine: evt-1 fails once and waits out its (1h) backoff on
	// endpoint A; evt-2 succeeds immediately on a separate endpoint B.
	var muA sync.Mutex
	callsA := 0
	epA := &mockEndpoint{handler: func(body []byte, hdr http.Header) int {
		muA.Lock()
		callsA++
		n := callsA
		muA.Unlock()
		if n == 1 {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	}}
	srvA := epA.serve()
	defer srvA.Close()
	epB := &mockEndpoint{}
	srvB := epB.serve()
	defer srvB.Close()

	eng1 := newEngine()
	subA := &model.Subscription{ID: "sub-restart-a", URL: srvA.URL, EventTypes: []string{"x"}, Secret: "sec"}
	if err := eng1.Subscribe(context.Background(), subA); err != nil {
		t.Fatal(err)
	}
	ev1 := &model.Event{ID: "evt-restart-1", Type: "x", Payload: []byte(`{"seq":1}`)}
	if _, _, err := eng1.Ingest(context.Background(), ev1); err != nil {
		t.Fatal(err)
	}
	waitDelivery(t, eng1, store.DeliveryID("evt-restart-1", "sub-restart-a"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusPending && d.Attempts == 1 })

	// Endpoint B subscribes after evt-1, so it must only ever see evt-2.
	fc.Advance(time.Second)
	subB := &model.Subscription{ID: "sub-restart-b", URL: srvB.URL, EventTypes: []string{"x"}, Secret: "sec"}
	if err := eng1.Subscribe(context.Background(), subB); err != nil {
		t.Fatal(err)
	}
	ev2 := &model.Event{ID: "evt-restart-2", Type: "x", Payload: []byte(`{"seq":2}`)}
	if _, _, err := eng1.Ingest(context.Background(), ev2); err != nil {
		t.Fatal(err)
	}
	waitDelivery(t, eng1, store.DeliveryID("evt-restart-2", "sub-restart-b"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusSucceeded })
	if err := eng1.Close(); err != nil {
		t.Fatal(err)
	}

	// Second engine on the same store: the pending delivery must wait out its
	// backoff and then resume; the already-succeeded delivery is not re-delivered.
	eng2 := newEngine()
	defer eng2.Close()
	waitDelivery(t, eng2, store.DeliveryID("evt-restart-1", "sub-restart-a"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusPending })
	fc.Advance(2 * time.Hour)
	d1 := waitDelivery(t, eng2, store.DeliveryID("evt-restart-1", "sub-restart-a"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusSucceeded })
	if d1.Attempts != 1 {
		t.Fatalf("resumed delivery should retry once after restart, got %d failures", d1.Attempts)
	}
	if len(epA.requests()) != 2 {
		t.Fatalf("endpoint A saw %d requests (want 2: fail, retry)", len(epA.requests()))
	}
	if len(epB.requests()) != 1 {
		t.Fatalf("endpoint B saw %d requests (want 1, no duplicate after restart)", len(epB.requests()))
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	env := newEnv(t, nil, nil)
	ep := &mockEndpoint{}
	srv := ep.serve()
	defer srv.Close()
	env.subscribe(t, "sub-unsub", srv.URL, "x")

	env.ingest(t, "evt-unsub-1", "x", 0)
	waitDelivery(t, env.eng, store.DeliveryID("evt-unsub-1", "sub-unsub"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusSucceeded })

	if err := env.eng.Unsubscribe(context.Background(), "sub-unsub"); err != nil {
		t.Fatal(err)
	}
	env.ingest(t, "evt-unsub-2", "x", 1)
	time.Sleep(200 * time.Millisecond)
	if len(ep.requests()) != 1 {
		t.Fatalf("unsubscribed endpoint got %d requests, want 1", len(ep.requests()))
	}
	// No delivery record should exist for the post-unsubscribe event.
	if _, ok := env.eng.GetDelivery(store.DeliveryID("evt-unsub-2", "sub-unsub")); ok {
		t.Fatal("delivery created after unsubscribe")
	}
}

func TestSignatureHeadersVerifiedByEndpoint(t *testing.T) {
	env := newEnv(t, nil, nil)
	secret := "whsec_sub-sign"
	var verified bool
	ep := &mockEndpoint{handler: func(body []byte, hdr http.Header) int {
		ts, err := sign.ParseTimestamp(hdr.Get(sign.HeaderTimestamp))
		if err != nil {
			t.Errorf("bad timestamp header: %v", err)
			return http.StatusBadRequest
		}
		if err := sign.Verify(secret, ts, body, hdr.Get(sign.HeaderSignature),
			5*time.Minute, time.Now()); err != nil {
			t.Errorf("signature verification failed: %v", err)
			return http.StatusUnauthorized
		}
		if hdr.Get(sign.HeaderEventID) == "" || hdr.Get(sign.HeaderDeliveryID) == "" ||
			hdr.Get(sign.HeaderIdempotency) == "" {
			t.Errorf("missing identity headers")
			return http.StatusBadRequest
		}
		if hdr.Get("Content-Type") != "application/json" {
			t.Errorf("bad content type: %q", hdr.Get("Content-Type"))
			return http.StatusBadRequest
		}
		verified = true
		return http.StatusOK
	}}
	srv := ep.serve()
	defer srv.Close()
	env.subscribe(t, "sub-sign", srv.URL, "x")
	env.ingest(t, "evt-sign", "x", 0)
	waitDelivery(t, env.eng, store.DeliveryID("evt-sign", "sub-sign"), 2*time.Second,
		func(d *model.Delivery) bool { return d.Status == model.StatusSucceeded })
	if !verified {
		t.Fatal("endpoint did not verify a valid signature")
	}
}

// waitFor polls pred until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
