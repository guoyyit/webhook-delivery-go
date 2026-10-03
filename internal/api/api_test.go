package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/webhook-delivery-go/internal/breaker"
	"github.com/guoyyit/webhook-delivery-go/internal/engine"
	"github.com/guoyyit/webhook-delivery-go/internal/retry"
)

func newTestServer(t *testing.T) (*httptest.Server, *engine.Engine) {
	t.Helper()
	eng, err := engine.New(engine.Config{
		StoreDir:       t.TempDir(),
		RequestTimeout: 2 * time.Second,
		Retry:          retry.DefaultPolicy(),
		Breaker:        breaker.DefaultConfig(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	return httptest.NewServer(New(eng).Routes()), eng
}

func request(t *testing.T, method, url string, body interface{}) (int, map[string]interface{}) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]interface{}
	data, _ := io.ReadAll(resp.Body)
	if len(data) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatalf("bad json %q: %v", string(data), err)
		}
	}
	return resp.StatusCode, out
}

func TestHealthz(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()
	status, body := request(t, http.MethodGet, srv.URL+"/healthz", nil)
	if status != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("healthz failed: %d %v", status, body)
	}
}

func TestSubscriptionLifecycle(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()

	status, body := request(t, http.MethodPost, srv.URL+"/v1/subscriptions", map[string]interface{}{
		"url":         "https://e.example/hook",
		"event_types": []string{"order.created"},
	})
	if status != http.StatusCreated {
		t.Fatalf("create: %d %v", status, body)
	}
	sub := body
	if sub["id"] == "" || sub["secret"] == "" {
		t.Fatal("id/secret not generated")
	}
	id := sub["id"].(string)

	status, body = request(t, http.MethodGet, srv.URL+"/v1/subscriptions", nil)
	if status != http.StatusOK || len(body["subscriptions"].([]interface{})) != 1 {
		t.Fatalf("list: %d %v", status, body)
	}

	status, _ = request(t, http.MethodDelete, srv.URL+"/v1/subscriptions/"+id, nil)
	if status != http.StatusNoContent {
		t.Fatalf("delete: %d", status)
	}
}

func TestValidationErrors(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()

	status, _ := request(t, http.MethodPost, srv.URL+"/v1/subscriptions", map[string]interface{}{
		"url": "not-a-url",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad url, got %d", status)
	}
	status, _ = request(t, http.MethodPost, srv.URL+"/v1/events", map[string]interface{}{
		"payload": map[string]interface{}{"x": 1},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing type, got %d", status)
	}
}

func TestEndToEndDelivery(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()

	var mu sync.Mutex
	got := 0
	ep := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ep.Close()

	_, _ = request(t, http.MethodPost, srv.URL+"/v1/subscriptions", map[string]interface{}{
		"url":         ep.URL,
		"event_types": []string{"order.created"},
	})
	status, body := request(t, http.MethodPost, srv.URL+"/v1/events", map[string]interface{}{
		"id":      "evt-e2e-1",
		"type":    "order.created",
		"payload": map[string]interface{}{"order_id": 42},
	})
	if status != http.StatusCreated {
		t.Fatalf("ingest: %d %v", status, body)
	}
	if len(body["deliveries"].([]interface{})) != 1 {
		t.Fatalf("expected 1 delivery: %v", body)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := got
		mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	if got != 1 {
		t.Fatalf("endpoint received %d deliveries, want 1", got)
	}
	mu.Unlock()

	status, body = request(t, http.MethodGet, srv.URL+"/v1/deliveries?event_id=evt-e2e-1", nil)
	if status != http.StatusOK {
		t.Fatalf("list deliveries: %d", status)
	}
	if len(body["deliveries"].([]interface{})) != 1 {
		t.Fatalf("expected 1 delivery record: %v", body)
	}
}

func TestReplayEndpoint(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()

	request(t, http.MethodPost, srv.URL+"/v1/subscriptions", map[string]interface{}{
		"url":         "https://e.example/hook",
		"event_types": []string{"x"},
	})
	request(t, http.MethodPost, srv.URL+"/v1/events", map[string]interface{}{
		"id": "evt-r", "type": "x", "payload": map[string]interface{}{},
	})
	// Replay with an inverted range is rejected.
	status, _ := request(t, http.MethodPost, srv.URL+"/v1/replay", map[string]interface{}{
		"from": "2026-01-02T00:00:00Z",
		"to":   "2026-01-01T00:00:00Z",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for inverted range, got %d", status)
	}
}
