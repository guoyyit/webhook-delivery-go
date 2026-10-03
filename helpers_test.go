package webhook

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

// fakeResp is a scripted response used by fakeSender.
type fakeResp struct {
	status int
	body   string
}

// fakeSender is a fully controllable Sender: responses (or errors) are scripted
// per endpoint, and received requests are recorded for assertions (bodies
// included; headers like signatures too).
type fakeSender struct {
	mu     sync.Mutex
	script map[string]func(nth int) (fakeResp, error) // keyed by endpoint URL
	calls  map[string][]*recordedRequest
	cond   *sync.Cond
	block  map[string]chan struct{} // optional per-URL gate: send blocks until closed
}

type recordedRequest struct {
	req  *http.Request
	body []byte
	nth  int
}

func newFakeSender() *fakeSender {
	f := &fakeSender{
		script: map[string]func(nth int) (fakeResp, error){},
		calls:  map[string][]*recordedRequest{},
		block:  map[string]chan struct{}{},
	}
	f.cond = sync.NewCond(&f.mu)
	return f
}

func (f *fakeSender) setScript(url string, fn func(nth int) (fakeResp, error)) {
	f.mu.Lock()
	f.script[url] = fn
	f.mu.Unlock()
}

func (f *fakeSender) setGate(url string, gate chan struct{}) {
	f.mu.Lock()
	f.block[url] = gate
	f.mu.Unlock()
}

func (f *fakeSender) Send(req *http.Request) (*http.Response, error) {
	url := req.URL.String()

	body, _ := io.ReadAll(req.Body)
	f.mu.Lock()
	n := len(f.calls[url]) + 1
	f.calls[url] = append(f.calls[url], &recordedRequest{req: cloneReq(req, body), body: append([]byte(nil), body...), nth: n})
	fn := f.script[url]
	gate := f.block[url]
	f.cond.Broadcast()
	f.mu.Unlock()

	// Gate (if any) is honored AFTER recording, so tests can observe an
	// attempt that is currently held in flight.
	if gate != nil {
		select {
		case <-gate:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}

	if fn == nil {
		return makeResp(200, "ok"), nil
	}
	r, err := fn(n)
	if err != nil {
		return nil, err
	}
	return makeResp(r.status, r.body), nil
}

func (f *fakeSender) getGate(url string) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.block[url]
}

// callsTo returns recorded requests for a url.
func (f *fakeSender) callsTo(url string) []*recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*recordedRequest, len(f.calls[url]))
	copy(out, f.calls[url])
	return out
}

func (f *fakeSender) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, v := range f.calls {
		n += len(v)
	}
	return n
}

// waitFor blocks until at least n calls have been made to url.
func (f *fakeSender) waitFor(t *testing.T, url string, n int, timeout time.Duration) []*recordedRequest {
	t.Helper()
	deadline := time.Now().Add(timeout)
	f.mu.Lock()
	for len(f.calls[url]) < n {
		if time.Now().After(deadline) {
			got := len(f.calls[url])
			f.mu.Unlock()
			t.Fatalf("timed out waiting for %d calls to %s, got %d", n, url, got)
		}
		f.cond.Wait()
	}
	out := make([]*recordedRequest, len(f.calls[url]))
	copy(out, f.calls[url])
	f.mu.Unlock()
	return out
}

func cloneReq(r *http.Request, body []byte) *http.Request {
	c := r.Clone(r.Context())
	c.Body = io.NopCloser(bytes.NewReader(body))
	return c
}

func makeResp(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     http.Header{},
	}
}

// testCfg builds a dispatcher config wired to the fake sender and clock.
func testCfg(clock Clock, sender Sender, storePath string) Config {
	c := DefaultConfig()
	c.Clock = clock
	c.Sender = sender
	c.StorePath = storePath
	c.Log = DiscardLogger
	c.HTTPTimeout = 50 * time.Millisecond
	c.MaxAttempts = 3
	c.TotalDeadline = time.Hour
	c.Backoff = BackoffConfig{Base: time.Millisecond, Max: 10 * time.Millisecond, Multiplier: 2, Jitter: 0}
	c.Circuit = CircuitConfig{FailureThreshold: 3, OpenTTL: 100 * time.Millisecond, HalfOpenMax: 1}
	c.QueueSize = 256
	c.HistoryRetention = 1000
	return c
}

func mustStart(t *testing.T, c Config) *Dispatcher {
	t.Helper()
	d, err := NewDispatcher(c)
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = d.Shutdown(ctx)
	})
	return d
}

func waitForStatus(t *testing.T, d *Dispatcher, id string, want Status, timeout time.Duration) *Delivery {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if dl, ok := d.Delivery(id); ok && dl.Status == want {
			return dl
		}
		time.Sleep(time.Millisecond)
	}
	dl, _ := d.Delivery(id)
	t.Fatalf("delivery %s did not reach %s (last=%v)", id, want, dl)
	return nil
}

func eventuallyTrue(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return fn()
}
