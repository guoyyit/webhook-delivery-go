package webhook

import (
	"net/http"
)

// Sender performs the actual HTTP POST. Abstracted so tests can replace the
// network with controllable fakes.
type Sender interface {
	Send(req *http.Request) (*http.Response, error)
}

// httpSender wraps net/http.
type httpSender struct {
	client *http.Client
}

// NewHTTPSender builds a Sender over a shared client. The caller is expected
// to bound each request with context timeout; the client itself sets no
// global timeout so deadlines stay under dispatcher control.
func NewHTTPSender() Sender {
	return &httpSender{client: http.DefaultClient}
}

func (s *httpSender) Send(req *http.Request) (*http.Response, error) {
	return s.client.Do(req)
}

// IsRetryableStatus classifies an HTTP status code:
//   - 2xx: success (not a failure at all);
//   - 408, 409, 425, 429 and any 5xx: transient, retryable;
//   - other 4xx: terminal — the endpoint refuses this request and retries
//     would not change the result, so the delivery goes straight to dead.
func IsRetryableStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout,
		http.StatusConflict,
		http.StatusTooEarly,
		http.StatusTooManyRequests:
		return true
	}
	return code >= 500
}

// IsSuccessStatus reports whether the endpoint accepted the delivery.
func IsSuccessStatus(code int) bool { return code >= 200 && code < 300 }
