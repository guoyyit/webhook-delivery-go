package retry

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestDelayBoundsAndGrowth(t *testing.T) {
	p := Policy{
		InitialInterval: time.Second,
		MaxInterval:     10 * time.Second,
		Multiplier:      2,
		MaxRetries:      10,
		Jitter:          0,
	}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 10 * time.Second}, // capped
	}
	for _, c := range cases {
		got, ok := p.Delay(c.attempt)
		if !ok {
			t.Fatalf("attempt %d: expected retry allowed", c.attempt)
		}
		if got != c.want {
			t.Fatalf("attempt %d: got %s, want %s", c.attempt, got, c.want)
		}
	}
}

func TestDelayJitterRange(t *testing.T) {
	p := Policy{
		InitialInterval: time.Second,
		MaxInterval:     time.Second,
		Multiplier:      2,
		MaxRetries:      10,
		Jitter:          0.5,
	}
	for i := 0; i < 100; i++ {
		got, _ := p.Delay(1)
		if got < 500*time.Millisecond || got > 1500*time.Millisecond {
			t.Fatalf("jittered delay out of range: %s", got)
		}
	}
}

func TestDelayMaxRetries(t *testing.T) {
	p := Policy{InitialInterval: time.Second, MaxRetries: 2, Jitter: 0}
	if _, ok := p.Delay(2); !ok {
		t.Fatal("attempt 2 should be allowed with MaxRetries=2")
	}
	if _, ok := p.Delay(3); ok {
		t.Fatal("attempt 3 should not be allowed with MaxRetries=2")
	}
}

func TestRetryableStatus(t *testing.T) {
	retryable := []int{408, 429, 500, 502, 503, 504}
	nonRetryable := []int{400, 401, 403, 404, 409, 422}
	for _, c := range retryable {
		if !RetryableStatus(c) {
			t.Errorf("status %d should be retryable", c)
		}
	}
	for _, c := range nonRetryable {
		if RetryableStatus(c) {
			t.Errorf("status %d should not be retryable", c)
		}
	}
}

func TestClassify(t *testing.T) {
	if !Classify(errors.New("boom"), 0) {
		t.Error("network error should be retryable")
	}
	if !Classify(nil, http.StatusServiceUnavailable) {
		t.Error("503 should be retryable")
	}
	if Classify(nil, http.StatusBadRequest) {
		t.Error("400 should not be retryable")
	}
}
