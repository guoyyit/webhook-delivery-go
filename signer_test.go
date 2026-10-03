package webhook

import (
	"strings"
	"testing"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	s := NewSigner()
	secret := []byte("top-secret")
	body := []byte(`{"a":1}`)
	ts := "1764000000"
	sig := s.Sign(secret, ts, body)

	if err := s.Verify(secret, body, ts, sig, func() int64 { return 1764000000 }, 300); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
}

func TestVerifyRejectsTamperedBody(t *testing.T) {
	s := NewSigner()
	secret := []byte("top-secret")
	ts := "1764000000"
	sig := s.Sign(secret, ts, []byte(`{"a":1}`))
	if err := s.Verify(secret, []byte(`{"a":2}`), ts, sig, func() int64 { return 1764000000 }, 300); err == nil {
		t.Fatal("tampered body accepted")
	}
}

func TestVerifyRejectsWrongSecret(t *testing.T) {
	s := NewSigner()
	sig := s.Sign([]byte("secret-a"), "1764000000", []byte("b"))
	if err := s.Verify([]byte("secret-b"), []byte("b"), "1764000000", sig, func() int64 { return 1764000000 }, 300); err == nil {
		t.Fatal("signature from another secret accepted")
	}
}

func TestVerifyRejectsStaleTimestamp(t *testing.T) {
	s := NewSigner()
	body := []byte("b")
	sig := s.Sign([]byte("k"), "1764000000", body)
	// "now" is 1 hour later than the signed timestamp; tolerance 5 minutes.
	if err := s.Verify([]byte("k"), body, "1764000000", sig, func() int64 { return 1764000000 + 3600 }, 300); err == nil {
		t.Fatal("stale request accepted")
	} else if !strings.Contains(err.Error(), "tolerance") {
		t.Fatalf("want tolerance error, got %v", err)
	}
}

func TestVerifyRejectsGarbage(t *testing.T) {
	s := NewSigner()
	if err := s.Verify([]byte("k"), []byte("b"), "not-a-number", "deadbeef", func() int64 { return 1 }, 0); err == nil {
		t.Fatal("garbage timestamp accepted")
	}
	if err := s.Verify(nil, []byte("b"), "1", "aa", func() int64 { return 1 }, 0); err == nil {
		t.Fatal("missing secret accepted")
	}
}
