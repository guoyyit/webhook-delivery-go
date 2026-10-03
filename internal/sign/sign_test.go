package sign

import (
	"testing"
	"time"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	secret := "whsec_test"
	body := []byte(`{"hello":"world"}`)
	ts := time.Unix(1700000000, 0)

	sig := Sign(secret, ts, body)
	if sig == "" {
		t.Fatal("empty signature")
	}
	if err := Verify(secret, ts, body, sig, 5*time.Minute, ts.Add(60*time.Second)); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}

func TestVerifyRejectsWrongSecret(t *testing.T) {
	body := []byte(`{"a":1}`)
	ts := time.Unix(1700000000, 0)
	sig := Sign("secret-a", ts, body)
	if err := Verify("secret-b", ts, body, sig, 5*time.Minute, ts); err == nil {
		t.Fatal("expected signature mismatch")
	}
}

func TestVerifyRejectsTamperedBody(t *testing.T) {
	body := []byte(`{"a":1}`)
	ts := time.Unix(1700000000, 0)
	sig := Sign("secret", ts, body)
	err := Verify("secret", ts, []byte(`{"a":2}`), sig, 5*time.Minute, ts)
	if err == nil {
		t.Fatal("expected tampered body to be rejected")
	}
}

func TestVerifyRejectsExpiredTimestamp(t *testing.T) {
	body := []byte(`{}`)
	ts := time.Unix(1700000000, 0)
	sig := Sign("secret", ts, body)
	err := Verify("secret", ts, body, sig, 5*time.Minute, ts.Add(6*time.Minute))
	if err == nil {
		t.Fatal("expected expired timestamp to be rejected")
	}
}

func TestVerifyRejectsFutureTimestamp(t *testing.T) {
	body := []byte(`{}`)
	ts := time.Unix(1700000000, 0)
	sig := Sign("secret", ts, body)
	err := Verify("secret", ts, body, sig, 5*time.Minute, ts.Add(-6*time.Minute))
	if err == nil {
		t.Fatal("expected future timestamp to be rejected")
	}
}

func TestParseTimestamp(t *testing.T) {
	ts, err := ParseTimestamp("1700000000")
	if err != nil {
		t.Fatal(err)
	}
	if !ts.Equal(time.Unix(1700000000, 0)) {
		t.Fatalf("bad timestamp: %v", ts)
	}
	if _, err := ParseTimestamp("not-a-time"); err == nil {
		t.Fatal("expected parse error")
	}
}
