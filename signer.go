package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
)

// Header names carried on every delivery request.
const (
	HeaderEventID       = "X-Webhook-Event-Id"
	HeaderDeliveryID    = "X-Webhook-Delivery-Id"
	HeaderEventSequence = "X-Webhook-Sequence"
	HeaderEventType     = "X-Webhook-Event-Type"
	HeaderTimestamp     = "X-Webhook-Timestamp" // unix seconds
	HeaderSignature     = "X-Webhook-Signature" // hex HMAC-SHA256
)

// Signer produces and verifies HMAC-SHA256 signatures over timestamp + body.
type Signer struct{}

// NewSigner returns the default signer.
func NewSigner() *Signer { return &Signer{} }

// signedContent is the exact canonical byte string covered by the signature.
// The timestamp is bound into the signed content so a captured signature
// cannot be replayed against altered headers or a different body.
func signedContent(timestamp string, body []byte) []byte {
	out := make([]byte, 0, len(timestamp)+1+len(body))
	out = append(out, timestamp...)
	out = append(out, '\n')
	out = append(out, body...)
	return out
}

// Sign returns lowercase hex(HMAC-SHA256(secret, timestamp + "\n" + body)).
func (s *Signer) Sign(secret []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(signedContent(timestamp, body))
	return hex.EncodeToString(mac.Sum(nil))
}

// Verify checks a signature with constant-time comparison and rejects stale
// requests outside the tolerance window. now is injectable for tests.
func (s *Signer) Verify(secret, body []byte, timestamp, signature string, now func() int64, toleranceSec int64) error {
	if len(secret) == 0 {
		return errors.New("missing signing secret")
	}
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp %q: %w", timestamp, err)
	}
	if toleranceSec > 0 {
		cur := now()
		if ts < cur-toleranceSec || ts > cur+toleranceSec {
			return errors.New("signature timestamp outside tolerance window")
		}
	}
	expected := s.Sign(secret, timestamp, body)
	got, err := hex.DecodeString(signature)
	if err != nil {
		return fmt.Errorf("invalid signature encoding: %w", err)
	}
	exp, err := hex.DecodeString(expected)
	if err != nil {
		return err
	}
	if !hmac.Equal(got, exp) {
		return errors.New("signature mismatch")
	}
	return nil
}
