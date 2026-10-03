// Package sign creates and verifies HMAC-SHA256 webhook signatures.
package sign

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Header names used on webhook requests.
const (
	HeaderEventID      = "X-Event-Id"
	HeaderDeliveryID   = "X-Delivery-Id"
	HeaderTimestamp    = "X-Webhook-Timestamp"
	HeaderSignature    = "X-Webhook-Signature"
	HeaderIdempotency  = "X-Idempotency-Key"
	signatureScheme    = "v1"
	signedPayloadDelim = "."
)

// Verification errors.
var (
	ErrMissingHeaders   = errors.New("sign: missing signature headers")
	ErrInvalidSignature = errors.New("sign: invalid signature")
	ErrExpired          = errors.New("sign: timestamp outside tolerance")
)

// Sign computes the signature header value for the given payload and timestamp.
// The signed payload is "<timestamp>.<body>", matching the Stripe-style scheme.
func Sign(secret string, timestamp time.Time, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(timestamp.Unix(), 10)))
	mac.Write([]byte(signedPayloadDelim))
	mac.Write(body)
	return signatureScheme + "=" + hex.EncodeToString(mac.Sum(nil))
}

// Verify checks the signature header against the body and rejects timestamps
// outside the allowed tolerance window.
func Verify(secret string, timestamp time.Time, body []byte, signatureHeader string, tolerance time.Duration, now time.Time) error {
	want := Sign(secret, timestamp, body)
	if !constantTimeEqual(want, signatureHeader) {
		return ErrInvalidSignature
	}
	if tolerance > 0 {
		diff := now.Sub(timestamp)
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			return fmt.Errorf("%w: diff=%s tolerance=%s", ErrExpired, diff, tolerance)
		}
	}
	return nil
}

// ParseTimestamp parses the X-Webhook-Timestamp header (unix seconds).
func ParseTimestamp(header string) (time.Time, error) {
	sec, err := strconv.ParseInt(strings.TrimSpace(header), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("sign: bad timestamp header: %w", err)
	}
	return time.Unix(sec, 0), nil
}

func constantTimeEqual(a, b string) bool {
	return hmac.Equal([]byte(a), []byte(b))
}
