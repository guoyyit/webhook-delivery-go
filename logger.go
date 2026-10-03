package webhook

import (
	"io"
	"log"
	"os"
)

// Logger is the minimal logging surface used by the dispatcher.
// Endpoint secrets and auth tokens must never reach it: the package only logs
// IDs, states, status codes and durations.
type Logger interface {
	Printf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// DiscardLogger drops all log output (useful in tests).
var DiscardLogger Logger = discardLogger{}

// NewLogger writes structured-ish lines to stderr.
func NewLogger() Logger { return NewLoggerTo(os.Stderr) }

// NewLoggerTo writes to an arbitrary writer.
func NewLoggerTo(w io.Writer) Logger {
	return log.New(w, "webhook ", log.LstdFlags|log.Lmsgprefix)
}

// Redact returns a masked representation safe for logs. It reveals only the
// last 4 characters and only when the secret is long enough to do so safely.
func Redact(secret []byte) string {
	n := len(secret)
	switch {
	case n == 0:
		return "<empty>"
	case n <= 4:
		return "****"
	default:
		return "****" + string(secret[n-4:])
	}
}

// redactString masks a string secret/token the same way.
func redactString(s string) string { return Redact([]byte(s)) }
