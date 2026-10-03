// Package engine orchestrates event ingestion, per-endpoint ordered delivery,
// retries, circuit breaking, dead-lettering and restart recovery.
package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/guoyyit/webhook-delivery-go/internal/breaker"
	"github.com/guoyyit/webhook-delivery-go/internal/clock"
	"github.com/guoyyit/webhook-delivery-go/internal/model"
	"github.com/guoyyit/webhook-delivery-go/internal/retry"
	"github.com/guoyyit/webhook-delivery-go/internal/sign"
	"github.com/guoyyit/webhook-delivery-go/internal/store"
	"github.com/guoyyit/webhook-delivery-go/internal/sub"
)

// Dead-letter reasons.
const (
	DeadLetterCircuitOpen      = "circuit_open"
	DeadLetterRetriesExhausted = "retries_exhausted"
	DeadLetterMaxElapsed       = "max_elapsed_time_exceeded"
	DeadLetterUnsubscribed     = "unsubscribed"
	DeadLetterSubscriptionGone = "subscription_gone"
	DeadLetterSubscriptionOff  = "subscription_disabled"
)

// Config holds engine configuration.
type Config struct {
	// StoreDir is the directory used for persistence.
	StoreDir string
	// RequestTimeout bounds a single HTTP delivery attempt.
	RequestTimeout time.Duration
	// Retry configures retry/backoff behavior.
	Retry retry.Policy
	// Breaker configures endpoint circuit breaking.
	Breaker breaker.Config
	// Clock is the time source; defaults to the real clock.
	Clock clock.Clock
	// Logger defaults to slog.Default.
	Logger *slog.Logger
	// HTTPClient defaults to http.DefaultClient.
	HTTPClient *http.Client
}

func (c *Config) defaults() {
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 10 * time.Second
	}
	if c.Retry.InitialInterval == 0 && c.Retry.MaxInterval == 0 && c.Retry.MaxElapsedTime == 0 {
		c.Retry = retry.DefaultPolicy()
	}
	if c.Breaker.FailureThreshold == 0 && c.Breaker.OpenInterval == 0 {
		c.Breaker = breaker.DefaultConfig()
	}
	if c.Clock == nil {
		c.Clock = clock.NewReal()
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.HTTPClient == nil {
		c.HTTPClient = http.DefaultClient
	}
}

// Engine is the delivery engine.
type Engine struct {
	cfg      Config
	store    *store.Store
	registry *sub.Registry
	breakers *breaker.Registry
	clock    clock.Clock
	logger   *slog.Logger
	client   *http.Client

	workers   map[string]chan struct{}
	workersMu sync.Mutex
	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// New creates an engine. Call Start to resume persisted deliveries.
func New(cfg Config) (*Engine, error) {
	cfg.defaults()
	st, err := store.Open(cfg.StoreDir)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg:      cfg,
		store:    st,
		registry: sub.NewRegistry(),
		breakers: breaker.NewRegistry(cfg.Breaker),
		clock:    cfg.Clock,
		logger:   cfg.Logger,
		client:   cfg.HTTPClient,
		workers:  make(map[string]chan struct{}),
		stop:     make(chan struct{}),
	}, nil
}

// Start loads persisted state and resumes unfinished deliveries.
func (e *Engine) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for _, s := range e.store.ListSubscriptions() {
		cp := *s
		e.registry.Add(&cp)
	}
	if err := e.reconcile(ctx); err != nil {
		return err
	}
	for _, endpoint := range e.store.PendingEndpoints() {
		e.signal(endpoint)
	}
	e.logger.Info("engine started",
		"subscriptions", len(e.registry.List()),
		"events", len(e.store.ListEvents()))
	return nil
}

// Close stops all workers and flushes persistence.
func (e *Engine) Close() error {
	e.closeOnce.Do(func() {
		close(e.stop)
	})
	e.wg.Wait()
	return e.store.Close()
}

// Subscribe registers a subscription.
func (e *Engine) Subscribe(_ context.Context, s *model.Subscription) error {
	if s == nil {
		return errors.New("subscription is nil")
	}
	if s.ID == "" {
		s.ID = newRandomID("sub")
	}
	if s.URL == "" {
		return errors.New("subscription url is required")
	}
	if u, err := url.Parse(s.URL); err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("subscription url is invalid: %q", s.URL)
	}
	if s.Secret == "" {
		s.Secret = newRandomSecret()
	}
	if err := e.store.PutSubscription(s); err != nil {
		return err
	}
	cp := *s
	e.registry.Add(&cp)
	e.logger.Info("subscription added", "subscription_id", s.ID,
		"endpoint", hostOf(s.URL), "event_types", s.EventTypes)
	return nil
}

// Unsubscribe removes a subscription and cancels its pending deliveries.
func (e *Engine) Unsubscribe(_ context.Context, id string) error {
	if _, ok := e.registry.Get(id); !ok {
		return fmt.Errorf("subscription %q not found", id)
	}
	e.registry.Remove(id)
	for _, d := range e.store.ListDeliveries(store.DeliveryFilter{SubscriptionID: id}) {
		if d.Terminal() {
			continue
		}
		d.Status = model.StatusFailed
		d.DeadLetterReason = DeadLetterUnsubscribed
		d.LastError = "subscription unsubscribed before delivery"
		if err := e.store.PutDelivery(d); err != nil {
			return err
		}
	}
	e.logger.Info("subscription removed", "subscription_id", id)
	return nil
}

// Ingest stores an event and creates deliveries for matching subscriptions.
// It is idempotent: an event with an existing id is returned unchanged and no
// duplicate deliveries are created.
func (e *Engine) Ingest(_ context.Context, ev *model.Event) (*model.Event, []*model.Delivery, error) {
	if ev == nil {
		return nil, nil, errors.New("event is nil")
	}
	if ev.Type == "" {
		return nil, nil, errors.New("event type is required")
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = e.clock.Now()
	}
	if err := e.store.AppendEvent(ev); err != nil {
		return nil, nil, err
	}
	matches := e.registry.Match(ev.Type)
	created := make([]*model.Delivery, 0, len(matches))
	for _, s := range matches {
		d := &model.Delivery{
			EventID:        ev.ID,
			EventSeq:       ev.Seq,
			EventType:      ev.Type,
			SubscriptionID: s.ID,
			EndpointURL:    s.URL,
			Status:         model.StatusPending,
		}
		if existing, ok := e.store.GetDelivery(store.DeliveryID(ev.ID, s.ID)); ok {
			created = append(created, existing)
			continue
		}
		if err := e.store.PutDelivery(d); err != nil {
			return nil, nil, err
		}
		created = append(created, d)
		e.signal(s.URL)
	}
	e.logger.Info("event ingested", "event_id", ev.ID, "type", ev.Type,
		"deliveries", len(created))
	return ev, created, nil
}

// Replay re-queues dead-lettered deliveries whose event creation time falls in
// the half-open range [from, to). It returns the number of re-queued deliveries.
func (e *Engine) Replay(_ context.Context, from, to time.Time) (int, error) {
	if to.Before(from) {
		return 0, errors.New("replay range is invalid: to before from")
	}
	n := 0
	for _, d := range e.store.ListDeliveries(store.DeliveryFilter{Statuses: []model.DeliveryStatus{model.StatusDeadLettered}}) {
		ev, ok := e.store.GetEvent(d.EventID)
		if !ok {
			continue
		}
		if ev.CreatedAt.Before(from) || !ev.CreatedAt.Before(to) {
			continue
		}
		d.Status = model.StatusPending
		d.Attempts = 0
		d.NextRetryAt = nil
		d.LastError = ""
		d.DeadLetterReason = ""
		d.FirstAttemptAt = nil
		if err := e.store.PutDelivery(d); err != nil {
			return n, err
		}
		e.signal(d.EndpointURL)
		n++
	}
	e.logger.Info("replay scheduled", "from", from, "to", to, "replayed", n)
	return n, nil
}

// ListSubscriptions returns all registered subscriptions.
func (e *Engine) ListSubscriptions() []*model.Subscription {
	return e.registry.List()
}

// GetDelivery returns a delivery by id.
func (e *Engine) GetDelivery(id string) (*model.Delivery, bool) {
	return e.store.GetDelivery(id)
}

// ListDeliveries returns deliveries matching the filter.
func (e *Engine) ListDeliveries(f store.DeliveryFilter) []*model.Delivery {
	return e.store.ListDeliveries(f)
}

// --- workers ---

func (e *Engine) signal(endpoint string) {
	e.workersMu.Lock()
	ch, ok := e.workers[endpoint]
	if !ok {
		ch = make(chan struct{}, 1)
		e.workers[endpoint] = ch
		e.wg.Add(1)
		go e.runWorker(endpoint, ch)
	}
	e.workersMu.Unlock()
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (e *Engine) runWorker(endpoint string, ch chan struct{}) {
	defer e.wg.Done()
	for {
		select {
		case <-e.stop:
			return
		case <-ch:
		}
		for {
			select {
			case <-e.stop:
				return
			default:
			}
			d, ok := e.store.NextPendingForEndpoint(endpoint)
			if !ok {
				break
			}
			e.process(d)
		}
	}
}

// process drives one delivery through attempts until it reaches a terminal
// state or a backoff wait. It runs on the endpoint's single worker goroutine,
// so events for one endpoint are delivered in sequence while endpoints never
// block each other.
func (e *Engine) process(d *model.Delivery) {
	for {
		if e.stopped() {
			return
		}
		// Respect the retry window, including after a restart: a delivery with
		// a future NextRetryAt waits until it is due instead of hammering the
		// endpoint immediately on resume.
		if d.Status == model.StatusPending && d.NextRetryAt != nil {
			if wait := d.NextRetryAt.Sub(e.clock.Now()); wait > 0 {
				select {
				case <-e.clock.After(wait):
				case <-e.stop:
					return
				}
				continue
			}
		}
		sub, ok := e.registry.Get(d.SubscriptionID)
		if !ok {
			e.terminal(d, model.StatusFailed, DeadLetterSubscriptionGone,
				"subscription no longer exists")
			return
		}
		if sub.Disabled {
			e.terminal(d, model.StatusFailed, DeadLetterSubscriptionOff,
				"subscription disabled")
			return
		}
		if !e.breakers.Get(d.EndpointURL).Allow() {
			e.terminal(d, model.StatusDeadLettered, DeadLetterCircuitOpen,
				"circuit breaker open")
			return
		}

		now := e.clock.Now()
		if d.FirstAttemptAt == nil {
			d.FirstAttemptAt = &now
		}
		d.Status = model.StatusInProgress
		if err := e.store.PutDelivery(d); err != nil {
			e.logger.Error("persist in_progress failed", "delivery_id", d.ID, "error", err)
			return
		}

		statusCode, err := e.attempt(sub, d)
		if err == nil && statusCode >= 200 && statusCode < 300 {
			// Guard against an unsubscribe that raced with the in-flight attempt.
			if _, ok := e.registry.Get(d.SubscriptionID); !ok {
				e.terminal(d, model.StatusFailed, DeadLetterUnsubscribed,
					"subscription unsubscribed during delivery")
				return
			}
			e.breakers.Get(d.EndpointURL).RecordSuccess()
			d.Status = model.StatusSucceeded
			d.LastError = ""
			if perr := e.store.PutDelivery(d); perr != nil {
				e.logger.Error("persist success failed", "delivery_id", d.ID, "error", perr)
			}
			e.logger.Info("delivery succeeded", "delivery_id", d.ID,
				"event_id", d.EventID, "endpoint", hostOf(d.EndpointURL),
				"attempts", d.Attempts+1, "status", statusCode)
			return
		}

		e.breakers.Get(d.EndpointURL).RecordFailure()
		d.Attempts++
		if err != nil {
			d.LastError = err.Error()
		} else {
			d.LastError = fmt.Sprintf("endpoint returned HTTP %d", statusCode)
		}
		e.logger.Warn("delivery attempt failed", "delivery_id", d.ID,
			"event_id", d.EventID, "endpoint", hostOf(d.EndpointURL),
			"attempt", d.Attempts, "status", statusCode, "error", d.LastError)

		if !retry.Classify(err, statusCode) {
			e.terminal(d, model.StatusFailed, "", d.LastError)
			return
		}
		if e.cfg.Retry.MaxElapsedTime > 0 && d.FirstAttemptAt != nil &&
			now.Sub(*d.FirstAttemptAt) >= e.cfg.Retry.MaxElapsedTime {
			e.terminal(d, model.StatusDeadLettered, DeadLetterMaxElapsed,
				"total delivery time limit exceeded")
			return
		}
		delay, ok := e.cfg.Retry.Delay(d.Attempts)
		if !ok {
			e.terminal(d, model.StatusDeadLettered, DeadLetterRetriesExhausted,
				"retry limit exhausted")
			return
		}
		next := e.clock.Now().Add(delay)
		d.Status = model.StatusPending
		d.NextRetryAt = &next
		if perr := e.store.PutDelivery(d); perr != nil {
			e.logger.Error("persist retry failed", "delivery_id", d.ID, "error", perr)
			return
		}
		e.logger.Info("delivery scheduled for retry", "delivery_id", d.ID,
			"attempt", d.Attempts, "backoff", delay, "next_retry_at", next)
		// Loop back and wait for the retry window (handled at the top of the
		// loop), so shutdown and restart interrupt the wait cleanly.
	}
}

// attempt performs a single signed HTTP delivery attempt.
func (e *Engine) attempt(sub *model.Subscription, d *model.Delivery) (int, error) {
	ev, ok := e.store.GetEvent(d.EventID)
	if !ok {
		return 0, fmt.Errorf("event %q not found", d.EventID)
	}
	body := ev.Payload
	if body == nil {
		body = []byte{}
	}
	now := e.clock.Now()
	req, err := http.NewRequest(http.MethodPost, sub.URL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(sign.HeaderEventID, ev.ID)
	req.Header.Set(sign.HeaderDeliveryID, d.ID)
	req.Header.Set(sign.HeaderIdempotency, d.ID)
	req.Header.Set(sign.HeaderTimestamp, fmt.Sprintf("%d", now.Unix()))
	req.Header.Set(sign.HeaderSignature, sign.Sign(sub.Secret, now, body))

	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.RequestTimeout)
	defer cancel()
	resp, err := e.client.Do(req.WithContext(ctx))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func (e *Engine) terminal(d *model.Delivery, status model.DeliveryStatus, reason, lastErr string) {
	d.Status = status
	if reason != "" {
		d.DeadLetterReason = reason
	}
	if lastErr != "" {
		d.LastError = lastErr
	}
	if err := e.store.PutDelivery(d); err != nil {
		e.logger.Error("persist terminal state failed", "delivery_id", d.ID, "error", err)
	}
	e.logger.Warn("delivery terminal", "delivery_id", d.ID, "status", status,
		"reason", reason, "event_id", d.EventID)
}

func (e *Engine) stopped() bool {
	select {
	case <-e.stop:
		return true
	default:
		return false
	}
}

// reconcile repairs state after a crash between the event append and the
// delivery snapshot, and resumes deliveries that were in flight.
func (e *Engine) reconcile(_ context.Context) error {
	for _, d := range e.store.ListDeliveries(store.DeliveryFilter{}) {
		if d.Status == model.StatusInProgress {
			d.Status = model.StatusPending
			if err := e.store.PutDelivery(d); err != nil {
				return err
			}
		}
	}
	for _, ev := range e.store.ListEvents() {
		for _, s := range e.registry.Match(ev.Type) {
			if s.CreatedAt.After(ev.CreatedAt) {
				continue
			}
			id := store.DeliveryID(ev.ID, s.ID)
			if _, ok := e.store.GetDelivery(id); ok {
				continue
			}
			d := &model.Delivery{
				ID:             id,
				EventID:        ev.ID,
				EventSeq:       ev.Seq,
				EventType:      ev.Type,
				SubscriptionID: s.ID,
				EndpointURL:    s.URL,
				Status:         model.StatusPending,
			}
			if err := e.store.PutDelivery(d); err != nil {
				return err
			}
		}
	}
	return nil
}

func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "?"
	}
	return u.Host
}

func newRandomID(prefix string) string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return prefix + "_" + hex.EncodeToString(b)
}

func newRandomSecret() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "whsec_" + hex.EncodeToString(b)
}
