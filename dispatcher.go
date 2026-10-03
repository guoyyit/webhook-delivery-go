package webhook

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Config holds all delivery-semantics knobs.
type Config struct {
	Clock              Clock         // defaults to wall clock
	StorePath          string        // persistence file; "" means in-memory
	HTTPTimeout        time.Duration // per-attempt HTTP timeout
	MaxAttempts        int           // retry budget (includes first attempt)
	TotalDeadline      time.Duration // wall-clock budget for one delivery
	Backoff            BackoffConfig
	Circuit            CircuitConfig
	QueueSize          int           // per-endpoint pending queue; overflow -> DLQ
	HistoryRetention   int           // terminal deliveries kept for replay/dedup
	Log                Logger        // nil -> stderr logger
	Sender             Sender        // injectable HTTP sender
	TimestampTolerance time.Duration // documented for receivers; not enforced sender-side
}

// DefaultConfig returns production-oriented defaults.
func DefaultConfig() Config {
	return Config{
		Clock:              RealClock,
		HTTPTimeout:        10 * time.Second,
		MaxAttempts:        8,
		TotalDeadline:      24 * time.Hour,
		Backoff:            DefaultBackoffConfig(),
		Circuit:            DefaultCircuitConfig(),
		QueueSize:          1024,
		HistoryRetention:   10000,
		TimestampTolerance: 5 * time.Minute,
	}
}

func (c *Config) applyDefaults() {
	if c.Clock == nil {
		c.Clock = RealClock
	}
	if c.HTTPTimeout <= 0 {
		c.HTTPTimeout = 10 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 8
	}
	if c.TotalDeadline <= 0 {
		c.TotalDeadline = 24 * time.Hour
	}
	if c.Backoff.Base <= 0 {
		c.Backoff = DefaultBackoffConfig()
	}
	if c.Circuit.FailureThreshold <= 0 {
		c.Circuit = DefaultCircuitConfig()
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 1024
	}
	if c.HistoryRetention <= 0 {
		c.HistoryRetention = 10000
	}
	if c.Log == nil {
		c.Log = NewLogger()
	}
	if c.Sender == nil {
		c.Sender = NewHTTPSender()
	}
	if c.TimestampTolerance <= 0 {
		c.TimestampTolerance = 5 * time.Minute
	}
}

// Dispatcher fans events out to subscribed endpoints with per-endpoint
// ordered, persisted, retried and circuit-protected delivery.
type Dispatcher struct {
	cfg       Config
	registry  *Registry
	store     *Store
	signer    *Signer
	backoffer *Backoffer

	rootCtx    context.Context
	rootCancel context.CancelFunc

	wmu     sync.Mutex
	workers map[string]*worker

	breaksMu sync.Mutex
	breaks   map[string]*Circuit

	// dedup serializes the check-then-create window for one (event, endpoint)
	// delivery id so concurrent publishes of the same event collapse to one.
	dedupMu sync.Mutex
	dedup   map[string]struct{}

	started bool
}

// NewDispatcher builds a dispatcher from config.
func NewDispatcher(cfg Config) (*Dispatcher, error) {
	cfg.applyDefaults()
	st, err := OpenStore(cfg.StorePath)
	if err != nil {
		return nil, err
	}
	d := &Dispatcher{
		cfg:       cfg,
		registry:  NewRegistry(),
		store:     st,
		signer:    NewSigner(),
		backoffer: NewBackoffer(cfg.Backoff),
		workers:   map[string]*worker{},
		breaks:    map[string]*Circuit{},
		dedup:     map[string]struct{}{},
	}
	return d, nil
}

// Start launches endpoint workers and restores persisted state.
func (d *Dispatcher) Start(ctx context.Context) error {
	d.rootCtx, d.rootCancel = context.WithCancel(ctx)
	d.started = true

	// Launch workers for endpoints registered before Start.
	d.wmu.Lock()
	for _, w := range d.workers {
		w.start()
	}
	d.wmu.Unlock()

	// Restore: any delivery left pending, or whose attempt was in flight when
	// the process died, is requeued. In-flight outcomes are unknowable after
	// a crash, so they are rolled back to pending; the stable delivery id
	// makes the possible duplicate harmless to idempotent receivers.
	stored, err := d.store.Load()
	if err != nil {
		return err
	}
	// Restore in Seq order so queue insertion stays ordered.
	sortDeliveriesBySeq(stored)
	for _, dl := range stored {
		switch dl.Status {
		case StatusPending, StatusInFlight:
			if _, ok := d.registry.Endpoint(dl.EndpointID); !ok {
				// Endpoint metadata is not persisted; skip until re-registered.
				continue
			}
			if dl.Status == StatusInFlight {
				dl.Status = StatusPending
				if err := d.store.Put(dl); err != nil {
					return err
				}
			}
			d.enqueue(dl)
		}
	}
	return nil
}

// Shutdown drains in-flight work and flushes state. No half-finished delivery
// state is left: the root context cancels HTTP attempts, each in-flight
// delivery is rolled back to pending and persisted before the worker exits.
func (d *Dispatcher) Shutdown(shutdownCtx context.Context) error {
	if !d.started {
		return nil
	}
	d.rootCancel()

	d.wmu.Lock()
	ws := make([]*worker, 0, len(d.workers))
	for _, w := range d.workers {
		close(w.stop)
		ws = append(ws, w)
	}
	d.wmu.Unlock()

	exit := make(chan struct{})
	go func() {
		for _, w := range ws {
			<-w.done
		}
		close(exit)
	}()
	var err error
	select {
	case <-exit:
	case <-shutdownCtx.Done():
		err = shutdownCtx.Err()
	}
	if cerr := d.store.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// AddEndpoint registers an endpoint and starts its worker immediately so that
// deliveries persisted before a restart are resumed even before new events.
func (d *Dispatcher) AddEndpoint(ep Endpoint) error {
	if err := d.registry.AddEndpoint(ep); err != nil {
		return err
	}
	d.workerFor(ep.ID)
	d.restoreEndpoint(ep.ID)
	return nil
}

// restoreEndpoint requeues persisted unfinished deliveries for an endpoint.
func (d *Dispatcher) restoreEndpoint(endpointID string) {
	stored, _ := d.store.Load()
	sortDeliveriesBySeq(stored)
	for _, dl := range stored {
		if dl.EndpointID != endpointID {
			continue
		}
		if dl.Status == StatusPending || dl.Status == StatusInFlight {
			if dl.Status == StatusInFlight {
				dl.Status = StatusPending
				_ = d.store.Put(dl)
			}
			d.enqueue(dl)
		}
	}
}

// PutSubscription registers a subscription.
func (d *Dispatcher) PutSubscription(sub Subscription) error {
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = d.cfg.Clock.Now()
	}
	return d.registry.PutSubscription(sub)
}

// RemoveSubscription unsubscribes; queued deliveries for it stop immediately
// when they reach the worker head and are dead-lettered with a clear reason
// (never silently dropped).
func (d *Dispatcher) RemoveSubscription(id string) {
	d.registry.RemoveSubscription(id)
	for _, w := range d.allWorkers() {
		w.signal()
	}
}

// Publish fans one event out to all matching subscriptions. It never blocks
// on slow endpoints: matching is snapshot-based and enqueue is O(queue).
// When an endpoint queue is full the delivery is persisted and dead-lettered
// with ReasonQueueOverflow instead of stalling the producer.
// Returns the number of subscriptions the event matched.
func (d *Dispatcher) Publish(ev Event) (int, error) {
	if ev.ID == "" {
		return 0, errors.New("event id is required")
	}
	matches := d.registry.Matches(ev.Type)
	for _, sub := range matches {
		ep, ok := d.registry.Endpoint(sub.EndpointID)
		if !ok {
			continue
		}
		id := DeliveryID(ev.ID, ep.ID)

		// Atomicize check-then-create for this delivery id so concurrent
		// publishes of the same event collapse to a single delivery.
		d.dedupMu.Lock()
		if _, inflight := d.dedup[id]; inflight {
			d.dedupMu.Unlock()
			continue
		}
		if _, ok := d.store.Get(id); ok {
			// Stable-id dedup: a re-triggered/replayed publish of the same
			// event must never create a second semantic delivery. Terminal
			// failures are only resurrected via the explicit Replay API.
			d.dedupMu.Unlock()
			continue
		}
		d.dedup[id] = struct{}{}
		d.dedupMu.Unlock()

		created := false
		defer func() {
			if !created {
				d.dedupMu.Lock()
				delete(d.dedup, id)
				d.dedupMu.Unlock()
			}
		}()

		seq, err := d.store.NextSeq()
		if err != nil {
			return len(matches), err
		}
		dl := &Delivery{
			ID:             id,
			EventID:        ev.ID,
			EventType:      ev.Type,
			Seq:            seq,
			EndpointID:     ep.ID,
			SubscriptionID: sub.ID,
			Payload:        append([]byte(nil), ev.Payload...),
			Status:         StatusPending,
			CreatedAt:      d.cfg.Clock.Now(),
			Deadline:       d.cfg.Clock.Now().Add(d.cfg.TotalDeadline),
		}
		if err := d.store.Put(dl); err != nil {
			return len(matches), err
		}
		created = true // the store now owns dedup; the ephemeral marker is dropped
		d.dedupMu.Lock()
		delete(d.dedup, id)
		d.dedupMu.Unlock()

		w := d.workerFor(ep.ID)
		if w.size() >= d.cfg.QueueSize {
			// Backpressure: never block the producer; dead-letter explicitly.
			if err := d.store.DeadLetter(dl, ReasonQueueOverflow); err != nil {
				return len(matches), err
			}
			d.cfg.Log.Printf("delivery %s endpoint=%s dead-lettered: %s", dl.ID, ep.ID, ReasonQueueOverflow)
			d.trimHistory()
			continue
		}
		d.enqueue(dl)
	}
	return len(matches), nil
}

// enqueue hands a pending delivery to its endpoint worker.
func (d *Dispatcher) enqueue(dl *Delivery) {
	w := d.workerFor(dl.EndpointID)
	w.enqueue(dl.ID, dl.Seq)
	w.signal()
}

// Delivery looks up a delivery's state for troubleshooting.
func (d *Dispatcher) Delivery(id string) (*Delivery, bool) { return d.store.Get(id) }

// DeliveryByEvent returns deliveries produced for one event.
func (d *Dispatcher) DeliveryByEvent(eventID string) []*Delivery {
	all, _ := d.store.Load()
	out := make([]*Delivery, 0)
	for _, dl := range all {
		if dl.EventID == eventID {
			out = append(out, dl)
		}
	}
	sortDeliveriesBySeq(out)
	return out
}

// DeadLetters lists terminal-failure deliveries across endpoints.
func (d *Dispatcher) DeadLetters() []*Delivery { return d.store.DeadLetters() }

// Replay requeues one dead-lettered delivery inside the explicit retention
// bound: entries already truncated cannot be replayed (ErrNotFound).
func Replay(d *Dispatcher, id string) (*Delivery, error) {
	dl, err := d.store.ReplayDeadLetter(id)
	if err != nil {
		return nil, err
	}
	// Fresh overall deadline and attempt budget for the requeued delivery;
	// the stable id and Seq stay, so dedup and ordering are preserved.
	dl.Deadline = d.cfg.Clock.Now().Add(d.cfg.TotalDeadline)
	if err := d.store.Put(dl); err != nil {
		return nil, err
	}
	d.enqueue(dl)
	return dl, nil
}

// Circuit exposes the breaker for an endpoint (ForceOpen in tests/admin).
func (d *Dispatcher) Circuit(endpointID string) *Circuit { return d.circuit(endpointID) }

func (d *Dispatcher) circuit(endpointID string) *Circuit {
	d.breaksMu.Lock()
	defer d.breaksMu.Unlock()
	cb, ok := d.breaks[endpointID]
	if !ok {
		cb = NewCircuit(d.cfg.Circuit, d.cfg.Clock)
		d.breaks[endpointID] = cb
	}
	return cb
}

func (d *Dispatcher) workerFor(endpointID string) *worker {
	d.wmu.Lock()
	defer d.wmu.Unlock()
	w, ok := d.workers[endpointID]
	if !ok {
		w = newWorker(d, endpointID)
		d.workers[endpointID] = w
		if d.started {
			w.start()
		}
	}
	return w
}

func (d *Dispatcher) allWorkers() []*worker {
	d.wmu.Lock()
	defer d.wmu.Unlock()
	out := make([]*worker, 0, len(d.workers))
	for _, w := range d.workers {
		out = append(out, w)
	}
	return out
}

// ---- attempt outcome -------------------------------------------------------

type outcomeKind int

const (
	outcomeRetry    outcomeKind = iota // transient failure, back off and retry
	outcomeSuccess                     // 2xx
	outcomeTerminal                    // non-retryable 4xx
	outcomeShutdown                    // root context canceled mid-attempt
	outcomeDead                        // already transitioned to a terminal state
)

type attemptOutcome struct {
	kind      outcomeKind
	retryable bool
	at        time.Time
	status    int
	errText   string
}

// attempt performs exactly one signed HTTP attempt and records it.
func (d *Dispatcher) attempt(dl *Delivery) attemptOutcome {
	now := d.cfg.Clock.Now()

	// Total deadline guard (distinct from the per-attempt timeout).
	if dl.deadlinePassed(now) {
		d.finishDead(dl, ReasonDeadline)
		return attemptOutcome{kind: outcomeDead}
	}

	ep, ok := d.registry.Endpoint(dl.EndpointID)
	if !ok {
		d.finishDead(dl, ReasonUnsubscribed)
		return attemptOutcome{kind: outcomeDead}
	}

	dl.Status = StatusInFlight
	if err := d.store.Put(dl); err != nil {
		d.cfg.Log.Printf("persist inflight failed delivery=%s: %v", dl.ID, err)
	}

	ctx, cancel := context.WithTimeout(d.rootCtx, d.cfg.HTTPTimeout)
	defer cancel()

	ts := strconv.FormatInt(now.Unix(), 10)
	body := append([]byte(nil), dl.Payload...)
	sig := d.signer.Sign(ep.Secret, ts, body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.URL, bytes.NewReader(body))
	if err != nil {
		// Malformed URL is a permanent configuration error.
		a := Attempt{Number: len(dl.Attempts) + 1, StartedAt: now, FinishedAt: d.cfg.Clock.Now(), Retryable: false, Error: err.Error()}
		dl.Attempts = append(dl.Attempts, a)
		_ = d.store.Put(dl)
		return attemptOutcome{kind: outcomeTerminal}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderEventID, dl.EventID)
	req.Header.Set(HeaderDeliveryID, dl.ID)
	req.Header.Set(HeaderEventSequence, strconv.FormatUint(dl.Seq, 10))
	req.Header.Set(HeaderEventType, dl.EventType)
	req.Header.Set(HeaderTimestamp, ts)
	req.Header.Set(HeaderSignature, sig)
	if ep.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+ep.AuthToken)
	}

	start := d.cfg.Clock.Now()
	resp, sendErr := d.cfg.Sender.Send(req)
	finish := d.cfg.Clock.Now()

	a := Attempt{Number: len(dl.Attempts) + 1, StartedAt: start, FinishedAt: finish}

	// Shutdown wins regardless of what the attempt observed: if the root
	// context is canceled the delivery's disposition is unknown, so it is
	// rolled back to pending rather than classified from a racy response.
	shutdown := d.rootCtx.Err() != nil

	if sendErr != nil {
		a.Error = sendErr.Error()
		if shutdown {
			a.Retryable = true
			dl.Attempts = append(dl.Attempts, a)
			_ = d.store.Put(dl)
			return attemptOutcome{kind: outcomeShutdown, retryable: true}
		}
		// Timeouts, connection resets, 5xx-with-connection issues: retryable.
		a.Retryable = true
		dl.Attempts = append(dl.Attempts, a)
		_ = d.store.Put(dl)
		return attemptOutcome{kind: outcomeRetry, retryable: true, errText: a.Error}
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	a.HTTPStatus = resp.StatusCode
	if shutdown {
		// Even if a 2xx/4xx/5xx arrived, treat as unknown during shutdown.
		a.Retryable = true
		dl.Attempts = append(dl.Attempts, a)
		_ = d.store.Put(dl)
		return attemptOutcome{kind: outcomeShutdown, retryable: true, status: resp.StatusCode}
	}
	switch {
	case IsSuccessStatus(resp.StatusCode):
		a.Retryable = false
		dl.Attempts = append(dl.Attempts, a)
		_ = d.store.Put(dl)
		return attemptOutcome{kind: outcomeSuccess, at: finish, status: resp.StatusCode}
	case IsRetryableStatus(resp.StatusCode):
		a.Retryable = true
		dl.Attempts = append(dl.Attempts, a)
		_ = d.store.Put(dl)
		return attemptOutcome{kind: outcomeRetry, retryable: true, status: resp.StatusCode}
	default:
		a.Retryable = false
		dl.Attempts = append(dl.Attempts, a)
		_ = d.store.Put(dl)
		return attemptOutcome{kind: outcomeTerminal, status: resp.StatusCode}
	}
}

// ---- terminal transitions --------------------------------------------------

func (d *Dispatcher) finishSuccess(dl *Delivery, at time.Time) {
	dl.Status = StatusSucceeded
	dl.NextAttemptAt = zeroTime()
	dl.SucceededAt = &at
	dl.DeadReason = ""
	if err := d.store.Put(dl); err != nil {
		d.cfg.Log.Printf("persist success failed delivery=%s: %v", dl.ID, err)
	}
	d.cfg.Log.Printf("delivery %s endpoint=%s event=%s succeeded attempts=%d",
		dl.ID, dl.EndpointID, dl.EventID, len(dl.Attempts))
	d.trimHistory()
}

func (d *Dispatcher) finishDead(dl *Delivery, reason string) {
	if err := d.store.DeadLetter(dl, reason); err != nil {
		d.cfg.Log.Printf("persist dead-letter failed delivery=%s: %v", dl.ID, err)
	}
	d.cfg.Log.Printf("delivery %s endpoint=%s event=%s dead reason=%s attempts=%d",
		dl.ID, dl.EndpointID, dl.EventID, reason, len(dl.Attempts))
	d.trimHistory()
}

// shouldDead decides whether a transient failure has exhausted its budget.
func (d *Dispatcher) shouldDead(dl *Delivery, o attemptOutcome) bool {
	if dl.attemptsExhausted(d.cfg.MaxAttempts) {
		return true
	}
	if dl.deadlinePassed(d.cfg.Clock.Now()) {
		return true
	}
	return false
}

// deadWithReason maps an exhausted retry outcome to its terminal reason.
func (d *Dispatcher) deadWithReason(dl *Delivery, o attemptOutcome) {
	switch {
	case dl.deadlinePassed(d.cfg.Clock.Now()):
		d.finishDead(dl, ReasonDeadline)
	case dl.attemptsExhausted(d.cfg.MaxAttempts):
		d.finishDead(dl, ReasonMaxAttempts)
	default:
		d.finishDead(dl, ReasonMaxAttempts)
	}
}

// scheduleRetry parks the delivery in pending with a backoff deadline.
func (d *Dispatcher) scheduleRetry(dl *Delivery, o attemptOutcome) {
	delay := d.backoffer.Next(len(dl.Attempts))
	dl.Status = StatusPending
	dl.NextAttemptAt = d.cfg.Clock.Now().Add(delay)
	if err := d.store.Put(dl); err != nil {
		d.cfg.Log.Printf("persist retry failed delivery=%s: %v", dl.ID, err)
	}
	d.cfg.Log.Printf("delivery %s endpoint=%s retry #%d in %s (last_status=%d last_err=%q)",
		dl.ID, dl.EndpointID, len(dl.Attempts), delay, o.status, o.errText)
}

// rollbackToPending restores a clean pending state on shutdown/cancel: no
// attempt beyond the persisted history is considered confirmed.
func (d *Dispatcher) rollbackToPending(dl *Delivery) {
	dl.Status = StatusPending
	_ = d.store.Put(dl)
}

func (d *Dispatcher) trimHistory() {
	if d.cfg.HistoryRetention > 0 {
		d.store.TruncateHistory(d.cfg.HistoryRetention)
	}
}

// sortDeliveriesBySeq orders ascending by Seq for deterministic restore.
func sortDeliveriesBySeq(ds []*Delivery) {
	for i := 1; i < len(ds); i++ {
		for j := i; j > 0 && ds[j-1].Seq > ds[j].Seq; j-- {
			ds[j-1], ds[j] = ds[j], ds[j-1]
		}
	}
}
