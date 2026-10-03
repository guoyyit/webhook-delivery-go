package webhook

import (
	"sync"
	"time"
)

// worker serializes delivery processing for a single endpoint.
//
// Exactly one goroutine runs loop(), so deliveries for that endpoint are
// processed strictly in Seq order: the next delivery never starts before the
// current one is done (success, terminal dead-letter, or parked for retry).
// Different endpoints have different workers, hence independent goroutines —
// a slow endpoint blocks only itself. Publishers merely enqueue + signal a
// buffered (size 1) channel and therefore never block on slow endpoints.
type worker struct {
	d        *Dispatcher
	endpoint string

	mu    sync.Mutex
	queue []string // delivery IDs in Seq order

	notify chan struct{} // size 1, coalesced "queue changed" signal
	stop   chan struct{} // closed on shutdown
	done   chan struct{} // closed when loop() has exited
}

func newWorker(d *Dispatcher, endpointID string) *worker {
	return &worker{
		d:        d,
		endpoint: endpointID,
		notify:   make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// signal asks the worker to re-evaluate its queue (new delivery, unsubscribed,
// replay). It never blocks: signals coalesce because the worker always drains
// everything pending after waking.
func (w *worker) signal() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// enqueue inserts a delivery id keeping queue order by Seq. New events are
// normally already ordered, but replay/restoration may interleave.
func (w *worker) enqueue(id string, seq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, x := range w.queue {
		if x == id {
			return
		}
	}
	idx := len(w.queue)
	for i, qid := range w.queue {
		if dd, ok := w.d.store.Get(qid); ok && dd.Seq > seq {
			idx = i
			break
		}
	}
	w.queue = append(w.queue, "")
	copy(w.queue[idx+1:], w.queue[idx:len(w.queue)-1])
	w.queue[idx] = id
}

// popHead removes and returns the first queued id. Returns "" when empty.
func (w *worker) popHead() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.queue) == 0 {
		return ""
	}
	id := w.queue[0]
	w.queue = w.queue[1:]
	return id
}

// pushFront reinserts a delivery at the head (used after a parked retry or
// on shutdown rollback) so nothing can overtake it.
func (w *worker) pushFront(id string) {
	w.mu.Lock()
	for _, x := range w.queue {
		if x == id {
			w.mu.Unlock()
			return
		}
	}
	w.queue = append([]string{id}, w.queue...)
	w.mu.Unlock()
}

// lenLocked returns the current queue length.
func (w *worker) size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.queue)
}

// start launches the worker goroutine.
func (w *worker) start() { go w.loop() }

// liveSub reports whether the delivery's subscription is still registered.
// Removed subscriptions must not receive further attempts.
func (w *worker) liveSub(dl *Delivery) bool {
	w.d.registry.mu.RLock()
	_, ok := w.d.registry.subs[dl.SubscriptionID]
	w.d.registry.mu.RUnlock()
	return ok
}

// loop is the strict-order delivery state machine for one endpoint.
func (w *worker) loop() {
	defer close(w.done)
	for {
		id, until := w.readyHead()
		if id == "" {
			// Nothing runnable: wait for a queue signal, the parked retry
			// timer, or shutdown.
			var timerC <-chan time.Time
			if !until.IsZero() {
				timerC = w.d.cfg.Clock.After(until.Sub(w.d.cfg.Clock.Now()))
			}
			select {
			case <-w.stop:
				return
			case <-w.notify:
			case <-timerC:
			}
			continue
		}

		// A runnable head exists; take it out of the queue while in flight so
		// a later failure can reinsert it at the front. Ordering is kept
		// because nothing else can overtake it.
		_ = w.popHead()

		dl, ok := w.d.store.Get(id)
		if !ok {
			continue
		}
		if !w.liveSub(dl) {
			w.d.finishDead(dl, ReasonUnsubscribed)
			continue
		}
		if dl.deadlinePassed(w.d.cfg.Clock.Now()) {
			w.d.finishDead(dl, ReasonDeadline)
			continue
		}

		cb := w.d.circuit(w.endpoint)
		if !cb.Allow() {
			// Isolated endpoint: per policy, do not silently drop. Hand the
			// event to the dead-letter queue while the breaker is open.
			w.d.finishDead(dl, ReasonCircuitOpen)
			continue
		}

		outcome := w.d.attempt(dl)
		// Only real attempts move the breaker; shutdown rolls back and the
		// outcomeDead terminal transitions are not endpoint-health signals.
		if outcome.kind != outcomeShutdown && outcome.kind != outcomeDead {
			cb.NoteResult(outcome.retryable)
		}

		switch outcome.kind {
		case outcomeShutdown:
			// Rolling back to pending preserves at-least-once semantics:
			// nothing is confirmed, state is persisted before exit.
			w.d.rollbackToPending(dl)
			w.pushFront(dl.ID)
			<-w.stop // park until shutdown completes
			return
		case outcomeSuccess:
			w.d.finishSuccess(dl, outcome.at)
		case outcomeTerminal:
			w.d.finishDead(dl, ReasonNonRetryable)
		case outcomeDead:
			// Terminal transition already persisted inside attempt().
		case outcomeRetry:
			if w.d.shouldDead(dl, outcome) {
				w.d.deadWithReason(dl, outcome)
			} else {
				w.d.scheduleRetry(dl, outcome)
				// Park strictly behind this delivery: it goes back to the
				// front and everything behind it stays blocked, preserving
				// per-endpoint order across retries.
				w.pushFront(dl.ID)
			}
		}
	}
}

// readyHead returns the first queue entry eligible to attempt now, plus the
// time when the current head will become eligible (zero if none is parked or
// the queue is empty).
func (w *worker) readyHead() (string, time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.queue) == 0 {
		return "", time.Time{}
	}
	id := w.queue[0]
	dl, ok := w.d.store.Get(id)
	if !ok {
		// Vanished (retention/truncation): drop and inspect the next one.
		w.queue = w.queue[1:]
		return w.readyHead()
	}
	now := w.d.cfg.Clock.Now()
	if dl.NextAttemptAt.IsZero() || !now.Before(dl.NextAttemptAt) {
		return id, time.Time{}
	}
	return "", dl.NextAttemptAt
}
