// Package store persists events, subscriptions and deliveries to disk.
//
// Persistence strategy:
//   - Events are appended to an append-only log (one JSON object per line,
//     fsynced after every append). A torn final line caused by a crash is
//     ignored on load, so an interrupted append never corrupts state.
//   - Subscriptions and deliveries are kept as JSON snapshots. Writes go to a
//     temp file in the same directory, which is fsynced and then atomically
//     renamed over the real file; an interrupted write therefore leaves
//     either the old or the new complete state, never a partial one.
package store

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/guoyyit/webhook-delivery-go/internal/model"
)

const (
	eventsFile        = "events.log"
	subscriptionsFile = "subscriptions.json"
	deliveriesFile    = "deliveries.json"
)

// Now is the timestamp source used for newly persisted records. Tests may
// override it.
var Now = time.Now

// Store is the durable persistence layer. All methods are safe for
// concurrent use.
type Store struct {
	mu            sync.Mutex
	dir           string
	events        map[string]*model.Event
	eventSeq      int64
	subscriptions map[string]*model.Subscription
	deliveries    map[string]*model.Delivery
}

// Open opens (or creates) a store rooted at dir.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: create dir: %w", err)
	}
	s := &Store{
		dir:           dir,
		events:        make(map[string]*model.Event),
		subscriptions: make(map[string]*model.Subscription),
		deliveries:    make(map[string]*model.Delivery),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close flushes and releases the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nil
}

// AppendEvent stores a new event, assigning it a monotonic sequence number
// and creation timestamp. Events with an existing id are returned unchanged.
func (s *Store) AppendEvent(ev *model.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ev.ID != "" {
		if existing, ok := s.events[ev.ID]; ok {
			*ev = *existing
			return nil
		}
	}
	s.eventSeq++
	ev.Seq = s.eventSeq
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = Now()
	}
	if ev.ID == "" {
		ev.ID = newEventID()
	}
	cp := *ev
	if err := s.appendEventLog(&cp); err != nil {
		s.eventSeq--
		return err
	}
	s.events[cp.ID] = &cp
	*ev = cp
	return nil
}

// GetEvent returns the event with the given id.
func (s *Store) GetEvent(id string) (*model.Event, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.events[id]
	if !ok {
		return nil, false
	}
	cp := *ev
	return &cp, true
}

// ListEvents returns all events ordered by sequence number.
func (s *Store) ListEvents() []*model.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*model.Event, 0, len(s.events))
	for _, ev := range s.events {
		cp := *ev
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// PutSubscription upserts a subscription.
func (s *Store) PutSubscription(sub *model.Subscription) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *sub
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = Now()
	}
	if cp.ID == "" {
		cp.ID = newID("sub")
	}
	s.subscriptions[cp.ID] = &cp
	if err := s.atomicWrite(subscriptionsFile, s.subscriptions); err != nil {
		delete(s.subscriptions, cp.ID)
		return err
	}
	*sub = cp
	return nil
}

// DeleteSubscription removes a subscription by id.
func (s *Store) DeleteSubscription(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[id]; !ok {
		return nil
	}
	removed := s.subscriptions[id]
	delete(s.subscriptions, id)
	if err := s.atomicWrite(subscriptionsFile, s.subscriptions); err != nil {
		s.subscriptions[id] = removed
		return err
	}
	return nil
}

// GetSubscription returns the subscription with the given id.
func (s *Store) GetSubscription(id string) (*model.Subscription, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subscriptions[id]
	if !ok {
		return nil, false
	}
	cp := *sub
	return &cp, true
}

// ListSubscriptions returns all subscriptions.
func (s *Store) ListSubscriptions() []*model.Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*model.Subscription, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		cp := *sub
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// PutDelivery upserts a delivery.
func (s *Store) PutDelivery(d *model.Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d.ID == "" {
		d.ID = DeliveryID(d.EventID, d.SubscriptionID)
	}
	if d.CreatedAt.IsZero() {
		d.CreatedAt = Now()
	}
	d.UpdatedAt = Now()
	cp := *d
	s.deliveries[cp.ID] = &cp
	if err := s.atomicWrite(deliveriesFile, s.deliveries); err != nil {
		delete(s.deliveries, cp.ID)
		return err
	}
	*d = cp
	return nil
}

// GetDelivery returns the delivery with the given id.
func (s *Store) GetDelivery(id string) (*model.Delivery, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deliveries[id]
	if !ok {
		return nil, false
	}
	cp := *d
	return &cp, true
}

// DeliveryFilter selects deliveries in ListDeliveries.
type DeliveryFilter struct {
	EndpointURL    string
	SubscriptionID string
	EventID        string
	Statuses       []model.DeliveryStatus
}

// ListDeliveries returns deliveries matching the filter, ordered by event
// sequence then delivery id.
func (s *Store) ListDeliveries(f DeliveryFilter) []*model.Delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*model.Delivery, 0)
	for _, d := range s.deliveries {
		if f.EndpointURL != "" && d.EndpointURL != f.EndpointURL {
			continue
		}
		if f.SubscriptionID != "" && d.SubscriptionID != f.SubscriptionID {
			continue
		}
		if f.EventID != "" && d.EventID != f.EventID {
			continue
		}
		if len(f.Statuses) > 0 && !containsStatus(f.Statuses, d.Status) {
			continue
		}
		cp := *d
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventSeq != out[j].EventSeq {
			return out[i].EventSeq < out[j].EventSeq
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// NextPendingForEndpoint returns the oldest pending delivery for an endpoint,
// ordered by event sequence.
func (s *Store) NextPendingForEndpoint(endpoint string) (*model.Delivery, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *model.Delivery
	for _, d := range s.deliveries {
		if d.EndpointURL != endpoint || d.Status != model.StatusPending {
			continue
		}
		if best == nil || d.EventSeq < best.EventSeq ||
			(d.EventSeq == best.EventSeq && d.ID < best.ID) {
			best = d
		}
	}
	if best == nil {
		return nil, false
	}
	cp := *best
	return &cp, true
}

// PendingEndpoints returns the endpoint URLs that have pending deliveries.
func (s *Store) PendingEndpoints() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]struct{})
	for _, d := range s.deliveries {
		if d.Status == model.StatusPending {
			seen[d.EndpointURL] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for e := range seen {
		out = append(out, e)
	}
	sort.Strings(out)
	return out
}

// DeliveryID returns the stable, deterministic id for the delivery of an event
// to a subscription. Retries and restarts compute the same id, so downstream
// consumers can use it as an idempotency key.
func DeliveryID(eventID, subscriptionID string) string {
	sum := sha256.Sum256([]byte(eventID + "|" + subscriptionID))
	return "del_" + hex.EncodeToString(sum[:])[:24]
}

// --- persistence internals ---

func (s *Store) load() error {
	if err := s.loadEvents(); err != nil {
		return err
	}
	if err := s.loadSnapshot(subscriptionsFile, &s.subscriptions); err != nil {
		return err
	}
	if err := s.loadSnapshot(deliveriesFile, &s.deliveries); err != nil {
		return err
	}
	for _, ev := range s.events {
		if ev.Seq > s.eventSeq {
			s.eventSeq = ev.Seq
		}
	}
	return nil
}

func (s *Store) loadEvents() error {
	path := filepath.Join(s.dir, eventsFile)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: read events: %w", err)
	}
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var ev model.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			if i == len(lines)-1 || (i == len(lines)-2 && lines[len(lines)-1] == "") {
				// A torn final line from a crashed append is safe to drop.
				break
			}
			return fmt.Errorf("store: %s line %d: %w", eventsFile, i+1, err)
		}
		s.events[ev.ID] = &ev
	}
	return nil
}

func (s *Store) loadSnapshot(name string, target interface{}) error {
	path := filepath.Join(s.dir, name)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: read %s: %w", name, err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("store: parse %s: %w", name, err)
	}
	return nil
}

func (s *Store) appendEventLog(ev *model.Event) error {
	f, err := os.OpenFile(filepath.Join(s.dir, eventsFile),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("store: open events: %w", err)
	}
	defer f.Close()
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("store: append event: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("store: fsync event: %w", err)
	}
	return nil
}

// atomicWrite writes a JSON snapshot to a temp file, fsyncs it, and renames it
// over the destination so the destination is always a complete file.
func (s *Store) atomicWrite(name string, v interface{}) error {
	final := filepath.Join(s.dir, name)
	tmp, err := os.CreateTemp(s.dir, name+".tmp-*")
	if err != nil {
		return fmt.Errorf("store: create temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { os.Remove(tmpName) }

	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		tmp.Close()
		cleanup()
		return fmt.Errorf("store: encode %s: %w", name, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return fmt.Errorf("store: fsync %s: %w", name, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("store: close %s: %w", name, err)
	}
	if err := os.Rename(tmpName, final); err != nil {
		cleanup()
		return fmt.Errorf("store: rename %s: %w", name, err)
	}
	// fsync the directory so the rename itself is durable.
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("store: sync dir: %w", err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func containsStatus(statuses []model.DeliveryStatus, s model.DeliveryStatus) bool {
	for _, st := range statuses {
		if st == s {
			return true
		}
	}
	return false
}

func newEventID() string { return newID("evt") }

func newID(prefix string) string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return prefix + "_" + hex.EncodeToString(b)
}
