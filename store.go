package webhook

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// storeData is the on-disk JSON layout. Everything needed to resume after a
// restart lives here: sequence counter and every non-truncated delivery.
type storeData struct {
	Version    int                  `json:"version"`
	NextSeqVal uint64               `json:"next_seq"`
	Deliveries map[string]*Delivery `json:"deliveries"`
}

const storeVersion = 1

// Store persists delivery progress and the dead-letter queue so a restart
// can resume unfinished deliveries without re-delivering confirmed ones.
// Writes are serialized in-process and made atomic with a temp file + rename,
// so an interrupted flush can never leave a half-written/corrupt file: the
// previous complete file stays in place.
type Store struct {
	mu   sync.Mutex
	path string
	data storeData
}

// OpenStore opens (or creates) the persistence file at path.
// An empty path yields an in-memory store (still crash-consistent within the
// process, used by some tests).
func OpenStore(path string) (*Store, error) {
	s := &Store{
		path: path,
		data: storeData{Version: storeVersion, Deliveries: map[string]*Delivery{}},
	}
	if path == "" {
		return s, nil
	}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		var loaded storeData
		if err := json.Unmarshal(raw, &loaded); err != nil {
			// Do not clobber an unreadable file; fail loudly so an operator
			// can investigate instead of silently starting from a blank slate.
			return nil, fmt.Errorf("store %s corrupt: %w", path, err)
		}
		if loaded.Version != storeVersion {
			return nil, fmt.Errorf("store %s unsupported version %d", path, loaded.Version)
		}
		if loaded.Deliveries == nil {
			loaded.Deliveries = map[string]*Delivery{}
		}
		s.data = loaded
	case os.IsNotExist(err):
		if err := s.flushLocked(); err != nil {
			return nil, err
		}
	default:
		return nil, err
	}
	return s, nil
}

// flushLocked writes the whole state to a temp file and renames it into place.
// Caller must hold mu.
func (s *Store) flushLocked() error {
	if s.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(&s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, s.path)
}

// Close flushes pending state. With write-through semantics it currently only
// needs to exist for symmetry/future batching.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

// Load returns a snapshot copy of every retained delivery. In-flight
// deliveries are left as-is; the dispatcher rolls them back to pending when
// recovering (their outcome after a crash is unknowable, so pending preserves
// at-least-once delivery — downstream dedup via the stable delivery id makes
// the possible duplicate harmless).
func (s *Store) Load() ([]*Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Delivery, 0, len(s.data.Deliveries))
	for _, d := range s.data.Deliveries {
		out = append(out, cloneDelivery(d))
	}
	return out, nil
}

// Put persists one delivery (write-through). Callers must not mutate d after
// handing it over; the store keeps its own copy.
func (s *Store) Put(d *Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Deliveries[d.ID] = cloneDelivery(d)
	return s.flushLocked()
}

// Get returns a stored delivery by id (copy).
func (s *Store) Get(id string) (*Delivery, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.data.Deliveries[id]
	if !ok {
		return nil, false
	}
	return cloneDelivery(d), true
}

// NextSeq returns a process-wide monotonic sequence for per-endpoint ordering
// and persists the advance before the caller can use it.
func (s *Store) NextSeq() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.NextSeqVal++
	if err := s.flushLocked(); err != nil {
		s.data.NextSeqVal-- // roll back counter on failed persistence
		return 0, err
	}
	return s.data.NextSeqVal, nil
}

// DeadLetter marks a delivery terminal with the given reason code and persists it.
func (s *Store) DeadLetter(d *Delivery, reason string) error {
	dd := cloneDelivery(d)
	dd.Status = StatusDead
	dd.DeadReason = reason
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Deliveries[dd.ID] = dd
	return s.flushLocked()
}

// DeadLetters returns all dead-lettered deliveries, newest Seq first.
func (s *Store) DeadLetters() []*Delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Delivery, 0)
	for _, d := range s.data.Deliveries {
		if d.Status == StatusDead {
			out = append(out, cloneDelivery(d))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq > out[j].Seq })
	return out
}

// ErrNotFound / ErrNotDeadLetter bound replay to retained terminal history.
var (
	ErrNotFound      = errors.New("delivery not found in retained history")
	ErrNotDeadLetter = errors.New("delivery is not in the dead-letter queue")
)

// ReplayDeadLetter requeues a retained dead-lettered delivery as pending with
// a fresh attempt budget. The stable delivery id and sequence are preserved,
// so a replay is still deduplicable downstream and stays in endpoint order.
// Only entries still inside the retention window can be replayed; truncated
// history yields ErrNotFound, giving replay an explicit boundary.
func (s *Store) ReplayDeadLetter(id string) (*Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.data.Deliveries[id]
	if !ok {
		return nil, ErrNotFound
	}
	if d.Status != StatusDead {
		return nil, ErrNotDeadLetter
	}
	rd := cloneDelivery(d)
	rd.Status = StatusPending
	rd.Attempts = nil
	rd.NextAttemptAt = zeroTime()
	rd.Deadline = zeroTime()
	rd.DeadReason = ""
	rd.SucceededAt = nil
	s.data.Deliveries[rd.ID] = rd
	if err := s.flushLocked(); err != nil {
		return nil, err
	}
	return cloneDelivery(rd), nil
}

// TruncateHistory bounds replay/dedup memory: only the newest `keep` terminal
// (succeeded or dead) deliveries are retained. Pending/in-flight deliveries
// are never removed.
func (s *Store) TruncateHistory(keep int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	term := make([]*Delivery, 0)
	for _, d := range s.data.Deliveries {
		if d.Status == StatusSucceeded || d.Status == StatusDead {
			term = append(term, d)
		}
	}
	if len(term) <= keep {
		return
	}
	sort.Slice(term, func(i, j int) bool { return term[i].Seq > term[j].Seq })
	for _, d := range term[keep:] {
		delete(s.data.Deliveries, d.ID)
	}
	_ = s.flushLocked()
}

func zeroTime() time.Time { return time.Time{} }
