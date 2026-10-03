package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/guoyyit/webhook-delivery-go/internal/model"
)

func openTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return s, dir
}

func TestEventAppendAndOrdering(t *testing.T) {
	s, _ := openTestStore(t)
	defer s.Close()

	e1 := &model.Event{Type: "a", Payload: []byte("1")}
	e2 := &model.Event{Type: "b", Payload: []byte("2")}
	if err := s.AppendEvent(e1); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvent(e2); err != nil {
		t.Fatal(err)
	}
	if e1.Seq != 1 || e2.Seq != 2 {
		t.Fatalf("expected seqs 1,2 got %d,%d", e1.Seq, e2.Seq)
	}
	if e1.ID == "" {
		t.Fatal("event id not generated")
	}
	// Idempotent append: same id returns the stored event.
	dup := &model.Event{ID: e1.ID, Type: "different"}
	if err := s.AppendEvent(dup); err != nil {
		t.Fatal(err)
	}
	if dup.Type != "a" || dup.Seq != 1 {
		t.Fatal("duplicate event id must return stored event unchanged")
	}
	evs := s.ListEvents()
	if len(evs) != 2 || evs[0].ID != e1.ID || evs[1].ID != e2.ID {
		t.Fatal("events not ordered by seq")
	}
}

func TestSubscriptionCRUD(t *testing.T) {
	s, _ := openTestStore(t)
	defer s.Close()

	sub := &model.Subscription{URL: "https://e.example/hook", EventTypes: []string{"a"}}
	if err := s.PutSubscription(sub); err != nil {
		t.Fatal(err)
	}
	if sub.ID == "" {
		t.Fatal("subscription id not generated")
	}
	got, ok := s.GetSubscription(sub.ID)
	if !ok || got.URL != sub.URL {
		t.Fatal("subscription not found")
	}
	if err := s.DeleteSubscription(sub.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.GetSubscription(sub.ID); ok {
		t.Fatal("subscription not deleted")
	}
}

func TestDeliveryLifecycleAndNextPending(t *testing.T) {
	s, _ := openTestStore(t)
	defer s.Close()

	ev := &model.Event{Type: "a"}
	if err := s.AppendEvent(ev); err != nil {
		t.Fatal(err)
	}
	d1 := &model.Delivery{EventID: ev.ID, EventSeq: ev.Seq, EventType: "a",
		SubscriptionID: "s1", EndpointURL: "https://e1.example", Status: model.StatusPending}
	d2 := &model.Delivery{EventID: ev.ID, EventSeq: ev.Seq, EventType: "a",
		SubscriptionID: "s2", EndpointURL: "https://e2.example", Status: model.StatusPending}
	if err := s.PutDelivery(d1); err != nil {
		t.Fatal(err)
	}
	if err := s.PutDelivery(d2); err != nil {
		t.Fatal(err)
	}
	if d1.ID == d2.ID {
		t.Fatal("deliveries to different subscriptions need distinct ids")
	}
	if got, ok := s.NextPendingForEndpoint("https://e1.example"); !ok || got.ID != d1.ID {
		t.Fatal("next pending mismatch")
	}
	d1.Status = model.StatusSucceeded
	if err := s.PutDelivery(d1); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.NextPendingForEndpoint("https://e1.example"); ok {
		t.Fatal("succeeded delivery must not be pending")
	}
	if got, ok := s.NextPendingForEndpoint("https://e2.example"); !ok || got.ID != d2.ID {
		t.Fatal("e2 pending delivery missing")
	}
}

func TestDeliveryIDDeterministic(t *testing.T) {
	a := DeliveryID("evt_1", "sub_1")
	b := DeliveryID("evt_1", "sub_1")
	c := DeliveryID("evt_1", "sub_2")
	if a != b {
		t.Fatal("same event+subscription must produce the same delivery id")
	}
	if a == c {
		t.Fatal("different subscriptions need different delivery ids")
	}
}

func TestReopenPersistsState(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ev := &model.Event{Type: "a", Payload: []byte("x")}
	if err := s.AppendEvent(ev); err != nil {
		t.Fatal(err)
	}
	sub := &model.Subscription{URL: "https://e.example", EventTypes: []string{"a"}, Secret: "sec"}
	if err := s.PutSubscription(sub); err != nil {
		t.Fatal(err)
	}
	d := &model.Delivery{EventID: ev.ID, EventSeq: ev.Seq, EventType: "a",
		SubscriptionID: sub.ID, EndpointURL: sub.URL, Status: model.StatusInProgress}
	if err := s.PutDelivery(d); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got, ok := s2.GetEvent(ev.ID); !ok || string(got.Payload) != "x" {
		t.Fatal("event lost after reopen")
	}
	if got, ok := s2.GetSubscription(sub.ID); !ok || got.Secret != "sec" {
		t.Fatal("subscription lost after reopen")
	}
	if got, ok := s2.GetDelivery(d.ID); !ok || got.Status != model.StatusInProgress {
		t.Fatal("delivery lost after reopen")
	}
}

func TestReopenTornFinalLineIgnored(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ev := &model.Event{Type: "a"}
	if err := s.AppendEvent(ev); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash mid-append: a partial JSON line at the end of the log.
	f, err := os.OpenFile(filepath.Join(dir, eventsFile), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"id":"evt_999","type":"a","paylo`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with torn line: %v", err)
	}
	defer s2.Close()
	if _, ok := s2.GetEvent(ev.ID); !ok {
		t.Fatal("valid event lost despite torn final line")
	}
	if _, ok := s2.GetEvent("evt_999"); ok {
		t.Fatal("torn event must not be loaded")
	}
}

func TestReopenCorruptSnapshotFails(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sub := &model.Subscription{URL: "https://e.example"}
	if err := s.PutSubscription(sub); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// A complete-but-unparseable snapshot means something is badly wrong;
	// fail fast rather than silently losing data.
	if err := os.WriteFile(filepath.Join(dir, subscriptionsFile), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("expected error for corrupt snapshot")
	}
}

func TestPendingEndpoints(t *testing.T) {
	s, _ := openTestStore(t)
	defer s.Close()
	ev := &model.Event{Type: "a"}
	s.AppendEvent(ev)
	for _, url := range []string{"https://a.example", "https://b.example"} {
		d := &model.Delivery{EventID: ev.ID, EventSeq: ev.Seq, EventType: "a",
			SubscriptionID: "s-" + url, EndpointURL: url, Status: model.StatusPending}
		s.PutDelivery(d)
	}
	got := s.PendingEndpoints()
	if len(got) != 2 {
		t.Fatalf("expected 2 pending endpoints, got %v", got)
	}
}

func TestListDeliveriesFilter(t *testing.T) {
	s, _ := openTestStore(t)
	defer s.Close()
	ev := &model.Event{Type: "a"}
	s.AppendEvent(ev)
	d := &model.Delivery{EventID: ev.ID, EventSeq: ev.Seq, EventType: "a",
		SubscriptionID: "s1", EndpointURL: "https://a.example", Status: model.StatusPending}
	s.PutDelivery(d)
	if len(s.ListDeliveries(DeliveryFilter{Statuses: []model.DeliveryStatus{model.StatusSucceeded}})) != 0 {
		t.Fatal("filter mismatch")
	}
	if len(s.ListDeliveries(DeliveryFilter{EndpointURL: "https://a.example"})) != 1 {
		t.Fatal("endpoint filter mismatch")
	}
	if len(s.ListDeliveries(DeliveryFilter{SubscriptionID: "s1"})) != 1 {
		t.Fatal("subscription filter mismatch")
	}
	if len(s.ListDeliveries(DeliveryFilter{EventID: ev.ID})) != 1 {
		t.Fatal("event filter mismatch")
	}
}

var _ = time.Now
