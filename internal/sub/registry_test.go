package sub

import (
	"fmt"
	"sync"
	"testing"

	"github.com/guoyyit/webhook-delivery-go/internal/model"
)

func mkSub(id string, types ...string) *model.Subscription {
	return &model.Subscription{ID: id, URL: "https://example.com/" + id, EventTypes: types}
}

func TestMatchExactAndOrderIndependent(t *testing.T) {
	r := NewRegistry()
	r.Add(mkSub("sub-b", "order.created"))
	r.Add(mkSub("sub-a", "order.created"))
	r.Add(mkSub("sub-c", "order.cancelled"))

	got := r.Match("order.created")
	if len(got) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(got))
	}
	if got[0].ID != "sub-a" || got[1].ID != "sub-b" {
		t.Fatalf("matches not ordered by id: %s, %s", got[0].ID, got[1].ID)
	}
	for _, m := range got {
		if m.URL != "https://example.com/"+m.ID {
			t.Fatalf("bad url for %s", m.ID)
		}
	}

	// Matching is unaffected by registration order: rebuild in different order.
	r2 := NewRegistry()
	r2.Add(mkSub("sub-c", "order.cancelled"))
	r2.Add(mkSub("sub-a", "order.created"))
	r2.Add(mkSub("sub-b", "order.created"))
	got2 := r2.Match("order.created")
	if len(got2) != 2 || got2[0].ID != "sub-a" || got2[1].ID != "sub-b" {
		t.Fatal("match result changed with registration order")
	}
}

func TestMatchNoDuplicatesAndTypeFiltering(t *testing.T) {
	r := NewRegistry()
	r.Add(mkSub("sub-a", "order.created", "order.updated"))
	r.Add(mkSub("sub-b", "order.created"))

	got := r.Match("order.updated")
	if len(got) != 1 || got[0].ID != "sub-a" {
		t.Fatalf("expected only sub-a, got %v", got)
	}
	got = r.Match("order.deleted")
	if len(got) != 0 {
		t.Fatalf("expected no matches, got %v", got)
	}
}

func TestMatchWildcardEmptyTypes(t *testing.T) {
	r := NewRegistry()
	r.Add(mkSub("sub-all"))
	r.Add(mkSub("sub-one", "x"))
	if len(r.Match("anything")) != 1 {
		t.Fatal("empty event types should match every event")
	}
	if r.Match("anything")[0].ID != "sub-all" {
		t.Fatal("wrong subscriber")
	}
}

func TestMatchDisabledExcluded(t *testing.T) {
	r := NewRegistry()
	s := mkSub("sub-a", "x")
	s.Disabled = true
	r.Add(s)
	if len(r.Match("x")) != 0 {
		t.Fatal("disabled subscription must not match")
	}
}

func TestRemoveStopsMatching(t *testing.T) {
	r := NewRegistry()
	r.Add(mkSub("sub-a", "x"))
	if len(r.Match("x")) != 1 {
		t.Fatal("expected match before remove")
	}
	r.Remove("sub-a")
	if len(r.Match("x")) != 0 {
		t.Fatal("unsubscribed endpoint must not receive events")
	}
	if _, ok := r.Get("sub-a"); ok {
		t.Fatal("removed subscription still present")
	}
}

func TestAddReplacesSubscription(t *testing.T) {
	r := NewRegistry()
	r.Add(mkSub("sub-a", "x"))
	r.Add(mkSub("sub-a", "y"))
	if len(r.Match("x")) != 0 {
		t.Fatal("replaced subscription should no longer match x")
	}
	if len(r.Match("y")) != 1 {
		t.Fatal("replaced subscription should match y")
	}
}

func TestConcurrentMatchAndMutate(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("sub-%02d", i)
			r.Add(mkSub(id, "x"))
			_ = r.Match("x")
			if i%2 == 0 {
				r.Remove(id)
			}
		}(i)
	}
	wg.Wait()
	// After all goroutines finish, even-numbered subs are gone.
	matches := r.Match("x")
	for _, m := range matches {
		if m.ID[len(m.ID)-1]%2 == '0' {
			t.Fatalf("removed subscription %s still matches", m.ID)
		}
	}
}
