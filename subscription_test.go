package webhook

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRegistryMatchFanOut(t *testing.T) {
	r := NewRegistry()
	must(t, r.AddEndpoint(Endpoint{ID: "e1", URL: "http://1", Secret: []byte("k1")}))
	must(t, r.AddEndpoint(Endpoint{ID: "e2", URL: "http://2", Secret: []byte("k2")}))
	must(t, r.AddEndpoint(Endpoint{ID: "e3", URL: "http://3", Secret: []byte("k3")}))

	// Register in deliberately scrambled order; matching must be order-independent.
	must(t, r.PutSubscription(Subscription{ID: "s-b", EndpointID: "e2", EventTypes: []string{"order.created"}}))
	must(t, r.PutSubscription(Subscription{ID: "s-c", EndpointID: "e3", EventTypes: []string{"*"}}))
	must(t, r.PutSubscription(Subscription{ID: "s-a", EndpointID: "e1", EventTypes: []string{"order.created", "order.paid"}}))

	got := r.Matches("order.created")
	ids := subIDs(got)
	want := []string{"s-a", "s-b", "s-c"}
	if !equalStrings(ids, want) {
		t.Fatalf("matches = %v, want %v", ids, want)
	}

	paid := subIDs(r.Matches("order.paid"))
	if !equalStrings(paid, []string{"s-a", "s-c"}) {
		t.Fatalf("paid matches = %v", paid)
	}

	unknown := r.Matches("unknown.type") // only wildcard s-c
	if len(unknown) != 1 || unknown[0].ID != "s-c" {
		t.Fatalf("unknown type should match wildcard only, got %v", subIDs(unknown))
	}
}

func TestRegistryUnsubscribeStopsMatching(t *testing.T) {
	r := NewRegistry()
	must(t, r.AddEndpoint(Endpoint{ID: "e1", URL: "http://1", Secret: []byte("k")}))
	must(t, r.PutSubscription(Subscription{ID: "s1", EndpointID: "e1", EventTypes: []string{"x"}}))
	if len(r.Matches("x")) != 1 {
		t.Fatal("expected one match before unsubscribe")
	}
	r.RemoveSubscription("s1")
	if len(r.Matches("x")) != 0 {
		t.Fatal("expected no matches after unsubscribe")
	}
	r.RemoveSubscription("s1") // removing again is harmless
}

func TestRegistryConcurrentRegisterAndMatch(t *testing.T) {
	// Snapshot matching must stay consistent under concurrent registration
	// (also exercised under -race).
	r := NewRegistry()
	must(t, r.AddEndpoint(Endpoint{ID: "e0", URL: "http://0", Secret: []byte("k")}))

	stop := make(chan struct{})
	var writers, readers sync.WaitGroup

	for i := 0; i < 20; i++ {
		writers.Add(1)
		go func(i int) {
			defer writers.Done()
			epID := fmt.Sprintf("e%d", i)
			if err := r.AddEndpoint(Endpoint{ID: epID, URL: "http://" + epID, Secret: []byte("k")}); err != nil {
				t.Error(err)
				return
			}
			if err := r.PutSubscription(Subscription{
				ID: fmt.Sprintf("s%d", i), EndpointID: epID,
				EventTypes: []string{"t"}, CreatedAt: time.Now(),
			}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = subIDs(r.Matches("t"))
				}
			}
		}()
	}

	writers.Wait()
	close(stop)
	readers.Wait()

	got := subIDs(r.Matches("t"))
	if len(got) != 20 {
		t.Fatalf("after concurrent registration want 20 matches, got %d", len(got))
	}
}

func subIDs(ss []Subscription) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = s.ID
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
