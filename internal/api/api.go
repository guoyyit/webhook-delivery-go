// Package api exposes the delivery engine over HTTP.
package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/guoyyit/webhook-delivery-go/internal/engine"
	"github.com/guoyyit/webhook-delivery-go/internal/model"
	"github.com/guoyyit/webhook-delivery-go/internal/store"
)

// Handler holds the HTTP API state.
type Handler struct {
	engine *engine.Engine
}

// New creates an API handler.
func New(eng *engine.Engine) *Handler {
	return &Handler{engine: eng}
}

// Routes returns the configured mux.
func (h *Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.handleHealth)
	mux.HandleFunc("/v1/subscriptions", h.handleSubscriptions)
	mux.HandleFunc("/v1/subscriptions/", h.handleSubscriptionByID)
	mux.HandleFunc("/v1/events", h.handleEvents)
	mux.HandleFunc("/v1/deliveries", h.handleDeliveries)
	mux.HandleFunc("/v1/replay", h.handleReplay)
	return mux
}

type subscriptionRequest struct {
	ID         string   `json:"id"`
	URL        string   `json:"url"`
	EventTypes []string `json:"event_types"`
	Secret     string   `json:"secret"`
	Disabled   bool     `json:"disabled"`
}

type eventRequest struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type replayRequest struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"subscriptions": h.engine.ListSubscriptions(),
		})
	case http.MethodPost:
		var req subscriptionRequest
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
		sub := &model.Subscription{
			ID:         req.ID,
			URL:        req.URL,
			EventTypes: req.EventTypes,
			Secret:     req.Secret,
			Disabled:   req.Disabled,
		}
		if err := h.engine.Subscribe(r.Context(), sub); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, sub)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) handleSubscriptionByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/subscriptions/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	switch r.Method {
	case http.MethodDelete:
		if err := h.engine.Unsubscribe(r.Context(), id); err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req eventRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	ev := &model.Event{
		ID:      req.ID,
		Type:    req.Type,
		Payload: req.Payload,
	}
	stored, deliveries, err := h.engine.Ingest(r.Context(), ev)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"event":      stored,
		"deliveries": deliveries,
	})
}

func (h *Handler) handleDeliveries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	f := store.DeliveryFilter{
		EndpointURL:    q.Get("endpoint"),
		SubscriptionID: q.Get("subscription_id"),
		EventID:        q.Get("event_id"),
	}
	for _, s := range q["status"] {
		f.Statuses = append(f.Statuses, model.DeliveryStatus(s))
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"deliveries": h.engine.ListDeliveries(f),
	})
}

func (h *Handler) handleReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req replayRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	n, err := h.engine.Replay(r.Context(), req.From, req.To)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"replayed": n})
}

func decodeJSON(r *http.Request, v interface{}) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if len(body) == 0 {
		return io.EOF
	}
	return json.Unmarshal(body, v)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
