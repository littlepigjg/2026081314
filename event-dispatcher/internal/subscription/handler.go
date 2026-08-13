package subscription

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"event-dispatcher/internal/event"
)

type Handler struct {
	store *event.Store
	mux   *http.ServeMux
}

func NewHandler(store *event.Store, mux *http.ServeMux) *Handler {
	h := &Handler{store: store, mux: mux}
	mux.HandleFunc("/subscribe", h.HandleSubscribe)
	mux.HandleFunc("/pull", h.HandlePull)
	return h
}

func (h *Handler) HandleSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SubscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.SubscriberID == "" {
		writeError(w, http.StatusBadRequest, "subscriber_id is required")
		return
	}

	if req.TopicPattern == "" {
		writeError(w, http.StatusBadRequest, "topic_pattern is required")
		return
	}

	if err := ValidatePattern(req.TopicPattern); err != nil {
		writeError(w, http.StatusBadRequest, "invalid topic_pattern: "+err.Error())
		return
	}

	subID, matchedTopics, err := h.store.CreateSubscription(req.SubscriberID, req.TopicPattern, req.HistoryLimit)
	if err != nil {
		log.Printf("[SubHandler] Failed to create subscription: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to create subscription")
		return
	}

	resp := SubscribeResponse{
		SubscriptionID: subID,
		MatchedTopics:  matchedTopics,
	}

	log.Printf("[SubHandler] Created subscription %s for %s with pattern %s", subID, req.SubscriberID, req.TopicPattern)
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandlePull(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PullRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.SubscriptionID == "" {
		writeError(w, http.StatusBadRequest, "subscription_id is required")
		return
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	events, hasMore, err := h.store.Pull(req.SubscriptionID, limit)
	if err != nil {
		if strings.Contains(err.Error(), "subscription not found") {
			writeError(w, http.StatusNotFound, err.Error())
		} else {
			log.Printf("[SubHandler] Failed to pull events: %v", err)
			writeError(w, http.StatusInternalServerError, "failed to pull events")
		}
		return
	}

	pulledEvents := make([]PulledEvent, 0, len(events))
	for _, e := range events {
		pulledEvents = append(pulledEvents, PulledEvent{
			EventID:     e.ID,
			Topic:       e.Topic,
			Payload:     e.Payload,
			PublishedAt: e.PublishedAt,
		})
	}

	resp := PullResponse{
		Events:  pulledEvents,
		HasMore: hasMore,
	}

	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("[SubHandler] Failed to write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{
		"error": msg,
	})
}

func BuildSubscriptionResponse(subID string, matchedTopics []string) SubscribeResponse {
	if matchedTopics == nil {
		matchedTopics = make([]string, 0)
	}
	return SubscribeResponse{
		SubscriptionID: subID,
		MatchedTopics:  matchedTopics,
	}
}

func ValidateSubscribeRequest(req *SubscribeRequest) error {
	if req.SubscriberID == "" {
		return fmt.Errorf("subscriber_id is required")
	}
	if req.TopicPattern == "" {
		return fmt.Errorf("topic_pattern is required")
	}
	if err := ValidatePattern(req.TopicPattern); err != nil {
		return err
	}
	return nil
}

func ValidatePullRequest(req *PullRequest) error {
	if req.SubscriptionID == "" {
		return fmt.Errorf("subscription_id is required")
	}
	if req.Limit < 0 {
		return fmt.Errorf("limit must be non-negative")
	}
	if req.Limit > 100 {
		return fmt.Errorf("limit must not exceed 100")
	}
	return nil
}
