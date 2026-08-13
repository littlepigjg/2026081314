package event

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

type Handler struct {
	store *Store
	mux   *http.ServeMux
}

func NewHandler(store *Store, mux *http.ServeMux) *Handler {
	h := &Handler{store: store, mux: mux}
	mux.HandleFunc("/event", h.HandleEvent)
	mux.HandleFunc("/events/", h.HandleEventByID)
	return h
}

func (h *Handler) HandleEvent(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.handlePublish(w, r)
	case http.MethodGet:
		h.handleListEvents(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handlePublish(w http.ResponseWriter, r *http.Request) {
	var req PublishRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.Topic == "" {
		writeError(w, http.StatusBadRequest, "topic is required")
		return
	}

	if err := ValidateTopic(req.Topic); err != nil {
		writeError(w, http.StatusBadRequest, "invalid topic: "+err.Error())
		return
	}

	if req.Payload == nil {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}

	retention := req.RetentionSeconds
	if retention <= 0 {
		retention = DefaultRetentionSeconds
	}

	eventID, err := h.store.Publish(req.Topic, req.Payload, retention)
	if err != nil {
		log.Printf("[Handler] Failed to publish event: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to publish event")
		return
	}

	resp := PublishResponse{
		EventID:     eventID,
		PublishedAt: time.Now().UTC(),
	}

	log.Printf("[Handler] Published event %s on topic %s", eventID, req.Topic)
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleListEvents(w http.ResponseWriter, r *http.Request) {
	topicFilter := r.URL.Query().Get("topic")
	events := h.store.Events()

	var result []Event
	for _, e := range events {
		if topicFilter != "" && e.Topic != topicFilter {
			continue
		}
		if !e.IsExpired() {
			result = append(result, *e)
		}
	}

	if result == nil {
		result = make([]Event, 0)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"events": result,
		"count":  len(result),
	})
}

func (h *Handler) HandleEventByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/events/")
	if path == "" {
		writeError(w, http.StatusBadRequest, "event ID is required")
		return
	}

	event, err := h.store.GetEvent(path)
	if err != nil {
		writeError(w, http.StatusNotFound, "event not found")
		return
	}

	status := "active"
	if event.IsExpired() {
		status = "expired"
	}

	resp := EventDetailResponse{
		Event:  *event,
		Status: status,
	}

	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("[Handler] Failed to write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{
		"error": msg,
	})
}

func parseEventIDFromPath(path string) (string, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid path: %s", path)
	}
	return parts[2], nil
}

func validatePublishRequest(req *PublishRequest) error {
	if req.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if req.Payload == nil {
		return fmt.Errorf("payload is required")
	}
	if err := ValidateTopic(req.Topic); err != nil {
		return err
	}
	return nil
}
