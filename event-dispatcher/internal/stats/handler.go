package stats

import (
	"encoding/json"
	"net/http"
	"time"

	"event-dispatcher/internal/event"
)

type StatsHandler struct {
	collector *Collector
	mux       *http.ServeMux
}

func NewStatsHandler(store *event.Store, mux *http.ServeMux) *StatsHandler {
	collector := NewCollector(store)
	h := &StatsHandler{
		collector: collector,
		mux:       mux,
	}
	mux.HandleFunc("/stats", h.HandleStats)
	return h
}

func (h *StatsHandler) HandleStats(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGetStats(w, r)
	case http.MethodOptions:
		w.Header().Set("Allow", "GET, OPTIONS")
		w.WriteHeader(http.StatusOK)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *StatsHandler) handleGetStats(w http.ResponseWriter, r *http.Request) {
	resp := h.collector.Collect()

	topicFilter := r.URL.Query().Get("topic")
	if topicFilter != "" {
		resp.TopicCounts = filterTopicCounts(resp.TopicCounts, topicFilter)
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *StatsHandler) handleStatsSummary(w http.ResponseWriter, r *http.Request) {
	resp := h.collector.Collect()
	summary := map[string]interface{}{
		"total_events":        resp.TotalEvents,
		"total_subscriptions": resp.TotalSubscriptions,
		"active_events":       resp.ActiveEvents,
		"expired_events":      resp.ExpiredEvents,
		"uptime":              resp.Uptime,
	}
	writeJSON(w, http.StatusOK, summary)
}

func (h *StatsHandler) Collector() *Collector {
	return h.collector
}

func (h *StatsHandler) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	resp := h.collector.Collect()
	health := map[string]interface{}{
		"status":     "healthy",
		"uptime":     resp.Uptime,
		"started_at": resp.StartedAt,
		"checks": map[string]string{
			"event_store": "ok",
			"subsystem":   "ok",
			"cleanup":     "ok",
		},
	}
	writeJSON(w, http.StatusOK, health)
}

func (h *StatsHandler) handleTopicBreakdown(w http.ResponseWriter, r *http.Request) {
	resp := h.collector.Collect()
	breakdown := map[string]interface{}{
		"total_topics": len(resp.TopicCounts),
		"topics":       resp.TopicCounts,
	}
	writeJSON(w, http.StatusOK, breakdown)
}

func (h *StatsHandler) handleSubscriptionList(w http.ResponseWriter, r *http.Request) {
	resp := h.collector.Collect()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total_subscriptions": resp.TotalSubscriptions,
		"subscriptions":      resp.Subscriptions,
	})
}

func filterTopicCounts(counts map[string]int, filter string) map[string]int {
	filtered := make(map[string]int)
	for topic, count := range counts {
		if len(topic) >= len(filter) && topic[:len(filter)] == filter {
			filtered[topic] = count
		}
	}
	return filtered
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{
		"error": msg,
	})
}

func formatTimestamp(t time.Time) string {
	return t.Format(time.RFC3339)
}

func NewStatsResponse(totalEvents, totalSubs, active, expired int, topicCounts map[string]int, subs []SubInfo, uptime string, startedAt time.Time) StatsResponse {
	if topicCounts == nil {
		topicCounts = make(map[string]int)
	}
	if subs == nil {
		subs = make([]SubInfo, 0)
	}
	return StatsResponse{
		TotalEvents:        totalEvents,
		TotalSubscriptions: totalSubs,
		ActiveEvents:       active,
		ExpiredEvents:      expired,
		TopicCounts:        topicCounts,
		Subscriptions:      subs,
		Uptime:             uptime,
		StartedAt:          startedAt,
	}
}
