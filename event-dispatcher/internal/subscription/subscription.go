package subscription

import (
	"fmt"
	"strings"
	"time"
)

const (
	WildcardSingle = "*"
	WildcardMulti  = "#"
	DefaultHistory = 0
	MaxHistory     = 1000
	MinPatternLen  = 1
	MaxPatternLen  = 256
)

type Subscription struct {
	ID           string    `json:"subscription_id"`
	SubscriberID string    `json:"subscriber_id"`
	TopicPattern string    `json:"topic_pattern"`
	HistoryLimit int       `json:"history_limit"`
	CreatedAt    time.Time `json:"created_at"`
	LastPullAt   time.Time `json:"last_pull_at"`
	Active       bool      `json:"active"`
}

type SubscribeRequest struct {
	SubscriberID string `json:"subscriber_id"`
	TopicPattern string `json:"topic_pattern"`
	HistoryLimit int    `json:"history_limit,omitempty"`
}

type SubscribeResponse struct {
	SubscriptionID string   `json:"subscription_id"`
	MatchedTopics  []string `json:"matched_topics"`
}

type PullRequest struct {
	SubscriptionID string `json:"subscription_id"`
	Limit          int    `json:"limit,omitempty"`
}

type PullResponse struct {
	Events  []PulledEvent `json:"events"`
	HasMore bool          `json:"has_more"`
}

type PulledEvent struct {
	EventID     string      `json:"event_id"`
	Topic       string      `json:"topic"`
	Payload     interface{} `json:"payload"`
	PublishedAt time.Time   `json:"published_at"`
}

type PatternMatcher struct {
	cache map[string][]string
}

func NewPatternMatcher() *PatternMatcher {
	return &PatternMatcher{
		cache: make(map[string][]string),
	}
}

func NewSubscription(id, subscriberID, topicPattern string, historyLimit int) *Subscription {
	if historyLimit < 0 {
		historyLimit = 0
	}
	if historyLimit > MaxHistory {
		historyLimit = MaxHistory
	}
	now := time.Now()
	return &Subscription{
		ID:           id,
		SubscriberID: subscriberID,
		TopicPattern: topicPattern,
		HistoryLimit: historyLimit,
		CreatedAt:    now,
		LastPullAt:   now,
		Active:       true,
	}
}

func ValidatePattern(pattern string) error {
	if len(pattern) < MinPatternLen {
		return fmt.Errorf("pattern is too short: %s", pattern)
	}
	if len(pattern) > MaxPatternLen {
		return fmt.Errorf("pattern is too long: %s", pattern)
	}

	segments := strings.Split(pattern, ".")
	for i, seg := range segments {
		if seg == "" {
			return fmt.Errorf("pattern contains empty segment at position %d", i)
		}
		if i < len(segments)-1 && seg == WildcardMulti {
			return fmt.Errorf("wildcard # must be the last segment in pattern: %s", pattern)
		}
		if seg != WildcardSingle && seg != WildcardMulti {
			if !isValidSegment(seg) {
				return fmt.Errorf("invalid segment '%s' in pattern: %s", seg, pattern)
			}
		}
	}
	return nil
}

func isValidSegment(seg string) bool {
	if seg == "" {
		return false
	}
	for _, c := range seg {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func MatchPattern(pattern, topic string) bool {
	patternParts := splitPattern(pattern)
	topicParts := splitPattern(topic)

	for i, pp := range patternParts {
		if pp == WildcardMulti {
			return true
		}
		if i >= len(topicParts) {
			return false
		}
		if pp != WildcardSingle && pp != topicParts[i] {
			return false
		}
	}

	return len(patternParts) == len(topicParts)
}

func splitPattern(pattern string) []string {
	var parts []string
	current := ""
	for _, c := range pattern {
		if c == '.' {
			parts = append(parts, current)
			current = ""
		} else {
			current += string(c)
		}
	}
	parts = append(parts, current)
	return parts
}

func MatchTopicsToPatterns(patterns []string, topics []string) map[string][]string {
	result := make(map[string][]string)
	for _, pattern := range patterns {
		var matched []string
		for _, topic := range topics {
			if MatchPattern(pattern, topic) {
				matched = append(matched, topic)
			}
		}
		result[pattern] = matched
	}
	return result
}

func (s *Subscription) MatchesTopic(topic string) bool {
	return MatchPattern(s.TopicPattern, topic)
}

func (s *Subscription) Deactivate() {
	s.Active = false
}

func (s *Subscription) TouchLastPull() {
	s.LastPullAt = time.Now()
}

func (s *Subscription) Age() time.Duration {
	return time.Since(s.CreatedAt)
}

func (s *Subscription) IdleDuration() time.Duration {
	return time.Since(s.LastPullAt)
}

func BuildPullResponse(events []PulledEvent, hasMore bool) PullResponse {
	if events == nil {
		events = make([]PulledEvent, 0)
	}
	return PullResponse{
		Events:  events,
		HasMore: hasMore,
	}
}

func BuildPulledEvent(eventID, topic string, payload interface{}, publishedAt time.Time) PulledEvent {
	return PulledEvent{
		EventID:     eventID,
		Topic:       topic,
		Payload:     payload,
		PublishedAt: publishedAt,
	}
}

func (pm *PatternMatcher) AddCachedMatch(pattern string, topics []string) {
	pm.cache[pattern] = topics
}

func (pm *PatternMatcher) GetCachedMatch(pattern string) ([]string, bool) {
	topics, ok := pm.cache[pattern]
	return topics, ok
}

func (pm *PatternMatcher) ClearCache() {
	pm.cache = make(map[string][]string)
}
