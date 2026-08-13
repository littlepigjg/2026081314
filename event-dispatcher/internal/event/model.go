package event

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	DefaultRetentionSeconds = 60
	MaxTopicDepth           = 10
	EventIDPrefix           = "evt"
)

type Event struct {
	ID            string      `json:"event_id"`
	Topic         string      `json:"topic"`
	Payload       interface{} `json:"payload"`
	PublishedAt   time.Time   `json:"published_at"`
	RetentionSec  int         `json:"retention_seconds"`
	ExpiresAt     time.Time   `json:"-"`
}

type PublishRequest struct {
	Topic             string      `json:"topic"`
	Payload           interface{} `json:"payload"`
	RetentionSeconds  int         `json:"retention_seconds,omitempty"`
}

type PublishResponse struct {
	EventID     string    `json:"event_id"`
	PublishedAt time.Time `json:"published_at"`
}

type EventDetailResponse struct {
	Event
	Status string `json:"status"`
}

func NewEvent(topic string, payload interface{}, retention int) *Event {
	if retention <= 0 {
		retention = DefaultRetentionSeconds
	}
	now := time.Now()
	return &Event{
		ID:           GenerateEventID(),
		Topic:        topic,
		Payload:      payload,
		PublishedAt:  now,
		RetentionSec: retention,
		ExpiresAt:    now.Add(time.Duration(retention) * time.Second),
	}
}

func GenerateEventID() string {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		panic(fmt.Sprintf("failed to generate random bytes for event ID: %v", err))
	}
	return fmt.Sprintf("%s-%s", EventIDPrefix, hex.EncodeToString(b))
}

func ValidateTopic(topic string) error {
	if topic == "" {
		return fmt.Errorf("topic must not be empty")
	}
	parts := strings.Split(topic, ".")
	if len(parts) > MaxTopicDepth {
		return fmt.Errorf("topic depth exceeds maximum of %d", MaxTopicDepth)
	}
	for _, part := range parts {
		if part == "" {
			return fmt.Errorf("topic contains empty segment: %s", topic)
		}
		if part == "*" || part == "#" {
			return fmt.Errorf("topic contains wildcard character: %s", topic)
		}
	}
	return nil
}

func ParseTopicSegments(topic string) []string {
	return strings.Split(topic, ".")
}

func (e *Event) IsExpired() bool {
	return time.Now().After(e.ExpiresAt)
}

func (e *Event) RemainingSeconds() int {
	remaining := time.Until(e.ExpiresAt).Seconds()
	if remaining < 0 {
		return 0
	}
	return int(remaining)
}

func (e *Event) ToJSON() (string, error) {
	data, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal event: %v", err)
	}
	return string(data), nil
}

func (e *Event) MatchesPattern(pattern string) bool {
	patternParts := strings.Split(pattern, ".")
	topicParts := strings.Split(e.Topic, ".")

	for i, pp := range patternParts {
		if pp == "#" {
			return true
		}
		if i >= len(topicParts) {
			return false
		}
		if pp != "*" && pp != topicParts[i] {
			return false
		}
	}

	return len(patternParts) == len(topicParts)
}

func NormalizeTopic(topic string) string {
	return strings.TrimSpace(strings.ToLower(topic))
}

func BuildEventIndex(events []*Event) map[string][]string {
	index := make(map[string][]string)
	for _, e := range events {
		index[e.Topic] = append(index[e.Topic], e.ID)
	}
	return index
}

func FilterActiveEvents(events []*Event) []*Event {
	var active []*Event
	for _, e := range events {
		if !e.IsExpired() {
			active = append(active, e)
		}
	}
	return active
}

func CountEventsByTopic(events []*Event) map[string]int {
	counts := make(map[string]int)
	for _, e := range events {
		counts[e.Topic]++
	}
	return counts
}

func NewEventFromSnapshot(id, topic string, payload interface{}, publishedAt time.Time, retentionSec int) *Event {
	return &Event{
		ID:           id,
		Topic:        topic,
		Payload:      payload,
		PublishedAt:  publishedAt,
		RetentionSec: retentionSec,
		ExpiresAt:    publishedAt.Add(time.Duration(retentionSec) * time.Second),
	}
}
