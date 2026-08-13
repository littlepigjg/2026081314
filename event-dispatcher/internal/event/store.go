package event

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type SubscriptionInfo struct {
	ID           string
	SubscriberID string
	TopicPattern string
	HistoryLimit int
	CreatedAt    time.Time
	LastPullAt   time.Time
}

type Store struct {
	mu            sync.RWMutex
	events         []*Event
	topicIndex     map[string][]string
	subscriptions  map[string]*SubscriptionInfo
	subCounter     int64
	eventCounter   int64
}

func NewStore() *Store {
	return &Store{
		events:        make([]*Event, 0),
		topicIndex:    make(map[string][]string),
		subscriptions: make(map[string]*SubscriptionInfo),
	}
}

func (s *Store) Publish(topic string, payload interface{}, retention int) (string, error) {
	event := NewEvent(topic, payload, retention)

	s.mu.Lock()
	s.events = append(s.events, event)

	if s.topicIndex[topic] == nil {
		s.topicIndex[topic] = make([]string, 0)
	}
	s.topicIndex[topic] = append(s.topicIndex[topic], event.ID)
	s.mu.Unlock()

	atomic.AddInt64(&s.eventCounter, 1)

	return event.ID, nil
}

func (s *Store) Pull(subscriptionID string, limit int) ([]Event, bool, error) {
	s.mu.RLock()
	sub, exists := s.subscriptions[subscriptionID]
	if !exists {
		s.mu.RUnlock()
		return nil, false, fmt.Errorf("subscription not found: %s", subscriptionID)
	}

	matchedTopics := s.findMatchingTopics(sub.TopicPattern)

	var result []Event
	for _, topic := range matchedTopics {
		eventIDs, ok := s.topicIndex[topic]
		if !ok {
			continue
		}
		for _, eid := range eventIDs {
			evt := s.findEventByID(eid)
			if evt != nil && !evt.IsExpired() {
				result = append(result, *evt)
			}
		}
	}

	if len(result) > limit && limit > 0 {
		result = result[len(result)-limit:]
	}

	sub.LastPullAt = time.Now()

	hasMore := false
	if limit > 0 && len(result) > limit {
		hasMore = true
	}
	s.mu.RUnlock()

	return result, hasMore, nil
}

func (s *Store) GetEvent(id string) (*Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, e := range s.events {
		if e.ID == id {
			return e, nil
		}
	}
	return nil, fmt.Errorf("event not found: %s", id)
}

func (s *Store) CreateSubscription(subscriberID, topicPattern string, historyLimit int) (string, []string, error) {
	counter := atomic.AddInt64(&s.subCounter, 1)
	subID := fmt.Sprintf("sub-%03d", counter)

	if historyLimit <= 0 {
		historyLimit = 0
	}

	sub := &SubscriptionInfo{
		ID:           subID,
		SubscriberID: subscriberID,
		TopicPattern: topicPattern,
		HistoryLimit: historyLimit,
		CreatedAt:    time.Now(),
		LastPullAt:   time.Now(),
	}

	s.mu.Lock()
	s.subscriptions[subID] = sub
	matchedTopics := s.findMatchingTopics(topicPattern)
	s.mu.Unlock()

	return subID, matchedTopics, nil
}

func (s *Store) findMatchingTopics(pattern string) []string {
	var matched []string
	for topic := range s.topicIndex {
		if matchPattern(pattern, topic) {
			matched = append(matched, topic)
		}
	}
	return matched
}

func matchPattern(pattern, topic string) bool {
	patternParts := splitTopic(pattern)
	topicParts := splitTopic(topic)

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

func splitTopic(topic string) []string {
	var parts []string
	current := ""
	for _, c := range topic {
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

func (s *Store) findEventByID(id string) *Event {
	for _, e := range s.events {
		if e.ID == id {
			return e
		}
	}
	return nil
}

func (s *Store) CleanupExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	expiredCount := 0
	now := time.Now()

	var activeEvents []*Event
	for _, e := range s.events {
		if now.After(e.ExpiresAt) {
			expiredCount++
		} else {
			activeEvents = append(activeEvents, e)
		}
	}

	s.events = activeEvents

	for topic, ids := range s.topicIndex {
		var activeIDs []string
		for _, id := range ids {
			e := s.findEventByID(id)
			if e != nil && !now.After(e.ExpiresAt) {
				activeIDs = append(activeIDs, id)
			}
		}
		if len(activeIDs) == 0 {
			delete(s.topicIndex, topic)
		} else {
			s.topicIndex[topic] = activeIDs
		}
	}

	if expiredCount > 0 {
		log.Printf("[Cleanup] Removed %d expired events, remaining: %d", expiredCount, len(s.events))
	}

	return expiredCount
}

func (s *Store) TotalEvents() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.events)
}

func (s *Store) TotalSubscriptions() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subscriptions)
}

func (s *Store) TopicCounts() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := make(map[string]int)
	for _, e := range s.events {
		counts[e.Topic]++
	}
	return counts
}

func (s *Store) Events() []*Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Event, len(s.events))
	copy(result, s.events)
	return result
}

func (s *Store) Subscriptions() map[string]*SubscriptionInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*SubscriptionInfo)
	for k, v := range s.subscriptions {
		result[k] = v
	}
	return result
}

func (s *Store) TopicIndex() map[string][]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string][]string)
	for k, v := range s.topicIndex {
		cp := make([]string, len(v))
		copy(cp, v)
		result[k] = cp
	}
	return result
}

func (s *Store) LoadFromSnapshot(events []*Event) {
	s.mu.Lock()
	for _, e := range events {
		s.events = append(s.events, e)
		if s.topicIndex[e.Topic] == nil {
			s.topicIndex[e.Topic] = make([]string, 0)
		}
		s.topicIndex[e.Topic] = append(s.topicIndex[e.Topic], e.ID)
	}
	s.mu.Unlock()
	atomic.StoreInt64(&s.eventCounter, int64(len(events)))
	log.Printf("[Store] Loaded %d events from snapshot", len(events))
}
