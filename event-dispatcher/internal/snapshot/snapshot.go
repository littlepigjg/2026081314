package snapshot

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"event-dispatcher/internal/event"
)

const (
	DefaultSnapshotFile = "events_snapshot.json"
	backupSuffix        = ".bak"
	readPermission      = 0644
	writePermission     = 0644
)

type SnapshotService struct {
	filePath string
	store    *event.Store
}

type SnapshotData struct {
	Version       string              `json:"version"`
	Timestamp     time.Time           `json:"timestamp"`
	TotalEvents   int                 `json:"total_events"`
	TotalTopics   int                 `json:"total_topics"`
	Events        []event.Event       `json:"events"`
	Subscriptions []event.SubscriptionInfo `json:"subscriptions"`
}

func NewSnapshotService(store *event.Store, filePath string) *SnapshotService {
	if filePath == "" {
		filePath = DefaultSnapshotFile
	}
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		absPath = filePath
	}
	return &SnapshotService{
		filePath: absPath,
		store:    store,
	}
}

func (s *SnapshotService) Save() error {
	snapshot := s.buildSnapshot()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %v", err)
	}

	if err := s.backup(); err != nil {
		log.Printf("[Snapshot] Warning: backup failed: %v", err)
	}

	if err := os.WriteFile(s.filePath, data, writePermission); err != nil {
		return fmt.Errorf("failed to write snapshot file: %v", err)
	}

	log.Printf("[Snapshot] Saved snapshot to %s (%d events, %d topics)",
		s.filePath, snapshot.TotalEvents, snapshot.TotalTopics)

	return nil
}

func (s *SnapshotService) Load() error {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[Snapshot] No snapshot file found at %s", s.filePath)
			return nil
		}
		return fmt.Errorf("failed to read snapshot file: %v", err)
	}

	var snapshot SnapshotData
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %v", err)
	}

	var events []*event.Event
	for _, e := range snapshot.Events {
		evt := e
		events = append(events, &evt)
	}

	s.store.LoadFromSnapshot(events)

	log.Printf("[Snapshot] Loaded snapshot from %s (%d events)", s.filePath, len(events))

	return nil
}

func (s *SnapshotService) buildSnapshot() SnapshotData {
	events := s.store.Events()
	subs := s.store.Subscriptions()

	var eventList []event.Event
	for _, e := range events {
		if !e.IsExpired() {
			eventList = append(eventList, *e)
		}
	}

	var subList []event.SubscriptionInfo
	for _, sub := range subs {
		subList = append(subList, *sub)
	}

	topicIndex := s.store.TopicIndex()

	return SnapshotData{
		Version:       "1.0",
		Timestamp:     time.Now().UTC(),
		TotalEvents:   len(eventList),
		TotalTopics:   len(topicIndex),
		Events:        eventList,
		Subscriptions: subList,
	}
}

func (s *SnapshotService) backup() error {
	backupPath := s.filePath + backupSuffix

	if _, err := os.Stat(s.filePath); os.IsNotExist(err) {
		return nil
	}

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return err
	}

	return os.WriteFile(backupPath, data, writePermission)
}

func (s *SnapshotService) FilePath() string {
	return s.filePath
}

func (s *SnapshotService) Exists() bool {
	_, err := os.Stat(s.filePath)
	return err == nil
}

func (s *SnapshotService) Delete() error {
	if !s.Exists() {
		return nil
	}
	return os.Remove(s.filePath)
}

func SaveSnapshot(store *event.Store, filePath string) error {
	svc := NewSnapshotService(store, filePath)
	return svc.Save()
}

func LoadSnapshot(store *event.Store, filePath string) error {
	svc := NewSnapshotService(store, filePath)
	return svc.Load()
}

func NewSnapshotData(version string, timestamp time.Time, events []event.Event, subs []event.SubscriptionInfo, topicCount int) SnapshotData {
	if events == nil {
		events = make([]event.Event, 0)
	}
	if subs == nil {
		subs = make([]event.SubscriptionInfo, 0)
	}
	return SnapshotData{
		Version:       version,
		Timestamp:     timestamp,
		TotalEvents:   len(events),
		TotalTopics:   topicCount,
		Events:        events,
		Subscriptions: subs,
	}
}
