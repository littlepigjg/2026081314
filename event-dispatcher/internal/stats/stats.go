package stats

import (
	"time"

	"event-dispatcher/internal/event"
)

type Collector struct {
	store     *event.Store
	startTime time.Time
}

type StatsResponse struct {
	TotalEvents        int            `json:"total_events"`
	TotalSubscriptions int            `json:"total_subscriptions"`
	ActiveEvents       int            `json:"active_events"`
	ExpiredEvents      int            `json:"expired_events"`
	TopicCounts        map[string]int `json:"topic_counts"`
	Subscriptions      []SubInfo      `json:"subscriptions"`
	Uptime             string         `json:"uptime"`
	StartedAt          time.Time      `json:"started_at"`
}

type SubInfo struct {
	ID           string    `json:"subscription_id"`
	SubscriberID string    `json:"subscriber_id"`
	TopicPattern string    `json:"topic_pattern"`
	CreatedAt    time.Time `json:"created_at"`
	LastPullAt   time.Time `json:"last_pull_at"`
}

func NewCollector(store *event.Store) *Collector {
	return &Collector{
		store:     store,
		startTime: time.Now(),
	}
}

func (c *Collector) Collect() StatsResponse {
	events := c.store.Events()

	activeCount := 0
	expiredCount := 0
	for _, e := range events {
		if e.IsExpired() {
			expiredCount++
		} else {
			activeCount++
		}
	}

	topicCounts := c.store.TopicCounts()

	var subInfos []SubInfo
	subs := c.store.Subscriptions()
	for _, sub := range subs {
		subInfos = append(subInfos, SubInfo{
			ID:           sub.ID,
			SubscriberID: sub.SubscriberID,
			TopicPattern: sub.TopicPattern,
			CreatedAt:    sub.CreatedAt,
			LastPullAt:   sub.LastPullAt,
		})
	}
	if subInfos == nil {
		subInfos = make([]SubInfo, 0)
	}

	uptime := time.Since(c.startTime)

	return StatsResponse{
		TotalEvents:        len(events),
		TotalSubscriptions: len(subs),
		ActiveEvents:       activeCount,
		ExpiredEvents:      expiredCount,
		TopicCounts:        topicCounts,
		Subscriptions:      subInfos,
		Uptime:             formatDuration(uptime),
		StartedAt:          c.startTime,
	}
}

func (c *Collector) Store() *event.Store {
	return c.store
}

func (c *Collector) StartTime() time.Time {
	return c.startTime
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return formatDurationParts(h, m, s)
	}
	if m > 0 {
		return formatDurationParts(0, m, s)
	}
	return formatDurationParts(0, 0, s)
}

func formatDurationParts(h, m, s time.Duration) string {
	hs := int(h.Hours())
	ms := int(m.Minutes())
	ss := int(s.Seconds())

	if hs > 0 {
		return pad(hs) + ":" + pad(ms) + ":" + pad(ss)
	}
	return pad(ms) + ":" + pad(ss)
}

func pad(n int) string {
	if n < 10 {
		return "0" + string(rune('0'+n))
	}
	return itoa(n)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	if n < 0 {
		return "-" + itoa(-n)
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
