package cleanup

import (
	"context"
	"log"
	"sync"
	"time"

	"event-dispatcher/internal/event"
)

const (
	DefaultInterval = 10 * time.Second
	initialDelay    = 5 * time.Second
)

type Cleaner struct {
	store    *event.Store
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	running  bool
	mu       sync.Mutex
}

func NewCleaner(store *event.Store) *Cleaner {
	return &Cleaner{
		store:    store,
		interval: DefaultInterval,
	}
}

func (c *Cleaner) Start() {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.running = true
	c.mu.Unlock()

	c.wg.Add(1)
	go c.run()
	log.Printf("[Cleanup] Started background cleanup goroutine, interval: %v", c.interval)
}

func (c *Cleaner) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return
	}

	c.cancel()
	c.wg.Wait()
	c.running = false
	log.Printf("[Cleanup] Stopped background cleanup goroutine")
}

func (c *Cleaner) IsRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

func (c *Cleaner) run() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	timer := time.NewTimer(initialDelay)
	defer timer.Stop()

	log.Printf("[Cleanup] Waiting %v before first cleanup cycle", initialDelay)

	for {
		select {
		case <-c.ctx.Done():
			log.Printf("[Cleanup] Context cancelled, exiting cleanup goroutine")
			return
		case <-timer.C:
			c.performCleanup()
		case <-ticker.C:
			c.performCleanup()
		}
	}
}

func (c *Cleaner) performCleanup() {
	start := time.Now()
	expiredCount := c.store.CleanupExpired()
	elapsed := time.Since(start)

	if expiredCount > 0 {
		log.Printf("[Cleanup] Cleaned up %d expired events in %v", expiredCount, elapsed)
	}
}

func (c *Cleaner) CleanupNow() int {
	return c.store.CleanupExpired()
}

func (c *Cleaner) SetInterval(d time.Duration) {
	if d < time.Second {
		d = time.Second
	}
	c.interval = d
}

func (c *Cleaner) Store() *event.Store {
	return c.store
}

func RunPeriodicCleanup(ctx context.Context, store *event.Store, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("[Cleanup] Periodic cleanup started with interval %v", interval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Cleanup] Periodic cleanup stopped")
			return
		case <-ticker.C:
			count := store.CleanupExpired()
			if count > 0 {
				log.Printf("[Cleanup] Periodic cleanup removed %d expired events", count)
			}
		}
	}
}
