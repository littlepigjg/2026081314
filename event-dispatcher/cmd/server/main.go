package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"event-dispatcher/internal/cleanup"
	"event-dispatcher/internal/event"
	"event-dispatcher/internal/snapshot"
	"event-dispatcher/internal/stats"
	"event-dispatcher/internal/subscription"
)

const (
	defaultPort         = "8080"
	shutdownTimeout     = 5 * time.Second
	defaultSnapshotFile = "events_snapshot.json"
)

func main() {
	port := flag.String("port", defaultPort, "server listen port")
	snapshotFile := flag.String("snapshot", defaultSnapshotFile, "snapshot file path")
	flag.Parse()

	store := event.NewStore()

	if err := loadInitialSnapshot(store, *snapshotFile); err != nil {
		log.Printf("[Main] Warning: failed to load snapshot: %v", err)
	}

	mux := http.NewServeMux()

	event.NewHandler(store, mux)
	subscription.NewHandler(store, mux)
	stats.NewStatsHandler(store, mux)

	cleaner := cleanup.NewCleaner(store)
	cleaner.Start()

	srv := &http.Server{
		Addr:         ":" + *port,
		Handler:      loggingMiddleware(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("[Main] Starting event dispatcher on port %s", *port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("[Main] Received signal %v, initiating graceful shutdown...", sig)
	case err := <-errCh:
		log.Printf("[Main] Server error: %v", err)
	}

	cleaner.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("[Main] Server shutdown error: %v", err)
	}

	saveSnapshot(store, *snapshotFile)

	log.Printf("[Main] Server stopped")
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		elapsed := time.Since(start)
		log.Printf("[HTTP] %s %s %d %v %s", r.Method, r.URL.Path, wrapped.statusCode, elapsed, r.RemoteAddr)
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func loadInitialSnapshot(store *event.Store, snapshotFile string) error {
	absPath, err := filepath.Abs(snapshotFile)
	if err != nil {
		absPath = snapshotFile
	}

	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		log.Printf("[Main] No existing snapshot at %s, starting fresh", absPath)
		return nil
	}

	svc := snapshot.NewSnapshotService(store, absPath)
	return svc.Load()
}

func saveSnapshot(store *event.Store, snapshotFile string) {
	absPath, err := filepath.Abs(snapshotFile)
	if err != nil {
		absPath = snapshotFile
	}

	svc := snapshot.NewSnapshotService(store, absPath)
	if err := svc.Save(); err != nil {
		log.Printf("[Main] Failed to save snapshot: %v", err)
	} else {
		fmt.Printf("Snapshot saved to %s\n", absPath)
	}
}
