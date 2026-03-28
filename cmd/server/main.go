package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hakantatli/event-ingestion-service/internal/api"
	"github.com/hakantatli/event-ingestion-service/internal/batcher"
	"github.com/hakantatli/event-ingestion-service/internal/repository"
)

func main() {
	// Connect to ClickHouse
	chAddr := os.Getenv("CLICKHOUSE_ADDR")
	if chAddr == "" {
		chAddr = "localhost:9000"
	}

	log.Println("Connecting to ClickHouse at", chAddr)
	conn, err := repository.Connect(chAddr)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	log.Println("Connected to ClickHouse successfully")

	// Initialize Repository
	repo := repository.NewEventRepository(conn)

	// Buffer single-events explicitly to bypass socket constraints
	// Flushes every 50ms guaranteed, or instantly if 500 hit
	b := batcher.NewBatcher(repo, 200000, 5000, 50*time.Millisecond)

	// Setup HTTP Server
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Register application routes injecting the batcher specifically for single-mode
	api.RegisterRoutes(mux, b, repo)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Graceful Shutdown
	go func() {
		log.Println("Starting server on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// The context is used to inform the server it has 5 seconds to finish
	// the request it is currently handling
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	// Drain remaining events in channels before terminating OS process
	log.Println("Flushing remaining single-events asynchronously...")
	_ = b.Close(ctx)

	log.Println("Server exiting")
}
