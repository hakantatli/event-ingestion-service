package api

import (
	"net/http"

	"github.com/hakantatli/event-ingestion-service/internal/batcher"
	"github.com/hakantatli/event-ingestion-service/internal/repository"
)

func RegisterRoutes(mux *http.ServeMux, b batcher.Batcher, repo repository.EventRepository) {
	h := NewEventHandlers(b, repo)
	mux.HandleFunc("POST /events", h.HandleIngest)
	mux.HandleFunc("POST /events/bulk", h.HandleBulkIngest)
	mux.HandleFunc("GET /metrics", h.HandleMetrics)
}
