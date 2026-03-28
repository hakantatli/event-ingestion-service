package api

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/hakantatli/event-ingestion-service/internal/batcher"
	"github.com/hakantatli/event-ingestion-service/internal/domain"
	"github.com/hakantatli/event-ingestion-service/internal/repository"
)

type EventHandlers struct {
	batcher batcher.Batcher
	repo    repository.EventRepository
}

func NewEventHandlers(b batcher.Batcher, repo repository.EventRepository) *EventHandlers {
	return &EventHandlers{
		batcher: b,
		repo:    repo,
	}
}

func generateEventID(e domain.Event) string {
	// md5(event_name + user_id + timestamp)
	data := fmt.Sprintf("%s|%s|%d", e.EventName, e.UserID, e.Timestamp)
	hash := md5.Sum([]byte(data))
	return hex.EncodeToString(hash[:])
}

func validateEvent(e domain.Event) error {
	if e.EventName == "" {
		return fmt.Errorf("event_name is required")
	}
	if e.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if e.Timestamp <= 0 {
		return fmt.Errorf("invalid timestamp")
	}
	return nil
}

func mapToRecord(e domain.Event) domain.EventRecord {
	metadataStr := make(map[string]string)
	for k, v := range e.Metadata {
		metadataStr[k] = fmt.Sprintf("%v", v)
	}

	return domain.EventRecord{
		EventID:    generateEventID(e),
		EventName:  e.EventName,
		UserID:     e.UserID,
		Timestamp:  time.Unix(e.Timestamp, 0),
		Channel:    e.Channel,
		CampaignID: e.CampaignID,
		Tags:       e.Tags,
		Metadata:   metadataStr,
		InsertedAt: time.Now(),
	}
}

func (h *EventHandlers) HandleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event domain.Event
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields() // Be flexible but we defined flexible Metadata map
	if err := decoder.Decode(&event); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	if err := validateEvent(event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record := mapToRecord(event)

	// Fast async insertion for single events mode
	if err := h.batcher.Add(record); err != nil {
		http.Error(w, "Server overloaded: queue full", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "accepted"}`))
}

func (h *EventHandlers) HandleBulkIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var events []domain.Event
	if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	var records []domain.EventRecord
	for _, event := range events {
		if err := validateEvent(event); err != nil {
			http.Error(w, fmt.Sprintf("Validation failed: %v", err), http.StatusBadRequest)
			return
		}
		records = append(records, mapToRecord(event))
	}

	if len(records) > 0 {
		// Write the entire payload synchronously as one batch
		if err := h.repo.InsertBatch(r.Context(), records); err != nil {
			http.Error(w, "Failed to write bulk batch to database", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status": "created", "count": ` + fmt.Sprintf("%d", len(events)) + `}`))
}

func (h *EventHandlers) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	eventName := r.URL.Query().Get("event_name")
	if eventName == "" {
		http.Error(w, "event_name is required", http.StatusBadRequest)
		return
	}

	var from, to int64
	var err error

	if f := r.URL.Query().Get("from"); f != "" {
		if from, err = strconv.ParseInt(f, 10, 64); err != nil {
			http.Error(w, "invalid from timestamp", http.StatusBadRequest)
			return
		}
	}

	if t := r.URL.Query().Get("to"); t != "" {
		if to, err = strconv.ParseInt(t, 10, 64); err != nil {
			http.Error(w, "invalid to timestamp", http.StatusBadRequest)
			return
		}
	}

	groupBy := r.URL.Query().Get("group_by")

	resp, err := h.repo.GetMetrics(r.Context(), from, to, eventName, groupBy)
	if err != nil {
		if err.Error() == "invalid group by column" {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, "Failed to get metrics", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
