package domain

import "time"

// Event represents an incoming event payload.
type Event struct {
	EventName  string         `json:"event_name"`
	UserID     string         `json:"user_id"`
	Timestamp  int64          `json:"timestamp"`
	Channel    string         `json:"channel,omitempty"`
	CampaignID string         `json:"campaign_id,omitempty"`
	Tags       []string       `json:"tags,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"` // using any because some values might be numbers/booleans
}

// EventRecord represents the structure stored in ClickHouse.
type EventRecord struct {
	EventID    string
	EventName  string
	UserID     string
	Timestamp  time.Time
	Channel    string
	CampaignID string
	Tags       []string
	Metadata   map[string]string // Stringified for ClickHouse Map(String, String)
	InsertedAt time.Time
}

// MetricResponse represents the structure for the aggregated metrics API response.
type MetricResponse struct {
	TotalEventCount  uint64           `json:"total_event_count"`
	UniqueEventCount uint64           `json:"unique_event_count"`
	GroupedData      []map[string]any `json:"grouped_data,omitempty"`
}
