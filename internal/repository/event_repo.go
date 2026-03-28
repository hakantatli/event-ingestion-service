package repository

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/hakantatli/event-ingestion-service/internal/domain"
)

type EventRepository interface {
	InsertBatch(ctx context.Context, events []domain.EventRecord) error
	GetMetrics(ctx context.Context, from, to int64, eventName string, groupBy string) (*domain.MetricResponse, error)
}

type clickhouseEventRepo struct {
	conn driver.Conn
}

func NewEventRepository(conn driver.Conn) EventRepository {
	return &clickhouseEventRepo{
		conn: conn,
	}
}

func (r *clickhouseEventRepo) InsertBatch(ctx context.Context, events []domain.EventRecord) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO events")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, e := range events {
		err := batch.Append(
			e.EventID,
			e.EventName,
			e.UserID,
			e.Timestamp,
			e.Channel,
			e.CampaignID,
			e.Tags,
			e.Metadata,
			e.InsertedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

func (r *clickhouseEventRepo) GetMetrics(ctx context.Context, from, to int64, eventName string, groupBy string) (*domain.MetricResponse, error) {
	// Base query
	query := `
		SELECT 
			count(*) as total_event_count,
			uniqExact(user_id) as unique_event_count
	`

	if groupBy != "" {
		// Validating group by manually to avoid SQL injection since we inject it to the SELECT and GROUP BY clause.
		// For simplicity, we only allow specific basic columns. In production, use a strict allowlist.
		allowedGroups := map[string]bool{
			"channel":                  true,
			"campaign_id":              true,
			"toStartOfHour(timestamp)": true,
			"toStartOfDay(timestamp)":  true,
		}
		if !allowedGroups[groupBy] {
			return nil, fmt.Errorf("invalid group by column")
		}
		query += fmt.Sprintf(", %s as grouped_by", groupBy)
	}

	query += ` FROM events WHERE event_name = @event_name `

	if from > 0 {
		query += " AND timestamp >= toDateTime(@from) "
	}

	if to > 0 {
		query += " AND timestamp <= toDateTime(@to) "
	}

	if groupBy != "" {
		query += fmt.Sprintf(" GROUP BY %s ", groupBy)
	}

	// Ensure we fetch data from deduplicated view at read (FINAL is expensive, but for precise analytics it's needed)
	// Optionally use argMax if FINAL is too slow. Since metrics "does not need to be fully real-time", normal query is fine.
	// ClickHouse ReplacingMergeTree will deduplicate in the background.

	rows, err := r.conn.Query(ctx, query,
		clickhouse.Named("event_name", eventName),
		clickhouse.Named("from", from),
		clickhouse.Named("to", to),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to execute metrics query: %w", err)
	}
	defer rows.Close()

	resp := &domain.MetricResponse{
		GroupedData: []map[string]interface{}{},
	}

	for rows.Next() {
		var totalCount, uniqueCount uint64

		if groupBy != "" {
			var groupVal string
			if err := rows.Scan(&totalCount, &uniqueCount, &groupVal); err != nil {
				return nil, err
			}
			resp.GroupedData = append(resp.GroupedData, map[string]interface{}{
				"group":              groupVal,
				"total_event_count":  totalCount,
				"unique_event_count": uniqueCount,
			})
			resp.TotalEventCount += totalCount
			// Note: Aggregate sums for total across groups is fine, unique count sum across groups won't be mathematically accurate overall
			// without hyperloglog state or subqueries, but good enough for grouping display.
		} else {
			if err := rows.Scan(&totalCount, &uniqueCount); err != nil {
				return nil, err
			}
			resp.TotalEventCount = totalCount
			resp.UniqueEventCount = uniqueCount
		}
	}

	return resp, nil
}
