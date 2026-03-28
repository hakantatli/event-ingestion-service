package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// InitSchema creates the required tables if they do not exist
func InitSchema(conn driver.Conn) error {
	query := `
	CREATE TABLE IF NOT EXISTS events (
		event_id String,
		event_name String,
		user_id String,
		timestamp DateTime,
		channel String,
		campaign_id String,
		tags Array(String),
		metadata Map(String, String),
		inserted_at DateTime DEFAULT now()
	) ENGINE = ReplacingMergeTree(inserted_at)
	ORDER BY (event_name, timestamp, user_id, event_id);
	`
	err := conn.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to create events table: %w", err)
	}
	return nil
}

// Connect establishes a connection to ClickHouse and initializes the schema
func Connect(addr string) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "events_db",
			Username: "event_user",
			Password: "event_password",
		},
		DialTimeout:     time.Second * 30,
		MaxOpenConns:    200,
		MaxIdleConns:    100,
		ConnMaxLifetime: time.Hour,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
	})
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(context.Background()); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return nil, fmt.Errorf("clickhouse exception [%d] %s \n%s", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}

	if err := InitSchema(conn); err != nil {
		return nil, err
	}

	return conn, nil
}
