package batcher

import (
	"context"
	"log"
	"time"

	"github.com/hakantatli/event-ingestion-service/internal/domain"
	"github.com/hakantatli/event-ingestion-service/internal/repository"
)

type Batcher interface {
	Add(event domain.EventRecord) error
	Close(ctx context.Context) error
}

type eventBatcher struct {
	repo          repository.EventRepository
	eventsCh      chan domain.EventRecord
	batchSize     int
	flushInterval time.Duration
	quit          chan struct{}
}

func NewBatcher(repo repository.EventRepository, maxQueueSize, batchSize int, flushInterval time.Duration) Batcher {
	b := &eventBatcher{
		repo:          repo,
		eventsCh:      make(chan domain.EventRecord, maxQueueSize), // Highly buffered
		batchSize:     batchSize,
		flushInterval: flushInterval,
		quit:          make(chan struct{}),
	}
	go b.worker()
	return b
}

func (b *eventBatcher) Add(event domain.EventRecord) error {
	select {
	case <-b.quit:
		return context.Canceled
	case b.eventsCh <- event:
		return nil
	default:
		// Instantly reject if maxQueueSize is breached to avoid tcp socket destruction
		return context.DeadlineExceeded
	}
}

func (b *eventBatcher) worker() {
	batch := make([]domain.EventRecord, 0, b.batchSize)
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) > 0 {
			batchCopy := make([]domain.EventRecord, len(batch))
			copy(batchCopy, batch)

			go func(entities []domain.EventRecord) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := b.repo.InsertBatch(ctx, entities); err != nil {
					log.Printf("Failed to flush %d events: %v", len(entities), err)
				}
			}(batchCopy)

			batch = batch[:0]
		}
	}

	for {
		select {
		case <-b.quit:
			flush()
			return
		case <-ticker.C:
			flush()
		case event := <-b.eventsCh:
			batch = append(batch, event)
			if len(batch) >= b.batchSize {
				flush()
				ticker.Reset(b.flushInterval)
			}
		}
	}
}

func (b *eventBatcher) Close(ctx context.Context) error {
	close(b.quit)
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
