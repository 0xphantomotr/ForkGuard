package publisher

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/0xphantomotr/ForkGuard/internal/storage"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Publisher is responsible for reading events from the outbox and publishing them.
type Publisher struct {
	storage  storage.Storage
	producer *kafka.Producer
}

// New creates a new Publisher.
func New(storage storage.Storage, kafkaBrokers string) (*Publisher, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBrokers})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &Publisher{
		storage:  storage,
		producer: producer,
	}, nil
}

// Close closes the Kafka producer.
func (p *Publisher) Close() {
	p.producer.Close()
}

// Run starts the publisher's main loop.
func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			events, err := p.storage.GetUnpublishedOutboxEvents(ctx)
			if err != nil {
				log.Printf("Failed to get unpublished outbox events: %v", err)
				continue
			}

			if len(events) > 0 {
				log.Printf("Found %d unpublished events", len(events))
			}

			for _, event := range events {
				p.producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &event.Topic, Partition: kafka.PartitionAny},
					Value:          event.Payload,
				}, nil)

				if err := p.storage.MarkOutboxEventAsPublished(ctx, event.ID); err != nil {
					log.Printf("Failed to mark outbox event as published: %v", err)
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
