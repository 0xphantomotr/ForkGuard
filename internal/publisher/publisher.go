package publisher

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/0xphantomotr/ForkGuard/internal/storage"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Publisher is responsible for reading events from the outbox and publishing them.
type Publisher struct {
	storage  storage.Storage
	producer *kafka.Producer
	wg       sync.WaitGroup
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
	p.producer.Flush(15 * 1000) // Wait up to 15 seconds for outstanding messages
	p.producer.Close()
	p.wg.Wait() // Wait for the delivery goroutine to finish
}

// Run starts the publisher's main loop.
func (p *Publisher) Run(ctx context.Context) error {
	p.wg.Add(1)
	go p.handleDeliveryReports()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("Polling outbox for new events...")
			events, err := p.storage.GetUnpublishedOutboxEvents(ctx)
			if err != nil {
				log.Printf("Failed to get unpublished outbox events: %v", err)
				continue
			}

			if len(events) > 0 {
				log.Printf("Found %d unpublished events, publishing to Kafka...", len(events))
			}

			for _, event := range events {
				err := p.producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &event.Topic, Partition: kafka.PartitionAny},
					Value:          event.Payload,
					Opaque:         event, // Pass the event through to the delivery report
				}, nil)

				if err != nil {
					log.Printf("Failed to produce message: %v", err)
				}
			}
		case <-ctx.Done():
			log.Println("Publisher run loop stopping.")
			return ctx.Err()
		}
	}
}

func (p *Publisher) handleDeliveryReports() {
	defer p.wg.Done()
	for e := range p.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if event, ok := ev.Opaque.(*storage.OutboxEvent); ok {
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed for event %d: %v\n", event.ID, ev.TopicPartition.Error)
				} else {
					log.Printf("Delivered message to %v\n", ev.TopicPartition)
					// Use a new context here as the parent context might have been canceled
					if err := p.storage.MarkOutboxEventAsPublished(context.Background(), event.ID); err != nil {
						log.Printf("Failed to mark outbox event %d as published: %v", event.ID, err)
					}
				}
			}
		}
	}
}
