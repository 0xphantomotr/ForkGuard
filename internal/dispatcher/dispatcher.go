package dispatcher

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/0xphantomotr/ForkGuard/internal/config"
	"github.com/0xphantomotr/ForkGuard/internal/metrics"
	"github.com/0xphantomotr/ForkGuard/internal/storage"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const maxRetries = 10

// Dispatcher is responsible for consuming events and delivering them to subscribers.
type Dispatcher struct {
	storage           storage.Storage
	consumer          *kafka.Consumer
	producer          *kafka.Producer
	redisClient       *redis.Client
	idempotencyKeyTTL time.Duration
	metrics           *metrics.DispatcherMetrics
}

// New creates a new Dispatcher.
func New(storage storage.Storage, redisClient *redis.Client, cfg *config.Config, reg prometheus.Registerer) (*Dispatcher, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.KafkaBrokers,
		"group.id":          "forkguard-dispatcher",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.KafkaBrokers})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &Dispatcher{
		storage:           storage,
		consumer:          consumer,
		producer:          producer,
		redisClient:       redisClient,
		idempotencyKeyTTL: cfg.IdempotencyKeyTTL,
		metrics:           metrics.NewDispatcherMetrics(reg),
	}, nil
}

// Close closes the Kafka consumer.
func (d *Dispatcher) Close() {
	d.consumer.Close()
	d.producer.Close()
	d.redisClient.Close()
}

// Run starts the dispatcher's main loop, launching the consumer and worker.
func (d *Dispatcher) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := d.runConsumer(ctx); err != nil {
			log.Printf("🚨 Consumer exited with error: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		d.runWorker(ctx)
	}()

	wg.Wait()
	return nil
}

// runConsumer consumes events from Kafka and creates delivery jobs in the database.
func (d *Dispatcher) runConsumer(ctx context.Context) error {
	topics := []string{"forkguard.evm.log.confirmed", "forkguard.evm.log.retracted"}
	if err := d.consumer.SubscribeTopics(topics, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}
	log.Printf("Subscribed to Kafka topics: %v", topics)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			ev := d.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				log.Printf("📬 Received message from topic %s", *e.TopicPartition.Topic)

				var eventPayload storage.EventPayload
				if err := json.Unmarshal(e.Value, &eventPayload); err != nil {
					log.Printf("🚨 Failed to unmarshal event payload: %v", err)
					continue
				}

				subs, err := d.storage.GetMatchingSubscriptions(ctx, &eventPayload)
				if err != nil {
					log.Printf("🚨 Failed to get matching subscriptions: %v", err)
					continue
				}

				if len(subs) > 0 {
					log.Printf("Found %d matching subscriptions for event %s, creating delivery jobs...", len(subs), eventPayload.ID)
					if err := d.storage.CreateDeliveryJobs(ctx, &eventPayload, subs); err != nil {
						log.Printf("🚨 Failed to create delivery jobs: %v", err)
					}
				}

			case kafka.Error:
				// This is a non-fatal error, so we just log it.
				if e.Code() != kafka.ErrAllBrokersDown {
					log.Printf("🚨 Kafka consumer error: %v", e)
				}
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

// runWorker periodically fetches and processes pending deliveries.
func (d *Dispatcher) runWorker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			deliveries, err := d.storage.GetPendingDeliveries(ctx, 100)
			if err != nil {
				log.Printf("🚨 Failed to get pending deliveries: %v", err)
				continue
			}

			if len(deliveries) > 0 {
				log.Printf("Processing %d pending deliveries", len(deliveries))
			}

			for _, delivery := range deliveries {
				d.processDelivery(ctx, delivery)
			}
		case <-ctx.Done():
			return
		}
	}
}

// processDelivery attempts to deliver a webhook and updates its status.
func (d *Dispatcher) processDelivery(ctx context.Context, delivery *storage.Delivery) {
	idempotencyKey := fmt.Sprintf("delivery:%s:%s", delivery.SubscriptionID, delivery.EventID)

	// Check for idempotency
	err := d.redisClient.Get(ctx, idempotencyKey).Err()
	if err == nil {
		// Key exists, meaning we've already successfully processed this delivery.
		log.Printf(" idempotency key %s found, skipping duplicate delivery", idempotencyKey)
		delivery.Status = "OK"
		if err := d.storage.UpdateDeliveryStatus(ctx, delivery); err != nil {
			log.Printf(" CRITICAL: Failed to update delivery status for event %s: %v", delivery.EventID, err)
		}
		d.metrics.WebhooksDelivered.WithLabelValues("success").Inc() // Idempotent skip is a success
		return
	} else if !errors.Is(err, redis.Nil) {
		// An actual error occurred with Redis
		log.Printf(" ERROR: Failed to check idempotency key %s: %v", idempotencyKey, err)
		// We'll proceed with the delivery attempt, as this might be a transient Redis issue.
	}

	err = d.deliverWebhook(ctx, delivery.Subscription, delivery.Payload)

	if err != nil {
		log.Printf("⚠️ Delivery attempt %d for event %s failed: %v", delivery.Attempt, delivery.EventID, err)
		delivery.Attempt++
		errorString := err.Error()
		delivery.LastError = &errorString

		if delivery.Attempt > maxRetries {
			delivery.Status = "FAILED" // Terminal state
			log.Printf("🚨 Event %s has failed its final delivery attempt.", delivery.EventID)
			d.publishToDLQ(delivery)
		} else {
			delivery.Status = "PENDING"
			// Exponential backoff: 1s, 2s, 4s, 8s, ...
			backoff := time.Duration(math.Pow(2, float64(delivery.Attempt-1))) * time.Second
			delivery.NextAttemptAt = time.Now().Add(backoff)
			log.Printf("Retrying event %s in %s", delivery.EventID, backoff)
		}
		d.metrics.WebhooksDelivered.WithLabelValues("failed").Inc()
	} else {
		log.Printf("✅ Successfully delivered webhook for event %s", delivery.EventID)
		delivery.Status = "OK"
		d.metrics.WebhooksDelivered.WithLabelValues("success").Inc()

		// Set the idempotency key in Redis on successful delivery
		err := d.redisClient.Set(ctx, idempotencyKey, "delivered", d.idempotencyKeyTTL).Err()
		if err != nil {
			log.Printf(" ERROR: Failed to set idempotency key %s: %v", idempotencyKey, err)
		}
	}

	if err := d.storage.UpdateDeliveryStatus(ctx, delivery); err != nil {
		log.Printf("🚨🚨 CRITICAL: Failed to update delivery status for event %s: %v", delivery.EventID, err)
	}
}

// publishToDLQ sends a failed event to the dead-letter queue.
func (d *Dispatcher) publishToDLQ(delivery *storage.Delivery) {
	topic := "forkguard.evm.log.dlq"
	d.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          delivery.Payload,
		Headers: []kafka.Header{
			{Key: "original_event_id", Value: []byte(delivery.EventID)},
			{Key: "final_error", Value: []byte(*delivery.LastError)},
		},
	}, nil)
	log.Printf("📬 Published event %s to DLQ topic: %s", delivery.EventID, topic)
}

func (d *Dispatcher) deliverWebhook(ctx context.Context, sub *storage.Subscription, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", sub.URL, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}

	// Sign the payload using HMAC-SHA256
	mac := hmac.New(sha256.New, []byte(sub.Secret))
	mac.Write(payload)
	signature := hex.EncodeToString(mac.Sum(nil))

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ForkGuard-Signature-256", signature)
	req.Header.Set("User-Agent", "ForkGuard/1.0")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http client error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil // Success
	}

	return fmt.Errorf("delivery failed with status: %s", resp.Status)
}
