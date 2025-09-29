package dispatcher

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/0xphantomotr/ForkGuard/internal/storage"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Consumes events and delivers them to subscribers.
type Dispatcher struct {
	storage  storage.Storage
	consumer *kafka.Consumer
}

// Creates a new Dispatcher.
func New(storage storage.Storage, kafkaBrokers string, groupID string) (*Dispatcher, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &Dispatcher{
		storage:  storage,
		consumer: consumer,
	}, nil
}

// Closes the Kafka consumer.
func (d *Dispatcher) Close() {
	d.consumer.Close()
}

// Starts the dispatcher's main loop.
func (d *Dispatcher) Run(ctx context.Context) error {
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
					log.Printf("Found %d matching subscriptions for event %s", len(subs), eventPayload.ID)
					for _, sub := range subs {
						if err := d.deliverWebhook(ctx, sub, e.Value); err != nil {
							log.Printf("🚨 Failed to deliver webhook for subscription %s: %v", sub.ID, err)
						}
					}
				}

			case kafka.Error:
				log.Printf("🚨 Kafka consumer error: %v", e)
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
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

	log.Printf("Delivering webhook to %s for subscription %s", sub.URL, sub.ID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("✅ Successfully delivered webhook to %s (Status: %s)", sub.URL, resp.Status)
	} else {
		log.Printf("⚠️ Webhook delivery to %s failed (Status: %s)", sub.URL, resp.Status)
	}

	return nil
}
