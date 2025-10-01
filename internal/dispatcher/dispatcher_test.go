package dispatcher

import (
	"context"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/0xphantomotr/ForkGuard/internal/storage"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/redis/go-redis/v9"
)

// mockStorage is a mock implementation of the storage.Storage interface for testing.
type mockStorage struct {
	mu         sync.Mutex
	deliveries map[string]*storage.Delivery
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		deliveries: make(map[string]*storage.Delivery),
	}
}

func (s *mockStorage) UpdateDeliveryStatus(ctx context.Context, delivery *storage.Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliveries[delivery.EventID] = delivery
	return nil
}

// Add empty implementations for the other methods of the storage.Storage interface.
func (s *mockStorage) AddBlock(ctx context.Context, chainID *big.Int, block *types.Block) error {
	return nil
}
func (s *mockStorage) AddLogs(ctx context.Context, chainID *big.Int, logs []types.Log) error {
	return nil
}
func (s *mockStorage) GetBlockByNumber(ctx context.Context, chainID *big.Int, blockNumber uint64) (*types.Block, error) {
	return nil, nil
}
func (s *mockStorage) GetBlockByHash(ctx context.Context, hash []byte) (*types.Block, error) {
	return nil, nil
}
func (s *mockStorage) GetLatestBlock(ctx context.Context, chainID *big.Int) (*types.Block, error) {
	return nil, nil
}
func (s *mockStorage) SetBlockCanonical(ctx context.Context, hash []byte, canonical bool) error {
	return nil
}
func (s *mockStorage) RetractLogsInBlock(ctx context.Context, hash []byte) ([]*storage.OutboxEvent, error) {
	return nil, nil
}
func (s *mockStorage) ConfirmBlock(ctx context.Context, hash []byte) ([]*storage.OutboxEvent, error) {
	return nil, nil
}
func (s *mockStorage) GetMatchingSubscriptions(ctx context.Context, event *storage.EventPayload) ([]*storage.Subscription, error) {
	return nil, nil
}
func (s *mockStorage) CreateDeliveryJobs(ctx context.Context, event *storage.EventPayload, subs []*storage.Subscription) error {
	return nil
}
func (s *mockStorage) GetPendingDeliveries(ctx context.Context, limit int) ([]*storage.Delivery, error) {
	return nil, nil
}
func (s *mockStorage) GetUnpublishedOutboxEvents(ctx context.Context) ([]*storage.OutboxEvent, error) {
	return nil, nil
}
func (s *mockStorage) MarkOutboxEventAsPublished(ctx context.Context, eventID int64) error {
	return nil
}

func (s *mockStorage) CreateSubscription(ctx context.Context, sub *storage.Subscription) error {
	return nil
}
func (s *mockStorage) GetSubscription(ctx context.Context, id, tenant string) (*storage.Subscription, error) {
	return nil, nil
}
func (s *mockStorage) ListSubscriptions(ctx context.Context, tenant string) ([]*storage.Subscription, error) {
	return nil, nil
}
func (s *mockStorage) UpdateSubscription(ctx context.Context, sub *storage.Subscription) error {
	return nil
}
func (s *mockStorage) DeleteSubscription(ctx context.Context, id, tenant string) error {
	return nil
}

func TestDispatcher_ProcessDelivery_Idempotency(t *testing.T) {
	// --- Setup ---
	var requestCount int
	var mu sync.Mutex

	// Mock webhook receiver
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	// Mock storage
	ms := newMockStorage()

	// Real Redis client (requires Redis to be running)
	redisOpts, err := redis.ParseURL("redis://localhost:6379/0")
	if err != nil {
		t.Fatalf("failed to parse redis url: %v", err)
	}
	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()

	// Flush Redis to ensure a clean state for the test
	if err := redisClient.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("failed to flush redis: %v", err)
	}

	// Dispatcher instance
	dispatcher := &Dispatcher{
		storage:           ms,
		redisClient:       redisClient,
		idempotencyKeyTTL: 1 * time.Minute, // Short TTL for testing
	}

	delivery := &storage.Delivery{
		ID:             1,
		SubscriptionID: "sub-1",
		EventID:        "event-1",
		Attempt:        1,
		Status:         "PENDING",
		Payload:        []byte(`{"foo":"bar"}`),
		Subscription: &storage.Subscription{
			URL:    mockServer.URL,
			Secret: "secret",
		},
	}

	// --- Action ---
	// First call
	dispatcher.processDelivery(context.Background(), delivery)

	// Second call
	dispatcher.processDelivery(context.Background(), delivery)

	// --- Assertion ---
	mu.Lock()
	defer mu.Unlock()
	if requestCount != 1 {
		t.Errorf("expected 1 request, got %d", requestCount)
	}

	finalStatus := ms.deliveries[delivery.EventID].Status
	if finalStatus != "OK" {
		t.Errorf("expected delivery status to be OK, got %s", finalStatus)
	}
}
