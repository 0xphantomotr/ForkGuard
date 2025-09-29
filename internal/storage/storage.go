package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EventPayload defines the structure of the JSON payload for outbox events.
type EventPayload struct {
	ID       string    `json:"id"`
	ChainID  int64     `json:"chainId"`
	Block    BlockInfo `json:"block"`
	TxHash   string    `json:"txHash"`
	LogIndex uint      `json:"logIndex"`
	Address  string    `json:"address"`
	Topics   []string  `json:"topics"`
	Data     string    `json:"data"`
	Status   string    `json:"status"`
}

// BlockInfo contains block-specific details for the event payload.
type BlockInfo struct {
	Number    uint64 `json:"number"`
	Hash      string `json:"hash"`
	Timestamp uint64 `json:"timestamp"`
}

type Subscription struct {
	ID      string `json:"id"`
	URL     string `json:"url"`
	Secret  string `json:"secret"`
	ChainID int64  `json:"chainId"`
}

// Storage provides an interface for all database operations.
type Storage interface {
	// AddBlock adds a new block to the database.
	AddBlock(ctx context.Context, chainID *big.Int, block *types.Block) error
	// AddLogs adds a batch of logs to the database.
	AddLogs(ctx context.Context, chainID *big.Int, logs []types.Log) error
	// GetBlockByNumber retrieves a block from the database by its number.
	GetBlockByNumber(ctx context.Context, chainID *big.Int, blockNumber uint64) (*types.Block, error)
	// GetBlockByHash retrieves a block from the database by its hash.
	GetBlockByHash(ctx context.Context, hash []byte) (*types.Block, error)
	// GetLatestBlock retrieves the most recent block from the database.
	GetLatestBlock(ctx context.Context, chainID *big.Int) (*types.Block, error)
	// SetBlockCanonical updates the canonical status of a block.
	SetBlockCanonical(ctx context.Context, hash []byte, canonical bool) error
	// RetractLogsInBlock marks all logs in a given block as 'RETRACTED'.
	RetractLogsInBlock(ctx context.Context, hash []byte) ([]*OutboxEvent, error)
	// ConfirmBlock marks a block and its logs as 'CONFIRMED'.
	ConfirmBlock(ctx context.Context, hash []byte) ([]*OutboxEvent, error)
	// GetMatchingSubscriptions retrieves all subscriptions that match a given event.
	GetMatchingSubscriptions(ctx context.Context, event *EventPayload) ([]*Subscription, error)
	// GetUnpublishedOutboxEvents retrieves all unpublished events from the outbox.
	GetUnpublishedOutboxEvents(ctx context.Context) ([]*OutboxEvent, error)
	// MarkOutboxEventAsPublished marks an outbox event as published.
	MarkOutboxEventAsPublished(ctx context.Context, eventID int64) error
}

// PostgresStorage is the PostgreSQL implementation of the Storage interface.
type PostgresStorage struct {
	pool *pgxpool.Pool
}

// NewPostgresStorage creates a new PostgresStorage instance.
func NewPostgresStorage(pool *pgxpool.Pool) *PostgresStorage {
	return &PostgresStorage{pool: pool}
}

// AddBlock inserts a new block into the 'blocks' table.
// It assumes the block's status is 'unconfirmed' upon insertion.
func (s *PostgresStorage) AddBlock(ctx context.Context, chainID *big.Int, block *types.Block) error {
	query := `
		INSERT INTO blocks (chain_id, number, hash, parent_hash, ts, canonical)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (hash) DO NOTHING
	`
	_, err := s.pool.Exec(ctx, query,
		chainID.Int64(),
		block.NumberU64(),
		block.Hash().Bytes(),
		block.ParentHash().Bytes(),
		time.Unix(int64(block.Time()), 0), // Convert uint64 timestamp to time.Time
		false,                             // Initially, we don't know if it's canonical
	)
	if err != nil {
		return fmt.Errorf("failed to insert block: %w", err)
	}
	return nil
}

// AddLogs inserts a batch of EVM logs into the 'evm_logs' table.
func (s *PostgresStorage) AddLogs(ctx context.Context, chainID *big.Int, logs []types.Log) error {
	if len(logs) == 0 {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, log := range logs {
		query := `
			INSERT INTO evm_logs (chain_id, block_hash, block_number, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'UNCONFIRMED')
			ON CONFLICT (chain_id, tx_hash, log_index, block_hash) DO NOTHING
		`
		// Safely get topics, using nil for missing topics
		topic0, topic1, topic2, topic3 := getTopics(log)

		_, err := tx.Exec(ctx, query,
			chainID.Int64(),
			log.BlockHash.Bytes(),
			log.BlockNumber,
			log.TxHash.Bytes(),
			log.Index,
			log.Address.Bytes(),
			topic0,
			topic1,
			topic2,
			topic3,
			log.Data,
		)
		if err != nil {
			return fmt.Errorf("failed to insert log: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// getTopics safely extracts up to four topics from a log.
// It returns nil for topics that are not present.
func getTopics(log types.Log) (topic0, topic1, topic2, topic3 []byte) {
	if len(log.Topics) > 0 {
		topic0 = log.Topics[0].Bytes()
	}
	if len(log.Topics) > 1 {
		topic1 = log.Topics[1].Bytes()
	}
	if len(log.Topics) > 2 {
		topic2 = log.Topics[2].Bytes()
	}
	if len(log.Topics) > 3 {
		topic3 = log.Topics[3].Bytes()
	}
	return
}

// GetBlockByNumber retrieves a block from the 'blocks' table by its number.
func (s *PostgresStorage) GetBlockByNumber(ctx context.Context, chainID *big.Int, blockNumber uint64) (*types.Block, error) {
	query := `SELECT number, hash, parent_hash, ts FROM blocks WHERE chain_id = $1 AND number = $2`
	row := s.pool.QueryRow(ctx, query, chainID.Int64(), blockNumber)
	return s.scanBlock(row)
}

// GetLatestBlock retrieves the most recent block from the 'blocks' table.
func (s *PostgresStorage) GetLatestBlock(ctx context.Context, chainID *big.Int) (*types.Block, error) {
	query := `SELECT number, hash, parent_hash, ts FROM blocks WHERE chain_id = $1 AND canonical = true ORDER BY number DESC LIMIT 1`
	row := s.pool.QueryRow(ctx, query, chainID.Int64())
	return s.scanBlock(row)
}

// GetBlockByHash retrieves a block from the database by its hash.
func (s *PostgresStorage) GetBlockByHash(ctx context.Context, hash []byte) (*types.Block, error) {
	query := `SELECT number, hash, parent_hash, ts FROM blocks WHERE hash = $1`
	row := s.pool.QueryRow(ctx, query, hash)
	return s.scanBlock(row)
}

func (s *PostgresStorage) scanBlock(row pgx.Row) (*types.Block, error) {
	var number uint64
	var hash, parentHash []byte
	var ts time.Time

	if err := row.Scan(&number, &hash, &parentHash, &ts); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to scan block: %w", err)
	}

	header := &types.Header{
		Number:     big.NewInt(int64(number)),
		ParentHash: common.BytesToHash(parentHash),
		Time:       uint64(ts.Unix()),
	}

	// Note: This is not a complete block, but it's enough for reorg detection.
	// We are missing fields like transaction hashes, receipts root, etc.
	// If you need the full block, you'll need to store more data in the DB.
	return types.NewBlockWithHeader(header), nil
}

// SetBlockCanonical updates the 'canonical' flag for a block in the database.
func (s *PostgresStorage) SetBlockCanonical(ctx context.Context, hash []byte, canonical bool) error {
	query := `UPDATE blocks SET canonical = $1 WHERE hash = $2`
	_, err := s.pool.Exec(ctx, query, canonical, hash)
	if err != nil {
		return fmt.Errorf("failed to update block canonical status: %w", err)
	}
	return nil
}

// RetractLogsInBlock updates the status of all logs within a specific block to 'RETRACTED'
// and creates corresponding outbox events.
func (s *PostgresStorage) RetractLogsInBlock(ctx context.Context, hash []byte) ([]*OutboxEvent, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Get block timestamp first
	var blockTimestamp time.Time
	err = tx.QueryRow(ctx, `SELECT ts FROM blocks WHERE hash = $1`, hash).Scan(&blockTimestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to get block timestamp for retraction: %w", err)
	}

	// Mark the logs as retracted and return them
	rows, err := tx.Query(ctx, `
		UPDATE evm_logs SET status = 'RETRACTED' WHERE block_hash = $1 AND status != 'RETRACTED'
		RETURNING chain_id, block_hash, block_number, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data
	`, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to retract logs: %w", err)
	}
	defer rows.Close()

	var events []*OutboxEvent
	for rows.Next() {
		var log FullLog
		if err := rows.Scan(&log.ChainID, &log.BlockHash, &log.BlockNumber, &log.TxHash, &log.LogIndex, &log.Address, &log.Topic0, &log.Topic1, &log.Topic2, &log.Topic3, &log.Data); err != nil {
			return nil, fmt.Errorf("failed to scan retracted log: %w", err)
		}
		payload := EventPayload{
			ID:      fmt.Sprintf("fg:%d:%s:%d", log.ChainID, log.TxHash.Hex(), log.LogIndex),
			ChainID: log.ChainID,
			Block: BlockInfo{
				Number:    log.BlockNumber,
				Hash:      log.BlockHash.Hex(),
				Timestamp: uint64(blockTimestamp.Unix()),
			},
			TxHash:   log.TxHash.Hex(),
			LogIndex: log.LogIndex,
			Address:  log.Address.Hex(),
			Topics:   getTopicsAsHex(log),
			Data:     fmt.Sprintf("0x%x", log.Data),
			Status:   "RETRACTED",
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal event payload for retraction: %w", err)
		}

		events = append(events, &OutboxEvent{
			Topic:   "forkguard.evm.log.retracted",
			Payload: payloadBytes,
		})
	}

	// Add the events to the outbox
	for _, event := range events {
		if _, err := tx.Exec(ctx, `INSERT INTO outbox (topic, payload) VALUES ($1, $2)`, event.Topic, event.Payload); err != nil {
			return nil, fmt.Errorf("failed to insert retraction event into outbox: %w", err)
		}
	}

	return events, tx.Commit(ctx)
}

// OutboxEvent represents an event to be published to the outbox.
type OutboxEvent struct {
	ID      int64
	Topic   string
	Payload []byte
}

// This is a placeholder for creating the event payload.
// In a real implementation, you would serialize the log into the desired JSON format.
type FullLog struct {
	ChainID     int64
	BlockHash   common.Hash
	BlockNumber uint64
	TxHash      common.Hash
	LogIndex    uint
	Address     common.Address
	Topic0      []byte
	Topic1      []byte
	Topic2      []byte
	Topic3      []byte
	Data        []byte
}

// ConfirmBlock updates the status of a block's logs to 'CONFIRMED'.
func (s *PostgresStorage) ConfirmBlock(ctx context.Context, hash []byte) ([]*OutboxEvent, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Get block timestamp first
	var blockTimestamp time.Time
	err = tx.QueryRow(ctx, `SELECT ts FROM blocks WHERE hash = $1`, hash).Scan(&blockTimestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to get block timestamp: %w", err)
	}

	// Mark the block as canonical
	if _, err := tx.Exec(ctx, `UPDATE blocks SET canonical = true WHERE hash = $1`, hash); err != nil {
		return nil, fmt.Errorf("failed to set block as canonical: %w", err)
	}

	// Mark the logs as confirmed and return them
	rows, err := tx.Query(ctx, `
		UPDATE evm_logs SET status = 'CONFIRMED' WHERE block_hash = $1
		RETURNING chain_id, block_hash, block_number, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data
	`, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to confirm logs: %w", err)
	}
	defer rows.Close()

	var events []*OutboxEvent
	for rows.Next() {
		var log FullLog
		if err := rows.Scan(&log.ChainID, &log.BlockHash, &log.BlockNumber, &log.TxHash, &log.LogIndex, &log.Address, &log.Topic0, &log.Topic1, &log.Topic2, &log.Topic3, &log.Data); err != nil {
			return nil, fmt.Errorf("failed to scan confirmed log: %w", err)
		}

		payload := EventPayload{
			ID:      fmt.Sprintf("fg:%d:%s:%d", log.ChainID, log.TxHash.Hex(), log.LogIndex),
			ChainID: log.ChainID,
			Block: BlockInfo{
				Number:    log.BlockNumber,
				Hash:      log.BlockHash.Hex(),
				Timestamp: uint64(blockTimestamp.Unix()),
			},
			TxHash:   log.TxHash.Hex(),
			LogIndex: log.LogIndex,
			Address:  log.Address.Hex(),
			Topics:   getTopicsAsHex(log),
			Data:     fmt.Sprintf("0x%x", log.Data),
			Status:   "CONFIRMED",
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal event payload: %w", err)
		}

		events = append(events, &OutboxEvent{
			Topic:   "forkguard.evm.log.confirmed",
			Payload: payloadBytes,
		})
	}

	// Add the events to the outbox
	for _, event := range events {
		if _, err := tx.Exec(ctx, `INSERT INTO outbox (topic, payload) VALUES ($1, $2)`, event.Topic, event.Payload); err != nil {
			return nil, fmt.Errorf("failed to insert into outbox: %w", err)
		}
	}

	return events, tx.Commit(ctx)
}

// GetMatchingSubscriptions finds all active subscriptions that match the criteria of an event.
func (s *PostgresStorage) GetMatchingSubscriptions(ctx context.Context, event *EventPayload) ([]*Subscription, error) {
	// Note: This is a simplified query. A real implementation would need to handle
	// wildcard topic matching (e.g., a subscription for topic0 should match events
	// that have topic0 and any other topics). The current query requires an exact match
	// on the topics provided in the subscription.
	query := `
		SELECT id, url, secret, chain_id
		FROM subscriptions
		WHERE active = true
		  AND chain_id = $1
		  AND (address IS NULL OR address = $2)
		  AND (topic0 IS NULL OR topic0 = $3)
		  AND (topic1 IS NULL OR topic1 = $4)
		  AND (topic2 IS NULL OR topic2 = $5)
		  AND (topic3 IS NULL OR topic3 = $6)
	`

	addressBytes := common.HexToAddress(event.Address).Bytes()
	topic0, topic1, topic2, topic3 := getTopicsFromHex(event.Topics)

	rows, err := s.pool.Query(ctx, query, event.ChainID, addressBytes, topic0, topic1, topic2, topic3)
	if err != nil {
		return nil, fmt.Errorf("failed to query for matching subscriptions: %w", err)
	}
	defer rows.Close()

	var subs []*Subscription
	for rows.Next() {
		var sub Subscription
		if err := rows.Scan(&sub.ID, &sub.URL, &sub.Secret, &sub.ChainID); err != nil {
			return nil, fmt.Errorf("failed to scan subscription: %w", err)
		}
		subs = append(subs, &sub)
	}

	return subs, nil
}

// GetUnpublishedOutboxEvents retrieves all unpublished events from the 'outbox' table.
func (s *PostgresStorage) GetUnpublishedOutboxEvents(ctx context.Context) ([]*OutboxEvent, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, topic, payload FROM outbox WHERE published_at IS NULL ORDER BY id ASC`)
	if err != nil {
		return nil, fmt.Errorf("failed to get unpublished outbox events: %w", err)
	}
	defer rows.Close()

	var events []*OutboxEvent
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(&event.ID, &event.Topic, &event.Payload); err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, &event)
	}

	return events, nil
}

// MarkOutboxEventAsPublished marks an outbox event as published by setting the 'published_at' timestamp.
func (s *PostgresStorage) MarkOutboxEventAsPublished(ctx context.Context, eventID int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = $1`, eventID)
	if err != nil {
		return fmt.Errorf("failed to mark outbox event as published: %w", err)
	}
	return nil
}

func getTopicsAsHex(log FullLog) []string {
	var topics []string
	if log.Topic0 != nil {
		topics = append(topics, common.BytesToHash(log.Topic0).Hex())
	}
	if log.Topic1 != nil {
		topics = append(topics, common.BytesToHash(log.Topic1).Hex())
	}
	if log.Topic2 != nil {
		topics = append(topics, common.BytesToHash(log.Topic2).Hex())
	}
	if log.Topic3 != nil {
		topics = append(topics, common.BytesToHash(log.Topic3).Hex())
	}
	return topics
}

func getTopicsFromHex(topics []string) (topic0, topic1, topic2, topic3 []byte) {
	if len(topics) > 0 && topics[0] != "" {
		topic0 = common.HexToHash(topics[0]).Bytes()
	}
	if len(topics) > 1 && topics[1] != "" {
		topic1 = common.HexToHash(topics[1]).Bytes()
	}
	if len(topics) > 2 && topics[2] != "" {
		topic2 = common.HexToHash(topics[2]).Bytes()
	}
	if len(topics) > 3 && topics[3] != "" {
		topic3 = common.HexToHash(topics[3]).Bytes()
	}
	return
}
