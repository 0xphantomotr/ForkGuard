package storage

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Storage provides an interface for all database operations.
type Storage interface {
	// AddBlock adds a new block to the database.
	AddBlock(ctx context.Context, chainID *big.Int, block *types.Block) error
	// AddLogs adds a batch of logs to the database.
	AddLogs(ctx context.Context, chainID *big.Int, logs []types.Log) error
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
		block.Time(),
		false, // Initially, we don't know if it's canonical
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
