package blockprocessor

import (
	"context"
	"fmt"
	"log"
	"math/big"

	"github.com/0xphantomotr/ForkGuard/internal/storage"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// BlockProcessor handles the logic for processing a single Ethereum block.
type BlockProcessor struct {
	client  *ethclient.Client
	storage storage.Storage
	chainID *big.Int
}

// New creates a new BlockProcessor.
func New(client *ethclient.Client, storage storage.Storage, chainID *big.Int) *BlockProcessor {
	return &BlockProcessor{
		client:  client,
		storage: storage,
		chainID: chainID,
	}
}

// ProcessBlock fetches a block's logs and stores the block and logs in the database.
func (p *BlockProcessor) ProcessBlock(ctx context.Context, block *types.Block) error {
	if err := p.storage.AddBlock(ctx, p.chainID, block); err != nil {
		return fmt.Errorf("failed to add block: %w", err)
	}

	blockHash := block.Hash()
	logs, err := p.client.FilterLogs(ctx, ethereum.FilterQuery{
		BlockHash: &blockHash,
	})
	if err != nil {
		return fmt.Errorf("failed to get logs for block: %w", err)
	}

	if err := p.storage.AddLogs(ctx, p.chainID, logs); err != nil {
		return fmt.Errorf("failed to add logs for block: %w", err)
	}

	log.Printf("📦 Processed block %d (%s) with %d transactions and %d logs",
		block.Number(), block.Hash().Hex(), len(block.Transactions()), len(logs))

	return nil
}
