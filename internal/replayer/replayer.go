package replayer

import (
	"context"
	"fmt"
	"log"
	"math/big"

	"github.com/0xphantomotr/ForkGuard/internal/blockprocessor"
	"github.com/0xphantomotr/ForkGuard/internal/storage"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Replayer struct {
	client    *ethclient.Client
	storage   storage.Storage
	chainID   *big.Int
	processor *blockprocessor.BlockProcessor
}

func New(ctx context.Context, rpcURL string, storage storage.Storage) (*Replayer, error) {
	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Ethereum node: %w", err)
	}

	chainID, err := client.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	processor := blockprocessor.New(client, storage, chainID)

	return &Replayer{
		client:    client,
		storage:   storage,
		chainID:   chainID,
		processor: processor,
	}, nil
}

func (r *Replayer) Run(ctx context.Context, startBlock, endBlock uint64) error {
	log.Printf("Starting replay from block %d to %d", startBlock, endBlock)

	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			block, err := r.client.BlockByNumber(ctx, new(big.Int).SetUint64(blockNum))
			if err != nil {
				return fmt.Errorf("failed to get block %d: %w", blockNum, err)
			}

			if err := r.processor.ProcessBlock(ctx, block); err != nil {
				return fmt.Errorf("failed to process block %d: %w", blockNum, err)
			}
		}
	}

	log.Printf("Finished replay from block %d to %d", startBlock, endBlock)
	return nil
}
