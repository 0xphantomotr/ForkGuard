package ingestor

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

// Ingestor connects to an Ethereum node, subscribing to new heads, and fetching block data.
type Ingestor struct {
	client  *ethclient.Client
	storage storage.Storage
	chainID *big.Int
}

// Create a new Ingestor and connects to the Ethereum node.
func New(ctx context.Context, rpcURL string, storage storage.Storage) (*Ingestor, error) {
	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Ethereum node: %w", err)
	}

	chainID, err := client.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	log.Printf("✅ Successfully connected to Ethereum node (Chain ID: %s)", chainID.String())

	return &Ingestor{
		client:  client,
		storage: storage,
		chainID: chainID,
	}, nil
}

// Run starts the ingestor's main loop.
// It subscribes to new blocks and processes them.
func (i *Ingestor) Run(ctx context.Context) error {
	headers := make(chan *types.Header)
	sub, err := i.client.SubscribeNewHead(ctx, headers)
	if err != nil {
		return fmt.Errorf("failed to subscribe to new heads: %w", err)
	}
	defer sub.Unsubscribe()

	log.Println("Subscribed to new blocks")

	for {
		select {
		case err := <-sub.Err():
			return fmt.Errorf("subscription error: %w", err)
		case header := <-headers:
			block, err := i.client.BlockByHash(ctx, header.Hash())
			if err != nil {
				log.Printf("Failed to get block %s: %v", header.Hash().Hex(), err)
				continue
			}

			if err := i.storage.AddBlock(ctx, i.chainID, block); err != nil {
				log.Printf("Failed to add block %s: %v", block.Hash().Hex(), err)
				continue
			}

			blockHash := header.Hash()
			logs, err := i.client.FilterLogs(ctx, ethereum.FilterQuery{
				BlockHash: &blockHash,
			})
			if err != nil {
				log.Printf("Failed to get logs for block %s: %v", header.Hash().Hex(), err)
				continue
			}

			if err := i.storage.AddLogs(ctx, i.chainID, logs); err != nil {
				log.Printf("Failed to add logs for block %s: %v", header.Hash().Hex(), err)
				continue
			}

			log.Printf("📦 Ingested block %d (%s) with %d transactions and %d logs",
				block.Number(), block.Hash().Hex(), len(block.Transactions()), len(logs))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close the connection to the Ethereum node.
func (i *Ingestor) Close() {
	i.client.Close()
}
