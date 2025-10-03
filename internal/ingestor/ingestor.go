package ingestor

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/0xphantomotr/ForkGuard/internal/blockprocessor"
	"github.com/0xphantomotr/ForkGuard/internal/metrics"
	"github.com/0xphantomotr/ForkGuard/internal/storage"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/prometheus/client_golang/prometheus"
)

// Ingestor connects to an Ethereum node, subscribing to new heads, and fetching block data.
type Ingestor struct {
	client             *ethclient.Client
	storage            storage.Storage
	chainID            *big.Int
	lastBlock          *types.Block
	confirmationDepth  uint64
	confirmationWindow []*types.Block
	processor          *blockprocessor.BlockProcessor
	metrics            *metrics.IngestorMetrics
}

// Create a new Ingestor and connects to the Ethereum node.
func New(ctx context.Context, rpcURL string, storage storage.Storage, confirmationDepth uint64, reg prometheus.Registerer) (*Ingestor, error) {
	var client *ethclient.Client
	var err error
	for i := 0; i < 5; i++ {
		client, err = ethclient.DialContext(ctx, rpcURL)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to Ethereum node, retrying in 2 seconds... (%v)", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Ethereum node after multiple retries: %w", err)
	}

	chainID, err := client.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	log.Printf("✅ Successfully connected to Ethereum node (Chain ID: %s)", chainID.String())

	processor := blockprocessor.New(client, storage, chainID)

	lastBlock, err := storage.GetLatestBlock(ctx, chainID)

	if err != nil {
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}

	if lastBlock != nil {
		log.Printf("Resuming from block %d (%s)", lastBlock.NumberU64(), lastBlock.Hash().Hex())
	} else {
		log.Println("No previous blocks found, starting from genesis")
	}

	return &Ingestor{
		client:            client,
		storage:           storage,
		chainID:           chainID,
		lastBlock:         lastBlock,
		confirmationDepth: confirmationDepth,
		processor:         processor,
		metrics:           metrics.NewIngestorMetrics(reg),
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
			if i.lastBlock != nil && header.ParentHash != i.lastBlock.Hash() {
				if err := i.handleReorg(ctx, header); err != nil {
					log.Printf("Failed to handle reorg: %v", err)
					continue
				}
			}

			block, err := i.client.BlockByHash(ctx, header.Hash())
			if err != nil {
				log.Printf("Failed to get block %s: %v", header.Hash().Hex(), err)
				continue
			}

			if err := i.processor.ProcessBlock(ctx, block); err != nil {
				log.Printf("Failed to process block %s: %v", block.Hash().Hex(), err)
				continue
			}
			i.metrics.BlocksProcessed.Inc()
			i.metrics.LatestBlock.Set(float64(block.NumberU64()))

			i.confirmationWindow = append(i.confirmationWindow, block)
			if len(i.confirmationWindow) > int(i.confirmationDepth) {
				confirmedBlock := i.confirmationWindow[0]
				i.confirmationWindow = i.confirmationWindow[1:]
				events, err := i.storage.ConfirmBlock(ctx, confirmedBlock.Hash().Bytes())
				if err != nil {
					log.Printf("Failed to confirm block %s: %v", confirmedBlock.Hash().Hex(), err)
					continue
				}
				log.Printf("✅ Confirmed block %d (%s) and created %d outbox events",
					confirmedBlock.NumberU64(), confirmedBlock.Hash().Hex(), len(events))
			}

			i.lastBlock = block
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (i *Ingestor) handleReorg(ctx context.Context, newHeader *types.Header) error {
	log.Printf("🚨 Reorg detected! New block %d (%s) has parent %s, but last block was %d (%s)",
		newHeader.Number.Uint64(), newHeader.Hash().Hex(), newHeader.ParentHash.Hex(),
		i.lastBlock.NumberU64(), i.lastBlock.Hash().Hex())
	i.metrics.ReorgsDetected.Inc()

	// Find the common ancestor
	var oldBlock *types.Block = i.lastBlock
	var newBlock *types.Header = newHeader
	var orphanedBlocks []*types.Block
	var newBlocks []*types.Header

	for oldBlock.Hash() != newBlock.Hash() {
		// If the old chain is longer, walk it back
		if oldBlock.NumberU64() >= newBlock.Number.Uint64() {
			orphanedBlocks = append(orphanedBlocks, oldBlock)
			parent, err := i.storage.GetBlockByHash(ctx, oldBlock.ParentHash().Bytes())
			if err != nil {
				return fmt.Errorf("failed to get parent block during reorg: %w", err)
			}
			if parent == nil {
				// We've reached the end of our known history.
				// This means the new chain is completely different.
				// We'll treat this as a fresh start.
				i.lastBlock = nil
				return nil
			}
			oldBlock = parent
		}

		// If the new chain is longer or chains are equal length, walk it back
		if newBlock.Number.Uint64() > oldBlock.NumberU64() {
			newBlocks = append(newBlocks, newBlock)
			parent, err := i.client.HeaderByHash(ctx, newBlock.ParentHash)
			if err != nil {
				return fmt.Errorf("failed to get new parent header during reorg: %w", err)
			}
			newBlock = parent
		}
	}

	log.Printf("Found common ancestor at block %d (%s)", oldBlock.NumberU64(), oldBlock.Hash().Hex())

	// Rollback: Mark orphaned blocks as non-canonical and retract their logs
	for _, blk := range orphanedBlocks {
		log.Printf("Orphaned block %d (%s)", blk.NumberU64(), blk.Hash().Hex())
		if err := i.storage.SetBlockCanonical(ctx, blk.Hash().Bytes(), false); err != nil {
			return fmt.Errorf("failed to set block as non-canonical: %w", err)
		}
		events, err := i.storage.RetractLogsInBlock(ctx, blk.Hash().Bytes())
		if err != nil {
			return fmt.Errorf("failed to retract logs in block: %w", err)
		}
		if len(events) > 0 {
			log.Printf("Created %d retraction events for orphaned block %d", len(events), blk.NumberU64())
		}
	}

	// Roll forward: Process the new blocks in reverse order (from parent to child)
	for j := len(newBlocks) - 1; j >= 0; j-- {
		header := newBlocks[j]
		fullBlock, err := i.client.BlockByHash(ctx, header.Hash())
		if err != nil {
			return fmt.Errorf("failed to get new block during reorg: %w", err)
		}
		if err := i.processor.ProcessBlock(ctx, fullBlock); err != nil {
			return fmt.Errorf("failed to process new block during reorg: %w", err)
		}
	}

	// Update the ingestor's state to the new tip
	newTip, err := i.client.BlockByHash(ctx, newHeader.Hash())
	if err != nil {
		return fmt.Errorf("failed to get new tip after reorg: %w", err)
	}
	i.lastBlock = newTip

	return nil
}

// Close the connection to the Ethereum node.
func (i *Ingestor) Close() {
	i.client.Close()
}
