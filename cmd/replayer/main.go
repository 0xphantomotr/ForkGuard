package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/0xphantomotr/ForkGuard/internal/config"
	"github.com/0xphantomotr/ForkGuard/internal/db"
	"github.com/0xphantomotr/ForkGuard/internal/replayer"
	"github.com/0xphantomotr/ForkGuard/internal/storage"
)

func main() {
	startBlock := flag.Uint64("start", 0, "Start block for replay")
	endBlock := flag.Uint64("end", 0, "End block for replay")
	flag.Parse()

	if *endBlock == 0 {
		log.Fatal("end block must be specified")
	}
	if *startBlock > *endBlock {
		log.Fatal("start block cannot be greater than end block")
	}

	log.Println("Starting ForkGuard Replayer...")

	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.New(ctx, cfg.DatabaseDSN)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer pool.Close()

	pgStorage := storage.NewPostgresStorage(pool)

	rep, err := replayer.New(ctx, cfg.EthRpcURL, pgStorage)
	if err != nil {
		log.Fatalf("Failed to create replayer: %v", err)
	}

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := rep.Run(ctx, *startBlock, *endBlock); err != nil {
			log.Fatalf("Replayer failed: %v", err)
		}
		log.Println("Replay completed successfully.")
		cancel() // Stop the process once done
	}()

	select {
	case <-stop:
		log.Println("Shutting down Replayer...")
		cancel()
	case <-ctx.Done():
	}
}
