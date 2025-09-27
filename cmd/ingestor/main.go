package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/0xphantomotr/ForkGuard/internal/config"
	"github.com/0xphantomotr/ForkGuard/internal/db"
	"github.com/0xphantomotr/ForkGuard/internal/ingestor"
	"github.com/0xphantomotr/ForkGuard/internal/storage"
)

func main() {
	log.Println("Starting ForkGuard Ingestor...")

	// Load configuration
	cfg := config.Load()

	// Create a new database connection pool
	pool, err := db.New(context.Background(), cfg.DatabaseDSN)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer pool.Close()

	log.Println("✅ Successfully connected to the database!")

	// Create a new storage instance
	pgStorage := storage.NewPostgresStorage(pool)

	// Create a new ingestor
	ing, err := ingestor.New(context.Background(), cfg.EthRpcURL, pgStorage)
	if err != nil {
		log.Fatalf("Failed to create ingestor: %v", err)
	}
	defer ing.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the ingestor in a separate goroutine
	go func() {
		if err := ing.Run(ctx); err != nil {
			log.Fatalf("Ingestor failed: %v", err)
		}
	}()

	// Set up a channel to listen for OS signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Block until a signal is received
	<-stop

	log.Println("Shutting down Ingestor...")
	cancel() // Signal the ingestor to stop
}
