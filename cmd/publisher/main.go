package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/0xphantomotr/ForkGuard/internal/config"
	"github.com/0xphantomotr/ForkGuard/internal/db"
	"github.com/0xphantomotr/ForkGuard/internal/publisher"
	"github.com/0xphantomotr/ForkGuard/internal/storage"
)

func main() {
	log.Println("Starting ForkGuard Publisher...")

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

	// Create a new publisher
	pub, err := publisher.New(pgStorage, cfg.KafkaBrokers)
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer pub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the publisher in a separate goroutine
	go func() {
		if err := pub.Run(ctx); err != nil {
			log.Fatalf("Publisher failed: %v", err)
		}
	}()

	// Set up a channel to listen for OS signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Block until a signal is received
	<-stop

	log.Println("Shutting down Publisher...")
	cancel() // Signal the publisher to stop
}