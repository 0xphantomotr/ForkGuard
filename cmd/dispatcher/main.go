package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/0xphantomotr/ForkGuard/internal/config"
	"github.com/0xphantomotr/ForkGuard/internal/db"
	"github.com/0xphantomotr/ForkGuard/internal/dispatcher"
	"github.com/0xphantomotr/ForkGuard/internal/storage"
)

func main() {
	log.Println("Starting ForkGuard Dispatcher...")

	// Load config
	cfg := config.Load()

	// New database connection pool
	pool, err := db.New(context.Background(), cfg.DatabaseDSN)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer pool.Close()

	log.Println("✅ Successfully connected to the database!")

	// New storage instance
	pgStorage := storage.NewPostgresStorage(pool)

	// New dispatcher
	disp, err := dispatcher.New(pgStorage, cfg.KafkaBrokers, "forkguard-dispatcher-group")
	if err != nil {
		log.Fatalf("Failed to create dispatcher: %v", err)
	}
	defer disp.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Starts the dispatcher in a separate goroutine
	go func() {
		if err := disp.Run(ctx); err != nil {
			log.Fatalf("Dispatcher failed: %v", err)
		}
	}()

	// Sets up a channel to listen for OS signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Block until a signal is received
	<-stop

	log.Println("Shutting down Dispatcher...")
	cancel() // Signal the dispatcher to stop
}
