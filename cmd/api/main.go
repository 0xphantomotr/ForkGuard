package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/0xphantomotr/ForkGuard/internal/api"
	"github.com/0xphantomotr/ForkGuard/internal/db"
	"github.com/0xphantomotr/ForkGuard/internal/storage"
	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	fmt.Println("Starting ForkGuard Admin API...")

	// Get the database DSN from the environment
	dsn := os.Getenv("FORKGUARD_DB_DSN")
	if dsn == "" {
		log.Fatal("FORKGUARD_DB_DSN environment variable is not set")
	}

	// Create a new database connection pool
	pool, err := db.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer pool.Close()

	fmt.Println("✅ Successfully connected to the database!")

	// Initialize storage
	pgStorage := storage.NewPostgresStorage(pool)

	// Create and run the API server
	server := api.NewApiServer(":8080", pgStorage)

	go func() {
		fmt.Println("Admin API listening on :8080")
		if err := server.Run(); err != nil {
			log.Fatalf("API server failed: %v", err)
		}
	}()

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	fmt.Println("Shutting down Admin API...")
	// Note: We don't have a graceful shutdown on the server yet, but this structure allows for it.
	fmt.Println("Admin API stopped.")
}
