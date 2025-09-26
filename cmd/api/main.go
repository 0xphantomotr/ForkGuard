package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/0xphantomotr/ForkGuard/internal/db"
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

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})

	server := &http.Server{Addr: ":8080"}

	go func() {
		fmt.Println("Admin API listening on :8080")
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Printf("API server error: %v\n", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop

	fmt.Println("Shutting down Admin API...")

	server.Shutdown(nil)
	fmt.Println("Admin API stopped.")
}
