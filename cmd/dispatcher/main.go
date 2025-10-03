package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/0xphantomotr/ForkGuard/internal/config"
	"github.com/0xphantomotr/ForkGuard/internal/db"
	"github.com/0xphantomotr/ForkGuard/internal/dispatcher"
	"github.com/0xphantomotr/ForkGuard/internal/metrics"
	"github.com/0xphantomotr/ForkGuard/internal/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
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

	// New Redis client
	rdbOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Fatalf("Failed to parse Redis URL: %v", err)
	}
	rdb := redis.NewClient(rdbOpts)
	if _, err := rdb.Ping(context.Background()).Result(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("✅ Successfully connected to Redis!")

	// New storage instance
	pgStorage := storage.NewPostgresStorage(pool)

	disp, err := dispatcher.New(pgStorage, rdb, cfg, metrics.Registry)
	if err != nil {
		log.Fatalf("Failed to create dispatcher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the dispatcher in a separate goroutine
	go func() {
		if err := disp.Run(ctx); err != nil {
			log.Fatalf("Dispatcher failed: %v", err)
		}
	}()

	// Start a separate goroutine for the metrics server
	go func() {
		http.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
		log.Println("Metrics server listening on :9092")
		if err := http.ListenAndServe(":9092", nil); err != nil {
			log.Printf("Metrics server failed: %v", err)
		}
	}()

	// Set up a channel to listen for OS signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Block until a signal is received
	<-stop

	log.Println("Shutting down Dispatcher...")
	cancel() // Signal the dispatcher to stop
}
