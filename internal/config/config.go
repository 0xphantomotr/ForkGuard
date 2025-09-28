package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds all configuration for the application.
type Config struct {
	DatabaseDSN       string
	EthRpcURL         string
	ConfirmationDepth uint64
	KafkaBrokers      string
}

// Load the configuration from env
func Load() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	dsn := os.Getenv("FORKGUARD_DB_DSN")
	if dsn == "" {
		log.Fatal("FORKGUARD_DB_DSN environment variable is not set")
	}

	rpcURL := os.Getenv("FG_ETH_RPC_URL")
	if rpcURL == "" {
		log.Fatal("FG_ETH_RPC_URL environment variable is not set")
	}

	confDepthStr := os.Getenv("FG_CONFIRMATION_DEPTH")
	if confDepthStr == "" {
		confDepthStr = "12" // Default to 12 confirmations
	}
	confDepth, err := strconv.ParseUint(confDepthStr, 10, 64)
	if err != nil {
		log.Fatalf("Invalid FG_CONFIRMATION_DEPTH: %v", err)
	}

	kafkaBrokers := os.Getenv("FG_KAFKA_BROKERS")
	if kafkaBrokers == "" {
		log.Fatal("FG_KAFKA_BROKERS environment variable is not set")
	}

	return &Config{
		DatabaseDSN:       dsn,
		EthRpcURL:         rpcURL,
		ConfirmationDepth: confDepth,
		KafkaBrokers:      kafkaBrokers,
	}
}
