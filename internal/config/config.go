package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

// Config holds all configuration for the application.
type Config struct {
	DatabaseDSN string
	EthRpcURL   string
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

	return &Config{
		DatabaseDSN: dsn,
		EthRpcURL:   rpcURL,
	}
}
