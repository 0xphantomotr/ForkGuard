package config

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all configuration for the application.
type Config struct {
	DatabaseDSN       string
	EthRpcURL         string
	ConfirmationDepth uint64
	KafkaBrokers      string
	RedisURL          string
	IdempotencyKeyTTL time.Duration
}

// Load the configuration from env
func Load() *Config {
	// Load .env file if it exists
	godotenv.Load()

	return &Config{
		DatabaseDSN:       getEnv("FORKGUARD_DB_DSN", ""),
		EthRpcURL:         getEnv("FG_ETH_RPC_URL", ""),
		KafkaBrokers:      getEnv("FG_KAFKA_BROKERS", ""),
		RedisURL:          getEnv("FG_REDIS_URL", ""),
		IdempotencyKeyTTL: getEnvAsDuration("FG_IDEMPOTENCY_KEY_TTL", 5*time.Minute),
		ConfirmationDepth: getEnvAsUint64("FG_CONFIRMATION_DEPTH", 10),
	}
}

// Helper to get an environment variable with a default value
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	if fallback == "" {
		// Only log if there's no fallback, indicating it's likely a required field for some services
		log.Printf("%s environment variable is not set", key)
	}
	return fallback
}

// Helper to get an environment variable as a uint64
func getEnvAsUint64(key string, fallback uint64) uint64 {
	strVal := getEnv(key, "")
	if strVal == "" {
		return fallback
	}

	val, err := strconv.ParseUint(strVal, 10, 64)
	if err != nil {
		log.Printf("Invalid value for %s: %v. Using fallback %d.", key, err, fallback)
		return fallback
	}
	return val
}

// Helper to get an environment variable as a time.Duration
func getEnvAsDuration(key string, fallback time.Duration) time.Duration {
	strVal := getEnv(key, "")
	if strVal == "" {
		return fallback
	}

	val, err := time.ParseDuration(strVal)
	if err != nil {
		log.Printf("Invalid value for %s: %v. Using fallback %s.", key, err, fallback)
		return fallback
	}
	return val
}
