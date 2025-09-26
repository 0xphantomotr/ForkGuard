package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// New creates a new database connection pool.
// It uses pgxpool for efficient connection management.
// The caller is responsible for closing the pool when the application shuts down.
func New(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Ping the database to verify the connection is alive.
	if err := pool.Ping(ctx); err != nil {
		pool.Close() // Clean up the pool if ping fails
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}
