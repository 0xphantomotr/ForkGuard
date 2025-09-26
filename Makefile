.PHONY: help migrateup migratedown

# Database connection string for the local Postgres instance
DB_URL="postgres://forkguard:forkguard@localhost:5433/forkguard?sslmode=disable"

help:
	@echo "Available commands:"
	@echo "  up             - Start all services with docker compose"
	@echo "  down           - Stop all services with docker compose"
	@echo "  migrateup      - Apply all up migrations to the database"
	@echo "  migratedown    - Revert the last migration"

up:
	docker compose up -d

down:
	docker compose down

migrateup:
	migrate -path db/migrations -database "$(DB_URL)" up

migratedown:
	migrate -path db/migrations -database "$(DB_URL)" down