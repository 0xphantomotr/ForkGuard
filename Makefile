.PHONY: help migrateup migratedown

# Database connection string for the local Postgres instance
DB_URL="postgres://forkguard:forkguard@localhost:5433/forkguard?sslmode=disable"

help:
	@echo "Available commands:"
	@echo "  up             - Start all services with docker compose"
	@echo "  down           - Stop all services with docker compose"
	@echo "  migrateup      - Apply all up migrations to the database"
	@echo "  migratedown    - Revert the last migration"
	@echo "  run-ingestor   - Run the ingestor service"
	@echo "  run-publisher  - Run the outbox publisher"
	@echo "  db-reset       - Drop and recreate the database"
	@echo "  run-services   - Start all services except the ingestor"

up:
	@echo "Starting all services... Migrations will be applied automatically."
	docker compose up -d --build

down:
	docker compose down

migrateup:
	migrate -path db/migrations -database "$(DB_URL)" up

migratedown:
	migrate -path db/migrations -database "$(DB_URL)" down

run-ingestor:
	docker compose up ingestor --build

run-services:
	docker compose up -d postgres redis redpanda anvil prometheus grafana jaeger api
	
run-publisher:
	docker compose up publisher --build

db-reset:
	docker compose exec -T postgres psql -U forkguard -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'forkguard' AND pid <> pg_backend_pid();"
	docker compose exec -T postgres dropdb --if-exists --username=forkguard forkguard
	docker compose exec -T postgres createdb --username=forkguard forkguard

clean:
	@echo "Cleaning up unused Docker objects..."
	docker system prune -a --force