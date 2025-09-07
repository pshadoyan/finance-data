# Finance Data Pipeline Makefile
.PHONY: help build up down run stop worker flower logs clean test discover enqueue status

# Default target
help:
	@echo "Finance Data Pipeline - Available Commands:"
	@echo ""
	@echo "  make run        - Full pipeline: build, start services, discover US equities, enqueue jobs"
	@echo "  make build      - Build Docker images"
	@echo "  make up         - Start all services (Redis, App, Worker, Flower)"
	@echo "  make down       - Stop and remove all containers"
	@echo "  make stop       - Stop all containers without removing"
	@echo "  make worker     - Run additional worker instance"
	@echo "  make flower     - Open Flower monitoring UI"
	@echo "  make logs       - Show logs from all services"
	@echo "  make clean      - Remove all containers, volumes, and data"
	@echo ""
	@echo "  make discover   - Discover US equities universe (1000 tickers)"
	@echo "  make enqueue    - Enqueue download jobs for discovered universe"
	@echo "  make status     - Check system status and queue info"
	@echo ""
	@echo "  make test-ticker TICKER=AAPL - Test download for a single ticker"
	@echo ""

# Build Docker images
build:
	@echo "🔨 Building Docker images..."
	docker-compose build

# Start all services
up: build
	@echo "🚀 Starting services..."
	docker-compose up -d redis
	@sleep 3
	docker-compose up -d flower
	@sleep 2
	@echo "✅ Services started!"
	@echo "   Flower UI: http://localhost:5555"
	@echo "   Redis: localhost:6379"
	@echo "   Run 'make workers' to start all workers"

# Stop services
down:
	@echo "🛑 Stopping services..."
	docker-compose down
	@echo "✅ Services stopped!"

# Stop without removing containers
stop:
	@echo "⏸️  Pausing services..."
	docker-compose stop

# Full pipeline run
run: up
	@echo "🎯 Starting full pipeline with parallel processing..."
	@sleep 5
	@echo ""
	@echo "👷 Step 1: Starting 6 parallel workers..."
	docker-compose up -d worker-discovery worker-1 worker-2 worker-3 worker-4 worker-5
	@echo "   ✓ 1 discovery worker + 5 download workers started"
	@echo ""
	@echo "📊 Step 2: Discovering US equities (limited to 100 for demo)..."
	docker-compose run --rm app python -m app.cli discover us_equities --limit 100
	@echo ""
	@echo "📥 Step 3: Enqueueing parallel discovery/download jobs..."
	@UNIVERSE_FILE=$$(ls -t universe/us_equities_*.json 2>/dev/null | head -1); \
	if [ -n "$$UNIVERSE_FILE" ]; then \
		echo "   Using universe file: $$UNIVERSE_FILE"; \
		echo "   Each ticker will:"; \
		echo "     1. Discover all available timeframes (1m,5m,15m,30m,1h,2h,4h,1d,1w,1M,1Q,1Y)"; \
		echo "     2. Spawn parallel download tasks for each timeframe"; \
		docker-compose run --rm app python -m app.cli enqueue $$UNIVERSE_FILE \
			--async; \
	else \
		echo "❌ No universe file found. Run 'make discover' first."; \
	fi
	@echo ""
	@echo "✅ Pipeline started with 6 parallel workers!"
	@echo "   Monitor progress at http://localhost:5555"
	@echo "   Run 'make logs' to see worker output"

# Run all workers (6 total: 1 discovery + 5 download)
workers:
	@echo "👷 Starting all workers..."
	docker-compose up -d worker-discovery worker-1 worker-2 worker-3 worker-4 worker-5
	@echo "✅ Started 6 workers:"
	@echo "   - 1 discovery worker (handles timeframe discovery)"
	@echo "   - 5 download workers (parallel downloads)"
	@echo "   Monitor at http://localhost:5555"

# Run a single worker (for development)
worker:
	@echo "👷 Starting single worker..."
	docker-compose up worker-1

# Open Flower UI
flower:
	@echo "🌸 Opening Flower monitoring UI..."
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:5555 || \
	command -v open >/dev/null 2>&1 && open http://localhost:5555 || \
	echo "Please open http://localhost:5555 in your browser"

# Show logs
logs:
	docker-compose logs -f --tail=100

# Show worker logs only
logs-worker:
	docker-compose logs -f --tail=100 worker

# Clean everything
clean:
	@echo "🧹 Cleaning up..."
	docker-compose down -v
	rm -rf data/* universe/*
	@echo "✅ Cleanup complete!"

# Discover universe
discover:
	@echo "🔍 Discovering US equities universe..."
	docker-compose run --rm app python -m app.cli discover us_equities --limit 100

# Discover all categories
discover-all:
	@echo "🔍 Discovering all categories..."
	docker-compose run --rm app python -m app.cli discover us_equities --limit 100
	docker-compose run --rm app python -m app.cli discover etf --limit 100
	docker-compose run --rm app python -m app.cli discover crypto --limit 50
	docker-compose run --rm app python -m app.cli discover fx --limit 25

# Enqueue jobs with auto-detection
enqueue:
	@UNIVERSE_FILE=$$(ls -t universe/us_equities_*.json 2>/dev/null | head -1); \
	if [ -n "$$UNIVERSE_FILE" ]; then \
		echo "📥 Enqueueing jobs from $$UNIVERSE_FILE with auto-detection..."; \
		docker-compose run --rm app python -m app.cli enqueue $$UNIVERSE_FILE --async; \
	else \
		echo "❌ No universe file found. Run 'make discover' first."; \
	fi

# Enqueue with specific parameters
enqueue-custom:
	@if [ -f universe/us_equities_*.json ]; then \
		UNIVERSE_FILE=$$(ls -t universe/us_equities_*.json | head -1); \
		echo "📥 Enqueueing custom jobs from $$UNIVERSE_FILE..."; \
		docker-compose run --rm app python -m app.cli enqueue $$UNIVERSE_FILE \
			--timeframes $(TIMEFRAMES) \
			--start $(START) \
			--end $(END) \
			--async; \
	else \
		echo "❌ No universe file found. Run 'make discover' first."; \
	fi

# Check status
status:
	@echo "📊 System Status"
	@echo "================"
	@echo ""
	@echo "🐳 Container Status:"
	@docker-compose ps
	@echo ""
	@echo "📈 Queue Status:"
	@docker-compose exec -T redis redis-cli -h redis LLEN celery || true
	@echo ""
	@echo "📁 Data Status:"
	@echo "   Universe files: $$(find universe -name "*.json" 2>/dev/null | wc -l)"
	@echo "   Data files: $$(find data -name "*.parquet" 2>/dev/null | wc -l)"
	@echo "   Total size: $$(du -sh data 2>/dev/null | cut -f1)"

# Test single ticker download with auto-detection
test-ticker:
	@if [ -z "$(TICKER)" ]; then \
		echo "❌ Please specify TICKER, e.g., make test-ticker TICKER=AAPL"; \
	else \
		echo "🧪 Testing download for $(TICKER) with auto-detection..."; \
		docker-compose run --rm app python -m app.cli run-once $(TICKER) --organize; \
	fi

# Test single ticker with specific parameters
test-ticker-custom:
	@if [ -z "$(TICKER)" ]; then \
		echo "❌ Please specify TICKER, e.g., make test-ticker-custom TICKER=AAPL TIMEFRAMES=1d,1h"; \
	else \
		echo "🧪 Testing custom download for $(TICKER)..."; \
		docker-compose run --rm app python -m app.cli run-once $(TICKER) \
			--timeframes $(TIMEFRAMES) \
			--start $(START) \
			--end $(END); \
	fi

# List available universes
list-universes:
	@echo "📋 Available universes:"
	@docker-compose run --rm app python -m app.cli list-universes

# Development mode - mount code for live reload
dev:
	@echo "🔧 Starting in development mode..."
	docker-compose run --rm -v $$(pwd)/app:/app/app app bash

# Run tests
test:
	@echo "🧪 Running tests..."
	docker-compose run --rm app python -m pytest tests/ -v

# Quick health check
health:
	@echo "🏥 Health Check:"
	@docker-compose exec -T redis redis-cli ping && echo "   ✅ Redis: OK" || echo "   ❌ Redis: Failed"
	@curl -s http://localhost:5555/api/workers >/dev/null 2>&1 && echo "   ✅ Flower: OK" || echo "   ❌ Flower: Failed"
	@docker-compose ps | grep -q "worker.*Up" && echo "   ✅ Worker: OK" || echo "   ❌ Worker: Failed"
