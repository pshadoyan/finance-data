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
	docker-compose up -d api flower
	@sleep 2
	@echo "✅ Services started!"
	@echo "   SwaggerUI: http://localhost:8000/docs"
	@echo "   FastAPI: http://localhost:8000"
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

# Full pipeline run with equities, crypto, and ETFs
run: up
	@echo "🎯 Starting full pipeline with parallel processing (Equities + Crypto + ETFs)..."
	@sleep 5
	@echo ""
	@echo "🔍 Checking for leftover tasks..."
	@QUEUE_SIZE=$$(docker-compose exec -T redis redis-cli -h redis LLEN celery 2>/dev/null || echo "0"); \
	if [ "$$QUEUE_SIZE" != "0" ] && [ "$$QUEUE_SIZE" != "" ]; then \
		echo "   ⚠️  Found $$QUEUE_SIZE leftover tasks in queue!"; \
		echo "   Clearing old tasks..."; \
		docker-compose exec -T redis redis-cli FLUSHDB >/dev/null 2>&1; \
		echo "   ✅ Queue cleared"; \
	else \
		echo "   ✅ Queue is clean"; \
	fi
	@echo ""
	@echo "👷 Step 1: Starting 10 parallel workers..."
	docker-compose up -d worker-discovery worker-1 worker-2 worker-3 worker-4 worker-5 worker-6 worker-7 worker-8 worker-9
	@echo "   ✓ 1 discovery worker + 9 download workers started"
	@echo ""
	@echo "📊 Step 2: Discovering tickers..."
	@echo "   📈 Discovering US equities (limited to 100 for demo)..."
	docker-compose run --rm app python -m app.cli discover us_equities --limit 100
	@echo "   🪙 Discovering crypto (limited to 50)..."
	docker-compose run --rm app python -m app.cli discover crypto --limit 50
	@echo "   📊 Discovering ALL ETFs..."
	docker-compose run --rm app python -m app.cli discover etf
	@echo ""
	@echo "📥 Step 3: Enqueueing parallel discovery/download jobs..."
	@echo "   Processing US Equities..."
	@UNIVERSE_FILE=$$(ls -t universe/us_equities_*.json 2>/dev/null | head -1); \
	if [ -n "$$UNIVERSE_FILE" ]; then \
		echo "   Using universe file: $$UNIVERSE_FILE (batch size: 100)"; \
		docker-compose run --rm app python -m app.cli enqueue $$UNIVERSE_FILE --async --batch-size 100; \
	fi
	@echo "   Processing Crypto..."
	@UNIVERSE_FILE=$$(ls -t universe/crypto_*.json 2>/dev/null | head -1); \
	if [ -n "$$UNIVERSE_FILE" ]; then \
		echo "   Using universe file: $$UNIVERSE_FILE (batch size: 100)"; \
		docker-compose run --rm app python -m app.cli enqueue $$UNIVERSE_FILE --async --batch-size 100; \
	fi
	@echo "   Processing ETFs..."
	@UNIVERSE_FILE=$$(ls -t universe/etf_*.json 2>/dev/null | head -1); \
	if [ -n "$$UNIVERSE_FILE" ]; then \
		echo "   Using universe file: $$UNIVERSE_FILE (batch size: 100)"; \
		docker-compose run --rm app python -m app.cli enqueue $$UNIVERSE_FILE --async --batch-size 100; \
	fi
	@echo ""
	@echo "✅ Pipeline started with 10 workers!"
	@echo "   Processing: Equities + Crypto + ETFs"
	@echo "   Monitor progress at http://localhost:5555"
	@echo "   Check queue: make queue-status"
	@echo "   View logs: make logs"

# Full production pipeline (ALL equities in batches)
run-full: up
	@echo "🚀 Starting FULL pipeline with ALL US equities..."
	@sleep 5
	@echo ""
	@echo "👷 Step 1: Starting 10 parallel workers..."
	docker-compose up -d worker-discovery worker-1 worker-2 worker-3 worker-4 worker-5 worker-6 worker-7 worker-8 worker-9
	@echo "   ✓ 1 discovery worker + 9 download workers started"
	@echo ""
	@echo "📊 Step 2: Discovering ALL US equities (up to 15k tickers)..."
	docker-compose run --rm app python -m app.cli discover-all
	@echo ""
	@echo "📦 Step 3: Processing in batches of 100..."
	@UNIVERSE_FILE=$$(ls -t universe/us_equities_*.json 2>/dev/null | head -1); \
	if [ -n "$$UNIVERSE_FILE" ]; then \
		echo "   Using universe file: $$UNIVERSE_FILE"; \
		echo "   Processing first 10 batches (1000 tickers)..."; \
		echo "   Note: Tasks will automatically check existing data and skip up-to-date files"; \
		echo "   Run 'make enqueue-batch START_BATCH=10' to continue with next batches"; \
		docker-compose run --rm app python -m app.cli enqueue-batch $$UNIVERSE_FILE \
			--batch-size 100 \
			--start-batch 0 \
			--max-batches 10 \
			--delay 10; \
	else \
		echo "❌ No universe file found."; \
	fi
	@echo ""
	@echo "✅ Full pipeline started!"
	@echo "   Monitor progress at http://localhost:5555"
	@echo "   Continue with: make enqueue-batch START_BATCH=10"

# Verify what would be downloaded (use the verify-pipeline command)
check-pipeline:
	@echo "🔍 Checking what the pipeline would do..."
	@UNIVERSE_FILE=$$(ls -t universe/us_equities_*.json 2>/dev/null | head -1); \
	if [ -n "$$UNIVERSE_FILE" ]; then \
		echo "   Using universe file: $$UNIVERSE_FILE"; \
		docker-compose run --rm app python -m app.cli verify-pipeline $$UNIVERSE_FILE \
			--dry-run --limit 20; \
	else \
		echo "❌ No universe file found. Run 'make discover' first."; \
	fi

# Force refresh (bypass automatic smart checking)
run-force:
	@echo "⚠️  WARNING: Force refresh will re-download ALL data!"
	@read -p "Are you sure you want to bypass smart checking? (y/n) " -n 1 -r; \
	echo ""; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		@UNIVERSE_FILE=$$(ls -t universe/us_equities_*.json 2>/dev/null | head -1); \
		if [ -n "$$UNIVERSE_FILE" ]; then \
			docker-compose run --rm app python -m app.cli enqueue $$UNIVERSE_FILE \
				--async --force-refresh; \
		fi \
	else \
		echo "Cancelled."; \
	fi

# Run all workers (10 total: 1 discovery + 9 download)
workers:
	@echo "👷 Starting all workers..."
	docker-compose up -d worker-discovery worker-1 worker-2 worker-3 worker-4 worker-5 worker-6 worker-7 worker-8 worker-9
	@echo "✅ Started 10 workers:"
	@echo "   - 1 discovery worker (handles timeframe discovery)"
	@echo "   - 9 download workers (parallel downloads)"
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

# Open API SwaggerUI
api-docs:
	@echo "📚 Opening API documentation..."
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8000/docs || \
	command -v open >/dev/null 2>&1 && open http://localhost:8000/docs || \
	echo "Please open http://localhost:8000/docs in your browser"

# Start API service only
api:
	@echo "🌐 Starting API service..."
	docker-compose up -d redis
	@sleep 2
	docker-compose up api

# Check API health
api-health:
	@echo "🏥 Checking API health..."
	@curl -s http://localhost:8000/health | python -m json.tool || echo "API not responding"

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

# Discover universe (limited for testing)
discover:
	@echo "🔍 Discovering US equities universe (limited to 100 for testing)..."
	docker-compose run --rm app python -m app.cli discover us_equities --limit 100

# Discover ALL US equities (up to 15k)
discover-full:
	@echo "🔍 Discovering ALL US equities (this may take a few minutes)..."
	@echo "   Expected: ~15,000 tickers"
	docker-compose run --rm app python -m app.cli discover-all

# Discover all categories
discover-all:
	@echo "🔍 Discovering all categories..."
	docker-compose run --rm app python -m app.cli discover us_equities --limit 100
	docker-compose run --rm app python -m app.cli discover etf
	docker-compose run --rm app python -m app.cli discover crypto --limit 50
	docker-compose run --rm app python -m app.cli discover fx --limit 25

# Run crypto pipeline
run-crypto:
	@echo "🪙 Starting crypto pipeline..."
	@echo "Discovering crypto tickers..."
	docker-compose run --rm app python -m app.cli discover crypto --limit 100
	@UNIVERSE_FILE=$$(ls -t universe/crypto_*.json 2>/dev/null | head -1); \
	if [ -n "$$UNIVERSE_FILE" ]; then \
		echo "Enqueueing crypto download tasks..."; \
		docker-compose run --rm app python -m app.cli enqueue $$UNIVERSE_FILE --async; \
	fi

# Run ETF pipeline
run-etf:
	@echo "📊 Starting ETF pipeline..."
	@echo "Discovering ALL ETF tickers..."
	docker-compose run --rm app python -m app.cli discover etf
	@UNIVERSE_FILE=$$(ls -t universe/etf_*.json 2>/dev/null | head -1); \
	if [ -n "$$UNIVERSE_FILE" ]; then \
		echo "Enqueueing ETF download tasks..."; \
		docker-compose run --rm app python -m app.cli enqueue $$UNIVERSE_FILE --async; \
	fi

# Enqueue jobs with auto-detection
enqueue:
	@UNIVERSE_FILE=$$(ls -t universe/us_equities_*.json 2>/dev/null | head -1); \
	if [ -n "$$UNIVERSE_FILE" ]; then \
		echo "📥 Enqueueing jobs from $$UNIVERSE_FILE with auto-detection..."; \
		docker-compose run --rm app python -m app.cli enqueue $$UNIVERSE_FILE --async; \
	else \
		echo "❌ No universe file found. Run 'make discover' first."; \
	fi

# Enqueue in batches (for large universes)
enqueue-batch:
	@UNIVERSE_FILE=$$(ls -t universe/us_equities_*.json 2>/dev/null | head -1); \
	if [ -n "$$UNIVERSE_FILE" ]; then \
		echo "📦 Processing universe in batches..."; \
		echo "   File: $$UNIVERSE_FILE"; \
		echo "   Batch size: $${BATCH_SIZE:-100}"; \
		echo "   Starting batch: $${START_BATCH:-0}"; \
		echo "   Max batches: $${MAX_BATCHES:-10}"; \
		docker-compose run --rm app python -m app.cli enqueue-batch $$UNIVERSE_FILE \
			--batch-size $${BATCH_SIZE:-100} \
			--start-batch $${START_BATCH:-0} \
			--max-batches $${MAX_BATCHES:-10} \
			--delay $${DELAY:-5}; \
	else \
		echo "❌ No universe file found. Run 'make discover' or 'make discover-full' first."; \
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

# Queue management commands
queue-status:
	@echo "📊 Checking Celery queue status..."
	@echo ""
	@echo "📈 Queue lengths:"
	@docker-compose exec -T redis redis-cli LLEN celery | xargs echo "   Default queue (celery):"
	@docker-compose exec -T redis redis-cli LLEN discovery | xargs echo "   Discovery queue:"
	@docker-compose exec -T redis redis-cli LLEN download | xargs echo "   Download queue:"
	@docker-compose exec -T redis redis-cli LLEN default | xargs echo "   Default tasks:"
	@echo ""
	@echo "🔄 Active tasks:"
	@docker-compose run --rm app celery -A app.celery_app inspect active --timeout=2 2>/dev/null || echo "   No active tasks or workers not running"
	@echo ""
	@echo "⏳ Reserved tasks:"
	@docker-compose run --rm app celery -A app.celery_app inspect reserved --timeout=2 2>/dev/null || echo "   No reserved tasks or workers not running"

queue-clear:
	@echo "⚠️  WARNING: This will clear ALL pending tasks from the queue!"
	@read -p "Are you sure? (y/n) " -n 1 -r; \
	echo ""; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo "🧹 Clearing all queues..."; \
		docker-compose exec -T redis redis-cli DEL celery; \
		docker-compose exec -T redis redis-cli DEL discovery; \
		docker-compose exec -T redis redis-cli DEL download; \
		docker-compose exec -T redis redis-cli DEL default; \
		docker-compose exec -T redis redis-cli FLUSHDB; \
		echo "✅ All queues cleared!"; \
	else \
		echo "Cancelled."; \
	fi

queue-purge:
	@echo "🧹 Purging all tasks from Celery..."
	docker-compose run --rm app celery -A app.celery_app purge -f
	@echo "✅ All tasks purged!"

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

# Safe run that checks queue first
run-clean: queue-status
	@echo ""
	@QUEUE_SIZE=$$(docker-compose exec -T redis redis-cli LLEN celery 2>/dev/null || echo "0"); \
	if [ "$$QUEUE_SIZE" -gt "0" ]; then \
		echo "⚠️  Found $$QUEUE_SIZE tasks in queue from previous run!"; \
		read -p "Clear queue before starting? (y/n) " -n 1 -r; \
		echo ""; \
		if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
			$(MAKE) queue-clear; \
		fi; \
	fi
	@echo ""
	$(MAKE) run

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
