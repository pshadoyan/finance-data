# Finance Data Pipeline 📊

A high-performance, Celery-based data pipeline for downloading and managing financial market data from Polygon.io. Features auto-discovery, bulk downloads, resume capability, and Docker orchestration.

## Features ✨

- **Celery + Redis** for distributed job orchestration and queueing
- **Auto-discovery CLI** for finding tickers by category (stocks, ETFs, crypto, FX)
- **Multi-timeframe support** (1m, 5m, 15m, 1h, 1d) with resume capability
- **Polygon.io integration** with both REST API and Flat Files (S3) support
- **Rate limiting & backoff** to respect API limits
- **Dockerized** with one-command deployment
- **Flower UI** for monitoring job progress
- **Parquet format** for efficient storage

## Quick Start 🚀

### 1. Clone and Setup

```bash
# Clone the repository
git clone <your-repo-url>
cd finance-data

# Copy environment file (already configured with your keys)
# .env is already set up with your Polygon.io credentials
```

### 2. Run the Pipeline

```bash
# Full pipeline: build, start services, discover universe, enqueue jobs
make run

# In another terminal, monitor the workers
make logs-worker

# Open Flower UI to monitor jobs (http://localhost:5555)
make flower
```

## Architecture 🏗️

### Components

1. **Polygon Client** (`app/polygon_client.py`)
   - REST API integration with rate limiting
   - Automatic retry with exponential backoff
   - Pagination support for large datasets

2. **Flat Files Client** (`app/flat_files.py`)
   - S3-based bulk data access for historical data
   - Optimized for large-scale downloads
   - Aggregation from tick to OHLCV bars

3. **Downloader** (`app/downloader.py`)
   - Resume capability from last downloaded timestamp
   - Duplicate detection and merging
   - Parquet/CSV output formats

4. **Universe Manager** (`app/universe.py`)
   - Auto-discovery by category
   - Ticker filtering and management
   - JSON-based universe storage

5. **Celery Tasks** (`app/tasks.py`)
   - Distributed backfill tasks
   - Progress tracking
   - Bulk enqueue capabilities

6. **CLI** (`app/cli.py`)
   - `discover` - Find tickers by category
   - `enqueue` - Queue download jobs
   - `run-once` - Direct download for testing

## Usage Examples 📝

### Discover Tickers

```bash
# Discover US equities (limited to 1000)
docker-compose run --rm app python -m app.cli discover us_equities --limit 1000

# Discover ETFs
docker-compose run --rm app python -m app.cli discover etf --limit 500

# Discover crypto
docker-compose run --rm app python -m app.cli discover crypto --limit 100

# List all available categories
docker-compose run --rm app python -m app.cli discover --help
```

### Enqueue Download Jobs

```bash
# Enqueue jobs for discovered universe (async with Celery)
docker-compose run --rm app python -m app.cli enqueue universe/us_equities_20241201.json \
  --timeframes 1m,5m,15m,1h,1d \
  --start 2024-01-01 \
  --end 2024-12-01 \
  --async

# Synchronous download (without Celery)
docker-compose run --rm app python -m app.cli enqueue universe/etf_20241201.json \
  --timeframes 1d \
  --start 2024-01-01
```

### Test Single Ticker

```bash
# Quick test with single ticker
make test-ticker TICKER=AAPL

# Or using CLI directly
docker-compose run --rm app python -m app.cli run-once AAPL \
  --timeframes 1h,1d \
  --start 2024-11-01
```

### Monitor Progress

```bash
# Check system status
make status

# View worker logs
make logs-worker

# Open Flower UI
make flower  # Opens http://localhost:5555

# List downloaded data
docker-compose run --rm app python -m app.cli status AAPL --interval 1d
```

## Makefile Commands 🛠️

| Command | Description |
|---------|-------------|
| `make run` | Full pipeline: build, start, discover, enqueue |
| `make up` | Start all services |
| `make down` | Stop and remove containers |
| `make worker` | Run additional worker |
| `make flower` | Open Flower monitoring UI |
| `make logs` | Show all service logs |
| `make status` | Check system and queue status |
| `make discover` | Discover US equities |
| `make discover-all` | Discover all categories |
| `make clean` | Remove containers, volumes, data |

## Data Organization 📁

```
finance-data/
├── data/                   # Downloaded market data
│   ├── AAPL/
│   │   ├── AAPL.1m.parquet
│   │   ├── AAPL.5m.parquet
│   │   ├── AAPL.1h.parquet
│   │   └── AAPL.1d.parquet
│   └── TSLA/
│       └── ...
├── universe/               # Discovered ticker lists
│   ├── us_equities_20241201.json
│   ├── etf_20241201.json
│   └── crypto_20241201.json
└── app/                    # Application code
```

## Configuration ⚙️

### Environment Variables (.env)

```env
# Polygon.io API
POLYGON_API_KEY=your_api_key

# Polygon.io S3/Flat Files (optional)
POLYGON_S3_ACCESS_KEY=your_access_key
POLYGON_S3_SECRET_KEY=your_secret_key

# Rate limiting
POLYGON_RATE_LIMIT=5  # calls per minute

# Worker settings
WORKER_CONCURRENCY=2
BATCH_SIZE=10
```

### Categories Supported

- `us_equities` - US common stocks
- `etf` - Exchange-traded funds
- `crypto` - Cryptocurrencies
- `fx` - Foreign exchange pairs
- `options` - Options contracts
- `indices` - Market indices
- `otc` - Over-the-counter stocks

### Timeframes Available

- `1m` - 1 minute bars
- `5m` - 5 minute bars
- `15m` - 15 minute bars
- `1h` - 1 hour bars
- `1d` - Daily bars

## Advanced Features 🔧

### Hybrid Download Strategy

The pipeline automatically uses the optimal data source:
- **Flat Files (S3)** for bulk historical data (>30 days old)
- **REST API** for recent/incremental updates (<30 days)

### Resume Capability

Downloads automatically resume from the last successful timestamp:
- Checks existing parquet files
- Identifies last downloaded timestamp
- Continues from next available data point

### Rate Limiting

Configurable rate limiting per worker:
- Default: 5 calls/minute for free tier
- Adjustable via `POLYGON_RATE_LIMIT` env var
- Automatic backoff on 429 errors

## Customizing Pipeline Parameters 🎛️

### Quick Start Customization

The pipeline is designed to be easily customizable based on your system resources and API limits. Here are the most common adjustments:

```bash
# For testing (low resource usage)
make run  # Default: 100 tickers, 6 workers

# For production (full dataset)
make run-full  # All tickers, batch processing

# Custom ticker limit
docker-compose run --rm app python -m app.cli discover us_equities --limit 500
```

### Worker Configuration

#### Adjusting Worker Count

The default configuration runs 6 workers (1 discovery + 5 download). To scale:

**Method 1: Add More Workers**

Edit `docker-compose.yml` and duplicate a worker block:

```yaml
worker-6:
  build:
    context: .
    dockerfile: Dockerfile
  container_name: finance-worker-6
  env_file:
    - .env
  volumes:
    - ./data:/data
    - ./universe:/universe
    - ./app:/app/app:ro
  depends_on:
    redis:
      condition: service_healthy
  networks:
    - finance-network
  environment:
    - DOCKER_ENV=true
    - CELERY_BROKER_URL=redis://redis:6379/0
    - CELERY_RESULT_BACKEND=redis://redis:6379/0
    - C_FORCE_ROOT=true
  command: celery -A app.celery_app worker --loglevel=info --concurrency=2 -Q download,default -n worker6@%h
  deploy:
    resources:
      limits:
        cpus: '1'
        memory: 1G
```

Then update the `Makefile` to include the new workers:

```makefile
workers:
	@echo "👷 Starting all workers..."
	docker-compose up -d worker-discovery worker-1 worker-2 worker-3 worker-4 worker-5 worker-6
	@echo "✅ Started 7 workers:"
```

**Method 2: Scale Existing Workers**

```bash
# Run multiple instances of the same worker
docker-compose up -d --scale worker-1=3
```

#### Adjusting Concurrency

Each worker's concurrency setting controls parallel task execution:

```yaml
# In docker-compose.yml, modify the command:
command: celery -A app.celery_app worker --loglevel=info --concurrency=4 -Q download,default -n worker1@%h
#                                                         ^^^^^^^^^^^^^^^^
```

**Recommended Concurrency Settings:**

| System Resources | Workers | Concurrency | Total Tasks |
|-----------------|---------|-------------|-------------|
| Low (2-4 cores) | 2-3 | 1 | 2-3 |
| Medium (8-16 cores) | 4-6 | 2 | 8-12 |
| High (20+ cores) | 8-10 | 3-4 | 24-40 |

#### Resource Limits

Control CPU and memory allocation per worker:

```yaml
# In docker-compose.yml under each worker:
deploy:
  resources:
    limits:
      cpus: '2'      # Number of CPU cores
      memory: 2G     # RAM allocation
```

### Performance Optimization Profiles

#### Profile 1: Free Tier / Testing
```yaml
# Optimized for Polygon.io free tier (5 requests/minute)
Workers: 2
Concurrency: 1
CPU per worker: 0.5
Memory: 512M
Rate limit: 5/min
```

```bash
# Implementation:
# 1. Set in .env:
POLYGON_RATE_LIMIT=5
WORKER_CONCURRENCY=1

# 2. Reduce workers in Makefile:
docker-compose up -d worker-discovery worker-1
```

#### Profile 2: Standard / Paid Tier
```yaml
<code_block_to_apply_changes_from>
```

```bash
# This is the default configuration
make run
```

#### Profile 3: High Performance
```yaml
# Maximum throughput (1000+ requests/minute)
Workers: 10
Concurrency: 4
CPU per worker: 2
Memory: 2G
Rate limit: 1000/min
```

```bash
# Implementation:
# 1. Add workers 6-9 in docker-compose.yml
# 2. Increase concurrency to 4
# 3. Update CPU/memory limits
# 4. Set in .env:
POLYGON_RATE_LIMIT=1000
WORKER_CONCURRENCY=4
```

### Batch Processing Control

Fine-tune how tickers are processed:

```bash
# Process specific batch ranges
make enqueue-batch BATCH_SIZE=50 START_BATCH=0 MAX_BATCHES=20

# Add delays to respect rate limits
make enqueue-batch DELAY=30  # 30 seconds between batches

# Custom batch processing
docker-compose run --rm app python -m app.cli enqueue-batch \
  universe/us_equities_*.json \
  --batch-size 25 \
  --start-batch 10 \
  --max-batches 5 \
  --delay 60
```

### Environment Variables

Create custom configurations using environment variables:

```bash
# .env.production
POLYGON_API_KEY=your_key
POLYGON_RATE_LIMIT=500
WORKER_CONCURRENCY=3
WORKER_MAX_TASKS_PER_CHILD=200
BATCH_SIZE=100

# Use custom environment
docker-compose --env-file .env.production up
```

### Monitoring & Tuning

Track performance after adjustments:

```bash
# Real-time resource usage
docker stats

# Queue depth monitoring
watch -n 5 'docker-compose exec -T redis redis-cli LLEN celery'

# Worker throughput
docker-compose logs worker-1 | grep "succeeded in" | tail -20

# Task completion rate (Flower UI)
make flower  # Navigate to Tasks tab

# System health check
make status
```

### Optimization Decision Tree

```
Start Here
    ↓
How many API requests/minute allowed?
    ├─ 5 (Free) → Use 2 workers, concurrency=1
    ├─ 100-500 → Use 4-6 workers, concurrency=2
    └─ 1000+ → Use 8-10 workers, concurrency=3-4
        ↓
How many CPU cores available?
    ├─ <4 → Limit to 2-3 workers
    ├─ 8-16 → Use 4-6 workers
    └─ 20+ → Scale to 10+ workers
        ↓
How much RAM available?
    ├─ <4GB → 512MB per worker
    ├─ 8-16GB → 1GB per worker
    └─ 32GB+ → 2GB per worker
```

### Common Scenarios

#### Scenario: "I'm hitting rate limits"
```bash
# Reduce concurrent requests
- Decrease worker count to 2-3
- Set concurrency=1
- Add delays: TASK_DELAY=5
- Lower POLYGON_RATE_LIMIT in .env
```

#### Scenario: "Workers are idle"
```bash
# Increase throughput
- Add more workers (up to 10)
- Increase concurrency (up to 4)
- Remove CPU limits
- Check queue depth: tasks might be bottlenecked elsewhere
```

#### Scenario: "System running out of memory"
```bash
# Optimize memory usage
- Reduce worker_max_tasks_per_child to 50
- Limit memory per worker to 512MB
- Process smaller batches
- Reduce concurrency
```

## Troubleshooting 🔍

### Common Issues

1. **Rate limit errors (429)**
   - Reduce `POLYGON_RATE_LIMIT` in .env
   - Decrease worker concurrency
   - Add delays between batches

2. **Memory issues**
   - Adjust `WORKER_MAX_TASKS_PER_CHILD`
   - Reduce batch size
   - Limit worker memory in docker-compose

3. **Missing data**
   - Check market hours for the ticker
   - Verify date range is valid
   - Some tickers may have limited history

### Debug Commands

```bash
# Check Redis connection
docker-compose exec redis redis-cli ping

# Inspect Celery queues
docker-compose exec redis redis-cli LLEN celery

# View failed tasks
docker-compose run --rm app celery -A app.celery_app inspect failed

# Clear all queues (use with caution)
docker-compose exec redis redis-cli FLUSHALL
```

## Performance Tips 🚀

1. **Scale workers carefully** - More workers = more API calls
2. **Use Flat Files** for bulk historical downloads
3. **Download during off-hours** to avoid rate limits
4. **Start with daily bars** before higher frequencies
5. **Monitor with Flower** to identify bottlenecks

## License 📄

MIT License - See LICENSE file for details

## Support 💬

For issues or questions:
1. Check the logs: `make logs`
2. Monitor jobs: http://localhost:5555
3. Review status: `make status`

---

Built with ❤️ using Polygon.io, Celery, Redis, and Docker
