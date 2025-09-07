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
