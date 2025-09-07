# Project Structure

## Overview
The finance-data pipeline is organized for clarity and modularity, separating core functionality from optimizations and documentation.

## Directory Layout

```
finance-data/
├── app/                        # Core application code
│   ├── __init__.py
│   ├── celery_app.py          # Celery configuration
│   ├── cli.py                 # Command-line interface
│   ├── downloader.py          # Core download logic
│   ├── polygon_client.py      # Polygon.io API client
│   ├── tasks.py               # Celery task definitions
│   ├── universe.py            # Ticker discovery/management
│   └── optimizers/            # Advanced optimization modules (not yet integrated)
│       ├── __init__.py
│       ├── flat_files.py      # S3 Flat Files for bulk historical data
│       ├── grouped_daily.py   # Grouped Daily Bars for efficient daily data
│       └── intraday_optimizer.py # Smart routing for intraday data
│
├── data/                      # Downloaded market data
│   └── equities/              # Stock data organized by ticker
│       └── {TICKER}/          # Individual ticker directories
│           ├── {TICKER}.1s.parquet
│           ├── {TICKER}.1m.parquet
│           ├── {TICKER}.5m.parquet
│           ├── {TICKER}.1h.parquet
│           └── {TICKER}.1d.parquet
│
├── docs/                      # Documentation
│   ├── PROJECT_STRUCTURE.md  # This file
│   ├── optimization/          # Optimization strategies and plans
│   │   ├── OPTIMIZATION_TODO.md
│   │   ├── REFACTORING_PLAN.md
│   │   └── CORRECT_OPTIMIZATION_STRATEGY.md
│   └── examples/              # Example implementations
│       └── QUICK_WIN_IMPLEMENTATION.py
│
├── universe/                  # Discovered ticker lists
│   └── {category}_{date}.json
│
├── docker-compose.yml         # Docker orchestration
├── Dockerfile                 # Container definition
├── Makefile                   # Automation commands
├── README.md                  # Main documentation
├── requirements.txt           # Python dependencies
└── .env                       # Environment variables (not in git)
```

## Module Descriptions

### Core Modules (`app/`)

- **celery_app.py**: Configures Celery with Redis broker, task routing, and worker settings
- **cli.py**: Click-based CLI for discover, enqueue, and run-once commands
- **downloader.py**: Handles data downloads with resume capability and deduplication
- **polygon_client.py**: Wraps Polygon.io REST API with rate limiting and retry logic
- **tasks.py**: Defines Celery tasks for distributed processing
- **universe.py**: Manages ticker discovery and universe file creation

### Optimizers (`app/optimizers/`)
**Note: These modules are implemented but NOT YET INTEGRATED into the main pipeline**

- **flat_files.py**: S3-based bulk historical data access (100x faster for historical)
- **grouped_daily.py**: Grouped Daily Bars API (10,000x fewer calls for daily data)
- **intraday_optimizer.py**: Smart routing between REST/Flat Files based on data age

## Data Flow

1. **Discovery**: `universe.py` discovers tickers via Polygon API
2. **Enqueueing**: `cli.py` creates Celery tasks for each ticker
3. **Processing**: `tasks.py` workers execute downloads in parallel
4. **Downloading**: `downloader.py` fetches data with `polygon_client.py`
5. **Storage**: Data saved to `data/equities/{TICKER}/` as Parquet files

## Docker Services

- **redis**: Message broker for Celery
- **app**: Main application container
- **worker-discovery**: Handles ticker discovery tasks
- **worker-1 to worker-5**: Parallel download workers
- **flower**: Web UI for monitoring tasks

## Key Features

- ✅ **Parallel Processing**: 6 workers for distributed downloads
- ✅ **Resume Capability**: Continues from last successful timestamp
- ✅ **Rate Limiting**: Respects API limits with configurable delays
- ✅ **All Timeframes**: Supports seconds to yearly intervals
- ✅ **Full History**: Downloads all available data (60+ years for some stocks)

## Pending Optimizations

The `app/optimizers/` directory contains powerful optimization modules that could dramatically improve performance:

1. **Grouped Daily Bars** (`grouped_daily.py`)
   - Would reduce daily bar API calls from 10,000+ to ~250
   - Gets entire market's daily OHLC in one call
   - **Status**: Implemented but not integrated

2. **Flat Files** (`flat_files.py`)
   - Would enable bulk historical downloads via S3
   - No API rate limits for historical data
   - **Status**: Implemented but not integrated

3. **Smart Routing** (`intraday_optimizer.py`)
   - Would automatically choose optimal data source
   - Routes to Flat Files for historical, REST for recent
   - **Status**: Implemented but not integrated

## Next Steps

To realize the full potential of this system:

1. Integrate `grouped_daily.py` for daily bars (500x improvement)
2. Connect `flat_files.py` for historical data (unlimited downloads)
3. Implement smart routing from `intraday_optimizer.py`

See `/docs/optimization/REFACTORING_PLAN.md` for detailed implementation steps.
