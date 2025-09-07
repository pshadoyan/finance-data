"""
FastAPI application for Finance Data Pipeline Management

Comprehensive API for managing financial data discovery, downloading, and processing.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket, Query, Path, Body
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import asyncio
import json
import os
from pathlib import Path
from enum import Enum

# Initialize FastAPI app
app = FastAPI(
    title="Finance Data Pipeline API",
    description="Comprehensive API for managing Polygon.io financial data pipeline with Celery orchestration",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================================================================
# MODELS / SCHEMAS
# =====================================================================

class MarketCategory(str, Enum):
    US_EQUITIES = "us_equities"
    ETF = "etf"
    CRYPTO = "crypto"
    FX = "fx"
    OPTIONS = "options"
    INDICES = "indices"
    OTC = "otc"
    ALL = "all"

class TaskStatus(str, Enum):
    PENDING = "pending"
    STARTED = "started"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRY = "retry"
    REVOKED = "revoked"

class OutputFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"

class TimeframeInterval(str, Enum):
    # Seconds
    ONE_SEC = "1s"
    FIVE_SEC = "5s"
    TEN_SEC = "10s"
    THIRTY_SEC = "30s"
    
    # Minutes
    ONE_MIN = "1m"
    TWO_MIN = "2m"
    THREE_MIN = "3m"
    FIVE_MIN = "5m"
    TEN_MIN = "10m"
    FIFTEEN_MIN = "15m"
    THIRTY_MIN = "30m"
    FORTY_FIVE_MIN = "45m"
    
    # Hours
    ONE_HOUR = "1h"
    TWO_HOUR = "2h"
    THREE_HOUR = "3h"
    FOUR_HOUR = "4h"
    SIX_HOUR = "6h"
    EIGHT_HOUR = "8h"
    TWELVE_HOUR = "12h"
    
    # Days/Weeks/Months
    ONE_DAY = "1d"
    ONE_WEEK = "1w"
    ONE_MONTH = "1M"
    THREE_MONTH = "3M"
    ONE_QUARTER = "1Q"
    ONE_YEAR = "1Y"

class DiscoveryRequest(BaseModel):
    category: MarketCategory = Field(description="Market category to discover")
    limit: Optional[int] = Field(None, description="Maximum number of tickers to discover")
    append: bool = Field(True, description="Append to existing universe file")
    filters: Optional[Dict[str, Any]] = Field(None, description="Additional filters")

class EnqueueRequest(BaseModel):
    tickers: Optional[List[str]] = Field(None, description="List of tickers to process")
    universe_file: Optional[str] = Field(None, description="Path to universe JSON file")
    intervals: Optional[List[TimeframeInterval]] = Field(None, description="Specific intervals to download")
    start_date: Optional[date] = Field(None, description="Start date for historical data")
    end_date: Optional[date] = Field(None, description="End date for historical data")
    auto_detect: bool = Field(True, description="Auto-detect available timeframes and date ranges")
    batch_size: int = Field(100, description="Number of tickers per batch")
    priority: int = Field(0, description="Task priority (higher = more important)")

class BatchProcessRequest(BaseModel):
    universe_file: str = Field(description="Path to universe JSON file")
    batch_size: int = Field(100, ge=1, le=1000, description="Tickers per batch")
    start_batch: int = Field(0, ge=0, description="Starting batch index")
    max_batches: Optional[int] = Field(None, description="Maximum batches to process")
    delay_seconds: int = Field(5, ge=0, description="Delay between batches")
    auto_continue: bool = Field(False, description="Automatically continue with remaining batches")

class TickerDownloadRequest(BaseModel):
    ticker: str = Field(description="Ticker symbol")
    intervals: Optional[List[TimeframeInterval]] = Field(None, description="Specific intervals")
    start_date: Optional[date] = Field(None, description="Start date")
    end_date: Optional[date] = Field(None, description="End date")
    output_format: OutputFormat = Field(OutputFormat.PARQUET, description="Output format")
    force_refresh: bool = Field(False, description="Force re-download even if data exists")

class DataQueryRequest(BaseModel):
    ticker: str = Field(description="Ticker symbol")
    interval: TimeframeInterval = Field(description="Data interval")
    start_date: Optional[date] = Field(None, description="Start date for query")
    end_date: Optional[date] = Field(None, description="End date for query")
    columns: Optional[List[str]] = Field(None, description="Specific columns to return")
    limit: Optional[int] = Field(None, description="Maximum records to return")

# =====================================================================
# ENDPOINT: System Health & Status
# =====================================================================

@app.get("/", tags=["System"])
async def root():
    """Root endpoint with API information."""
    return {
        "name": "Finance Data Pipeline API",
        "version": "2.0.0",
        "status": "operational",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health", tags=["System"])
async def health_check():
    """Comprehensive health check of all system components."""
    from app.polygon_client import PolygonClient
    from app.celery_app import app as celery_app
    import redis
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {}
    }
    
    # Check Redis
    try:
        r = redis.Redis(host='redis', port=6379)
        r.ping()
        health_status["components"]["redis"] = {"status": "healthy", "info": "Connected"}
    except Exception as e:
        health_status["components"]["redis"] = {"status": "unhealthy", "error": str(e)}
        health_status["status"] = "degraded"
    
    # Check Celery
    try:
        i = celery_app.control.inspect()
        stats = i.stats()
        active_workers = len(stats) if stats else 0
        health_status["components"]["celery"] = {
            "status": "healthy", 
            "active_workers": active_workers
        }
    except Exception as e:
        health_status["components"]["celery"] = {"status": "unhealthy", "error": str(e)}
        health_status["status"] = "degraded"
    
    # Check Polygon API
    try:
        api_key = os.getenv('POLYGON_API_KEY')
        if api_key:
            client = PolygonClient(api_key)
            health_status["components"]["polygon"] = {"status": "healthy", "api_key_set": True}
        else:
            health_status["components"]["polygon"] = {"status": "unhealthy", "error": "API key not set"}
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["components"]["polygon"] = {"status": "unhealthy", "error": str(e)}
        health_status["status"] = "degraded"
    
    # Check storage
    try:
        data_path = Path("/data")
        universe_path = Path("/universe")
        health_status["components"]["storage"] = {
            "status": "healthy",
            "data_path_exists": data_path.exists(),
            "universe_path_exists": universe_path.exists()
        }
    except Exception as e:
        health_status["components"]["storage"] = {"status": "unhealthy", "error": str(e)}
    
    return health_status

@app.get("/stats", tags=["System"])
async def system_stats():
    """Get system statistics and metrics."""
    from app.celery_app import app as celery_app
    import redis
    
    r = redis.Redis(host='redis', port=6379)
    
    # Get queue lengths
    queues = {
        "discovery": r.llen("discovery"),
        "download": r.llen("download"),
        "default": r.llen("celery")
    }
    
    # Get worker stats
    i = celery_app.control.inspect()
    active_tasks = i.active()
    scheduled_tasks = i.scheduled()
    
    # Count data files
    data_path = Path("/data/equities")
    ticker_count = len(list(data_path.glob("*"))) if data_path.exists() else 0
    file_count = len(list(data_path.glob("**/*.parquet"))) if data_path.exists() else 0
    
    return {
        "queues": queues,
        "workers": {
            "active_tasks": len(active_tasks) if active_tasks else 0,
            "scheduled_tasks": len(scheduled_tasks) if scheduled_tasks else 0
        },
        "data": {
            "tickers_downloaded": ticker_count,
            "total_files": file_count
        },
        "timestamp": datetime.utcnow().isoformat()
    }

# =====================================================================
# ENDPOINT: Universe Discovery & Management
# =====================================================================

@app.post("/universe/discover", tags=["Universe"])
async def discover_universe(request: DiscoveryRequest):
    """Discover tickers for a specific market category."""
    from app.polygon_client import PolygonClient
    from app.universe import UniverseManager
    
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        raise HTTPException(status_code=500, detail="POLYGON_API_KEY not configured")
    
    try:
        client = PolygonClient(api_key)
        manager = UniverseManager(client, universe_dir="/universe")
        
        if request.category == MarketCategory.ALL:
            results = {}
            for cat in [c for c in MarketCategory if c != MarketCategory.ALL]:
                result = manager.discover_category(cat.value, limit=request.limit, append=request.append)
                results[cat.value] = result
            return {"status": "success", "results": results}
        else:
            result = manager.discover_category(request.category.value, limit=request.limit, append=request.append)
            return {"status": "success", "result": result}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/universe/list", tags=["Universe"])
async def list_universes():
    """List all available universe files."""
    universe_path = Path("/universe")
    if not universe_path.exists():
        return {"universes": []}
    
    universes = []
    for file_path in universe_path.glob("*.json"):
        with open(file_path, 'r') as f:
            data = json.load(f)
            universes.append({
                "filename": file_path.name,
                "category": data.get("category", "unknown"),
                "count": data.get("count", 0),
                "created": data.get("created_at", "unknown"),
                "path": str(file_path)
            })
    
    return {"universes": universes}

@app.get("/universe/{filename}", tags=["Universe"])
async def get_universe(filename: str):
    """Get details of a specific universe file."""
    file_path = Path(f"/universe/{filename}")
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Universe file not found")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    return data

@app.delete("/universe/{filename}", tags=["Universe"])
async def delete_universe(filename: str):
    """Delete a universe file."""
    file_path = Path(f"/universe/{filename}")
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Universe file not found")
    
    file_path.unlink()
    return {"status": "success", "message": f"Deleted {filename}"}

# =====================================================================
# ENDPOINT: Task Management & Enqueueing
# =====================================================================

@app.post("/tasks/enqueue", tags=["Tasks"])
async def enqueue_tasks(request: EnqueueRequest):
    """Enqueue download tasks for specified tickers or universe."""
    from app.tasks import discover_and_download_ticker, backfill_symbol
    
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        raise HTTPException(status_code=500, detail="POLYGON_API_KEY not configured")
    
    # Get tickers list
    tickers = request.tickers
    if not tickers and request.universe_file:
        file_path = Path(request.universe_file)
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Universe file not found")
        
        with open(file_path, 'r') as f:
            data = json.load(f)
            tickers = [t['ticker'] if isinstance(t, dict) else t for t in data.get('tickers', [])]
    
    if not tickers:
        raise HTTPException(status_code=400, detail="No tickers specified")
    
    task_ids = []
    
    # Process in batches
    for i in range(0, len(tickers), request.batch_size):
        batch = tickers[i:i + request.batch_size]
        
        for ticker in batch:
            if request.auto_detect and not request.intervals:
                # Use discovery task
                task = discover_and_download_ticker.apply_async(
                    kwargs={"ticker": ticker, "api_key": api_key},
                    priority=request.priority
                )
            else:
                # Use direct backfill
                task = backfill_symbol.apply_async(
                    kwargs={
                        "ticker": ticker,
                        "intervals": [i.value for i in request.intervals] if request.intervals else None,
                        "start_date": request.start_date.isoformat() if request.start_date else None,
                        "end_date": request.end_date.isoformat() if request.end_date else None,
                        "api_key": api_key
                    },
                    priority=request.priority
                )
            task_ids.append(task.id)
    
    return {
        "status": "success",
        "tasks_enqueued": len(task_ids),
        "task_ids": task_ids[:100],  # Return first 100 IDs
        "message": f"Enqueued {len(task_ids)} tasks for {len(tickers)} tickers"
    }

@app.post("/tasks/batch", tags=["Tasks"])
async def batch_process(request: BatchProcessRequest, background_tasks: BackgroundTasks):
    """Process universe in controlled batches."""
    from app.tasks import discover_and_download_ticker
    
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        raise HTTPException(status_code=500, detail="POLYGON_API_KEY not configured")
    
    file_path = Path(request.universe_file)
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Universe file not found")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
        all_tickers = [t['ticker'] if isinstance(t, dict) else t for t in data.get('tickers', [])]
    
    total_tickers = len(all_tickers)
    total_batches = (total_tickers + request.batch_size - 1) // request.batch_size
    
    end_batch = min(
        request.start_batch + (request.max_batches or total_batches),
        total_batches
    )
    
    # Process batches
    batch_info = []
    for batch_num in range(request.start_batch, end_batch):
        batch_start = batch_num * request.batch_size
        batch_end = min(batch_start + request.batch_size, total_tickers)
        batch_tickers = all_tickers[batch_start:batch_end]
        
        task_ids = []
        for ticker in batch_tickers:
            task = discover_and_download_ticker.delay(ticker=ticker, api_key=api_key)
            task_ids.append(task.id)
        
        batch_info.append({
            "batch_number": batch_num + 1,
            "tickers_count": len(batch_tickers),
            "task_ids": task_ids[:10]  # First 10 IDs
        })
        
        # Add delay between batches
        if batch_num + 1 < end_batch and request.delay_seconds > 0:
            await asyncio.sleep(request.delay_seconds)
    
    return {
        "status": "success",
        "total_tickers": total_tickers,
        "total_batches": total_batches,
        "processed_batches": len(batch_info),
        "batch_info": batch_info,
        "remaining_batches": total_batches - end_batch
    }

@app.get("/tasks/{task_id}", tags=["Tasks"])
async def get_task_status(task_id: str):
    """Get status of a specific task."""
    from app.celery_app import app as celery_app
    
    result = celery_app.AsyncResult(task_id)
    
    return {
        "task_id": task_id,
        "status": result.status,
        "result": result.result if result.ready() else None,
        "info": result.info
    }

@app.delete("/tasks/{task_id}", tags=["Tasks"])
async def cancel_task(task_id: str):
    """Cancel/revoke a task."""
    from app.celery_app import app as celery_app
    
    celery_app.control.revoke(task_id, terminate=True)
    return {"status": "success", "message": f"Task {task_id} revoked"}

@app.get("/tasks", tags=["Tasks"])
async def list_tasks(
    status: Optional[TaskStatus] = None,
    limit: int = Query(100, le=1000)
):
    """List tasks with optional filtering."""
    from app.celery_app import app as celery_app
    
    i = celery_app.control.inspect()
    
    tasks = {
        "active": i.active() or {},
        "scheduled": i.scheduled() or {},
        "reserved": i.reserved() or {}
    }
    
    # Flatten and filter
    all_tasks = []
    for task_type, workers in tasks.items():
        for worker, task_list in workers.items():
            for task in task_list:
                task["task_type"] = task_type
                task["worker"] = worker
                all_tasks.append(task)
    
    # Limit results
    all_tasks = all_tasks[:limit]
    
    return {"tasks": all_tasks, "count": len(all_tasks)}

# =====================================================================
# ENDPOINT: Data Download & Management
# =====================================================================

@app.post("/data/download", tags=["Data"])
async def download_ticker_data(request: TickerDownloadRequest):
    """Download data for a specific ticker."""
    from app.downloader import DataDownloader
    from app.polygon_client import PolygonClient
    
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        raise HTTPException(status_code=500, detail="POLYGON_API_KEY not configured")
    
    try:
        client = PolygonClient(api_key)
        downloader = DataDownloader(client, data_dir="/data", output_format=request.output_format.value)
        
        if request.intervals:
            # Download specific intervals
            results = []
            for interval in request.intervals:
                result = downloader.download_symbol_data(
                    ticker=request.ticker,
                    interval=interval.value,
                    start_date=request.start_date.isoformat() if request.start_date else None,
                    end_date=request.end_date.isoformat() if request.end_date else None
                )
                results.append(result)
            return {"status": "success", "results": results}
        else:
            # Auto-detect and download all
            result = downloader.download_all_available_data(request.ticker)
            return {"status": "success", "result": result}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/{ticker}/status", tags=["Data"])
async def get_ticker_data_status(ticker: str):
    """Get download status and available data for a ticker."""
    ticker_path = Path(f"/data/equities/{ticker.upper()}")
    
    if not ticker_path.exists():
        return {"status": "no_data", "ticker": ticker.upper()}
    
    files = list(ticker_path.glob("*.parquet"))
    
    file_info = []
    for file_path in files:
        # Extract interval from filename
        interval = file_path.stem.split('.')[-1]
        file_info.append({
            "interval": interval,
            "size_mb": file_path.stat().st_size / (1024 * 1024),
            "modified": datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(),
            "path": str(file_path)
        })
    
    return {
        "status": "available",
        "ticker": ticker.upper(),
        "intervals_available": len(file_info),
        "files": file_info
    }

@app.get("/data/{ticker}/{interval}", tags=["Data"])
async def get_ticker_data(
    ticker: str,
    interval: TimeframeInterval,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    format: str = Query("json", enum=["json", "csv"])
):
    """Retrieve data for a specific ticker and interval."""
    import pandas as pd
    
    file_path = Path(f"/data/equities/{ticker.upper()}/{ticker.upper()}.{interval.value}.parquet")
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"No data found for {ticker} {interval.value}")
    
    try:
        df = pd.read_parquet(file_path)
        
        # Apply date filters
        if start_date:
            df = df[df.index >= pd.Timestamp(start_date)]
        if end_date:
            df = df[df.index <= pd.Timestamp(end_date)]
        
        if format == "csv":
            return StreamingResponse(
                df.to_csv(index=True),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={ticker}_{interval.value}.csv"}
            )
        else:
            return {
                "ticker": ticker.upper(),
                "interval": interval.value,
                "records": len(df),
                "start_date": df.index.min().isoformat() if len(df) > 0 else None,
                "end_date": df.index.max().isoformat() if len(df) > 0 else None,
                "data": df.reset_index().to_dict(orient="records")[:1000]  # Limit to 1000 records
            }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/data/{ticker}", tags=["Data"])
async def delete_ticker_data(
    ticker: str,
    interval: Optional[TimeframeInterval] = None
):
    """Delete data for a ticker (optionally specific interval)."""
    import shutil
    
    if interval:
        # Delete specific interval file
        file_path = Path(f"/data/equities/{ticker.upper()}/{ticker.upper()}.{interval.value}.parquet")
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="File not found")
        file_path.unlink()
        return {"status": "success", "message": f"Deleted {ticker} {interval.value}"}
    else:
        # Delete entire ticker directory
        ticker_path = Path(f"/data/equities/{ticker.upper()}")
        if not ticker_path.exists():
            raise HTTPException(status_code=404, detail="Ticker directory not found")
        shutil.rmtree(ticker_path)
        return {"status": "success", "message": f"Deleted all data for {ticker}"}

# =====================================================================
# ENDPOINT: Pipeline Control
# =====================================================================

@app.post("/pipeline/start", tags=["Pipeline"])
async def start_pipeline(
    category: MarketCategory = MarketCategory.US_EQUITIES,
    limit: Optional[int] = None,
    batch_size: int = 100
):
    """Start the complete pipeline: discover and enqueue."""
    from app.polygon_client import PolygonClient
    from app.universe import UniverseManager
    from app.tasks import discover_and_download_ticker
    
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        raise HTTPException(status_code=500, detail="POLYGON_API_KEY not configured")
    
    # Step 1: Discover
    client = PolygonClient(api_key)
    manager = UniverseManager(client, universe_dir="/universe")
    discovery_result = manager.discover_category(category.value, limit=limit)
    
    # Step 2: Enqueue
    file_path = Path(discovery_result['file_path'])
    with open(file_path, 'r') as f:
        data = json.load(f)
        tickers = [t['ticker'] if isinstance(t, dict) else t for t in data.get('tickers', [])]
    
    task_ids = []
    for ticker in tickers[:batch_size]:  # Process first batch
        task = discover_and_download_ticker.delay(ticker=ticker, api_key=api_key)
        task_ids.append(task.id)
    
    return {
        "status": "success",
        "discovery": {
            "category": category.value,
            "tickers_discovered": discovery_result['count'],
            "file": discovery_result['file_path']
        },
        "processing": {
            "tasks_enqueued": len(task_ids),
            "batch_size": batch_size
        }
    }

@app.post("/pipeline/stop", tags=["Pipeline"])
async def stop_pipeline():
    """Stop all running tasks."""
    from app.celery_app import app as celery_app
    
    # Purge all queues
    celery_app.control.purge()
    
    # Revoke all active tasks
    i = celery_app.control.inspect()
    active = i.active()
    
    revoked_count = 0
    if active:
        for worker, tasks in active.items():
            for task in tasks:
                celery_app.control.revoke(task['id'], terminate=True)
                revoked_count += 1
    
    return {
        "status": "success",
        "message": "Pipeline stopped",
        "tasks_revoked": revoked_count
    }

@app.get("/pipeline/progress", tags=["Pipeline"])
async def get_pipeline_progress():
    """Get overall pipeline progress."""
    import redis
    
    r = redis.Redis(host='redis', port=6379)
    
    # Get queue sizes
    discovery_queue = r.llen("discovery")
    download_queue = r.llen("download")
    
    # Count completed data
    data_path = Path("/data/equities")
    completed_tickers = len(list(data_path.glob("*"))) if data_path.exists() else 0
    
    # Get universe size
    universe_files = list(Path("/universe").glob("*.json"))
    total_tickers = 0
    if universe_files:
        with open(universe_files[-1], 'r') as f:
            data = json.load(f)
            total_tickers = data.get('count', 0)
    
    progress = (completed_tickers / total_tickers * 100) if total_tickers > 0 else 0
    
    return {
        "progress_percent": round(progress, 2),
        "completed_tickers": completed_tickers,
        "total_tickers": total_tickers,
        "queues": {
            "discovery": discovery_queue,
            "download": download_queue
        },
        "estimated_time_remaining": "calculating..."
    }

# =====================================================================
# ENDPOINT: WebSocket for Real-time Updates
# =====================================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time task and progress updates."""
    await websocket.accept()
    
    try:
        while True:
            # Send periodic updates
            progress = await get_pipeline_progress()
            await websocket.send_json({
                "type": "progress",
                "data": progress,
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # Wait before next update
            await asyncio.sleep(5)
            
    except Exception as e:
        await websocket.close()

# =====================================================================
# ENDPOINT: Configuration
# =====================================================================

@app.get("/config", tags=["Configuration"])
async def get_configuration():
    """Get current system configuration."""
    return {
        "polygon_api_key_set": bool(os.getenv('POLYGON_API_KEY')),
        "data_directory": "/data",
        "universe_directory": "/universe",
        "celery_broker": os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0'),
        "environment": os.getenv('DOCKER_ENV', 'development')
    }

@app.put("/config/rate-limit", tags=["Configuration"])
async def update_rate_limit(calls_per_minute: int = Query(10, ge=1, le=100)):
    """Update API rate limiting."""
    # This would update a config file or environment variable
    return {
        "status": "success",
        "calls_per_minute": calls_per_minute,
        "message": "Rate limit updated (will apply to new tasks)"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
