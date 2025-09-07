# Finance Data Pipeline API Documentation

## 🚀 Quick Start

```bash
# Start the API service
make api

# Or start everything
make up

# Open SwaggerUI
make api-docs
# Or navigate to: http://localhost:8000/docs
```

## 📚 Complete API Endpoint Reference

### Base URL
- **Local**: `http://localhost:8000`
- **SwaggerUI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

---

## 🏥 System & Health

### `GET /`
Root endpoint with API information

### `GET /health`
Comprehensive health check of all components
- Redis connectivity
- Celery workers status
- Polygon API status
- Storage availability

### `GET /stats`
System statistics and metrics
- Queue lengths
- Active/scheduled tasks
- Downloaded data counts

### `GET /config`
Current system configuration

### `PUT /config/rate-limit`
Update API rate limiting
- Query param: `calls_per_minute` (1-100)

---

## 🔍 Universe Discovery & Management

### `POST /universe/discover`
Discover tickers for a market category
```json
{
  "category": "us_equities",  // us_equities, etf, crypto, fx, options, indices, otc, all
  "limit": 100,               // Optional: max tickers
  "append": true,             // Append to existing universe
  "filters": {}               // Additional filters
}
```

### `GET /universe/list`
List all available universe files

### `GET /universe/{filename}`
Get details of a specific universe file

### `DELETE /universe/{filename}`
Delete a universe file

---

## 📋 Task Management

### `POST /tasks/enqueue`
Enqueue download tasks
```json
{
  "tickers": ["AAPL", "GOOGL"],     // OR use universe_file
  "universe_file": "/universe/us_equities_20250907.json",
  "intervals": ["1d", "1h", "1m"],  // Optional: specific intervals
  "start_date": "2020-01-01",       // Optional
  "end_date": "2024-12-31",         // Optional
  "auto_detect": true,               // Auto-detect timeframes/ranges
  "batch_size": 100,
  "priority": 0
}
```

### `POST /tasks/batch`
Process universe in controlled batches
```json
{
  "universe_file": "/universe/us_equities_20250907.json",
  "batch_size": 100,      // Tickers per batch
  "start_batch": 0,        // Starting batch index
  "max_batches": 10,       // Max batches to process
  "delay_seconds": 5,      // Delay between batches
  "auto_continue": false
}
```

### `GET /tasks/{task_id}`
Get status of a specific task

### `DELETE /tasks/{task_id}`
Cancel/revoke a task

### `GET /tasks`
List all tasks
- Query params:
  - `status`: pending, started, success, failure, retry, revoked
  - `limit`: max results (default 100, max 1000)

---

## 💾 Data Download & Management

### `POST /data/download`
Download data for a ticker
```json
{
  "ticker": "AAPL",
  "intervals": ["1d", "1h", "1m"],  // Optional: specific intervals
  "start_date": "2020-01-01",       // Optional
  "end_date": "2024-12-31",         // Optional
  "output_format": "parquet",       // parquet, csv, json
  "force_refresh": false             // Force re-download
}
```

### `GET /data/{ticker}/status`
Get download status and available data for a ticker

### `GET /data/{ticker}/{interval}`
Retrieve data for specific ticker/interval
- Query params:
  - `start_date`: filter start
  - `end_date`: filter end
  - `format`: json or csv (default json)

### `DELETE /data/{ticker}`
Delete data for a ticker
- Query param: `interval` (optional, deletes specific interval only)

---

## 🔄 Pipeline Control

### `POST /pipeline/start`
Start complete pipeline (discover + enqueue)
- Query params:
  - `category`: us_equities, etf, crypto, fx, etc.
  - `limit`: max tickers to discover
  - `batch_size`: tickers per batch

### `POST /pipeline/stop`
Stop all running tasks

### `GET /pipeline/progress`
Get overall pipeline progress
- Returns: progress percentage, queue sizes, estimated time

---

## 🔌 WebSocket Real-time Updates

### `WS /ws`
WebSocket connection for real-time updates
```javascript
// JavaScript example
const ws = new WebSocket('ws://localhost:8000/ws');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Update:', data);
  // data.type: 'progress', 'task_update', etc.
  // data.data: actual update payload
};
```

---

## 📊 Timeframe Intervals

All available timeframe intervals:

### Seconds
- `1s` - 1 second
- `5s` - 5 seconds
- `10s` - 10 seconds
- `30s` - 30 seconds

### Minutes
- `1m` - 1 minute
- `2m` - 2 minutes
- `3m` - 3 minutes
- `5m` - 5 minutes
- `10m` - 10 minutes
- `15m` - 15 minutes
- `30m` - 30 minutes
- `45m` - 45 minutes

### Hours
- `1h` - 1 hour
- `2h` - 2 hours
- `3h` - 3 hours
- `4h` - 4 hours
- `6h` - 6 hours
- `8h` - 8 hours
- `12h` - 12 hours

### Daily+
- `1d` - 1 day
- `1w` - 1 week
- `1M` - 1 month
- `3M` - 3 months
- `1Q` - 1 quarter
- `1Y` - 1 year

---

## 🔥 Example Workflows

### 1. Full Pipeline via API
```bash
# 1. Discover all US equities
curl -X POST http://localhost:8000/universe/discover \
  -H "Content-Type: application/json" \
  -d '{"category": "us_equities", "limit": null}'

# 2. Process in batches
curl -X POST http://localhost:8000/tasks/batch \
  -H "Content-Type: application/json" \
  -d '{
    "universe_file": "/universe/us_equities_20250907.json",
    "batch_size": 100,
    "max_batches": 10
  }'

# 3. Monitor progress
curl http://localhost:8000/pipeline/progress
```

### 2. Download Specific Ticker
```bash
curl -X POST http://localhost:8000/data/download \
  -H "Content-Type: application/json" \
  -d '{
    "ticker": "AAPL",
    "intervals": ["1d", "1h", "1m"],
    "start_date": "2023-01-01"
  }'
```

### 3. Get Data for Analysis
```bash
# Get AAPL daily data as JSON
curl "http://localhost:8000/data/AAPL/1d?start_date=2023-01-01&format=json"

# Download as CSV
curl "http://localhost:8000/data/AAPL/1d?format=csv" -o aapl_daily.csv
```

### 4. Real-time Monitoring
```python
import asyncio
import websockets
import json

async def monitor():
    async with websockets.connect('ws://localhost:8000/ws') as websocket:
        while True:
            data = await websocket.recv()
            update = json.loads(data)
            print(f"Progress: {update['data']['progress_percent']}%")

asyncio.run(monitor())
```

---

## 🎯 Best Practices

1. **Batch Processing**: For large universes (>1000 tickers), use batch processing with delays
2. **Rate Limiting**: Keep `calls_per_minute` reasonable (10-20 for free tier)
3. **Monitoring**: Use WebSocket or periodic `/pipeline/progress` calls
4. **Error Handling**: Check task status before assuming completion
5. **Data Management**: Periodically clean old data with DELETE endpoints

---

## 🛠️ Development Tips

### Testing Endpoints
```bash
# Health check
curl http://localhost:8000/health | jq

# Quick test with single ticker
curl -X POST http://localhost:8000/data/download \
  -H "Content-Type: application/json" \
  -d '{"ticker": "AAPL", "intervals": ["1d"]}'

# Monitor with WebSocket
wscat -c ws://localhost:8000/ws
```

### Using SwaggerUI
1. Navigate to http://localhost:8000/docs
2. Click "Try it out" on any endpoint
3. Fill parameters
4. Click "Execute"
5. View response

### Python Client Example
```python
import requests

# Base URL
BASE_URL = "http://localhost:8000"

# Discover tickers
response = requests.post(f"{BASE_URL}/universe/discover", json={
    "category": "us_equities",
    "limit": 100
})
print(response.json())

# Enqueue tasks
response = requests.post(f"{BASE_URL}/tasks/enqueue", json={
    "tickers": ["AAPL", "GOOGL", "MSFT"],
    "auto_detect": True
})
print(response.json())

# Check progress
response = requests.get(f"{BASE_URL}/pipeline/progress")
print(response.json())
```

---

## 📝 Notes

- All endpoints return JSON responses
- Dates should be in ISO format (YYYY-MM-DD)
- Task IDs are Celery task UUIDs
- File paths are relative to container volumes
- WebSocket sends updates every 5 seconds
- API supports CORS for browser-based clients
