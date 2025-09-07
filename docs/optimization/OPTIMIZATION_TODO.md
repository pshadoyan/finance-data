# Polygon.io API Optimization TODO

Based on Polygon.io documentation review, we're not using their most efficient features.

## Critical Optimizations Needed

### 1. Use Grouped Daily Bars (HIGHEST PRIORITY)
- **Current**: Individual API calls for each ticker's daily data
- **Better**: Use `/v2/aggs/grouped/locale/us/market/stocks/{date}` 
- **Impact**: 1 API call instead of 10,000+
- **Implementation**: Add to `polygon_client.py`:
```python
def get_grouped_daily_bars(self, date: str):
    """Get ALL stocks' OHLC for a single day"""
    return self.client.get_grouped_daily_bars(date)
```

### 2. Integrate Flat Files for Historical Data
- **Current**: REST API for all historical data
- **Better**: Use S3 Flat Files for data >30 days old
- **Impact**: 100x faster bulk downloads
- **Implementation**: Already have `flat_files.py`, need to integrate with main pipeline

### 3. Use Snapshots for Universe Discovery
- **Current**: Paginating through `/v3/reference/tickers`
- **Better**: Use `/v2/snapshot/locale/us/markets/stocks/tickers`
- **Impact**: Get all active tickers with current prices in 1 call

## Hybrid Download Strategy

```python
def download_optimal(ticker, start_date, end_date):
    if data_age > 30_days:
        use_flat_files()  # Bulk S3 download
    elif interval == "1d" and multiple_tickers:
        use_grouped_daily()  # 1 call for all tickers
    else:
        use_rest_api()  # Current approach
```

## Features We Don't Need to Build

1. **Technical Indicators** - Polygon provides SMA, EMA, MACD, RSI
2. **Market Status** - `/v1/marketstatus/now` 
3. **Ticker News** - `/v2/reference/news`
4. **Financials** - `/vX/reference/financials`
5. **Dividends/Splits** - `/v3/reference/dividends` 

## Implementation Priority

1. **Week 1**: Integrate Grouped Daily Bars
   - Reduces daily bar downloads from hours to seconds
   
2. **Week 2**: Activate Flat Files integration
   - Use existing `flat_files.py` for historical data
   
3. **Week 3**: Hybrid strategy
   - Smart routing between REST, Grouped, and Flat Files

## API Limits Comparison

| Method | API Calls | Time | Data Volume |
|--------|-----------|------|-------------|
| Current (REST per ticker) | 10,000+ | Hours | Limited by rate |
| Grouped Daily | 1 per day | Seconds | All stocks |
| Flat Files | 0 (S3) | Minutes | Unlimited |

## Code Already Written but Unused

- `app/flat_files.py` - Complete S3 integration
- `HybridDataDownloader` class - Smart routing logic

## Next Steps

1. Test grouped daily bars endpoint
2. Connect flat_files.py to main pipeline  
3. Add smart routing in downloader.py
4. Update CLI to offer download strategies
