"""
Intraday Data Optimization Strategy
Since there's NO grouped API for intraday, we use Flat Files for historical
and smart caching for recent data.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import boto3
import pyarrow.parquet as pq
from .flat_files import PolygonFlatFilesClient

logger = logging.getLogger(__name__)


class IntradayOptimizer:
    """
    Optimizes intraday data downloads using the best available method.
    No magic bullets here - just smart routing.
    """
    
    def __init__(self, polygon_client, s3_credentials: Dict):
        self.rest_client = polygon_client
        self.flat_files = PolygonFlatFilesClient(
            access_key=s3_credentials.get('access_key'),
            secret_key=s3_credentials.get('secret_key')
        )
        self.cache_dir = Path("data/cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get_intraday_data(
        self,
        tickers: List[str],
        intervals: List[str],
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        Smart router for intraday data - uses best source based on date.
        
        REALITY CHECK:
        - No grouped API for intraday exists
        - We must choose between REST (recent) or Flat Files (historical)
        """
        
        cutoff_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        results = {
            "method_used": [],
            "api_calls": 0,
            "s3_downloads": 0,
            "data": {}
        }
        
        # Split date range by cutoff
        historical_end = min(end_date, cutoff_date)
        recent_start = max(start_date, cutoff_date)
        
        # HISTORICAL DATA: Use Flat Files (Best option)
        if start_date < cutoff_date:
            logger.info(f"Using Flat Files for historical data ({start_date} to {historical_end})")
            results["method_used"].append("flat_files")
            
            # Download raw tick data from S3
            tick_data = self._download_historical_ticks(
                tickers, start_date, historical_end
            )
            results["s3_downloads"] = len(tick_data)
            
            # Aggregate locally to desired intervals
            for ticker in tickers:
                for interval in intervals:
                    bars = self._aggregate_ticks_to_bars(
                        tick_data[ticker], interval
                    )
                    results["data"][f"{ticker}_{interval}"] = bars
        
        # RECENT DATA: Must use REST API (No alternative)
        if end_date >= cutoff_date:
            logger.info(f"Using REST API for recent data ({recent_start} to {end_date})")
            results["method_used"].append("rest_api")
            
            # Check cache first
            for ticker in tickers:
                for interval in intervals:
                    cache_key = f"{ticker}_{interval}_{recent_start}_{end_date}"
                    
                    if self._is_cached(cache_key):
                        results["data"][f"{ticker}_{interval}"] = self._get_cached(cache_key)
                    else:
                        # No choice but individual API calls
                        bars = self.rest_client.get_bars(
                            ticker, interval, recent_start, end_date
                        )
                        results["api_calls"] += 1
                        results["data"][f"{ticker}_{interval}"] = bars
                        self._cache_data(cache_key, bars)
        
        return results
    
    def _download_historical_ticks(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str
    ) -> Dict[str, pd.DataFrame]:
        """
        Download raw tick data from S3 Flat Files.
        This avoids API limits entirely!
        """
        tick_data = {}
        
        # Download each day's data
        current = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current <= end:
            date_str = current.strftime('%Y-%m-%d')
            
            # Download entire day's trades (all tickers) from S3
            day_file = self.flat_files.download_day_data(
                date_str,
                data_type="trades",
                symbols=tickers  # Filter to our tickers
            )
            
            # Parse and store
            for ticker in tickers:
                ticker_file = Path(day_file) / f"{ticker}.parquet"
                if ticker_file.exists():
                    df = pd.read_parquet(ticker_file)
                    if ticker not in tick_data:
                        tick_data[ticker] = df
                    else:
                        tick_data[ticker] = pd.concat([tick_data[ticker], df])
            
            current += timedelta(days=1)
        
        return tick_data
    
    def _aggregate_ticks_to_bars(
        self,
        tick_df: pd.DataFrame,
        interval: str
    ) -> pd.DataFrame:
        """
        Aggregate tick data to OHLCV bars locally.
        This is where we create 1m, 5m, 15m, etc. from raw ticks.
        """
        if tick_df.empty:
            return pd.DataFrame()
        
        # Map interval to pandas resample rule
        interval_map = {
            "1s": "1S",
            "5s": "5S",
            "10s": "10S",
            "30s": "30S",
            "1m": "1T",
            "2m": "2T",
            "3m": "3T",
            "5m": "5T",
            "10m": "10T",
            "15m": "15T",
            "30m": "30T",
            "45m": "45T",
            "1h": "1H",
            "2h": "2H",
            "3h": "3H",
            "4h": "4H",
            "6h": "6H",
            "8h": "8H",
            "12h": "12H"
        }
        
        resample_rule = interval_map.get(interval, "1T")
        
        # Ensure timestamp column and set as index
        tick_df['timestamp'] = pd.to_datetime(tick_df['timestamp'])
        tick_df.set_index('timestamp', inplace=True)
        
        # Aggregate to OHLCV
        bars = tick_df['price'].resample(resample_rule).agg([
            ('open', 'first'),
            ('high', 'max'),
            ('low', 'min'),
            ('close', 'last')
        ])
        
        # Add volume if available
        if 'size' in tick_df.columns:
            bars['volume'] = tick_df['size'].resample(resample_rule).sum()
        
        # Remove empty bars
        bars = bars.dropna()
        
        return bars
    
    def _is_cached(self, cache_key: str) -> bool:
        """Check if data is cached."""
        cache_file = self.cache_dir / f"{cache_key}.parquet"
        if cache_file.exists():
            # Check if cache is fresh (less than 1 hour old)
            age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            return age.total_seconds() < 3600
        return False
    
    def _get_cached(self, cache_key: str) -> pd.DataFrame:
        """Get cached data."""
        cache_file = self.cache_dir / f"{cache_key}.parquet"
        return pd.read_parquet(cache_file)
    
    def _cache_data(self, cache_key: str, data: pd.DataFrame):
        """Cache data for future use."""
        cache_file = self.cache_dir / f"{cache_key}.parquet"
        data.to_parquet(cache_file)


def compare_intraday_methods():
    """
    Show the reality of intraday data optimization.
    """
    print("=" * 70)
    print("INTRADAY DATA OPTIMIZATION - THE REALITY")
    print("=" * 70)
    
    print("\n📊 DAILY BARS - HUGE WIN:")
    print("  • Grouped Daily API available ✅")
    print("  • 10,000x fewer API calls")
    print("  • Get entire market in 1 call")
    
    print("\n⏱️ INTRADAY BARS - LIMITED OPTIONS:")
    print("  • NO grouped API exists ❌")
    print("  • Must choose between:")
    print()
    
    print("  Option 1: REST API (Recent Data)")
    print("    • Individual calls per ticker/interval")
    print("    • Rate limited (10 calls/min)")
    print("    • Good for: Today's data, real-time")
    print("    • Example: 500 tickers × 5 intervals = 2,500 API calls")
    print()
    
    print("  Option 2: Flat Files S3 (Historical)")
    print("    • Download raw ticks in bulk")
    print("    • No API limits")
    print("    • Aggregate locally to any interval")
    print("    • Good for: Historical data (T-1 and older)")
    print("    • Example: 1 S3 download = ALL tickers' ticks for a day")
    print()
    
    print("  Option 3: Hybrid Approach (Best)")
    print("    • Flat Files for historical (>1 day old)")
    print("    • REST API for recent (<1 day old)")
    print("    • Smart caching to avoid repeat calls")
    print()
    
    print("=" * 70)
    print("OPTIMIZATION IMPACT BY DATA TYPE")
    print("=" * 70)
    
    scenarios = [
        {
            "name": "Daily bars - 1 year - 500 stocks",
            "current": "126,000 API calls",
            "optimized": "252 API calls (Grouped Daily)",
            "improvement": "500x"
        },
        {
            "name": "Historical intraday - 1 year - 500 stocks",
            "current": "630,000 API calls",
            "optimized": "252 S3 downloads",
            "improvement": "2,500x"
        },
        {
            "name": "Today's intraday - 500 stocks - 5 intervals",
            "current": "2,500 API calls",
            "optimized": "2,500 API calls (no optimization)",
            "improvement": "None"
        }
    ]
    
    for s in scenarios:
        print(f"\n{s['name']}:")
        print(f"  Current:   {s['current']}")
        print(f"  Optimized: {s['optimized']}")
        print(f"  Improvement: {s['improvement']}")
    
    print("\n" + "=" * 70)
    print("KEY TAKEAWAY:")
    print("  • Historical data: MASSIVE optimization possible ✅")
    print("  • Daily bars: MASSIVE optimization possible ✅")
    print("  • Recent intraday: Limited optimization ⚠️")
    print("=" * 70)


if __name__ == "__main__":
    compare_intraday_methods()
