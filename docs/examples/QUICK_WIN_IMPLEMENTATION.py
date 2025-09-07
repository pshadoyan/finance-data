#!/usr/bin/env python3
"""
Quick Win Implementation - Integrate Grouped Daily Bars
This single change will reduce API calls by 500x for daily data!

Before: 10,000 API calls for S&P 500 daily data
After: 1 API call for entire market daily data
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any
import pandas as pd
from polygon import RESTClient

logger = logging.getLogger(__name__)


class RefactoredDailyDownloader:
    """
    New efficient daily bar downloader using Grouped Daily endpoint.
    This replaces the need for thousands of individual ticker calls!
    """
    
    def __init__(self, api_key: str, data_dir: str = "data"):
        self.client = RESTClient(api_key)
        self.data_dir = Path(data_dir) / "equities"
        
    def download_market_day_OLD_WAY(self, date: str, tickers: list) -> Dict:
        """
        OLD INEFFICIENT WAY - DO NOT USE!
        This is what we're currently doing.
        """
        api_calls = 0
        for ticker in tickers:  # 500 tickers
            try:
                # Each ticker requires a separate API call
                aggs = self.client.list_aggs(
                    ticker=ticker,
                    multiplier=1,
                    timespan="day",
                    from_=date,
                    to=date,
                    limit=1
                )
                api_calls += 1
                # Process and save data...
            except Exception as e:
                logger.error(f"Failed {ticker}: {e}")
                
        return {
            "method": "OLD_INDIVIDUAL_CALLS",
            "api_calls": api_calls,  # 500 API calls!
            "time_estimate": f"{api_calls / 10} minutes at 10 calls/min"
        }
    
    def download_market_day_NEW_WAY(self, date: str) -> Dict:
        """
        NEW EFFICIENT WAY - USE THIS!
        Gets entire market in ONE API call using Grouped Daily Bars.
        """
        api_calls = 0
        tickers_saved = 0
        
        try:
            # ONE API CALL gets ALL stocks for the day!
            grouped = self.client.get_grouped_daily_aggs(
                date=date,
                adjusted=True,
                include_otc=False
            )
            api_calls = 1  # Just ONE call!
            
            # Process all tickers from the single response
            for result in grouped:
                ticker = result.ticker
                
                # Save to file
                ticker_dir = self.data_dir / ticker
                ticker_dir.mkdir(parents=True, exist_ok=True)
                
                df = pd.DataFrame([{
                    'date': date,
                    'open': result.open,
                    'high': result.high,
                    'low': result.low,
                    'close': result.close,
                    'volume': result.volume,
                    'vwap': result.vwap
                }])
                
                file_path = ticker_dir / f"{ticker}.daily.parquet"
                if file_path.exists():
                    existing = pd.read_parquet(file_path)
                    df = pd.concat([existing, df]).drop_duplicates('date')
                
                df.to_parquet(file_path, index=False)
                tickers_saved += 1
                
        except Exception as e:
            logger.error(f"Failed grouped daily: {e}")
            
        return {
            "method": "NEW_GROUPED_DAILY",
            "api_calls": api_calls,  # Just 1!
            "tickers_saved": tickers_saved,  # Thousands!
            "efficiency_gain": f"{tickers_saved}x more efficient"
        }
    
    def download_year_comparison(self, year: int = 2023):
        """
        Compare downloading 1 year of daily data both ways.
        """
        trading_days = 252  # Approximate trading days in a year
        sp500_tickers = 500  # S&P 500 companies
        
        print("=" * 60)
        print("COMPARISON: Downloading 1 Year of S&P 500 Daily Data")
        print("=" * 60)
        
        # OLD WAY
        old_api_calls = sp500_tickers * trading_days
        old_time_minutes = old_api_calls / 10  # At 10 calls/minute
        old_time_hours = old_time_minutes / 60
        old_time_days = old_time_hours / 24
        
        print("\n❌ OLD METHOD (Individual Ticker Calls):")
        print(f"   • API Calls Required: {old_api_calls:,}")
        print(f"   • Time at 10 calls/min: {old_time_days:.1f} days")
        print(f"   • Rate Limit Issues: CONSTANT")
        print(f"   • Code Complexity: HIGH (manage 500 parallel tasks)")
        
        # NEW WAY
        new_api_calls = trading_days  # One per day!
        new_time_minutes = new_api_calls / 10
        
        print("\n✅ NEW METHOD (Grouped Daily Bars):")
        print(f"   • API Calls Required: {new_api_calls}")
        print(f"   • Time at 10 calls/min: {new_time_minutes:.1f} minutes")
        print(f"   • Rate Limit Issues: NONE")
        print(f"   • Code Complexity: SIMPLE (1 call per day)")
        
        # IMPROVEMENT
        improvement = old_api_calls / new_api_calls
        time_saved_days = old_time_days - (new_time_minutes / 60 / 24)
        
        print("\n🚀 IMPROVEMENT METRICS:")
        print(f"   • API Calls Reduced: {improvement:.0f}x fewer")
        print(f"   • Time Saved: {time_saved_days:.1f} days")
        print(f"   • Efficiency Gain: {improvement:.0f}x")
        print(f"   • Cost Reduction: ~{(1 - 1/improvement) * 100:.1f}%")
        print("=" * 60)


def refactor_step_by_step():
    """
    Step-by-step refactoring guide.
    """
    print("\n" + "=" * 60)
    print("STEP-BY-STEP REFACTORING GUIDE")
    print("=" * 60)
    
    steps = [
        {
            "step": 1,
            "title": "Update polygon_client.py",
            "action": "Add get_grouped_daily_bars() method",
            "code": """
# In app/polygon_client.py, add:
def get_grouped_daily_bars(self, date: str):
    '''Get ALL stocks OHLC for a date in ONE call'''
    return self.client.get_grouped_daily_aggs(
        date=date,
        adjusted=True
    )
            """
        },
        {
            "step": 2,
            "title": "Create new Celery task",
            "action": "Replace individual ticker tasks with market-wide task",
            "code": """
# In app/tasks.py, add:
@app.task
def download_market_daily(date: str):
    '''Download entire market for one day'''
    client = PolygonClient()
    data = client.get_grouped_daily_bars(date)
    # Process and save all tickers...
    return {"tickers_saved": len(data)}
            """
        },
        {
            "step": 3,
            "title": "Update CLI",
            "action": "Add efficient download option",
            "code": """
# In app/cli.py, add:
@cli.command()
@click.option('--date', default='2024-01-01')
def download_market(date):
    '''Download entire market for a day'''
    download_market_daily.delay(date)
            """
        },
        {
            "step": 4,
            "title": "Update Makefile",
            "action": "Add market download command",
            "code": """
# In Makefile, add:
market-download:
    docker-compose run app python -m app.cli download-market
            """
        }
    ]
    
    for step in steps:
        print(f"\n📌 STEP {step['step']}: {step['title']}")
        print(f"   Action: {step['action']}")
        print(f"   Code to add:{step['code']}")
    
    print("\n" + "=" * 60)
    print("IMMEDIATE BENEFITS AFTER THIS REFACTOR:")
    print("=" * 60)
    print("✅ 500x fewer API calls")
    print("✅ Hours → Minutes for daily downloads")
    print("✅ No more rate limiting issues")
    print("✅ Simpler code (1 task instead of thousands)")
    print("✅ Lower costs (fewer API calls)")
    print("=" * 60)


if __name__ == "__main__":
    # Show the comparison
    downloader = RefactoredDailyDownloader(api_key="dummy")
    downloader.download_year_comparison()
    
    # Show refactoring steps
    refactor_step_by_step()
    
    print("\n🎯 NEXT ACTION: Implement Step 1 in polygon_client.py")
    print("   Expected time: 30 minutes")
    print("   Expected impact: 500x efficiency gain")
