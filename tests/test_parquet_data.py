#!/usr/bin/env python3
"""
Test script to analyze parquet data for an equity and see how far back 
the historical data goes for each timeframe.
"""

import os
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
import argparse
from typing import Dict, Any, Optional

def analyze_parquet_file(file_path: Path) -> Dict[str, Any]:
    """Analyze a single parquet file and return statistics."""
    try:
        # Read the parquet file
        df = pd.read_parquet(file_path)
        
        if df.empty:
            return {
                "file": file_path.name,
                "status": "empty",
                "records": 0
            }
        
        # Ensure timestamp column is datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        else:
            return {
                "file": file_path.name,
                "status": "no_timestamp",
                "records": len(df)
            }
        
        # Get statistics
        stats = {
            "file": file_path.name,
            "status": "ok",
            "records": len(df),
            "first_date": df['timestamp'].min(),
            "last_date": df['timestamp'].max(),
            "date_range_days": (df['timestamp'].max() - df['timestamp'].min()).days,
            "columns": list(df.columns),
            "file_size_kb": file_path.stat().st_size / 1024
        }
        
        # Sample some data
        sample_data = {
            "first_5_records": df.head(5)[['timestamp', 'open', 'high', 'low', 'close', 'volume']].to_dict('records') if all(col in df.columns for col in ['open', 'high', 'low', 'close', 'volume']) else None,
            "last_5_records": df.tail(5)[['timestamp', 'open', 'high', 'low', 'close', 'volume']].to_dict('records') if all(col in df.columns for col in ['open', 'high', 'low', 'close', 'volume']) else None,
        }
        stats['sample_data'] = sample_data
        
        return stats
        
    except Exception as e:
        return {
            "file": file_path.name,
            "status": "error",
            "error": str(e)
        }

def analyze_ticker_data(ticker: str, data_dir: str = "/data") -> Dict[str, Any]:
    """Analyze all parquet files for a given ticker."""
    ticker = ticker.upper()
    ticker_dir = Path(data_dir) / "equities" / ticker
    
    if not ticker_dir.exists():
        return {
            "ticker": ticker,
            "status": "not_found",
            "message": f"No data directory found for {ticker}"
        }
    
    # Find all parquet files
    parquet_files = list(ticker_dir.glob("*.parquet"))
    
    if not parquet_files:
        return {
            "ticker": ticker,
            "status": "no_files",
            "message": f"No parquet files found for {ticker}"
        }
    
    # Analyze each file
    results = {
        "ticker": ticker,
        "status": "ok",
        "data_directory": str(ticker_dir),
        "total_files": len(parquet_files),
        "timeframes": {}
    }
    
    # Sort files by timeframe for better display
    timeframe_order = ['1s', '1m', '5m', '15m', '30m', '1h', '1d', '1w', '1M', '3M', '1Q', '1Y']
    
    for file_path in sorted(parquet_files, key=lambda x: (
        timeframe_order.index(x.stem.split('.')[-1]) 
        if x.stem.split('.')[-1] in timeframe_order 
        else 999
    )):
        timeframe = file_path.stem.split('.')[-1]
        results["timeframes"][timeframe] = analyze_parquet_file(file_path)
    
    return results

def print_analysis(analysis: Dict[str, Any]):
    """Pretty print the analysis results."""
    print("\n" + "=" * 80)
    print(f"📊 PARQUET DATA ANALYSIS FOR {analysis['ticker']}")
    print("=" * 80)
    
    if analysis['status'] != 'ok':
        print(f"❌ Status: {analysis['status']}")
        print(f"   Message: {analysis.get('message', 'Unknown error')}")
        return
    
    print(f"📁 Data Directory: {analysis['data_directory']}")
    print(f"📄 Total Files: {analysis['total_files']}")
    print()
    
    # Summary table
    print("📈 TIMEFRAME SUMMARY:")
    print("-" * 80)
    print(f"{'Timeframe':<12} {'Records':<12} {'First Date':<20} {'Last Date':<20} {'Days':<8} {'Size (KB)':<10}")
    print("-" * 80)
    
    for timeframe, data in analysis['timeframes'].items():
        if data['status'] == 'ok':
            first_date = data['first_date'].strftime('%Y-%m-%d %H:%M')
            last_date = data['last_date'].strftime('%Y-%m-%d %H:%M')
            print(f"{timeframe:<12} {data['records']:<12,} {first_date:<20} {last_date:<20} {data['date_range_days']:<8} {data['file_size_kb']:<10.1f}")
        else:
            print(f"{timeframe:<12} {'Error: ' + data['status']:<60}")
    
    print()
    
    # Detailed view for each timeframe
    print("\n📋 DETAILED TIMEFRAME ANALYSIS:")
    print("=" * 80)
    
    for timeframe, data in analysis['timeframes'].items():
        print(f"\n⏱️  Timeframe: {timeframe}")
        print("-" * 40)
        
        if data['status'] != 'ok':
            print(f"   Status: {data['status']}")
            if 'error' in data:
                print(f"   Error: {data['error']}")
            continue
        
        print(f"   Records: {data['records']:,}")
        print(f"   Date Range: {data['first_date']} to {data['last_date']}")
        print(f"   Total Days: {data['date_range_days']} days")
        print(f"   File Size: {data['file_size_kb']:.2f} KB")
        print(f"   Columns: {', '.join(data['columns'])}")
        
        # Show sample data
        if data.get('sample_data') and data['sample_data'].get('first_5_records'):
            print("\n   Sample Data (First 3 records):")
            for i, record in enumerate(data['sample_data']['first_5_records'][:3]):
                timestamp = pd.to_datetime(record['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                print(f"     {i+1}. {timestamp} | O:{record['open']:.2f} H:{record['high']:.2f} L:{record['low']:.2f} C:{record['close']:.2f} V:{record['volume']:,.0f}")
            
            print("\n   Sample Data (Last 3 records):")
            for i, record in enumerate(data['sample_data']['last_5_records'][-3:]):
                timestamp = pd.to_datetime(record['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                print(f"     {i+1}. {timestamp} | O:{record['open']:.2f} H:{record['high']:.2f} L:{record['low']:.2f} C:{record['close']:.2f} V:{record['volume']:,.0f}")

def main():
    parser = argparse.ArgumentParser(description='Analyze parquet data for a ticker')
    parser.add_argument('ticker', nargs='?', default='AAPL', help='Ticker symbol to analyze (default: AAPL)')
    parser.add_argument('--data-dir', default='/data', help='Data directory path (default: /data)')
    parser.add_argument('--list-tickers', action='store_true', help='List available tickers')
    
    args = parser.parse_args()
    
    # If listing tickers
    if args.list_tickers:
        equities_dir = Path(args.data_dir) / "equities"
        if equities_dir.exists():
            tickers = sorted([d.name for d in equities_dir.iterdir() if d.is_dir()])
            print(f"\n📊 Available tickers ({len(tickers)} total):")
            print("-" * 40)
            # Print in columns
            cols = 10
            for i in range(0, len(tickers), cols):
                print("  " + "  ".join(tickers[i:i+cols]))
            print()
        else:
            print(f"❌ No equities directory found at {equities_dir}")
        return
    
    # Analyze the ticker
    print(f"\n🔍 Analyzing {args.ticker}...")
    analysis = analyze_ticker_data(args.ticker, args.data_dir)
    print_analysis(analysis)
    
    # Summary statistics across all timeframes
    if analysis['status'] == 'ok':
        print("\n" + "=" * 80)
        print("📊 OVERALL STATISTICS")
        print("=" * 80)
        
        total_records = sum(
            tf['records'] for tf in analysis['timeframes'].values() 
            if tf['status'] == 'ok'
        )
        total_size_mb = sum(
            tf['file_size_kb'] for tf in analysis['timeframes'].values() 
            if tf['status'] == 'ok'
        ) / 1024
        
        earliest_date = min(
            (tf['first_date'] for tf in analysis['timeframes'].values() 
             if tf['status'] == 'ok'),
            default=None
        )
        latest_date = max(
            (tf['last_date'] for tf in analysis['timeframes'].values() 
             if tf['status'] == 'ok'),
            default=None
        )
        
        print(f"  Total Records Across All Timeframes: {total_records:,}")
        print(f"  Total Storage Size: {total_size_mb:.2f} MB")
        if earliest_date and latest_date:
            print(f"  Earliest Data Point: {earliest_date}")
            print(f"  Latest Data Point: {latest_date}")
            print(f"  Total Date Range: {(latest_date - earliest_date).days} days")
        print()

if __name__ == "__main__":
    main()
