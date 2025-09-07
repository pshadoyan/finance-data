#!/usr/bin/env python3
"""
Script to analyze parquet file compression ratios and statistics.
"""

import os
import sys
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
from datetime import datetime
import argparse
from typing import Dict, Any, Optional
import json

def format_bytes(size_bytes: int) -> str:
    """Format bytes into human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def analyze_parquet_compression(file_path: Path) -> Dict[str, Any]:
    """Analyze compression statistics for a parquet file."""
    
    if not file_path.exists():
        return {
            "status": "not_found",
            "file": str(file_path)
        }
    
    try:
        # Get file size on disk (compressed)
        file_size = file_path.stat().st_size
        
        # Open parquet file with pyarrow
        parquet_file = pq.ParquetFile(file_path)
        
        # Get metadata
        metadata = parquet_file.metadata
        
        # Read the dataframe to get uncompressed size
        df = pd.read_parquet(file_path)
        
        # Calculate uncompressed size (approximate)
        # This is done by getting the memory usage of the dataframe
        uncompressed_size = df.memory_usage(deep=True).sum()
        
        # Get detailed column information
        schema = parquet_file.schema_arrow
        column_info = []
        
        for i in range(metadata.num_row_groups):
            row_group = metadata.row_group(i)
            for j in range(row_group.num_columns):
                column = row_group.column(j)
                
                # Get compression codec
                compression = column.compression if hasattr(column, 'compression') else 'SNAPPY'
                
                # Get column name and type from arrow schema
                field = schema.field(j)
                
                column_info.append({
                    "column": field.name,
                    "type": str(field.type),
                    "compression": compression,
                    "compressed_size": column.total_compressed_size,
                    "uncompressed_size": column.total_uncompressed_size,
                    "encoding": str(column.encodings) if hasattr(column, 'encodings') else "unknown"
                })
        
        # Aggregate column statistics
        total_compressed = sum(col['compressed_size'] for col in column_info)
        total_uncompressed = sum(col['uncompressed_size'] for col in column_info)
        
        # Calculate compression ratio
        compression_ratio = total_uncompressed / total_compressed if total_compressed > 0 else 1
        space_savings_pct = ((total_uncompressed - total_compressed) / total_uncompressed * 100) if total_uncompressed > 0 else 0
        
        # Get additional metadata
        created_by = metadata.created_by if hasattr(metadata, 'created_by') else "unknown"
        num_rows = metadata.num_rows
        num_columns = metadata.num_columns
        num_row_groups = metadata.num_row_groups
        
        # Get data statistics
        timestamp_col = 'timestamp' if 'timestamp' in df.columns else None
        date_range = None
        if timestamp_col:
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])
            date_range = {
                "first": df[timestamp_col].min().isoformat() if not df.empty else None,
                "last": df[timestamp_col].max().isoformat() if not df.empty else None,
                "days": (df[timestamp_col].max() - df[timestamp_col].min()).days if not df.empty else 0
            }
        
        return {
            "status": "ok",
            "file": file_path.name,
            "file_path": str(file_path),
            "file_size_disk": file_size,
            "file_size_disk_human": format_bytes(file_size),
            "compression_stats": {
                "total_compressed_size": total_compressed,
                "total_compressed_human": format_bytes(total_compressed),
                "total_uncompressed_size": total_uncompressed,
                "total_uncompressed_human": format_bytes(total_uncompressed),
                "compression_ratio": round(compression_ratio, 2),
                "space_savings_pct": round(space_savings_pct, 2),
                "dataframe_memory_usage": uncompressed_size,
                "dataframe_memory_human": format_bytes(uncompressed_size)
            },
            "metadata": {
                "created_by": created_by,
                "num_rows": num_rows,
                "num_columns": num_columns,
                "num_row_groups": num_row_groups,
                "columns": df.columns.tolist()
            },
            "column_details": column_info,
            "date_range": date_range
        }
        
    except Exception as e:
        return {
            "status": "error",
            "file": str(file_path),
            "error": str(e)
        }

def print_compression_analysis(analysis: Dict[str, Any]):
    """Pretty print compression analysis results."""
    
    if analysis['status'] != 'ok':
        print(f"❌ Error analyzing file: {analysis.get('error', 'Unknown error')}")
        return
    
    print("\n" + "=" * 80)
    print(f"📊 PARQUET COMPRESSION ANALYSIS")
    print(f"📄 File: {analysis['file']}")
    print("=" * 80)
    
    # Compression Statistics
    comp_stats = analysis['compression_stats']
    print("\n🗜️  COMPRESSION STATISTICS:")
    print("-" * 40)
    print(f"  File Size on Disk:        {analysis['file_size_disk_human']}")
    print(f"  Total Compressed Size:    {comp_stats['total_compressed_human']}")
    print(f"  Total Uncompressed Size:  {comp_stats['total_uncompressed_human']}")
    print(f"  Compression Ratio:        {comp_stats['compression_ratio']}x")
    print(f"  Space Savings:            {comp_stats['space_savings_pct']:.1f}%")
    print(f"  DataFrame Memory Usage:   {comp_stats['dataframe_memory_human']}")
    
    # Metadata
    meta = analysis['metadata']
    print("\n📋 FILE METADATA:")
    print("-" * 40)
    print(f"  Created By:     {meta['created_by']}")
    print(f"  Total Rows:     {meta['num_rows']:,}")
    print(f"  Total Columns:  {meta['num_columns']}")
    print(f"  Row Groups:     {meta['num_row_groups']}")
    
    # Date Range
    if analysis.get('date_range'):
        dr = analysis['date_range']
        print("\n📅 DATE RANGE:")
        print("-" * 40)
        print(f"  First Record: {dr['first']}")
        print(f"  Last Record:  {dr['last']}")
        print(f"  Total Days:   {dr['days']}")
    
    # Column Details
    print("\n📊 COLUMN COMPRESSION DETAILS:")
    print("-" * 80)
    print(f"{'Column':<15} {'Type':<15} {'Compression':<12} {'Compressed':<12} {'Uncompressed':<12} {'Ratio':<8} {'Savings':<8}")
    print("-" * 80)
    
    for col in analysis['column_details']:
        ratio = col['uncompressed_size'] / col['compressed_size'] if col['compressed_size'] > 0 else 1
        savings = ((col['uncompressed_size'] - col['compressed_size']) / col['uncompressed_size'] * 100) if col['uncompressed_size'] > 0 else 0
        
        # Truncate long type names
        type_str = col['type']
        if len(type_str) > 14:
            type_str = type_str[:11] + "..."
            
        print(f"{col['column']:<15} {type_str:<15} {col['compression']:<12} "
              f"{format_bytes(col['compressed_size']):<12} {format_bytes(col['uncompressed_size']):<12} "
              f"{ratio:.2f}x{'':<2} {savings:.1f}%")
    
    print("-" * 80)
    
    # Summary
    total_comp = sum(col['compressed_size'] for col in analysis['column_details'])
    total_uncomp = sum(col['uncompressed_size'] for col in analysis['column_details'])
    print(f"{'TOTAL':<15} {'':<15} {'':<12} "
          f"{format_bytes(total_comp):<12} {format_bytes(total_uncomp):<12} "
          f"{comp_stats['compression_ratio']}x{'':<2} {comp_stats['space_savings_pct']:.1f}%")

def compare_timeframes(ticker: str, timeframes: list, data_dir: str = "/data"):
    """Compare compression across different timeframes for a ticker."""
    ticker = ticker.upper()
    ticker_dir = Path(data_dir) / "equities" / ticker
    
    print("\n" + "=" * 80)
    print(f"📊 COMPRESSION COMPARISON FOR {ticker}")
    print("=" * 80)
    
    results = []
    for timeframe in timeframes:
        file_path = ticker_dir / f"{ticker}.{timeframe}.parquet"
        if file_path.exists():
            analysis = analyze_parquet_compression(file_path)
            if analysis['status'] == 'ok':
                results.append({
                    'timeframe': timeframe,
                    'file_size': analysis['file_size_disk'],
                    'compressed': analysis['compression_stats']['total_compressed_size'],
                    'uncompressed': analysis['compression_stats']['total_uncompressed_size'],
                    'ratio': analysis['compression_stats']['compression_ratio'],
                    'savings': analysis['compression_stats']['space_savings_pct'],
                    'rows': analysis['metadata']['num_rows']
                })
    
    if results:
        print(f"\n{'Timeframe':<12} {'Rows':<10} {'File Size':<12} {'Compressed':<12} {'Uncompressed':<14} {'Ratio':<8} {'Savings':<8}")
        print("-" * 90)
        for r in results:
            print(f"{r['timeframe']:<12} {r['rows']:<10,} {format_bytes(r['file_size']):<12} "
                  f"{format_bytes(r['compressed']):<12} {format_bytes(r['uncompressed']):<14} "
                  f"{r['ratio']:.2f}x{'':<3} {r['savings']:.1f}%")
        
        # Calculate averages
        avg_ratio = sum(r['ratio'] for r in results) / len(results)
        avg_savings = sum(r['savings'] for r in results) / len(results)
        print("-" * 90)
        print(f"Average Compression Ratio: {avg_ratio:.2f}x")
        print(f"Average Space Savings: {avg_savings:.1f}%")

def main():
    parser = argparse.ArgumentParser(description='Analyze parquet file compression')
    parser.add_argument('ticker', help='Ticker symbol to analyze')
    parser.add_argument('timeframe', nargs='?', help='Specific timeframe to analyze (e.g., 1s, 1m, 1d)')
    parser.add_argument('--data-dir', default='/data', help='Data directory path')
    parser.add_argument('--compare', action='store_true', help='Compare compression across timeframes')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    
    args = parser.parse_args()
    
    ticker = args.ticker.upper()
    ticker_dir = Path(args.data_dir) / "equities" / ticker
    
    if not ticker_dir.exists():
        print(f"❌ No data directory found for {ticker}")
        sys.exit(1)
    
    if args.compare:
        # Compare multiple timeframes
        timeframes = ['1s', '1m', '5m', '15m', '30m', '1h', '1d', '1w', '1M', '3M', '1Q', '1Y']
        compare_timeframes(ticker, timeframes, args.data_dir)
    elif args.timeframe:
        # Analyze specific timeframe
        file_path = ticker_dir / f"{ticker}.{args.timeframe}.parquet"
        analysis = analyze_parquet_compression(file_path)
        
        if args.json:
            print(json.dumps(analysis, indent=2, default=str))
        else:
            print_compression_analysis(analysis)
    else:
        # Default: analyze 1s and 1m
        print(f"\n🔍 Analyzing compression for {ticker} (1s and 1m timeframes)\n")
        
        for timeframe in ['1s', '1m']:
            file_path = ticker_dir / f"{ticker}.{timeframe}.parquet"
            if file_path.exists():
                analysis = analyze_parquet_compression(file_path)
                if args.json:
                    print(json.dumps(analysis, indent=2, default=str))
                else:
                    print_compression_analysis(analysis)
            else:
                print(f"\n❌ File not found: {file_path}")

if __name__ == "__main__":
    main()
