"""
Command-line interface for the finance data pipeline.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import click
from dotenv import load_dotenv

from app.polygon_client import PolygonClient
from app.downloader import DataDownloader
from app.universe import UniverseManager
from app.tasks import backfill_symbol, discover_universe, bulk_enqueue_backfills

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.group()
@click.option('--api-key', envvar='POLYGON_API_KEY', required=True, help='Polygon API key')
@click.pass_context
def cli(ctx, api_key):
    """Finance Data Pipeline CLI - Download and manage financial data."""
    ctx.ensure_object(dict)
    ctx.obj['api_key'] = api_key


@cli.command()
@click.argument('category')
@click.option('--limit', '-l', type=int, help='Limit number of tickers to discover')
@click.option('--output-dir', '-o', default='universe', help='Output directory for universe files')
@click.option('--append/--no-append', default=True, help='Append to existing universe file')
@click.pass_context
def discover(ctx, category, limit, output_dir, append):
    """
    Discover tickers for a category.
    
    CATEGORY: Category name (us_equities, etf, crypto, fx, options, indices, otc)
    
    Examples:
        python -m app.cli discover us_equities --limit 100
        python -m app.cli discover crypto
    """
    api_key = ctx.obj['api_key']
    
    try:
        # Initialize clients
        # Use /universe when running in Docker (detected by container environment)
        universe_dir = "/universe" if os.path.exists("/.dockerenv") or os.environ.get("HOSTNAME") else output_dir
        polygon_client = PolygonClient(api_key, calls_per_minute=10)
        universe_manager = UniverseManager(polygon_client, universe_dir)
        
        click.echo(f"🔍 Discovering {category} tickers...")
        
        if limit:
            click.echo(f"   Limited to {limit} tickers")
        
        # Discover the universe with append option
        result = universe_manager.discover_category(category, limit, append=append)
        
        if result['status'] == 'success':
            new_tickers = result.get('new_tickers_added', 0)
            if append and new_tickers >= 0:
                click.echo(f"✅ Added {new_tickers} new {category} tickers (total: {result['count']})")
            else:
                click.echo(f"✅ Successfully discovered {result['count']} {category} tickers")
            click.echo(f"   Saved to: {result['file_path']}")
        else:
            click.echo(f"❌ Discovery failed: {result['error']}")
            exit(1)
            
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        exit(1)


@cli.command()
@click.argument('universe_file')
@click.option('--timeframes', '-t', default=None, 
              help='Comma-separated timeframes (1m,5m,15m,1h,1d) or leave empty for auto-detect')
@click.option('--start', '-s', 
              default=None,
              help='Start date (YYYY-MM-DD) - leave empty to get ALL available history')
@click.option('--end', '-e', 
              default=None,
              help='End date (YYYY-MM-DD) - leave empty for current date')
@click.option('--output-format', '-f', default='parquet', 
              type=click.Choice(['parquet', 'csv']),
              help='Output format')
@click.option('--batch-size', '-b', default=10, type=int,
              help='Number of tasks to enqueue at once')
@click.option('--force-refresh', is_flag=True,
              help='Force refresh of all data (ignore existing)')
@click.option('--async', 'use_async', is_flag=True,
              help='Use Celery for async processing')
@click.pass_context
def enqueue(ctx, universe_file, timeframes, start, end, output_format, 
           batch_size, force_refresh, use_async):
    """
    Enqueue download jobs for a universe of tickers.
    
    UNIVERSE_FILE: Path to universe JSON file created by discover command
    
    Examples:
        python -m app.cli enqueue universe/us_equities_20241201.json
        python -m app.cli enqueue universe/crypto_20241201.json --timeframes 1h,1d --async
    """
    api_key = ctx.obj['api_key']
    
    try:
        # Parse timeframes (None means auto-detect)
        intervals = None if not timeframes else [t.strip() for t in timeframes.split(',')]
        
        # Adjust file path for Docker environment
        if os.path.exists("/.dockerenv") or os.environ.get("HOSTNAME"):
            # Running in Docker - convert host path to container path
            if universe_file.startswith('universe/'):
                universe_file = '/universe/' + universe_file[9:]  # Remove 'universe/' prefix and add '/universe/'
        
        # Validate universe file exists
        if not Path(universe_file).exists():
            click.echo(f"❌ Universe file not found: {universe_file}")
            exit(1)
        
        # Load universe to get ticker count
        # Use /universe when running in Docker
        universe_dir = "/universe" if os.path.exists("/.dockerenv") or os.environ.get("HOSTNAME") else "universe"
        polygon_client = PolygonClient(api_key, calls_per_minute=5)
        universe_manager = UniverseManager(polygon_client, universe_dir)
        tickers = universe_manager.get_ticker_list(universe_file)
        
        click.echo(f"📊 Enqueueing jobs for {len(tickers)} tickers")
        if intervals:
            click.echo(f"   Timeframes: {', '.join(intervals)}")
        else:
            click.echo(f"   Timeframes: Auto-detect")
        click.echo(f"   Date range: {start if start else 'auto'} to {end if end else 'auto'}")
        click.echo(f"   Format: {output_format}")
        
        if use_async:
            click.echo(f"   Using Celery async processing")
            
            # Import the tasks
            from app.tasks import backfill_symbol, discover_and_download_ticker
            
            # Decide whether to use parallel discovery or direct download
            use_parallel = (intervals is None or not intervals)  # Use parallel if auto-detecting
            
            # Enqueue individual tasks for each ticker
            task_ids = []
            with click.progressbar(tickers, label='Enqueueing') as ticker_progress:
                for ticker in ticker_progress:
                    if use_parallel:
                        # Use the new parallel discovery and download task
                        task = discover_and_download_ticker.delay(
                            ticker=ticker,
                            api_key=api_key
                        )
                    else:
                        # Use traditional backfill when specific intervals are provided
                        task = backfill_symbol.delay(
                            ticker=ticker,
                            intervals=intervals,
                            start_date=start if start else None,
                            end_date=end if end else None,
                            output_format=output_format,
                            force_refresh=force_refresh,
                            auto_detect=False,
                            organize=True
                        )
                    task_ids.append(task.id)
            
            if use_parallel:
                click.echo(f"✅ Enqueued {len(task_ids)} parallel discovery tasks")
                click.echo(f"   Each ticker will discover timeframes and spawn download tasks")
            else:
                click.echo(f"✅ Enqueued {len(task_ids)} download tasks")
            
            click.echo(f"   Monitor with: celery -A app.celery_app inspect active")
            click.echo(f"   Or check Flower UI at: http://localhost:5555")
            
        else:
            click.echo(f"   Using synchronous processing")
            
            # Process synchronously
            downloader = DataDownloader(polygon_client)
            success_count = 0
            
            with click.progressbar(tickers, label='Downloading') as ticker_progress:
                for ticker in ticker_progress:
                    try:
                        results = downloader.download_multiple_intervals(
                            ticker=ticker,
                            intervals=intervals,
                            start_date=start,
                            end_date=end,
                            output_format=output_format,
                            force_refresh=force_refresh
                        )
                        
                        if any(r['status'] == 'success' for r in results):
                            success_count += 1
                            
                    except Exception as e:
                        logger.warning(f"Failed to download {ticker}: {e}")
                        continue
            
            click.echo(f"✅ Completed: {success_count}/{len(tickers)} tickers processed")
            
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        exit(1)


@cli.command()
@click.argument('ticker')
@click.option('--timeframes', '-t', default=None,
              help='Comma-separated timeframes (1m,5m,15m,1h,1d) or "auto" for auto-detect')
@click.option('--start', '-s', default=None,
              help='Start date (YYYY-MM-DD) or "auto" for auto-detect')
@click.option('--end', '-e', default=None,
              help='End date (YYYY-MM-DD) or "auto" for auto-detect')
@click.option('--output-format', '-f', default='parquet',
              type=click.Choice(['parquet', 'csv']),
              help='Output format')
@click.option('--force-refresh', is_flag=True,
              help='Force refresh of all data')
@click.option('--organize', is_flag=True,
              help='Organize data with proper structure')
@click.pass_context
def run_once(ctx, ticker, timeframes, start, end, output_format, force_refresh, organize):
    """
    Download data for a single ticker immediately (no Celery).
    
    TICKER: Stock ticker symbol (e.g., AAPL, TSLA)
    
    Examples:
        python -m app.cli run-once AAPL                     # Auto-detect everything
        python -m app.cli run-once TSLA --timeframes auto    # Auto-detect timeframes
        python -m app.cli run-once MSFT --start auto --end auto  # Auto-detect date range
        python -m app.cli run-once AAPL --organize           # Download and organize data
    """
    api_key = ctx.obj['api_key']
    
    try:
        # Initialize clients
        # Use /data when running in Docker (detected by container environment)
        data_dir = "/data" if os.path.exists("/.dockerenv") or os.environ.get("HOSTNAME") else "data"
        polygon_client = PolygonClient(api_key, calls_per_minute=10)
        downloader = DataDownloader(polygon_client, data_dir)
        
        # Handle auto-detection keywords
        if start == "auto":
            start = None
        if end == "auto":
            end = None
        
        # Parse timeframes or use auto-detection
        if timeframes == "auto" or timeframes is None:
            intervals = None  # Will trigger auto-detection
            click.echo(f"📈 Auto-detecting available data for {ticker.upper()}...")
        else:
            intervals = [t.strip() for t in timeframes.split(',')]
            click.echo(f"📈 Downloading {ticker.upper()} data...")
            click.echo(f"   Timeframes: {', '.join(intervals)}")
        
        # Show date range if specified
        if start or end:
            click.echo(f"   Date range: {start or 'auto'} to {end or 'auto'}")
        
        # Use the comprehensive download method if organizing
        if organize and intervals is None:
            click.echo(f"   Using comprehensive download with data organization")
            result = downloader.download_all_available_data(
                ticker=ticker.upper(),
                output_format=output_format,
                force_refresh=force_refresh,
                organize=True
            )
            
            # Display comprehensive results
            if result['status'] == 'completed':
                click.echo(f"✅ Downloaded all available data for {ticker.upper()}")
                click.echo(f"   Market type: {result['market_type']}")
                click.echo(f"   Timeframes: {result['timeframes_downloaded']}/{result['timeframes_available']}")
                click.echo(f"   Total records: {result['total_new_records']}")
                
                # Show details for each timeframe
                for tf, details in result['timeframe_details'].items():
                    click.echo(f"   {tf}: {details['start_date']} to {details['end_date']} ({details['estimated_bars']} bars)")
            else:
                click.echo(f"❌ {result.get('message', 'Download failed')}")
            
            return
        
        # Download data with auto-detection support
        results = downloader.download_multiple_intervals(
            ticker=ticker.upper(),
            intervals=intervals,
            start_date=start,
            end_date=end,
            output_format=output_format,
            force_refresh=force_refresh,
            auto_detect_timeframes=(intervals is None)
        )
        
        # Display results
        success_count = 0
        for result in results:
            status_icon = "✅" if result['status'] == 'success' else "⚠️" if result['status'] == 'up_to_date' else "❌"
            
            if result['status'] == 'success':
                click.echo(f"   {status_icon} {result['interval']}: {result['new_records']} new records")
                success_count += 1
            elif result['status'] == 'up_to_date':
                click.echo(f"   {status_icon} {result['interval']}: up to date ({result['records']} records)")
            elif result['status'] == 'no_new_data':
                click.echo(f"   {status_icon} {result['interval']}: no new data available")
            else:
                click.echo(f"   {status_icon} {result['interval']}: error - {result.get('error', 'unknown')}")
        
        if success_count > 0:
            click.echo(f"✅ Downloaded data for {success_count}/{len(intervals)} timeframes")
        else:
            click.echo(f"⚠️  No new data downloaded")
            
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        exit(1)


@cli.command()
@click.option('--universe-dir', '-u', default='universe',
              help='Universe directory to list')
def list_universes(universe_dir):
    """List all available universe files."""
    try:
        # Use /universe when running in Docker
        actual_universe_dir = "/universe" if os.path.exists("/.dockerenv") or os.environ.get("HOSTNAME") else universe_dir
        polygon_client = PolygonClient('dummy', calls_per_minute=5)  # Dummy client for listing
        universe_manager = UniverseManager(polygon_client, actual_universe_dir)
        universes = universe_manager.list_available_universes()
        
        if not universes:
            click.echo("No universe files found.")
            return
        
        click.echo("Available universes:")
        click.echo()
        
        for universe in universes:
            click.echo(f"📊 {Path(universe['file_path']).name}")
            click.echo(f"   Category: {universe['category']}")
            click.echo(f"   Created: {universe['created_at']}")
            click.echo(f"   Tickers: {universe['count']}")
            click.echo(f"   Size: {universe['file_size_mb']:.1f} MB")
            click.echo()
            
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        exit(1)


@cli.command()
@click.argument('ticker')
@click.option('--interval', '-i', default='1d', help='Time interval to check')
def status(ticker, interval):
    """Check download status for a ticker."""
    try:
        polygon_client = PolygonClient('dummy', calls_per_minute=5)  # Dummy client
        downloader = DataDownloader(polygon_client)
        
        summary = downloader.get_data_summary(ticker.upper(), interval)
        
        if summary['status'] == 'no_data':
            click.echo(f"❌ No data found for {ticker.upper()} {interval}")
        elif summary['status'] == 'empty':
            click.echo(f"⚠️  Data file exists but is empty for {ticker.upper()} {interval}")
        elif summary['status'] == 'available':
            click.echo(f"✅ {ticker.upper()} {interval} data:")
            click.echo(f"   Records: {summary['records']:,}")
            click.echo(f"   Date range: {summary['start_date']} to {summary['end_date']}")
            click.echo(f"   File size: {summary['file_size_mb']:.1f} MB")
            click.echo(f"   Path: {summary['file_path']}")
        else:
            click.echo(f"❌ Error checking {ticker.upper()}: {summary.get('error', 'unknown')}")
            
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        exit(1)


@cli.command('discover-all')
@click.option('--append/--no-append', default=True, help='Append to existing universe file')
def discover_all(append: bool):
    """Discover ALL available US equities (up to 15k) from Polygon API."""
    click.echo("🔍 Discovering ALL US equities (this may take a few minutes)...")
    
    # Get API key from environment
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        click.echo("❌ POLYGON_API_KEY not found in environment", err=True)
        raise click.Abort()
    
    # Determine universe directory based on environment
    if os.getenv('DOCKER_ENV'):
        universe_dir = "/universe"
    else:
        universe_dir = "universe"
    
    # Initialize PolygonClient first
    polygon_client = PolygonClient(api_key)
    manager = UniverseManager(polygon_client, universe_dir=universe_dir)
    
    try:
        # Discover ALL US equities (no limit)
        result = manager.discover_category('us_equities', limit=None, append=append)
        click.echo(f"✅ Successfully discovered {result['count']} US equities")
        click.echo(f"   New tickers added: {result.get('new_tickers_added', 0)}")
        click.echo(f"   Saved to: {result['file_path']}")
        
        # Show sample tickers
        if result['count'] > 0:
            click.echo(f"\n📊 Sample tickers (first 10):")
            with open(result['file_path'], 'r') as f:
                data = json.load(f)
                for ticker in data['tickers'][:10]:
                    click.echo(f"   - {ticker['ticker']}: {ticker['name']}")
            if result['count'] > 10:
                click.echo(f"   ... and {result['count'] - 10} more")
    
    except Exception as e:
        click.echo(f"❌ Discovery failed: {e}", err=True)
        raise click.Abort()


@cli.command('enqueue-batch')
@click.argument('universe_file', type=click.Path(exists=True))
@click.option('--batch-size', type=int, default=100, help='Number of tickers per batch')
@click.option('--start-batch', type=int, default=0, help='Starting batch number (0-indexed)')
@click.option('--max-batches', type=int, help='Maximum number of batches to process')
@click.option('--delay', type=int, default=5, help='Delay in seconds between batches')
def enqueue_batch(universe_file: str, batch_size: int, start_batch: int, max_batches: Optional[int], delay: int):
    """Process universe file in batches to avoid overwhelming the system."""
    import time
    
    # Get API key from environment
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        click.echo("❌ POLYGON_API_KEY not found in environment", err=True)
        raise click.Abort()
    
    # Load universe file
    if os.getenv('DOCKER_ENV'):
        # If in Docker, ensure path is correct
        if not universe_file.startswith('/universe'):
            universe_file = f"/universe/{os.path.basename(universe_file)}"
    
    # Initialize PolygonClient first
    polygon_client = PolygonClient(api_key)
    manager = UniverseManager(polygon_client)
    
    try:
        universe_data = manager.load_universe(universe_file)
        # Extract just the ticker symbols
        tickers = [t['ticker'] if isinstance(t, dict) else t for t in universe_data.get('tickers', [])]
        total_tickers = len(tickers)
        total_batches = (total_tickers + batch_size - 1) // batch_size  # Ceiling division
        
        click.echo(f"📊 Processing {total_tickers} tickers in {total_batches} batches of {batch_size}")
        
        # Import the tasks
        from app.tasks import discover_and_download_ticker
        
        # Determine ending batch
        end_batch = min(start_batch + (max_batches or total_batches), total_batches)
        
        for batch_num in range(start_batch, end_batch):
            batch_start = batch_num * batch_size
            batch_end = min(batch_start + batch_size, total_tickers)
            batch_tickers = tickers[batch_start:batch_end]
            
            click.echo(f"\n📦 Batch {batch_num + 1}/{total_batches}: {len(batch_tickers)} tickers")
            click.echo(f"   Range: {batch_tickers[0]} to {batch_tickers[-1]}")
            
            # Enqueue tasks for this batch
            task_ids = []
            with click.progressbar(batch_tickers, label=f'Enqueueing batch {batch_num + 1}') as ticker_progress:
                for ticker in ticker_progress:
                    task = discover_and_download_ticker.delay(
                        ticker=ticker,
                        api_key=api_key
                    )
                    task_ids.append(task.id)
            
            click.echo(f"   ✅ Enqueued {len(task_ids)} tasks")
            
            # Delay between batches (except for the last one)
            if batch_num + 1 < end_batch:
                click.echo(f"   ⏳ Waiting {delay} seconds before next batch...")
                time.sleep(delay)
        
        click.echo(f"\n✅ Successfully enqueued {end_batch - start_batch} batches")
        click.echo(f"   Total tasks: {(end_batch - start_batch) * batch_size}")
        click.echo(f"   Monitor with Flower UI at: http://localhost:5555")
        
        if end_batch < total_batches:
            remaining = total_batches - end_batch
            click.echo(f"\n💡 {remaining} batches remaining. Run with --start-batch {end_batch} to continue")
    
    except Exception as e:
        click.echo(f"❌ Batch processing failed: {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    cli()
