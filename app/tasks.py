"""
Celery tasks for distributed data downloading.
"""

import os
import logging
from typing import List, Dict, Any, Optional
from celery import current_task
from app.celery_app import app
from app.polygon_client import PolygonClient
from app.downloader import DataDownloader
from app.universe import UniverseManager
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def detect_asset_type(ticker: str, polygon_client: PolygonClient = None) -> str:
    """
    Detect asset type from ticker symbol.
    
    Returns: 'crypto', 'etf', or 'equities'
    """
    ticker = ticker.upper()
    
    # Common crypto patterns
    if ticker.startswith('X:') or ticker.endswith('USD') or ticker.endswith('USDT'):
        return 'crypto'
    
    # Check if it's in common crypto list
    common_crypto = ['BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'DOGE', 'SOL', 'DOT', 'MATIC', 
                     'SHIB', 'AVAX', 'LINK', 'UNI', 'ATOM', 'LTC', 'ETC', 'XLM', 'ALGO']
    if ticker in common_crypto or any(ticker.startswith(c) for c in common_crypto):
        return 'crypto'
    
    # Common ETF patterns - many ETFs are 3-4 letters and end with specific patterns
    etf_patterns = ['SPY', 'QQQ', 'IWM', 'EEM', 'GLD', 'SLV', 'USO', 'TLT', 'XLF', 'XLE', 
                    'XLK', 'XLI', 'XLV', 'XLY', 'XLP', 'XLU', 'XLB', 'XLRE', 'VTI', 'VOO',
                    'VEA', 'VWO', 'AGG', 'BND', 'LQD', 'HYG', 'EMB', 'TIP', 'SHY', 'IEF']
    if ticker in etf_patterns:
        return 'etf'
    
    # ETFs often have these suffixes
    if any(ticker.endswith(suffix) for suffix in ['ETF', 'ETN', 'ETV', 'FUND']):
        return 'etf'
    
    # If we have a polygon client, we can check the API
    if polygon_client:
        try:
            # Try to get ticker details
            details = polygon_client.get_ticker_details(ticker)
            if details:
                ticker_type = details.get('type', '').upper()
                market = details.get('market', '').upper()
                
                if 'CRYPTO' in market or 'CRYPTO' in ticker_type:
                    return 'crypto'
                elif 'ETF' in ticker_type or 'ETP' in ticker_type:
                    return 'etf'
        except:
            pass
    
    # Default to equities
    return 'equities'


@app.task(bind=True, name='app.tasks.discover_and_download_ticker')
def discover_and_download_ticker(
    self,
    ticker: str,
    api_key: Optional[str] = None,
    check_existing: bool = True  # New parameter to enable smart checking
) -> Dict[str, Any]:
    """
    Intelligently discover and download only missing or outdated data.
    
    This task:
    1. Discovers available timeframes from API
    2. Checks what's already downloaded locally
    3. Only enqueues tasks for missing or outdated data
    4. Returns summary of what was done
    """
    try:
        # Update task state
        self.update_state(
            state='PROGRESS',
            meta={'ticker': ticker, 'status': 'discovering timeframes', 'progress': 0}
        )
        
        # Get API key
        api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not api_key:
            raise ValueError("POLYGON_API_KEY not provided")
        
        # Initialize client with conservative rate limit for discovery
        polygon_client = PolygonClient(api_key, calls_per_minute=10)
        
        # Discover available timeframes
        logger.info(f"Discovering timeframes for {ticker}")
        timeframe_info = polygon_client.detect_available_timeframes(ticker)
        
        if not timeframe_info.get('available_timeframes'):
            logger.warning(f"No timeframes available for {ticker}")
            return {
                'task_id': self.request.id,
                'ticker': ticker,
                'status': 'no_data',
                'timeframes_found': 0
            }
        
        # Check existing data if enabled
        data_dir = Path("/data") if os.path.exists("/.dockerenv") else Path("data")
        ticker_dir = data_dir / "equities" / ticker
        
        download_tasks = []
        skipped_timeframes = []
        needs_update = []
        new_timeframes = []
        
        for timeframe, info in timeframe_info['available_timeframes'].items():
            file_path = ticker_dir / f"{ticker}.{timeframe}.parquet"
            
            # Check if we should skip this timeframe
            skip_reason = None
            needs_download = True
            
            if check_existing and file_path.exists():
                try:
                    # Read just the timestamp column for efficiency
                    df = pd.read_parquet(file_path, columns=['timestamp'])
                    
                    if not df.empty:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        last_local_timestamp = df['timestamp'].max()
                        
                        # Check if data is current
                        api_end_date = pd.to_datetime(info['end_date'])
                        
                        # Calculate how old the data is
                        days_behind = (api_end_date - last_local_timestamp).days
                        
                        if days_behind <= 0:
                            # Data is up to date
                            skip_reason = f"already up to date (last: {last_local_timestamp.date()})"
                            needs_download = False
                            skipped_timeframes.append({
                                'timeframe': timeframe,
                                'reason': skip_reason,
                                'records': len(df),
                                'last_timestamp': str(last_local_timestamp)
                            })
                        elif days_behind <= 1:
                            # Data is very recent, might just be missing today's data
                            needs_update.append({
                                'timeframe': timeframe,
                                'days_behind': days_behind,
                                'last_local': str(last_local_timestamp),
                                'api_end': info['end_date']
                            })
                        else:
                            # Data needs significant update
                            needs_update.append({
                                'timeframe': timeframe,
                                'days_behind': days_behind,
                                'last_local': str(last_local_timestamp),
                                'api_end': info['end_date']
                            })
                    else:
                        # File exists but is empty
                        logger.warning(f"Empty file for {ticker} {timeframe}, will re-download")
                        
                except Exception as e:
                    logger.warning(f"Could not check existing data for {ticker} {timeframe}: {e}")
            else:
                # New timeframe that doesn't exist locally
                new_timeframes.append(timeframe)
            
            # Enqueue download task if needed
            if needs_download:
                task = download_single_timeframe.delay(
                    ticker=ticker,
                    interval=timeframe,
                    start_date=info['start_date'],
                    end_date=info['end_date'],
                    api_key=api_key
                )
                download_tasks.append({
                    'task_id': task.id,
                    'timeframe': timeframe,
                    'date_range': f"{info['start_date']} to {info['end_date']}"
                })
                logger.info(f"Enqueued download for {ticker} {timeframe}")
        
        # Prepare detailed response
        result = {
            'task_id': self.request.id,
            'ticker': ticker,
            'status': 'completed',
            'summary': {
                'timeframes_available': len(timeframe_info['available_timeframes']),
                'timeframes_skipped': len(skipped_timeframes),
                'timeframes_to_update': len(needs_update),
                'timeframes_new': len(new_timeframes),
                'tasks_created': len(download_tasks)
            }
        }
        
        # Add details if there are any
        if skipped_timeframes:
            result['skipped'] = skipped_timeframes
            logger.info(f"{ticker}: Skipped {len(skipped_timeframes)} up-to-date timeframes")
        
        if needs_update:
            result['updates_needed'] = needs_update
            logger.info(f"{ticker}: {len(needs_update)} timeframes need updates")
        
        if new_timeframes:
            result['new_timeframes'] = new_timeframes
            logger.info(f"{ticker}: {len(new_timeframes)} new timeframes to download")
        
        if download_tasks:
            result['download_tasks'] = download_tasks
        
        # Set appropriate status message
        if len(download_tasks) == 0:
            result['status'] = 'all_up_to_date'
            result['message'] = f"All {len(timeframe_info['available_timeframes'])} timeframes are already up to date"
            logger.info(f"✅ {ticker}: All data is up to date, no downloads needed")
        elif len(download_tasks) < len(timeframe_info['available_timeframes']):
            result['status'] = 'partial_update'
            result['message'] = f"Updating {len(download_tasks)} of {len(timeframe_info['available_timeframes'])} timeframes"
        else:
            result['status'] = 'full_download'
            result['message'] = f"Downloading all {len(download_tasks)} timeframes"
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to discover/enqueue for {ticker}: {e}")
        return {
            'task_id': self.request.id,
            'ticker': ticker,
            'status': 'error',
            'error': str(e)
        }


@app.task(bind=True, name='app.tasks.download_single_timeframe')
def download_single_timeframe(
    self,
    ticker: str,
    interval: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_format: str = "parquet",
    force_refresh: bool = False,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Download data for a single ticker and timeframe.
    This task is designed to be run in parallel across multiple workers.
    """
    try:
        # Update task state
        self.update_state(
            state='PROGRESS',
            meta={
                'ticker': ticker,
                'interval': interval,
                'status': 'downloading',
                'progress': 0
            }
        )
        
        # Get API key
        api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not api_key:
            raise ValueError("POLYGON_API_KEY not provided")
        
        # Initialize clients with rate limiting
        # Each worker gets its own rate limit allocation
        polygon_client = PolygonClient(api_key, calls_per_minute=2)  # Conservative for parallel
        
        # Detect asset type
        asset_type = detect_asset_type(ticker, polygon_client)
        logger.info(f"Detected asset type for {ticker}: {asset_type}")
        
        # Use simplified data directory structure
        data_dir = "/data" if os.path.exists("/.dockerenv") else "data"
        downloader = DataDownloader(polygon_client, data_dir=data_dir, asset_type=asset_type)
        
        logger.info(f"Worker downloading {ticker} {interval} from {start_date} to {end_date}")
        
        # Auto-detect dates if not provided
        auto_detect_range = (start_date is None or end_date is None)
        
        result = downloader.download_symbol_data(
            ticker=ticker,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
            output_format=output_format,
            force_refresh=force_refresh,
            auto_detect_range=auto_detect_range
        )
        
        # Data is already saved to the correct location (equities/TICKER/)
        
        logger.info(f"Completed download for {ticker} {interval}: {result.get('status')}")
        
        return {
            'task_id': self.request.id,
            'ticker': ticker,
            'interval': interval,
            'status': result.get('status', 'unknown'),
            'records': result.get('records', 0),
            'file_path': result.get('file_path'),
            'date_range': f"{start_date or 'auto'} to {end_date or 'auto'}"
        }
        
    except Exception as e:
        logger.error(f"Failed to download {ticker} {interval}: {e}")
        return {
            'task_id': self.request.id,
            'ticker': ticker,
            'interval': interval,
            'status': 'error',
            'error': str(e)
        }


@app.task(bind=True, name='app.tasks.backfill_symbol')
def backfill_symbol(
    self,
    ticker: str,
    intervals: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_format: str = "parquet",
    force_refresh: bool = False,
    auto_detect: bool = True,
    organize: bool = True,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Celery task to download historical data for a single symbol.
    
    Args:
        ticker: Stock ticker symbol
        intervals: List of time intervals (auto-detected if None)
        start_date: Start date (auto-detected if None)
        end_date: End date (auto-detected if None)
        output_format: Output format (parquet or csv)
        force_refresh: Force refresh of all data
        auto_detect: Auto-detect timeframes and date ranges
        organize: Deprecated parameter (kept for compatibility)
        api_key: Polygon API key (optional, uses env var if not provided)
    
    Returns:
        Dict with task results
    """
    try:
        # Update task state
        self.update_state(
            state='PROGRESS',
            meta={'ticker': ticker, 'status': 'starting', 'progress': 0}
        )
        
        # Get API key from parameter or environment
        api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not api_key:
            raise ValueError("POLYGON_API_KEY not provided")
        
        # Initialize clients with rate limiting for worker safety
        polygon_client = PolygonClient(api_key, calls_per_minute=3)
        
        # Detect asset type
        asset_type = detect_asset_type(ticker, polygon_client)
        logger.info(f"Detected asset type for {ticker}: {asset_type}")
        
        downloader = DataDownloader(polygon_client, data_dir="/data" if os.path.exists("/.dockerenv") else "data", asset_type=asset_type)
        
        logger.info(f"Task {self.request.id}: Starting backfill for {ticker}")
        
        # Use comprehensive download if auto-detecting everything
        if auto_detect and intervals is None and organize:
            self.update_state(
                state='PROGRESS',
                meta={
                    'ticker': ticker,
                    'status': 'auto-detecting available data',
                    'progress': 10
                }
            )
            
            result = downloader.download_all_available_data(
                ticker=ticker,
                output_format=output_format,
                force_refresh=force_refresh,
                organize=organize
            )
            
            if result['status'] == 'completed':
                logger.info(f"Task {self.request.id}: Downloaded all available data for {ticker}")
                return {
                    'task_id': self.request.id,
                    'ticker': ticker,
                    'status': 'completed',
                    'method': 'comprehensive',
                    'result': result
                }
            else:
                logger.warning(f"Task {self.request.id}: No data available for {ticker}")
                return {
                    'task_id': self.request.id,
                    'ticker': ticker,
                    'status': 'no_data',
                    'result': result
                }
        
        # Otherwise use the multi-interval download with auto-detection
        results = downloader.download_multiple_intervals(
            ticker=ticker,
            intervals=intervals,
            start_date=start_date,
            end_date=end_date,
            output_format=output_format,
            force_refresh=force_refresh,
            auto_detect_timeframes=(intervals is None)
        )
        
        # Count successes
        success_count = sum(1 for r in results if r.get('status') == 'success')
        total_intervals = len([r for r in results if r.get('status') != 'timeframe_detection'])
        
        # Final status
        success_count = sum(1 for r in results if r['status'] == 'success')
        
        final_result = {
            'task_id': self.request.id,
            'ticker': ticker,
            'status': 'completed',
            'intervals_completed': success_count,
            'intervals_total': total_intervals,
            'results': results,
            'start_date': start_date,
            'end_date': end_date
        }
        
        logger.info(f"Task {self.request.id}: Backfill complete for {ticker} - {success_count}/{total_intervals} intervals")
        return final_result
        
    except Exception as e:
        error_msg = f"Task {self.request.id}: Failed to backfill {ticker}: {str(e)}"
        logger.error(error_msg)
        
        self.update_state(
            state='FAILURE',
            meta={
                'ticker': ticker,
                'status': 'error',
                'error': str(e)
            }
        )
        
        return {
            'task_id': self.request.id,
            'ticker': ticker,
            'status': 'error',
            'error': str(e)
        }


@app.task(bind=True, name='app.tasks.discover_universe')
def discover_universe(
    self,
    category: str,
    limit: Optional[int] = None,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Celery task to discover ticker universe for a category.
    
    Args:
        category: Category name (us_equities, etf, crypto, etc.)
        limit: Maximum number of tickers to discover
        api_key: Polygon API key
    
    Returns:
        Dict with discovery results
    """
    try:
        self.update_state(
            state='PROGRESS',
            meta={'category': category, 'status': 'starting', 'progress': 0}
        )
        
        # Get API key
        api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not api_key:
            raise ValueError("POLYGON_API_KEY not provided")
        
        # Initialize clients
        polygon_client = PolygonClient(api_key, calls_per_minute=5)
        universe_manager = UniverseManager(polygon_client)
        
        logger.info(f"Task {self.request.id}: Starting universe discovery for {category}")
        
        result = universe_manager.discover_category(category, limit)
        
        logger.info(f"Task {self.request.id}: Universe discovery complete for {category}")
        
        return {
            'task_id': self.request.id,
            'category': category,
            'status': 'completed',
            'result': result
        }
        
    except Exception as e:
        error_msg = f"Task {self.request.id}: Failed to discover {category}: {str(e)}"
        logger.error(error_msg)
        
        self.update_state(
            state='FAILURE',
            meta={
                'category': category,
                'status': 'error',
                'error': str(e)
            }
        )
        
        return {
            'task_id': self.request.id,
            'category': category,
            'status': 'error',
            'error': str(e)
        }


@app.task(bind=True, name='app.tasks.bulk_enqueue_backfills')
def bulk_enqueue_backfills(
    self,
    universe_file: str,
    intervals: List[str],
    start_date: str,
    end_date: str,
    output_format: str = "parquet",
    batch_size: int = 10,
    force_refresh: bool = False
) -> Dict[str, Any]:
    """
    Celery task to bulk enqueue backfill tasks for a universe.
    
    Args:
        universe_file: Path to universe JSON file
        intervals: List of intervals to download
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_format: Output format
        batch_size: Number of tasks to enqueue at once
        force_refresh: Force refresh all data
    
    Returns:
        Dict with enqueue results
    """
    try:
        self.update_state(
            state='PROGRESS',
            meta={'status': 'loading universe', 'progress': 0}
        )
        
        # Load universe
        polygon_client = PolygonClient(os.getenv('POLYGON_API_KEY', ''), calls_per_minute=5)
        universe_manager = UniverseManager(polygon_client)
        tickers = universe_manager.get_ticker_list(universe_file)
        
        logger.info(f"Task {self.request.id}: Bulk enqueueing {len(tickers)} tickers")
        
        task_ids = []
        total_tickers = len(tickers)
        
        # Enqueue in batches
        for i in range(0, total_tickers, batch_size):
            batch = tickers[i:i + batch_size]
            
            self.update_state(
                state='PROGRESS',
                meta={
                    'status': f'enqueueing batch {i//batch_size + 1}',
                    'progress': int((i / total_tickers) * 100)
                }
            )
            
            for ticker in batch:
                task = backfill_symbol.delay(
                    ticker=ticker,
                    intervals=intervals,
                    start_date=start_date,
                    end_date=end_date,
                    output_format=output_format,
                    force_refresh=force_refresh
                )
                task_ids.append(task.id)
            
            logger.info(f"Task {self.request.id}: Enqueued batch {i//batch_size + 1} ({len(batch)} tasks)")
        
        result = {
            'task_id': self.request.id,
            'status': 'completed',
            'universe_file': universe_file,
            'total_tasks_enqueued': len(task_ids),
            'task_ids': task_ids[:100],  # Limit task IDs in response
            'total_task_ids': len(task_ids)
        }
        
        logger.info(f"Task {self.request.id}: Bulk enqueue complete - {len(task_ids)} tasks")
        return result
        
    except Exception as e:
        error_msg = f"Task {self.request.id}: Failed to bulk enqueue: {str(e)}"
        logger.error(error_msg)
        
        return {
            'task_id': self.request.id,
            'status': 'error',
            'error': str(e)
        }


@app.task(name='app.tasks.cleanup_old_results')
def cleanup_old_results() -> Dict[str, Any]:
    """
    Periodic task to clean up old task results.
    """
    try:
        # This would typically clean up old result files, logs, etc.
        logger.info("Cleanup task completed")
        return {'status': 'completed', 'action': 'cleanup'}
    except Exception as e:
        logger.error(f"Cleanup task failed: {e}")
        return {'status': 'error', 'error': str(e)}
