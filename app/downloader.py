"""
Core downloader with resume capabilities and parquet output.
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd
from app.polygon_client import PolygonClient, PolygonClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataDownloader:
    """Main downloader class with resume capabilities."""
    
    def __init__(self, polygon_client: PolygonClient, data_dir: str = "/data", asset_type: str = "equities"):
        self.client = polygon_client
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True, parents=True)
        self.asset_type = asset_type  # Can be "equities", "crypto", "etf", etc.
        
    def _get_ticker_dir(self, ticker: str) -> Path:
        """Get the directory for a specific ticker."""
        ticker_dir = self.data_dir / self.asset_type / ticker.upper()
        ticker_dir.mkdir(parents=True, exist_ok=True)
        return ticker_dir
    
    def _get_file_path(self, ticker: str, interval: str, output_format: str = "parquet") -> Path:
        """Get the file path for ticker data."""
        ticker_dir = self._get_ticker_dir(ticker)
        extension = "parquet" if output_format == "parquet" else "csv"
        return ticker_dir / f"{ticker.upper()}.{interval}.{extension}"
    
    def _load_existing_data(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Load existing data if it exists."""
        if not file_path.exists():
            return None
            
        try:
            if file_path.suffix == '.parquet':
                df = pd.read_parquet(file_path)
            else:
                df = pd.read_csv(file_path, parse_dates=['timestamp'])
            
            if df.empty:
                return None
                
            # Ensure timestamp column is datetime
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp').reset_index(drop=True)
                
            logger.info(f"Loaded {len(df)} existing records from {file_path}")
            return df
            
        except Exception as e:
            logger.warning(f"Could not load existing data from {file_path}: {e}")
            return None
    
    def _save_data(self, df: pd.DataFrame, file_path: Path, output_format: str = "parquet"):
        """Save dataframe to file."""
        try:
            # Ensure directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            if output_format == "parquet":
                df.to_parquet(file_path, index=False, engine='pyarrow')
            else:
                df.to_csv(file_path, index=False)
                
            logger.info(f"Saved {len(df)} records to {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to save data to {file_path}: {e}")
            raise
    
    def _get_last_timestamp(self, df: pd.DataFrame) -> Optional[datetime]:
        """Get the last timestamp from existing data."""
        if df is None or df.empty or 'timestamp' not in df.columns:
            return None
            
        last_ts = df['timestamp'].max()
        return last_ts if pd.notna(last_ts) else None
    
    def _merge_data(self, existing_df: Optional[pd.DataFrame], new_df: pd.DataFrame) -> pd.DataFrame:
        """Merge existing and new data, removing duplicates."""
        if existing_df is None or existing_df.empty:
            return new_df
        
        if new_df.empty:
            return existing_df
        
        # Combine and remove duplicates based on timestamp
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=['timestamp'], keep='last')
        combined = combined.sort_values('timestamp').reset_index(drop=True)
        
        return combined
    
    def download_symbol_data(
        self,
        ticker: str,
        interval: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        output_format: str = "parquet",
        force_refresh: bool = False,
        auto_detect_range: bool = True
    ) -> Dict[str, Any]:
        """
        Download data for a single symbol with resume capability.
        
        Args:
            ticker: Stock ticker symbol
            interval: Time interval (1m, 5m, 15m, 1h, 1d)
            start_date: Start date (YYYY-MM-DD), auto-detected if None
            end_date: End date (YYYY-MM-DD), auto-detected if None
            output_format: Output format (parquet or csv)
            force_refresh: If True, ignore existing data and download everything
            auto_detect_range: If True and dates are None, detect available range
            
        Returns:
            Dict with download results
        """
        ticker = ticker.upper()
        file_path = self._get_file_path(ticker, interval, output_format)
        
        # Auto-detect date range if not provided
        if auto_detect_range and (start_date is None or end_date is None):
            logger.info(f"Auto-detecting available date range for {ticker} {interval}")
            date_range = self.client.get_available_date_range(ticker, interval)
            
            if not date_range.get('data_available', False):
                logger.warning(f"No data available for {ticker} {interval}")
                return {
                    "ticker": ticker,
                    "interval": interval,
                    "status": "no_data_available",
                    "message": date_range.get('message', 'No data found'),
                    "file_path": str(file_path)
                }
            
            # Use detected range
            if start_date is None:
                start_date = date_range['start_date']
                logger.info(f"Auto-detected start date: {start_date}")
            
            if end_date is None:
                end_date = date_range['end_date']
                logger.info(f"Auto-detected end date: {end_date}")
        
        # Fallback to defaults if still None
        if start_date is None:
            # Default to getting ALL available history (from Unix epoch)
            # Polygon API requires dates after 1970-01-01
            start_date = "1970-01-01"
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Starting download for {ticker} {interval} from {start_date} to {end_date}")
        
        # Load existing data unless forcing refresh
        existing_df = None if force_refresh else self._load_existing_data(file_path)
        
        # Determine actual start date (resume from last timestamp if available)
        actual_start_date = start_date
        if existing_df is not None:
            last_timestamp = self._get_last_timestamp(existing_df)
            if last_timestamp:
                # Calculate appropriate resume date based on interval
                if interval in ["1s", "5s", "10s", "30s"]:
                    # For second intervals, resume from next second
                    resume_date = (last_timestamp + timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
                elif interval in ["1m", "2m", "3m", "5m", "10m", "15m", "30m", "45m"]:
                    # For minute intervals, resume from next minute
                    resume_date = (last_timestamp + timedelta(minutes=1)).strftime('%Y-%m-%d')
                elif interval in ["1h", "2h", "3h", "4h", "6h", "8h", "12h"]:
                    # For hour intervals, resume from next hour
                    resume_date = (last_timestamp + timedelta(hours=1)).strftime('%Y-%m-%d')
                elif interval == "1w":
                    # For weekly, resume from next week
                    resume_date = (last_timestamp + timedelta(weeks=1)).strftime('%Y-%m-%d')
                elif interval in ["1M", "3M"]:
                    # For monthly, resume from next month
                    resume_date = (last_timestamp + timedelta(days=31)).strftime('%Y-%m-%d')
                elif interval == "1Q":
                    # For quarterly, resume from next quarter
                    resume_date = (last_timestamp + timedelta(days=91)).strftime('%Y-%m-%d')
                elif interval == "1Y":
                    # For yearly, resume from next year
                    resume_date = (last_timestamp + timedelta(days=365)).strftime('%Y-%m-%d')
                else:
                    # Default to next day
                    resume_date = (last_timestamp + timedelta(days=1)).strftime('%Y-%m-%d')
                
                if resume_date <= end_date:
                    actual_start_date = resume_date
                    logger.info(f"Resuming download for {ticker} {interval} from {actual_start_date}")
                else:
                    logger.info(f"Data for {ticker} {interval} is already up to date")
                    return {
                        "ticker": ticker,
                        "interval": interval,
                        "status": "up_to_date",
                        "records": len(existing_df),
                        "file_path": str(file_path)
                    }
        
        # Fetch new data
        try:
            new_bars = []
            bar_count = 0
            
            for bar in self.client.get_bars(ticker, interval, actual_start_date, end_date):
                new_bars.append(bar)
                bar_count += 1
                
                # Process in batches to avoid memory issues
                if len(new_bars) >= 10000:
                    logger.info(f"Processing batch of {len(new_bars)} bars for {ticker}")
                    break
            
            if not new_bars:
                logger.info(f"No new data available for {ticker} {interval}")
                return {
                    "ticker": ticker,
                    "interval": interval,
                    "status": "no_new_data",
                    "records": len(existing_df) if existing_df is not None else 0,
                    "file_path": str(file_path)
                }
            
            # Convert to DataFrame
            new_df = pd.DataFrame(new_bars)
            
            # Merge with existing data
            final_df = self._merge_data(existing_df, new_df)
            
            # Save the merged data
            self._save_data(final_df, file_path, output_format)
            
            logger.info(f"Successfully downloaded {len(new_bars)} new bars for {ticker} {interval}")
            
            return {
                "ticker": ticker,
                "interval": interval,
                "status": "success",
                "new_records": len(new_bars),
                "total_records": len(final_df),
                "file_path": str(file_path),
                "start_date": actual_start_date,
                "end_date": end_date
            }
            
        except PolygonClientError as e:
            logger.error(f"Polygon API error for {ticker}: {e}")
            return {
                "ticker": ticker,
                "interval": interval,
                "status": "error",
                "error": str(e),
                "file_path": str(file_path)
            }
        except Exception as e:
            logger.error(f"Unexpected error downloading {ticker}: {e}")
            return {
                "ticker": ticker,
                "interval": interval,
                "status": "error",
                "error": str(e),
                "file_path": str(file_path)
            }
    
    def download_multiple_intervals(
        self,
        ticker: str,
        intervals: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        output_format: str = "parquet",
        force_refresh: bool = False,
        auto_detect_timeframes: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Download data for multiple intervals for a single ticker.
        
        Args:
            ticker: Stock ticker symbol
            intervals: List of intervals, auto-detected if None
            start_date: Start date, auto-detected if None
            end_date: End date, auto-detected if None
            output_format: Output format
            force_refresh: Force refresh all data
            auto_detect_timeframes: Auto-detect available timeframes if intervals is None
            
        Returns:
            List of download results
        """
        results = []
        ticker = ticker.upper()
        
        # Auto-detect available timeframes if not provided
        if auto_detect_timeframes and intervals is None:
            logger.info(f"Auto-detecting available timeframes for {ticker}")
            timeframe_info = self.client.detect_available_timeframes(ticker)
            
            if not timeframe_info['available_timeframes']:
                logger.warning(f"No timeframes available for {ticker}")
                return [{
                    "ticker": ticker,
                    "status": "no_timeframes_available",
                    "message": "No data available for any timeframe"
                }]
            
            intervals = list(timeframe_info['available_timeframes'].keys())
            logger.info(f"Found {len(intervals)} available timeframes: {', '.join(intervals)}")
            
            # Store the timeframe info for later use
            results.append({
                "ticker": ticker,
                "status": "timeframe_detection",
                "available_timeframes": timeframe_info['available_timeframes'],
                "market_type": timeframe_info['market_type']
            })
        
        # Default to common timeframes if still None
        if intervals is None:
            intervals = ["1d", "1h", "15m", "5m", "1m"]
            logger.info(f"Using default timeframes: {', '.join(intervals)}")
        
        # Download each interval with its own auto-detected date range
        for interval in intervals:
            # Each interval may have different date ranges available
            result = self.download_symbol_data(
                ticker=ticker,
                interval=interval,
                start_date=start_date,
                end_date=end_date,
                output_format=output_format,
                force_refresh=force_refresh,
                auto_detect_range=True
            )
            results.append(result)
            
            # Log progress
            if result['status'] == 'success':
                logger.info(f"✓ Downloaded {ticker} {interval}: {result.get('new_records', 0)} new records")
            elif result['status'] == 'up_to_date':
                logger.info(f"✓ {ticker} {interval} is already up to date")
            else:
                logger.warning(f"✗ Failed to download {ticker} {interval}: {result.get('status', 'unknown')}")
        
        return results
    
    def download_all_available_data(
        self,
        ticker: str,
        output_format: str = "parquet",
        force_refresh: bool = False,
        organize: bool = True
    ) -> Dict[str, Any]:
        """
        Download all available data for a ticker across all timeframes and date ranges.
        
        Args:
            ticker: Stock ticker symbol
            output_format: Output format
            force_refresh: Force refresh all data
            organize: Deprecated parameter (kept for compatibility)
            
        Returns:
            Comprehensive download results
        """
        ticker = ticker.upper()
        logger.info(f"Starting comprehensive download for {ticker}")
        
        # Detect all available timeframes and their ranges
        timeframe_info = self.client.detect_available_timeframes(ticker)
        
        if not timeframe_info['available_timeframes']:
            return {
                "ticker": ticker,
                "status": "no_data",
                "message": "No data available for this ticker"
            }
        
        # Download all available timeframes with their full date ranges
        download_results = []
        success_count = 0
        total_records = 0
        
        for interval, range_info in timeframe_info['available_timeframes'].items():
            logger.info(f"Downloading {ticker} {interval} from {range_info['start_date']} to {range_info['end_date']}")
            
            result = self.download_symbol_data(
                ticker=ticker,
                interval=interval,
                start_date=range_info['start_date'],
                end_date=range_info['end_date'],
                output_format=output_format,
                force_refresh=force_refresh,
                auto_detect_range=False  # We already have the range
            )
            
            download_results.append(result)
            
            if result['status'] == 'success':
                success_count += 1
                total_records += result.get('new_records', 0)
        
        # No organization needed - data is already saved to equities/TICKER/
        summary = {
            "files_downloaded": success_count,
            "total_records": total_records,
            "data_location": f"{self.data_dir}/equities/{ticker}/"
        }
        
        return {
            "ticker": ticker,
            "status": "completed",
            "market_type": timeframe_info['market_type'],
            "timeframes_available": len(timeframe_info['available_timeframes']),
            "timeframes_downloaded": success_count,
            "total_new_records": total_records,
            "timeframe_details": timeframe_info['available_timeframes'],
            "download_results": download_results,
            "data_summary": summary
        }
    
    def get_data_summary(self, ticker: str, interval: str) -> Dict[str, Any]:
        """Get summary information about downloaded data."""
        file_path = self._get_file_path(ticker, interval)
        
        if not file_path.exists():
            return {"ticker": ticker, "interval": interval, "status": "no_data"}
        
        try:
            df = self._load_existing_data(file_path)
            if df is None or df.empty:
                return {"ticker": ticker, "interval": interval, "status": "empty"}
            
            return {
                "ticker": ticker,
                "interval": interval,
                "status": "available",
                "records": len(df),
                "start_date": df['timestamp'].min().strftime('%Y-%m-%d'),
                "end_date": df['timestamp'].max().strftime('%Y-%m-%d'),
                "file_path": str(file_path),
                "file_size_mb": file_path.stat().st_size / (1024 * 1024)
            }
            
        except Exception as e:
            return {
                "ticker": ticker,
                "interval": interval,
                "status": "error",
                "error": str(e)
            }
