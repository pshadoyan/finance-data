"""
Polygon.io Flat Files integration for bulk historical data access.
Based on Polygon.io docs recommendation for large volume downloads.
"""

import os
import logging
import boto3
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import pyarrow.parquet as pq
from botocore.client import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PolygonFlatFilesClient:
    """
    Client for accessing Polygon.io Flat Files via S3 for efficient bulk data downloads.
    As per Polygon docs: "Access large volumes of historical data efficiently."
    """
    
    def __init__(
        self, 
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        s3_endpoint: str = "https://files.polygon.io",
        bucket: str = "flatfiles"
    ):
        """
        Initialize Flat Files client with S3 credentials.
        
        Args:
            access_key: S3 access key ID
            secret_key: S3 secret access key  
            s3_endpoint: S3 endpoint URL
            bucket: S3 bucket name
        """
        self.access_key = access_key or os.getenv('POLYGON_S3_ACCESS_KEY')
        self.secret_key = secret_key or os.getenv('POLYGON_S3_SECRET_KEY')
        self.s3_endpoint = s3_endpoint
        self.bucket = bucket
        
        if not self.access_key or not self.secret_key:
            raise ValueError("S3 credentials required for Flat Files access")
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(signature_version='s3v4')
        )
        
        logger.info(f"Initialized Flat Files client for bucket: {self.bucket}")
    
    def list_available_dates(self, data_type: str = "trades", year: int = None) -> List[str]:
        """
        List available dates in the flat files bucket.
        
        Args:
            data_type: Type of data (trades, quotes, etc.)
            year: Filter by specific year
            
        Returns:
            List of available date paths
        """
        try:
            prefix = f"us_stocks_sip/{data_type}/"
            if year:
                prefix += f"{year}/"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
                Delimiter='/'
            )
            
            if 'CommonPrefixes' not in response:
                return []
            
            dates = [p['Prefix'].split('/')[-2] for p in response['CommonPrefixes']]
            return sorted(dates)
            
        except Exception as e:
            logger.error(f"Failed to list available dates: {e}")
            return []
    
    def download_day_data(
        self,
        date: str,
        data_type: str = "trades",
        output_dir: str = "data/flat_files",
        symbols: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Download a full day of data from flat files.
        
        Args:
            date: Date in YYYY-MM-DD format
            data_type: Type of data (trades, quotes)
            output_dir: Local output directory
            symbols: Filter for specific symbols (if None, downloads all)
            
        Returns:
            Dict with download results
        """
        try:
            # Format date for S3 path
            date_path = date.replace('-', '/')
            s3_key = f"us_stocks_sip/{data_type}/{date_path}/"
            
            # List all files for the date
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=s3_key
            )
            
            if 'Contents' not in response:
                logger.warning(f"No data found for {date}")
                return {
                    "status": "no_data",
                    "date": date,
                    "data_type": data_type
                }
            
            # Create output directory
            output_path = Path(output_dir) / data_type / date
            output_path.mkdir(parents=True, exist_ok=True)
            
            downloaded_files = []
            total_size = 0
            
            for obj in response['Contents']:
                file_key = obj['Key']
                file_name = file_key.split('/')[-1]
                
                # Filter by symbols if specified
                if symbols:
                    # Extract symbol from filename (assuming format like AAPL.parquet)
                    symbol = file_name.split('.')[0] if '.' in file_name else None
                    if symbol and symbol not in symbols:
                        continue
                
                # Download file
                local_file = output_path / file_name
                logger.info(f"Downloading {file_key} to {local_file}")
                
                self.s3_client.download_file(
                    Bucket=self.bucket,
                    Key=file_key,
                    Filename=str(local_file)
                )
                
                downloaded_files.append(str(local_file))
                total_size += obj['Size']
            
            logger.info(f"Downloaded {len(downloaded_files)} files, total size: {total_size / 1024 / 1024:.1f} MB")
            
            return {
                "status": "success",
                "date": date,
                "data_type": data_type,
                "files_downloaded": len(downloaded_files),
                "total_size_mb": total_size / 1024 / 1024,
                "output_dir": str(output_path)
            }
            
        except Exception as e:
            logger.error(f"Failed to download flat files for {date}: {e}")
            return {
                "status": "error",
                "date": date,
                "data_type": data_type,
                "error": str(e)
            }
    
    def download_date_range(
        self,
        start_date: str,
        end_date: str,
        data_type: str = "trades",
        output_dir: str = "data/flat_files",
        symbols: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Download data for a date range from flat files.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            data_type: Type of data
            output_dir: Local output directory
            symbols: Filter for specific symbols
            
        Returns:
            List of download results
        """
        results = []
        
        # Generate date range
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end:
            # Skip weekends
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                date_str = current_date.strftime('%Y-%m-%d')
                result = self.download_day_data(date_str, data_type, output_dir, symbols)
                results.append(result)
            
            current_date += timedelta(days=1)
        
        return results
    
    def read_flat_file(self, file_path: str) -> pd.DataFrame:
        """
        Read a downloaded flat file (parquet format).
        
        Args:
            file_path: Path to the parquet file
            
        Returns:
            DataFrame with the data
        """
        try:
            df = pd.read_parquet(file_path)
            return df
        except Exception as e:
            logger.error(f"Failed to read flat file {file_path}: {e}")
            raise
    
    def aggregate_day_to_bars(
        self,
        date: str,
        interval: str = "1m",
        data_type: str = "trades",
        input_dir: str = "data/flat_files",
        output_dir: str = "data"
    ) -> Dict[str, Any]:
        """
        Aggregate flat file tick data into OHLCV bars.
        
        Args:
            date: Date to process
            interval: Bar interval (1m, 5m, etc.)
            data_type: Type of data
            input_dir: Directory with flat files
            output_dir: Output directory for bars
            
        Returns:
            Dict with aggregation results
        """
        try:
            input_path = Path(input_dir) / data_type / date
            
            if not input_path.exists():
                return {
                    "status": "no_data",
                    "date": date,
                    "message": f"No flat files found at {input_path}"
                }
            
            # Map interval to pandas resample rule
            interval_map = {
                "1m": "1T",
                "5m": "5T",
                "15m": "15T",
                "1h": "1H",
                "1d": "1D"
            }
            
            if interval not in interval_map:
                raise ValueError(f"Unsupported interval: {interval}")
            
            resample_rule = interval_map[interval]
            processed_symbols = []
            
            # Process each symbol file
            for parquet_file in input_path.glob("*.parquet"):
                symbol = parquet_file.stem
                
                try:
                    # Read tick data
                    df = pd.read_parquet(parquet_file)
                    
                    # Ensure we have required columns
                    if 'timestamp' not in df.columns or 'price' not in df.columns:
                        logger.warning(f"Skipping {symbol}: missing required columns")
                        continue
                    
                    # Convert timestamp to datetime
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                    
                    # Aggregate to bars
                    bars = df.resample(resample_rule).agg({
                        'price': ['first', 'max', 'min', 'last'],
                        'size': 'sum'
                    })
                    
                    bars.columns = ['open', 'high', 'low', 'close', 'volume']
                    bars = bars.dropna()
                    
                    # Save aggregated bars
                    output_path = Path(output_dir) / symbol / f"{symbol}.{interval}.parquet"
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Merge with existing data if present
                    if output_path.exists():
                        existing = pd.read_parquet(output_path)
                        bars = pd.concat([existing, bars])
                        bars = bars[~bars.index.duplicated(keep='last')]
                        bars.sort_index(inplace=True)
                    
                    bars.to_parquet(output_path)
                    processed_symbols.append(symbol)
                    
                except Exception as e:
                    logger.warning(f"Failed to process {symbol}: {e}")
                    continue
            
            return {
                "status": "success",
                "date": date,
                "interval": interval,
                "symbols_processed": len(processed_symbols),
                "symbols": processed_symbols[:10]  # First 10 for display
            }
            
        except Exception as e:
            logger.error(f"Failed to aggregate data for {date}: {e}")
            return {
                "status": "error",
                "date": date,
                "error": str(e)
            }


class HybridDataDownloader:
    """
    Hybrid downloader that uses both REST API and Flat Files for optimal performance.
    Following Polygon.io docs best practices:
    - Flat Files for bulk historical data
    - REST API for recent/incremental updates
    """
    
    def __init__(
        self,
        polygon_client,
        flat_files_client: Optional[PolygonFlatFilesClient] = None,
        cutoff_days: int = 30
    ):
        """
        Initialize hybrid downloader.
        
        Args:
            polygon_client: Regular Polygon REST client
            flat_files_client: Flat Files S3 client
            cutoff_days: Use REST API for data within this many days
        """
        self.polygon_client = polygon_client
        self.flat_files_client = flat_files_client
        self.cutoff_days = cutoff_days
    
    def download_optimized(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        intervals: List[str]
    ) -> Dict[str, Any]:
        """
        Download data using the optimal method based on date range.
        
        Args:
            symbols: List of symbols to download
            start_date: Start date
            end_date: End date
            intervals: List of intervals
            
        Returns:
            Dict with download results
        """
        cutoff_date = (datetime.now() - timedelta(days=self.cutoff_days)).strftime('%Y-%m-%d')
        
        results = {
            "flat_files": None,
            "rest_api": None,
            "strategy": "hybrid"
        }
        
        # Use Flat Files for older data
        if start_date < cutoff_date and self.flat_files_client:
            flat_end = min(end_date, cutoff_date)
            logger.info(f"Using Flat Files for {start_date} to {flat_end}")
            
            results["flat_files"] = self.flat_files_client.download_date_range(
                start_date=start_date,
                end_date=flat_end,
                symbols=symbols
            )
        
        # Use REST API for recent data
        if end_date >= cutoff_date:
            rest_start = max(start_date, cutoff_date)
            logger.info(f"Using REST API for {rest_start} to {end_date}")
            
            # Use existing downloader logic
            from app.downloader import DataDownloader
            downloader = DataDownloader(self.polygon_client)
            
            rest_results = []
            for symbol in symbols:
                for interval in intervals:
                    result = downloader.download_symbol_data(
                        ticker=symbol,
                        interval=interval,
                        start_date=rest_start,
                        end_date=end_date
                    )
                    rest_results.append(result)
            
            results["rest_api"] = rest_results
        
        return results
