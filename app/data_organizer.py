"""
Data organization and management system for structured output.
Ensures consistent, organized data storage with metadata tracking.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd
import shutil

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataOrganizer:
    """
    Manages data organization with proper structure and metadata.
    
    Directory Structure:
    data/
    ├── market/
    │   ├── stocks/
    │   │   ├── daily/
    │   │   │   ├── AAPL/
    │   │   │   │   ├── 2024/
    │   │   │   │   │   ├── AAPL_2024-01.parquet
    │   │   │   │   │   └── AAPL_2024-02.parquet
    │   │   │   └── TSLA/
    │   │   ├── intraday/
    │   │   │   ├── 1m/
    │   │   │   ├── 5m/
    │   │   │   └── 15m/
    │   │   └── metadata/
    │   ├── etf/
    │   ├── crypto/
    │   └── fx/
    ├── flat_files/
    │   ├── trades/
    │   └── quotes/
    └── metadata/
        ├── downloads.json
        ├── universes.json
        └── data_catalog.json
    """
    
    # Market type mappings
    MARKET_TYPES = {
        'CS': 'stocks',      # Common Stock
        'ETF': 'etf',        # ETF
        'CRYPTO': 'crypto',  # Cryptocurrency
        'FX': 'fx',          # Foreign Exchange
        'INDEX': 'indices',  # Index
        'OPTION': 'options', # Options
        'OTC': 'otc'        # Over-the-counter
    }
    
    # Timeframe categorization
    INTRADAY_INTERVALS = ['1m', '5m', '15m', '30m']
    HOURLY_INTERVALS = ['1h', '2h', '4h']
    DAILY_INTERVALS = ['1d', '1w', '1M', '1Q', '1Y']
    
    def __init__(self, base_dir: str = "data"):
        """
        Initialize data organizer.
        
        Args:
            base_dir: Base directory for all data storage
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        
        # Initialize directory structure
        self._initialize_structure()
        
        # Load metadata
        self.metadata = self._load_metadata()
    
    def _initialize_structure(self):
        """Create the base directory structure."""
        # Main directories
        directories = [
            'market/stocks/daily',
            'market/stocks/intraday/1m',
            'market/stocks/intraday/5m',
            'market/stocks/intraday/15m',
            'market/stocks/intraday/1h',
            'market/stocks/metadata',
            'market/etf/daily',
            'market/etf/intraday',
            'market/crypto/daily',
            'market/crypto/intraday',
            'market/fx/daily',
            'market/fx/intraday',
            'market/indices/daily',
            'market/options',
            'market/otc',
            'flat_files/trades',
            'flat_files/quotes',
            'flat_files/aggregates',
            'metadata',
            'temp',
            'archive'
        ]
        
        for dir_path in directories:
            (self.base_dir / dir_path).mkdir(parents=True, exist_ok=True)
    
    def _load_metadata(self) -> Dict[str, Any]:
        """Load or initialize metadata."""
        metadata_file = self.base_dir / 'metadata' / 'data_catalog.json'
        
        if metadata_file.exists():
            try:
                with open(metadata_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load metadata: {e}")
        
        # Initialize empty metadata
        return {
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'total_files': 0,
            'total_size_mb': 0,
            'symbols': {},
            'downloads': []
        }
    
    def _save_metadata(self):
        """Save metadata to file."""
        metadata_file = self.base_dir / 'metadata' / 'data_catalog.json'
        
        self.metadata['last_updated'] = datetime.now().isoformat()
        
        try:
            with open(metadata_file, 'w') as f:
                json.dump(self.metadata, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
    
    def get_symbol_path(
        self,
        symbol: str,
        interval: str,
        market_type: str = 'stocks',
        data_source: str = 'rest_api'
    ) -> Path:
        """
        Get the organized path for a symbol's data.
        
        Args:
            symbol: Stock symbol
            interval: Time interval
            market_type: Type of market (stocks, etf, crypto, etc.)
            data_source: Source of data (rest_api, flat_files)
            
        Returns:
            Path object for the symbol's data location
        """
        symbol = symbol.upper()
        
        if data_source == 'flat_files':
            # Flat files organization
            base_path = self.base_dir / 'flat_files' / 'aggregates'
        else:
            # REST API organization
            base_path = self.base_dir / 'market' / market_type
            
            # Categorize by timeframe
            if interval in self.INTRADAY_INTERVALS:
                base_path = base_path / 'intraday' / interval
            elif interval in self.HOURLY_INTERVALS:
                base_path = base_path / 'hourly' / interval
            else:
                base_path = base_path / 'daily'
        
        # Create symbol directory
        symbol_path = base_path / symbol
        symbol_path.mkdir(parents=True, exist_ok=True)
        
        return symbol_path
    
    def organize_downloaded_file(
        self,
        source_file: Path,
        symbol: str,
        interval: str,
        market_type: str = 'stocks',
        data_source: str = 'rest_api',
        partition_by_year: bool = False  # Changed default to False for simpler structure
    ) -> Path:
        """
        Organize a downloaded file into simplified structure.
        
        Args:
            source_file: Path to the downloaded file
            symbol: Stock symbol
            interval: Time interval
            market_type: Market type
            data_source: Data source
            partition_by_year: Whether to partition by year (now disabled by default)
            
        Returns:
            New organized path
        """
        if not source_file.exists():
            raise FileNotFoundError(f"Source file not found: {source_file}")
        
        # Simplified structure: data/equities/TICKER/TICKER.{interval}.parquet
        if market_type in ['stocks', 'CS', 'ETF']:
            folder_name = 'equities'
        elif market_type == 'crypto':
            folder_name = 'crypto'
        elif market_type == 'fx':
            folder_name = 'fx'
        else:
            folder_name = market_type.lower() if market_type else 'other'
        
        # Create target directory
        target_dir = self.base_dir / folder_name / symbol
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Simple file naming: TICKER.interval.parquet
        target_file = target_dir / f"{symbol}.{interval}.parquet"
        
        # If file already exists, we append data instead of overwriting
        if target_file.exists():
            try:
                # Read existing data
                existing_df = pd.read_parquet(target_file)
                new_df = pd.read_parquet(source_file)
                
                # Combine and deduplicate based on timestamp
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                if 'timestamp' in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
                    combined_df = combined_df.sort_values('timestamp')
                
                # Save combined data
                combined_df.to_parquet(target_file, index=False)
                logger.info(f"Appended {len(new_df)} records to {target_file} (total: {len(combined_df)})")
                
                # Remove source file
                source_file.unlink()
                
            except Exception as e:
                logger.warning(f"Could not append to existing file: {e}, overwriting")
                shutil.move(str(source_file), str(target_file))
        else:
            # Move file to target location
            shutil.move(str(source_file), str(target_file))
            logger.info(f"Organized {symbol} {interval} -> {target_file}")
        
        # Update metadata
        self._update_symbol_metadata(symbol, interval, market_type)
        
        return target_file
    
    def _update_symbol_metadata(
        self,
        symbol: str,
        interval: str,
        market_type: str,
        record_count: int = 0
    ):
        """Update metadata for a symbol."""
        symbol = symbol.upper()
        
        if symbol not in self.metadata['symbols']:
            self.metadata['symbols'][symbol] = {
                'market_type': market_type,
                'intervals': {},
                'first_download': datetime.now().isoformat(),
                'last_update': datetime.now().isoformat()
            }
        
        symbol_meta = self.metadata['symbols'][symbol]
        symbol_meta['last_update'] = datetime.now().isoformat()
        
        if interval not in symbol_meta['intervals']:
            symbol_meta['intervals'][interval] = {
                'record_count': 0,
                'date_range': {'start': None, 'end': None},
                'last_update': datetime.now().isoformat()
            }
        
        interval_meta = symbol_meta['intervals'][interval]
        interval_meta['record_count'] += record_count
        interval_meta['last_update'] = datetime.now().isoformat()
        
        # Update global stats
        self.metadata['total_files'] = sum(
            len(s['intervals']) for s in self.metadata['symbols'].values()
        )
        
        self._save_metadata()
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get a summary of all organized data."""
        summary = {
            'total_symbols': len(self.metadata['symbols']),
            'total_files': self.metadata['total_files'],
            'markets': {},
            'intervals': {},
            'last_update': self.metadata.get('last_updated', 'unknown')
        }
        
        # Count by market type
        for symbol, data in self.metadata['symbols'].items():
            market = data.get('market_type', 'unknown')
            if market not in summary['markets']:
                summary['markets'][market] = {'count': 0, 'symbols': []}
            summary['markets'][market]['count'] += 1
            summary['markets'][market]['symbols'].append(symbol)
        
        # Count by interval
        for symbol, data in self.metadata['symbols'].items():
            for interval in data.get('intervals', {}):
                if interval not in summary['intervals']:
                    summary['intervals'][interval] = 0
                summary['intervals'][interval] += 1
        
        # Calculate disk usage
        try:
            total_size = sum(
                f.stat().st_size for f in self.base_dir.rglob('*') if f.is_file()
            )
            summary['total_size_mb'] = total_size / (1024 * 1024)
        except Exception as e:
            logger.warning(f"Could not calculate disk usage: {e}")
            summary['total_size_mb'] = 0
        
        return summary
    
    def cleanup_temp_files(self):
        """Clean up temporary files."""
        temp_dir = self.base_dir / 'temp'
        
        try:
            for file in temp_dir.glob('*'):
                if file.is_file():
                    # Delete files older than 1 day
                    age_days = (datetime.now().timestamp() - file.stat().st_mtime) / 86400
                    if age_days > 1:
                        file.unlink()
                        logger.info(f"Deleted temp file: {file}")
        except Exception as e:
            logger.error(f"Error cleaning temp files: {e}")
    
    def archive_old_data(self, days_old: int = 365):
        """
        Archive data older than specified days.
        
        Args:
            days_old: Archive data older than this many days
        """
        archive_dir = self.base_dir / 'archive'
        archive_dir.mkdir(exist_ok=True)
        
        cutoff_time = datetime.now().timestamp() - (days_old * 86400)
        archived_count = 0
        
        for market_dir in (self.base_dir / 'market').iterdir():
            if not market_dir.is_dir():
                continue
            
            for file_path in market_dir.rglob('*.parquet'):
                try:
                    if file_path.stat().st_mtime < cutoff_time:
                        # Create archive structure
                        relative_path = file_path.relative_to(self.base_dir / 'market')
                        archive_path = archive_dir / relative_path
                        archive_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        # Move to archive
                        shutil.move(str(file_path), str(archive_path))
                        archived_count += 1
                        logger.info(f"Archived: {file_path} -> {archive_path}")
                        
                except Exception as e:
                    logger.error(f"Failed to archive {file_path}: {e}")
        
        logger.info(f"Archived {archived_count} files older than {days_old} days")
        return archived_count
    
    def generate_report(self, output_file: Optional[str] = None) -> str:
        """
        Generate a detailed report of the data organization.
        
        Args:
            output_file: Optional file to save report to
            
        Returns:
            Report as string
        """
        summary = self.get_data_summary()
        
        report = []
        report.append("=" * 60)
        report.append("DATA ORGANIZATION REPORT")
        report.append("=" * 60)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Base Directory: {self.base_dir}")
        report.append("")
        
        report.append("SUMMARY")
        report.append("-" * 40)
        report.append(f"Total Symbols: {summary['total_symbols']}")
        report.append(f"Total Files: {summary['total_files']}")
        report.append(f"Total Size: {summary['total_size_mb']:.1f} MB")
        report.append(f"Last Update: {summary['last_update']}")
        report.append("")
        
        report.append("MARKETS")
        report.append("-" * 40)
        for market, data in summary['markets'].items():
            report.append(f"{market}: {data['count']} symbols")
            if data['count'] <= 10:
                report.append(f"  Symbols: {', '.join(data['symbols'])}")
            else:
                report.append(f"  Sample: {', '.join(data['symbols'][:10])}...")
        report.append("")
        
        report.append("INTERVALS")
        report.append("-" * 40)
        for interval, count in sorted(summary['intervals'].items()):
            report.append(f"{interval}: {count} symbols")
        report.append("")
        
        report.append("TOP SYMBOLS BY DATA")
        report.append("-" * 40)
        
        # Sort symbols by number of intervals
        symbol_intervals = [
            (symbol, len(data['intervals']))
            for symbol, data in self.metadata['symbols'].items()
        ]
        symbol_intervals.sort(key=lambda x: x[1], reverse=True)
        
        for symbol, interval_count in symbol_intervals[:20]:
            symbol_data = self.metadata['symbols'][symbol]
            intervals = list(symbol_data['intervals'].keys())
            report.append(f"{symbol}: {interval_count} intervals ({', '.join(intervals)})")
        
        report.append("")
        report.append("=" * 60)
        
        report_text = "\n".join(report)
        
        if output_file:
            output_path = self.base_dir / 'metadata' / output_file
            with open(output_path, 'w') as f:
                f.write(report_text)
            logger.info(f"Report saved to {output_path}")
        
        return report_text
