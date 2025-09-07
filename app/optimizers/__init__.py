"""
Optimization modules for the finance data pipeline.

These modules provide more efficient data access methods but are not yet
integrated into the main pipeline. They represent significant performance
improvements that can be activated when needed.
"""

from .flat_files import PolygonFlatFilesClient, HybridDataDownloader
from .grouped_daily import GroupedDailyDownloader
from .intraday_optimizer import IntradayOptimizer

__all__ = [
    'PolygonFlatFilesClient',
    'HybridDataDownloader', 
    'GroupedDailyDownloader',
    'IntradayOptimizer'
]

# Performance improvements available:
# - GroupedDailyDownloader: 500-10,000x fewer API calls for daily data
# - PolygonFlatFilesClient: Unlimited historical data via S3
# - IntradayOptimizer: Smart routing between data sources
# - HybridDataDownloader: Automatic selection of optimal data source
