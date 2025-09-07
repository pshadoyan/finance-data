"""
Polygon API client with rate limiting, resume support, and comprehensive data fetching.
"""

import os
import time
import logging
from typing import List, Dict, Optional, Iterator, Any
from datetime import datetime, timedelta
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from polygon import RESTClient
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PolygonRateLimiter:
    """Simple rate limiter for Polygon API calls."""
    
    def __init__(self, calls_per_minute: int = 5):
        self.calls_per_minute = calls_per_minute
        self.calls = []
        self.lock = False
    
    def wait_if_needed(self):
        """Wait if we're hitting rate limits."""
        if self.lock:
            time.sleep(1)
            return
        
        now = time.time()
        # Remove calls older than 1 minute
        self.calls = [call_time for call_time in self.calls if now - call_time < 60]
        
        if len(self.calls) >= self.calls_per_minute:
            sleep_time = 60 - (now - self.calls[0]) + 1
            logger.info(f"Rate limit reached, sleeping for {sleep_time:.1f} seconds")
            time.sleep(sleep_time)
        
        self.calls.append(now)


class PolygonClientError(Exception):
    """Custom exception for Polygon client errors."""
    pass


class TickerInfo(BaseModel):
    """Model for ticker information."""
    ticker: str
    name: str
    market: str
    locale: str
    primary_exchange: str
    type: str
    active: bool
    currency_name: Optional[str] = None
    cik: Optional[str] = None
    composite_figi: Optional[str] = None
    share_class_figi: Optional[str] = None
    last_updated_utc: Optional[str] = None


class PolygonClient:
    """Enhanced Polygon client with rate limiting and resume capabilities."""
    
    def __init__(self, api_key: str, calls_per_minute: int = 5):
        if not api_key:
            raise ValueError("API key is required")
        
        self.api_key = api_key
        self.client = RESTClient(api_key)
        self.rate_limiter = PolygonRateLimiter(calls_per_minute)
        self.session = requests.Session()
        
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.RequestException, PolygonClientError))
    )
    def get_tickers(
        self, 
        market: str = "stocks", 
        ticker_type: Optional[str] = None,
        active: bool = True,
        limit: int = 1000
    ) -> Iterator[TickerInfo]:
        """
        Get tickers from Polygon API with pagination.
        
        Args:
            market: Market type (stocks, crypto, fx, etc.)
            ticker_type: Specific ticker type (CS, ETF, etc.)
            active: Only active tickers
            limit: Results per page
        """
        self.rate_limiter.wait_if_needed()
        
        params = {
            "market": market,
            "active": active,
            "limit": limit
        }
        
        if ticker_type:
            params["type"] = ticker_type
        
        try:
            tickers_resp = self.client.list_tickers(**params)
            
            for ticker_data in tickers_resp:
                try:
                    yield TickerInfo(
                        ticker=ticker_data.ticker,
                        name=getattr(ticker_data, 'name', ''),
                        market=getattr(ticker_data, 'market', market),
                        locale=getattr(ticker_data, 'locale', ''),
                        primary_exchange=getattr(ticker_data, 'primary_exchange', ''),
                        type=getattr(ticker_data, 'type', ''),
                        active=getattr(ticker_data, 'active', True),
                        currency_name=getattr(ticker_data, 'currency_name', None),
                        cik=getattr(ticker_data, 'cik', None),
                        composite_figi=getattr(ticker_data, 'composite_figi', None),
                        share_class_figi=getattr(ticker_data, 'share_class_figi', None),
                        last_updated_utc=getattr(ticker_data, 'last_updated_utc', None)
                    )
                except Exception as e:
                    logger.warning(f"Skipping malformed ticker data: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to fetch tickers: {e}")
            raise PolygonClientError(f"Ticker fetch failed: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.RequestException, PolygonClientError))
    )
    def get_bars(
        self, 
        ticker: str, 
        interval: str,
        start_date: str, 
        end_date: str,
        limit: int = 5000
    ) -> Iterator[Dict[str, Any]]:
        """
        Get historical bars for a ticker with pagination.
        
        Args:
            ticker: Stock ticker symbol
            interval: Time interval (1m, 5m, 15m, 1h, 1d)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Results per page
        """
        self.rate_limiter.wait_if_needed()
        
        # Map intervals to Polygon format - support ALL Polygon timeframes
        interval_mapping = {
            # Seconds
            "1s": ("second", 1),
            "5s": ("second", 5),
            "10s": ("second", 10),
            "30s": ("second", 30),
            # Minutes
            "1m": ("minute", 1),
            "2m": ("minute", 2),
            "3m": ("minute", 3),
            "5m": ("minute", 5),
            "10m": ("minute", 10),
            "15m": ("minute", 15),
            "30m": ("minute", 30),
            "45m": ("minute", 45),
            # Hours
            "1h": ("hour", 1),
            "2h": ("hour", 2),
            "3h": ("hour", 3),
            "4h": ("hour", 4),
            "6h": ("hour", 6),
            "8h": ("hour", 8),
            "12h": ("hour", 12),
            # Daily and above
            "1d": ("day", 1),
            "1w": ("week", 1),
            "1M": ("month", 1),
            "3M": ("month", 3),
            "1Q": ("quarter", 1),
            "1Y": ("year", 1)
        }
        
        if interval not in interval_mapping:
            raise ValueError(f"Unsupported interval: {interval}")
        
        timespan, multiplier = interval_mapping[interval]
        
        try:
            bars_resp = self.client.list_aggs(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=start_date,
                to=end_date,
                limit=limit,
                adjusted=True,
                sort="asc"
            )
            
            if not bars_resp:
                logger.warning(f"No data returned for {ticker} {interval} from {start_date} to {end_date}")
                return
            
            for bar in bars_resp:
                try:
                    yield {
                        'timestamp': pd.Timestamp(bar.timestamp, unit='ms'),
                        'open': float(bar.open),
                        'high': float(bar.high),
                        'low': float(bar.low),
                        'close': float(bar.close),
                        'volume': int(bar.volume) if bar.volume else 0,
                        'vwap': float(getattr(bar, 'vwap', 0)) if hasattr(bar, 'vwap') else None,
                        'transactions': int(getattr(bar, 'transactions', 0)) if hasattr(bar, 'transactions') else None
                    }
                except (AttributeError, ValueError, TypeError) as e:
                    logger.warning(f"Skipping malformed bar data for {ticker}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to fetch bars for {ticker}: {e}")
            raise PolygonClientError(f"Bar fetch failed for {ticker}: {e}")

    def get_market_status(self) -> Dict[str, Any]:
        """Get current market status."""
        self.rate_limiter.wait_if_needed()
        
        try:
            status = self.client.get_market_status()
            return {
                "market": status.market,
                "serverTime": status.serverTime,
                "exchanges": {ex.name: ex.status for ex in status.exchanges} if hasattr(status, 'exchanges') else {},
                "currencies": {curr.name: curr.status for curr in status.currencies} if hasattr(status, 'currencies') else {}
            }
        except Exception as e:
            logger.error(f"Failed to get market status: {e}")
            raise PolygonClientError(f"Market status fetch failed: {e}")
    
    def get_ticker_details(self, ticker: str) -> Dict[str, Any]:
        """Get detailed information about a ticker including available date range."""
        self.rate_limiter.wait_if_needed()
        
        try:
            details = self.client.get_ticker_details(ticker)
            return {
                "ticker": details.ticker,
                "name": getattr(details, 'name', ''),
                "market_cap": getattr(details, 'market_cap', None),
                "description": getattr(details, 'description', ''),
                "homepage_url": getattr(details, 'homepage_url', ''),
                "list_date": getattr(details, 'list_date', None),
                "type": getattr(details, 'type', ''),
                "active": getattr(details, 'active', True)
            }
        except Exception as e:
            logger.error(f"Failed to get ticker details for {ticker}: {e}")
            raise PolygonClientError(f"Ticker details fetch failed: {e}")
    
    def get_available_date_range(self, ticker: str, interval: str = "1d") -> Dict[str, Any]:
        """
        Detect the available date range for a ticker by probing the API.
        
        Args:
            ticker: Stock ticker symbol
            interval: Time interval to check
            
        Returns:
            Dict with start_date, end_date, and total_bars available
        """
        self.rate_limiter.wait_if_needed()
        
        try:
            # Map interval to Polygon format
            interval_mapping = {
                "1m": ("minute", 1),
                "5m": ("minute", 5),
                "15m": ("minute", 15),
                "1h": ("hour", 1),
                "1d": ("day", 1)
            }
            
            if interval not in interval_mapping:
                raise ValueError(f"Unsupported interval: {interval}")
            
            timespan, multiplier = interval_mapping[interval]
            
            # Try to get the earliest available data
            # Polygon API requires dates after Unix epoch (1970-01-01)
            ticker_details = None
            try:
                ticker_details = self.get_ticker_details(ticker)
                list_date = ticker_details.get('list_date')
                if list_date and list_date > "1970-01-01":
                    # Use IPO date if it's after Unix epoch
                    start_probe = list_date
                else:
                    # Probe from 1970 (Unix epoch start)
                    start_probe = "1970-01-01"
            except:
                # Default to Unix epoch for maximum historical coverage
                start_probe = "1970-01-01"
            
            # Get today's date as end probe
            end_probe = datetime.now().strftime('%Y-%m-%d')
            
            # Get first available bar
            first_bar = None
            try:
                bars_resp = self.client.list_aggs(
                    ticker=ticker,
                    multiplier=multiplier,
                    timespan=timespan,
                    from_=start_probe,
                    to=end_probe,
                    limit=1,
                    adjusted=True,
                    sort="asc"
                )
                
                if bars_resp:
                    for bar in bars_resp:
                        first_bar = bar
                        break
            except Exception as e:
                logger.warning(f"Could not get first bar for {ticker}: {e}")
            
            # Get last available bar
            last_bar = None
            try:
                bars_resp = self.client.list_aggs(
                    ticker=ticker,
                    multiplier=multiplier,
                    timespan=timespan,
                    from_=start_probe,
                    to=end_probe,
                    limit=1,
                    adjusted=True,
                    sort="desc"
                )
                
                if bars_resp:
                    for bar in bars_resp:
                        last_bar = bar
                        break
            except Exception as e:
                logger.warning(f"Could not get last bar for {ticker}: {e}")
            
            if first_bar and last_bar:
                first_date = pd.Timestamp(first_bar.timestamp, unit='ms').strftime('%Y-%m-%d')
                last_date = pd.Timestamp(last_bar.timestamp, unit='ms').strftime('%Y-%m-%d')
                
                # Estimate total bars (this is approximate)
                days_diff = (pd.Timestamp(last_date) - pd.Timestamp(first_date)).days
                
                if interval == "1d":
                    estimated_bars = days_diff
                elif interval == "1h":
                    estimated_bars = days_diff * 6.5  # ~6.5 trading hours per day
                elif interval == "15m":
                    estimated_bars = days_diff * 26  # ~26 15-min bars per trading day
                elif interval == "5m":
                    estimated_bars = days_diff * 78  # ~78 5-min bars per trading day
                elif interval == "1m":
                    estimated_bars = days_diff * 390  # ~390 1-min bars per trading day
                else:
                    estimated_bars = days_diff
                
                return {
                    "ticker": ticker,
                    "interval": interval,
                    "start_date": first_date,
                    "end_date": last_date,
                    "days_available": days_diff,
                    "estimated_bars": int(estimated_bars),
                    "data_available": True,
                    "ticker_details": ticker_details
                }
            else:
                return {
                    "ticker": ticker,
                    "interval": interval,
                    "start_date": None,
                    "end_date": None,
                    "days_available": 0,
                    "estimated_bars": 0,
                    "data_available": False,
                    "ticker_details": ticker_details,
                    "message": "No data available for this ticker/interval combination"
                }
                
        except Exception as e:
            logger.error(f"Failed to get date range for {ticker}: {e}")
            return {
                "ticker": ticker,
                "interval": interval,
                "start_date": None,
                "end_date": None,
                "data_available": False,
                "error": str(e)
            }
    
    def detect_available_timeframes(self, ticker: str) -> Dict[str, Any]:
        """
        Dynamically detect ALL timeframes with data available for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dict with available timeframes and their date ranges
        """
        logger.info(f"Detecting available timeframes for {ticker}")
        
        available_timeframes = {}
        
        # Check commonly used timeframe combinations
        # Polygon supports any multiplier with: second, minute, hour, day, week, month, quarter, year
        timeframes_to_check = [
            # Seconds (for high-frequency data)
            ("second", 1, "1s"),
            ("second", 5, "5s"),
            ("second", 10, "10s"),
            ("second", 30, "30s"),
            
            # Minutes (most common intraday)
            ("minute", 1, "1m"),
            ("minute", 2, "2m"),
            ("minute", 3, "3m"),
            ("minute", 5, "5m"),
            ("minute", 10, "10m"),
            ("minute", 15, "15m"),
            ("minute", 30, "30m"),
            ("minute", 45, "45m"),
            
            # Hours
            ("hour", 1, "1h"),
            ("hour", 2, "2h"),
            ("hour", 3, "3h"),
            ("hour", 4, "4h"),
            ("hour", 6, "6h"),
            ("hour", 8, "8h"),
            ("hour", 12, "12h"),
            
            # Daily and above
            ("day", 1, "1d"),
            ("week", 1, "1w"),
            ("month", 1, "1M"),
            ("month", 3, "3M"),  # Quarterly alternative
            ("quarter", 1, "1Q"),
            ("year", 1, "1Y")
        ]
        
        for timespan, multiplier, label in timeframes_to_check:
            try:
                check_result = self._check_timeframe_availability(ticker, timespan, multiplier)
                if check_result:
                    available_timeframes[label] = check_result
                    logger.info(f"  ✓ {label}: {check_result['start_date']} to {check_result['end_date']} ({check_result['estimated_bars']} bars)")
                else:
                    logger.debug(f"  ✗ {label}: No data available")
            except Exception as e:
                logger.debug(f"  ✗ {label}: Error checking - {e}")
                continue
        
        # Determine market type based on available timeframes
        market_type = self._infer_market_type(ticker, available_timeframes)
        
        return {
            "ticker": ticker,
            "available_timeframes": available_timeframes,
            "total_timeframes": len(available_timeframes),
            "market_type": market_type,
            "has_seconds": any(tf in available_timeframes for tf in ["1s", "5s", "10s", "30s"]),
            "has_intraday": any(tf in available_timeframes for tf in ["1m", "2m", "3m", "5m", "10m", "15m", "30m", "45m"]),
            "has_hourly": any(tf in available_timeframes for tf in ["1h", "2h", "3h", "4h", "6h", "8h", "12h"]),
            "has_daily": any(tf in available_timeframes for tf in ["1d", "1w", "1M", "3M", "1Q", "1Y"])
        }
    
    def _check_timeframe_availability(self, ticker: str, timespan: str, multiplier: int) -> Optional[Dict[str, Any]]:
        """
        Check if a specific timeframe has data available.
        
        Args:
            ticker: Stock ticker symbol
            timespan: Polygon timespan (minute, hour, day)
            multiplier: Multiplier for the timespan
            
        Returns:
            Dict with date range info if available, None otherwise
        """
        try:
            # Use a wide date range to check available history
            # Polygon API requires dates after Unix epoch (1970-01-01)
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = "1970-01-01"
            
            # Try to get just one bar to check availability
            bars_resp = self.client.list_aggs(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=start_date,
                to=end_date,
                limit=1,
                adjusted=True,
                sort="asc"
            )
            
            first_bar = None
            for bar in bars_resp:
                first_bar = bar
                break
            
            if not first_bar:
                return None
            
            # Now get the last bar
            bars_resp = self.client.list_aggs(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=start_date,
                to=end_date,
                limit=1,
                adjusted=True,
                sort="desc"
            )
            
            last_bar = None
            for bar in bars_resp:
                last_bar = bar
                break
            
            if first_bar and last_bar:
                first_date = pd.Timestamp(first_bar.timestamp, unit='ms').strftime('%Y-%m-%d')
                last_date = pd.Timestamp(last_bar.timestamp, unit='ms').strftime('%Y-%m-%d')
                days_diff = (pd.Timestamp(last_date) - pd.Timestamp(first_date)).days
                
                # Estimate bars based on timespan
                if timespan == "day":
                    estimated_bars = days_diff
                elif timespan == "hour":
                    estimated_bars = days_diff * 6.5  # ~6.5 trading hours per day
                elif timespan == "minute":
                    estimated_bars = days_diff * 390 // multiplier  # ~390 minutes per trading day
                else:
                    estimated_bars = days_diff
                
                return {
                    "start_date": first_date,
                    "end_date": last_date,
                    "days_available": days_diff,
                    "estimated_bars": int(estimated_bars)
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"No data for {ticker} {timespan}x{multiplier}: {e}")
            return None
    
    def _infer_market_type(self, ticker: str, available_timeframes: Dict) -> str:
        """
        Infer market type based on available timeframes and ticker patterns.
        
        Args:
            ticker: Stock ticker symbol
            available_timeframes: Dict of available timeframes
            
        Returns:
            Inferred market type
        """
        # Crypto typically has 24/7 data including weekends
        # Check if we have continuous intraday data
        if "1m" in available_timeframes or "5m" in available_timeframes:
            # This could be crypto if it has weekend data
            # For now, use simple heuristics
            if "-USD" in ticker or ticker.startswith("X:"):
                return "crypto"
            elif "/" in ticker:
                return "fx"
        
        # Try to get ticker details for accurate type
        try:
            details = self.get_ticker_details(ticker)
            ticker_type = details.get('type', '')
            
            if ticker_type == 'CS':
                return "stocks"
            elif ticker_type == 'ETF':
                return "etf"
            elif ticker_type == 'CRYPTO' or ticker_type == 'FX':
                return ticker_type.lower()
            else:
                return "stocks"  # Default
        except:
            return "stocks"  # Default fallback


# Fix the import issue by importing pandas here
import pandas as pd
