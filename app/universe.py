"""
Universe discovery system for finding tickers by category.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from app.polygon_client import PolygonClient, TickerInfo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UniverseManager:
    """Manages ticker universe discovery and storage."""
    
    CATEGORY_MAPPINGS = {
        "us_equities": {"market": "stocks", "type": "CS", "locale": "us"},
        "etf": {"market": "stocks", "type": "ETF", "locale": "us"}, 
        "crypto": {"market": "crypto", "locale": "global"},
        "fx": {"market": "fx", "locale": "global"},
        "options": {"market": "options", "locale": "us"},
        "indices": {"market": "indices", "locale": "us"},
        "otc": {"market": "otc", "locale": "us"}
    }
    
    def __init__(self, polygon_client: PolygonClient, universe_dir: str = "universe"):
        self.client = polygon_client
        self.universe_dir = Path(universe_dir)
        self.universe_dir.mkdir(exist_ok=True)
    
    def _get_universe_file_path(self, category: str) -> Path:
        """Get the file path for a universe category."""
        timestamp = datetime.now().strftime("%Y%m%d")
        return self.universe_dir / f"{category}_{timestamp}.json"
    
    def _save_universe(self, category: str, tickers: List[Dict[str, Any]], append: bool = True):
        """Save discovered universe to JSON file."""
        file_path = self._get_universe_file_path(category)
        
        # Load existing tickers if append mode and file exists
        existing_tickers = []
        if append and file_path.exists():
            try:
                with open(file_path, 'r') as f:
                    existing_data = json.load(f)
                    existing_tickers = existing_data.get('tickers', [])
                    logger.info(f"Found {len(existing_tickers)} existing tickers in {file_path}")
            except Exception as e:
                logger.warning(f"Could not load existing file: {e}")
        
        # Combine and deduplicate tickers based on symbol
        all_tickers = existing_tickers + tickers
        unique_tickers = {}
        for ticker in all_tickers:
            symbol = ticker.get('ticker')
            if symbol and symbol not in unique_tickers:
                unique_tickers[symbol] = ticker
        
        # Convert back to list
        final_tickers = list(unique_tickers.values())
        new_count = len(final_tickers) - len(existing_tickers)
        
        universe_data = {
            "category": category,
            "created_at": datetime.now().isoformat(),
            "count": len(final_tickers),
            "new_tickers_added": new_count,
            "tickers": final_tickers
        }
        
        with open(file_path, 'w') as f:
            json.dump(universe_data, f, indent=2)
        
        if append and existing_tickers:
            logger.info(f"Added {new_count} new tickers to {file_path} (total: {len(final_tickers)})")
        else:
            logger.info(f"Saved {len(final_tickers)} tickers to {file_path}")
        return file_path
    
    def discover_category(self, category: str, limit: Optional[int] = None, append: bool = True) -> Dict[str, Any]:
        """
        Discover tickers for a specific category.
        
        Args:
            category: Category name (us_equities, etf, crypto, fx, etc.)
            limit: Maximum number of tickers to discover
            
        Returns:
            Dict with discovery results
        """
        if category not in self.CATEGORY_MAPPINGS:
            raise ValueError(f"Unknown category: {category}. Available: {list(self.CATEGORY_MAPPINGS.keys())}")
        
        mapping = self.CATEGORY_MAPPINGS[category]
        logger.info(f"Discovering {category} tickers with mapping: {mapping}")
        
        try:
            tickers = []
            count = 0
            
            ticker_kwargs = {
                "market": mapping["market"],
                "active": True,
                "limit": 1000  # API pagination limit
            }
            
            # Add type filter if specified
            if "type" in mapping:
                ticker_kwargs["ticker_type"] = mapping["type"]
            
            for ticker_info in self.client.get_tickers(**ticker_kwargs):
                # Additional filtering based on locale if needed
                if "locale" in mapping and mapping["locale"] != "global":
                    if hasattr(ticker_info, 'locale') and ticker_info.locale != mapping["locale"]:
                        continue
                
                ticker_data = {
                    "ticker": ticker_info.ticker,
                    "name": ticker_info.name,
                    "market": ticker_info.market,
                    "type": ticker_info.type,
                    "locale": ticker_info.locale,
                    "primary_exchange": ticker_info.primary_exchange,
                    "active": ticker_info.active,
                    "currency_name": ticker_info.currency_name,
                    "last_updated_utc": ticker_info.last_updated_utc
                }
                
                tickers.append(ticker_data)
                count += 1
                
                if limit and count >= limit:
                    logger.info(f"Reached limit of {limit} tickers")
                    break
                
                # Log progress for large discoveries
                if count % 500 == 0:
                    logger.info(f"Discovered {count} {category} tickers...")
            
            # Save the universe with append option
            file_path = self._save_universe(category, tickers, append=append)
            
            # Get the updated counts
            with open(file_path, 'r') as f:
                saved_data = json.load(f)
            
            return {
                "category": category,
                "status": "success",
                "count": saved_data.get("count", len(tickers)),
                "new_tickers_added": saved_data.get("new_tickers_added", 0),
                "file_path": str(file_path),
                "mapping_used": mapping
            }
            
        except Exception as e:
            logger.error(f"Failed to discover {category} tickers: {e}")
            return {
                "category": category,
                "status": "error",
                "error": str(e),
                "mapping_used": mapping
            }
    
    def load_universe(self, file_path: str) -> Dict[str, Any]:
        """Load a previously discovered universe."""
        try:
            with open(file_path, 'r') as f:
                universe_data = json.load(f)
            
            logger.info(f"Loaded universe with {universe_data['count']} tickers from {file_path}")
            return universe_data
            
        except Exception as e:
            logger.error(f"Failed to load universe from {file_path}: {e}")
            raise
    
    def list_available_universes(self) -> List[Dict[str, Any]]:
        """List all available universe files."""
        universes = []
        
        for file_path in self.universe_dir.glob("*.json"):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                universes.append({
                    "file_path": str(file_path),
                    "category": data.get("category", "unknown"),
                    "created_at": data.get("created_at", "unknown"),
                    "count": data.get("count", 0),
                    "file_size_mb": file_path.stat().st_size / (1024 * 1024)
                })
                
            except Exception as e:
                logger.warning(f"Could not read universe file {file_path}: {e}")
                continue
        
        # Sort by creation date (newest first)
        universes.sort(key=lambda x: x["created_at"], reverse=True)
        return universes
    
    def get_ticker_list(self, file_path: str) -> List[str]:
        """Get just the ticker symbols from a universe file."""
        universe_data = self.load_universe(file_path)
        return [ticker["ticker"] for ticker in universe_data["tickers"]]
    
    def filter_universe(
        self, 
        file_path: str, 
        exchange: Optional[str] = None,
        min_name_length: int = 1,
        exclude_test: bool = True
    ) -> List[str]:
        """
        Filter universe tickers based on criteria.
        
        Args:
            file_path: Path to universe file
            exchange: Filter by primary exchange
            min_name_length: Minimum length of ticker name
            exclude_test: Exclude test tickers (containing 'TEST', 'TEMP', etc.)
            
        Returns:
            List of filtered ticker symbols
        """
        universe_data = self.load_universe(file_path)
        filtered_tickers = []
        
        test_keywords = ["TEST", "TEMP", "DEMO"] if exclude_test else []
        
        for ticker_data in universe_data["tickers"]:
            ticker = ticker_data["ticker"]
            name = ticker_data.get("name", "")
            primary_exchange = ticker_data.get("primary_exchange", "")
            
            # Apply filters
            if exchange and primary_exchange != exchange:
                continue
            
            if len(name) < min_name_length:
                continue
            
            if exclude_test and any(keyword in ticker.upper() or keyword in name.upper() for keyword in test_keywords):
                continue
            
            filtered_tickers.append(ticker)
        
        logger.info(f"Filtered {len(universe_data['tickers'])} tickers down to {len(filtered_tickers)}")
        return filtered_tickers
    
    def discover_all_categories(self, limit_per_category: Optional[int] = 1000) -> Dict[str, Any]:
        """Discover tickers for all supported categories."""
        results = {}
        
        for category in self.CATEGORY_MAPPINGS.keys():
            logger.info(f"Discovering category: {category}")
            result = self.discover_category(category, limit_per_category)
            results[category] = result
        
        return results
