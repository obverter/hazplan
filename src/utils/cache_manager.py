"""
Cache manager for API requests.

This module provides functionality to cache API responses to reduce
redundant API calls and speed up development and testing.
"""

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class CacheManager:
    """
    Manager for caching API responses.

    This class provides methods to save and retrieve cached API responses
    to avoid redundant API calls during development and testing.
    """

    def __init__(self, cache_dir: Optional[str] = None, max_age: int = 86400):
        """
        Initialize the cache manager.

        Args:
            cache_dir: Directory to store cache files. If None, uses a
                      default directory in the project's data directory.
            max_age: Maximum age of cache entries in seconds (default: 1 day)
        """
        if cache_dir is None:
            # Get the project root directory (assuming this file is in src/utils/)
            project_root = Path(__file__).parent.parent.parent
            cache_dir = project_root / "data" / "cache"

        self.cache_dir = Path(cache_dir)
        self.max_age = max_age

        # Create the cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)
        logger.info(f"Cache initialized at {self.cache_dir}")

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get a cached response.

        Args:
            key: Cache key (typically a URL or query)

        Returns:
            Cached response or None if not found or expired
        """
        cache_file = self._get_cache_file(key)

        if not cache_file.exists():
            return None

        try:
            with open(cache_file, "r") as f:
                cached_data = json.load(f)

            # Check if the cache has expired
            if time.time() - cached_data.get("timestamp", 0) > self.max_age:
                logger.debug(f"Cache expired for key: {key}")
                return None

            logger.debug(f"Cache hit for key: {key}")
            return cached_data.get("data")
        except Exception as e:
            logger.warning(f"Error reading cache file for key {key}: {str(e)}")
            return None

    def set(self, key: str, data: Any) -> bool:
        """
        Set a cached response.

        Args:
            key: Cache key (typically a URL or query)
            data: Data to cache

        Returns:
            True if successfully cached, False otherwise
        """
        cache_file = self._get_cache_file(key)

        try:
            cached_data = {"timestamp": time.time(), "data": data}

            with open(cache_file, "w") as f:
                json.dump(cached_data, f)

            logger.debug(f"Cached data for key: {key}")
            return True
        except Exception as e:
            logger.warning(f"Error writing cache file for key {key}: {str(e)}")
            return False

    def clear(self, key: Optional[str] = None) -> bool:
        """
        Clear cache entries.

        Args:
            key: Optional specific cache key to clear. If None, clears all cache.

        Returns:
            True if successfully cleared, False otherwise
        """
        try:
            if key:
                # Clear specific cache entry
                cache_file = self._get_cache_file(key)
                if cache_file.exists():
                    os.remove(cache_file)
                    logger.info(f"Cleared cache for key: {key}")
            else:
                # Clear all cache
                for cache_file in self.cache_dir.glob("*.json"):
                    os.remove(cache_file)
                logger.info("Cleared all cache")

            return True
        except Exception as e:
            logger.warning(f"Error clearing cache: {str(e)}")
            return False

    def clear_expired(self) -> int:
        """
        Clear all expired cache entries.

        Returns:
            Number of cache entries cleared
        """
        cleared_count = 0
        try:
            for cache_file in self.cache_dir.glob("*.json"):
                try:
                    with open(cache_file, "r") as f:
                        cached_data = json.load(f)

                    # Check if the cache has expired
                    if time.time() - cached_data.get("timestamp", 0) > self.max_age:
                        os.remove(cache_file)
                        cleared_count += 1
                except Exception:
                    # If we can't read the file, consider it corrupted and remove it
                    os.remove(cache_file)
                    cleared_count += 1

            logger.info(f"Cleared {cleared_count} expired cache entries")
            return cleared_count
        except Exception as e:
            logger.warning(f"Error clearing expired cache: {str(e)}")
            return cleared_count

    def _get_cache_file(self, key: str) -> Path:
        """
        Get the cache file path for a key.

        Args:
            key: Cache key

        Returns:
            Path to the cache file
        """
        # Create a deterministic filename from the key
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{key_hash}.json"
