"""
Tests for the CacheManager class.
"""
import os
import shutil
import tempfile
import time
from pathlib import Path

import pytest

from src.utils.cache_manager import CacheManager


class TestCacheManager:
    """Tests for the CacheManager class."""
    
    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary directory for cache files."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        # Clean up after the test
        shutil.rmtree(temp_dir)
    
    def test_init(self, temp_cache_dir):
        """Test initialization."""
        cache = CacheManager(cache_dir=temp_cache_dir)
        assert cache.cache_dir == Path(temp_cache_dir)
        assert cache.max_age == 86400  # Default 1 day
        
        # Directory should be created
        assert os.path.exists(temp_cache_dir)
    
    def test_set_get(self, temp_cache_dir):
        """Test setting and getting cache entries."""
        cache = CacheManager(cache_dir=temp_cache_dir)
        
        # Set a cache entry
        data = {"key": "value"}
        assert cache.set("test_key", data) is True
        
        # Get the cache entry
        cached_data = cache.get("test_key")
        assert cached_data == data
        
        # Check for a non-existent key
        assert cache.get("non_existent_key") is None
    
    def test_expiration(self, temp_cache_dir):
        """Test cache expiration."""
        # Create cache with short expiration time
        cache = CacheManager(cache_dir=temp_cache_dir, max_age=1)
        
        # Set a cache entry
        data = {"key": "value"}
        assert cache.set("test_key", data) is True
        
        # Get the cache entry immediately
        assert cache.get("test_key") == data
        
        # Wait for the cache to expire
        time.sleep(1.5)
        
        # The cache entry should be expired now
        assert cache.get("test_key") is None
    
    def test_clear(self, temp_cache_dir):
        """Test clearing cache entries."""
        cache = CacheManager(cache_dir=temp_cache_dir)
        
        # Set multiple cache entries
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        
        # Clear a specific entry
        assert cache.clear("key1") is True
        assert cache.get("key1") is None
        assert cache.get("key2") == "value2"
        
        # Clear all entries
        assert cache.clear() is True
        assert cache.get("key2") is None
    
    def test_clear_expired(self, temp_cache_dir):
        """Test clearing expired cache entries."""
        # Create cache with short expiration time
        cache = CacheManager(cache_dir=temp_cache_dir, max_age=1)
        
        # Set multiple cache entries
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        
        # Wait for the cache to expire
        time.sleep(1.5)
        
        # Add another cache entry
        cache.set("key3", "value3")
        
        # Clear expired entries
        cleared = cache.clear_expired()
        assert cleared == 2
        
        # Expired entries should be gone
        assert cache.get("key1") is None
        assert cache.get("key2") is None
        
        # Non-expired entry should still be there
        assert cache.get("key3") == "value3"
    
    def test_invalid_json(self, temp_cache_dir):
        """Test handling corrupted cache files."""
        cache = CacheManager(cache_dir=temp_cache_dir)
        
        # Set a cache entry
        cache.set("test_key", "test_value")
        
        # Corrupt the cache file
        cache_file = cache._get_cache_file("test_key")
        with open(cache_file, "w") as f:
            f.write("not valid json")
        
        # Attempt to get the corrupted entry
        assert cache.get("test_key") is None