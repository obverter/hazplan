"""
Tests for the BaseScraper class.
"""
import pytest
import requests
from bs4 import BeautifulSoup

from src.scrapers.base_scraper import BaseScraper


# Define a concrete implementation of BaseScraper for testing
class TestScraper(BaseScraper):
    """Concrete implementation of BaseScraper for testing."""
    
    def search_chemical(self, query):
        """Implementation of abstract method."""
        return [{"name": "Test Chemical", "id": "123"}]
    
    def extract_chemical_data(self, identifier):
        """Implementation of abstract method."""
        return {"name": "Test Chemical", "formula": "C2H5OH"}


class TestBaseScraper:
    """Tests for the BaseScraper class."""
    
    def test_init(self):
        """Test initialization."""
        scraper = TestScraper("https://example.com")
        assert scraper.base_url == "https://example.com"
        assert "User-Agent" in scraper.headers
    
    def test_clean_text(self):
        """Test the clean_text method."""
        scraper = TestScraper("https://example.com")
        
        # Test with normal text
        assert scraper.clean_text("  Hello  World  ") == "Hello World"
        
        # Test with None
        assert scraper.clean_text(None) is None
        
        # Test with empty string
        assert scraper.clean_text("") == ""
    
    def test_context_manager(self):
        """Test using the scraper as a context manager."""
        with TestScraper("https://example.com") as scraper:
            assert isinstance(scraper, BaseScraper)
        # Session should be closed after exiting the context manager
    
    @pytest.mark.parametrize(
        "test_input,expected",
        [
            ("  Hello  World  ", "Hello World"),
            ("Line1\nLine2", "Line1 Line2"),
            ("Multiple    spaces", "Multiple spaces"),
            (None, None),
            ("", ""),
        ],
    )
    def test_clean_text_parametrized(self, test_input, expected):
        """Test the clean_text method with various inputs."""
        scraper = TestScraper("https://example.com")
        assert scraper.clean_text(test_input) == expected
    
    @pytest.fixture
    def mock_session(self, monkeypatch):
        """Mock the requests.Session object."""
        class MockResponse:
            def __init__(self, text, status_code=200):
                self.text = text
                self.status_code = status_code
            
            def raise_for_status(self):
                if self.status_code >= 400:
                    raise requests.exceptions.HTTPError(f"HTTP Error: {self.status_code}")
        
        class MockSession:
            def __init__(self):
                self.headers = {}
                self.get_called = False
                self.closed = False
            
            def get(self, url):
                self.get_called = True
                if "error" in url:
                    return MockResponse("", 404)
                return MockResponse("<html><body><p>Test</p></body></html>")
            
            def close(self):
                self.closed = True
        
        mock_session = MockSession()
        
        def mock_session_constructor(*args, **kwargs):
            return mock_session
        
        monkeypatch.setattr(requests, "Session", mock_session_constructor)
        
        return mock_session
    
    def test_get_page(self, mock_session):
        """Test the get_page method."""
        scraper = TestScraper("https://example.com")
        
        # Test successful request
        soup = scraper.get_page("https://example.com")
        assert isinstance(soup, BeautifulSoup)
        assert mock_session.get_called
        
        # Test error handling
        with pytest.raises(requests.exceptions.HTTPError):
            scraper.get_page("https://example.com/error")
    
    def test_close(self, mock_session):
        """Test the close method."""
        scraper = TestScraper("https://example.com")
        scraper.close()
        assert mock_session.closed