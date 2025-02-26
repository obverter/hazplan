"""
Base scraper module providing core functionality for all specific scrapers.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    Abstract base class for all scrapers.

    This class provides common functionality for web scraping, including
    making requests, parsing HTML, and handling errors. Specific scrapers
    should inherit from this class and implement the abstract methods.
    """

    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None):
        """
        Initialize the scraper with the base URL and optional headers.

        Args:
            base_url: The base URL of the website to scrape
            headers: Optional HTTP headers to use in requests
        """
        self.base_url = base_url
        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_page(self, url: str) -> BeautifulSoup:
        """
        Fetch a page and return a BeautifulSoup object.

        Args:
            url: The URL to fetch

        Returns:
            BeautifulSoup object of the parsed HTML

        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return BeautifulSoup(response.text, "lxml")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            raise

    @abstractmethod
    def search_chemical(self, query: str) -> List[Dict[str, str]]:
        """
        Search for a chemical and return a list of search results.

        Args:
            query: The search query (chemical name, CAS number, etc.)

        Returns:
            List of dictionaries containing search results
        """
        pass

    @abstractmethod
    def extract_chemical_data(
        self, identifier: Union[str, Dict[str, str]]
    ) -> Dict[str, any]:
        """
        Extract detailed data for a specific chemical.

        Args:
            identifier: Chemical identifier (URL, ID, or result dict from search)

        Returns:
            Dictionary containing the extracted chemical data
        """
        pass

    def clean_text(self, text: Optional[str]) -> Optional[str]:
        """
        Clean and normalize text data.

        Args:
            text: The text to clean

        Returns:
            Cleaned text
        """
        if text is None:
            return None
        return " ".join(text.strip().split())

    def close(self):
        """Close the session and free resources."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
