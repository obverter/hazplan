"""
Tests for the PubChemScraper class.
"""
import json

import pytest
import requests

from src.scrapers.pubchem_scraper import PubChemScraper


class TestPubChemScraper:
    """Tests for the PubChemScraper class."""
    
    @pytest.fixture
    def mock_session(self, monkeypatch):
        """Mock the requests.Session object for PubChem responses."""
        class MockResponse:
            def __init__(self, json_data, status_code=200):
                self.json_data = json_data
                self.text = json.dumps(json_data)
                self.status_code = status_code
            
            def raise_for_status(self):
                if self.status_code >= 400:
                    raise requests.exceptions.HTTPError(f"HTTP Error: {self.status_code}")
            
            def json(self):
                return self.json_data
        
        class MockSession:
            def __init__(self):
                self.headers = {}
                self.responses = {
                    # Search response
                    "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/acetone/cids/JSON": MockResponse({
                        "IdentifierList": {
                            "CID": [180]
                        }
                    }),
                    
                    # Basic properties response
                    "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/180/property/IUPACName,MolecularFormula,MolecularWeight/JSON": MockResponse({
                        "PropertyTable": {
                            "Properties": [
                                {
                                    "CID": 180,
                                    "IUPACName": "propan-2-one",
                                    "MolecularFormula": "C3H6O",
                                    "MolecularWeight": 58.08
                                }
                            ]
                        }
                    }),
                    
                    # Synonyms response
                    "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/180/synonyms/JSON": MockResponse({
                        "InformationList": {
                            "Information": [
                                {
                                    "CID": 180,
                                    "Synonym": [
                                        "acetone", 
                                        "propanone", 
                                        "67-64-1"  # CAS number
                                    ]
                                }
                            ]
                        }
                    }),
                    
                    # GHS Classifications response
                    "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/180/JSON?heading=GHS+Classification": MockResponse({
                        "Record": {
                            "RecordType": "Compound",
                            "Section": [
                                {
                                    "TOCHeading": "GHS Classification",
                                    "Section": [
                                        {
                                            "TOCHeading": "GHS Hazard Statements",
                                            "Information": [
                                                {
                                                    "Name": "GHS Hazard Statements",
                                                    "Value": {
                                                        "StringWithMarkup": [
                                                            {
                                                                "String": "H225: Highly flammable liquid and vapour"
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        },
                                        {
                                            "TOCHeading": "Pictogram(s)",
                                            "Information": [
                                                {
                                                    "Name": "GHS Pictogram",
                                                    "Value": {
                                                        "StringWithMarkup": [
                                                            {
                                                                "String": "Flame"
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        },
                                        {
                                            "TOCHeading": "GHS Signal Word",
                                            "Information": [
                                                {
                                                    "Name": "Signal Word",
                                                    "Value": {
                                                        "StringWithMarkup": [
                                                            {
                                                                "String": "Danger"
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    }),
                    
                    # Safety and Hazards response
                    "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/180/JSON?heading=Safety+and+Hazards": MockResponse({
                        "Record": {
                            "RecordType": "Compound",
                            "Section": [
                                {
                                    "TOCHeading": "Safety and Hazards",
                                    "Section": [
                                        {
                                            "TOCHeading": "Flash Point",
                                            "Information": [
                                                {
                                                    "Name": "Flash Point",
                                                    "Value": {
                                                        "StringWithMarkup": [
                                                            {
                                                                "String": "-20 °C"
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                },
                                {
                                    "TOCHeading": "Experimental Properties",
                                    "Section": [
                                        {
                                            "TOCHeading": "Physical Description",
                                            "Information": [
                                                {
                                                    "Name": "Physical Description",
                                                    "Value": {
                                                        "StringWithMarkup": [
                                                            {
                                                                "String": "Colorless liquid"
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        },
                                        {
                                            "TOCHeading": "Boiling Point",
                                            "Information": [
                                                {
                                                    "Name": "Boiling Point",
                                                    "Value": {
                                                        "StringWithMarkup": [
                                                            {
                                                                "String": "56.05 °C"
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    })
                }
            
            def get(self, url):
                # For property URLs with multiple properties, match the base URL
                for base_url, response in self.responses.items():
                    if url.startswith(base_url.split('property/')[0] + 'property/'):
                        return response
                
                # Direct URL matching
                return self.responses.get(url, MockResponse({"error": "Not found"}, 404))
            
            def close(self):
                pass
        
        mock_session = MockSession()
        
        def mock_session_constructor(*args, **kwargs):
            return mock_session
        
        monkeypatch.setattr(requests, "Session", mock_session_constructor)
        
        return mock_session
    
    def test_init(self):
        """Test initialization."""
        scraper = PubChemScraper()
        assert scraper.base_url == "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    
    def test_search_chemical(self, mock_session):
        """Test searching for a chemical."""
        scraper = PubChemScraper()
        results = scraper.search_chemical("acetone")
        
        assert len(results) > 0
        assert results[0]["cid"] == 180
        assert results[0]["name"] == "propan-2-one"
        assert results[0]["formula"] == "C3H6O"
        assert float(results[0]["molecular_weight"]) == 58.08
    
    def test_extract_chemical_data(self, mock_session):
        """Test extracting detailed chemical data."""
        scraper = PubChemScraper()
        data = scraper.extract_chemical_data("180")
        
        # Check basic properties
        assert data["cas_number"] == "67-64-1"
        assert data["name"] == "propan-2-one"
        assert data["formula"] == "C3H6O"
        assert data["molecular_weight"] == 58.08
        
        # Check physical properties
        assert data["physical_state"] == "Colorless liquid"
        assert data["boiling_point"] == "56.05 °C"
        assert data["flash_point"] == "-20 °C"
        
        # Check hazard information
        assert "H225" in data["hazard_statements"]
        assert "Flame" in data["ghs_pictograms"]
        assert data["signal_word"] == "Danger"
    
    def test_get_cas_number(self, mock_session):
        """Test extracting CAS number."""
        scraper = PubChemScraper()
        cas_number = scraper._get_cas_number("180")
        assert cas_number == "67-64-1"
    
    def test_get_properties(self, mock_session):
        """Test getting basic properties."""
        scraper = PubChemScraper()
        props = scraper._get_properties("180")
        
        assert props["IUPACName"] == "propan-2-one"
        assert props["MolecularFormula"] == "C3H6O"
        assert props["MolecularWeight"] == 58.08