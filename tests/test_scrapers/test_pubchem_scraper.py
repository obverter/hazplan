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
                    raise requests.exceptions.HTTPError(
                        f"HTTP Error: {self.status_code}"
                    )

            def json(self):
                return self.json_data

        class MockSession:
            def __init__(self):
                self.headers = {}
                self.responses = {
                    # Search response
                    "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/acetone/cids/JSON": MockResponse(
                        {"IdentifierList": {"CID": [180]}}
                    ),
                    # Basic properties response
                    "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/180/property/IUPACName,MolecularFormula,MolecularWeight/JSON": MockResponse(
                        {
                            "PropertyTable": {
                                "Properties": [
                                    {
                                        "CID": 180,
                                        "IUPACName": "propan-2-one",
                                        "MolecularFormula": "C3H6O",
                                        "MolecularWeight": 58.08,
                                    }
                                ]
                            }
                        }
                    ),
                    # Synonyms response
                    "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/180/synonyms/JSON": MockResponse(
                        {
                            "InformationList": {
                                "Information": [
                                    {
                                        "CID": 180,
                                        "Synonym": [
                                            "acetone",
                                            "propanone",
                                            "67-64-1",  # CAS number
                                        ],
                                    }
                                ]
                            }
                        }
                    ),
                    # GHS Classifications response
                    "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/180/JSON?heading=GHS+Classification": MockResponse(
                        {
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
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "TOCHeading": "Pictogram(s)",
                                                "Information": [
                                                    {
                                                        "Name": "GHS Pictogram",
                                                        "Value": {
                                                            "StringWithMarkup": [
                                                                {"String": "Flame"}
                                                            ]
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "TOCHeading": "GHS Signal Word",
                                                "Information": [
                                                    {
                                                        "Name": "Signal Word",
                                                        "Value": {
                                                            "StringWithMarkup": [
                                                                {"String": "Danger"}
                                                            ]
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    }
                                ],
                            }
                        }
                    ),
                    # Safety and Hazards response
                    "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/180/JSON?heading=Safety+and+Hazards": MockResponse(
                        {
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
                                                                {"String": "-20 °C"}
                                                            ]
                                                        },
                                                    }
                                                ],
                                            }
                                        ],
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
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "TOCHeading": "Boiling Point",
                                                "Information": [
                                                    {
                                                        "Name": "Boiling Point",
                                                        "Value": {
                                                            "StringWithMarkup": [
                                                                {"String": "56.05 °C"}
                                                            ]
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "TOCHeading": "Melting Point",
                                                "Information": [
                                                    {
                                                        "Name": "Melting Point",
                                                        "Value": {
                                                            "StringWithMarkup": [
                                                                {"String": "-94.7 °C"}
                                                            ]
                                                        },
                                                    }
                                                ],
                                            }
                                        ],
                                    },
                                ],
                            }
                        }
                    ),
                    # Full JSON view
                    "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/180/JSON": MockResponse(
                        {
                            "Record": {
                                "RecordType": "Compound",
                                "TOCHeading": "Acetone",
                                "Section": [
                                    {
                                        "TOCHeading": "Experimental Properties",
                                        "Section": [
                                            {
                                                "TOCHeading": "Density",
                                                "Information": [
                                                    {
                                                        "Name": "Density",
                                                        "Value": {
                                                            "StringWithMarkup": [
                                                                {"String": "0.79 g/cm³"}
                                                            ]
                                                        },
                                                    }
                                                ],
                                            }
                                        ],
                                    }
                                ],
                            }
                        }
                    ),
                }

            def get(self, url):
                # For property URLs with multiple properties, match the base URL
                for base_url, response in self.responses.items():
                    if url.startswith(base_url.split("property/")[0] + "property/"):
                        return response

                # Direct URL matching
                return self.responses.get(
                    url, MockResponse({"error": "Not found"}, 404)
                )

            def close(self):
                pass

        mock_session = MockSession()

        def mock_session_constructor(*args, **kwargs):
            return mock_session

        monkeypatch.setattr(requests, "Session", mock_session_constructor)

        return mock_session

    def test_extract_chemical_data_full_properties(self, mock_session):
        """Test extracting comprehensive chemical data with full properties."""
        scraper = PubChemScraper()
        data = scraper.extract_chemical_data("180")

        # Check basic properties
        assert data["cas_number"] == "67-64-1"
        assert data["name"] == "propan-2-one"
        assert data["formula"] == "C3H6O"
        assert data["molecular_weight"] == 58.08

        # Check comprehensive physical properties
        assert data["physical_state"] == "Colorless liquid"
        assert data["boiling_point"] == "56.05 °C"
        assert data["melting_point"] == "-94.7 °C"
        assert data["flash_point"] == "-20 °C"
        assert data["density"] == "0.79 g/cm³"

        # Check hazard information
        assert "H225" in data["hazard_statements"]
        assert "Flame" in data["ghs_pictograms"]
        assert data["signal_word"] == "Danger"

    def test_get_full_json_data(self, mock_session):
        """Test retrieving full JSON data for a compound."""
        scraper = PubChemScraper()
        
        # Test successful retrieval
        full_json = scraper._get_full_json_data("180")
        assert full_json is not None
        assert "Record" in full_json
        
        # Verify specific section exists
        assert any(
            section.get("TOCHeading") == "Experimental Properties" 
            for section in full_json.get("Record", {}).get("Section", [])
        )

    def test_extract_property_from_full_json(self, mock_session):
        """Test extracting specific properties from full JSON."""
        scraper = PubChemScraper()
        full_json = scraper._get_full_json_data("180")
        
        # Test successful extraction
        density = scraper._extract_property_from_full_json(
            full_json, 
            target_headings=["Experimental Properties"],
            section_types=["Density"]
        )
        assert density == "0.79 g/cm³"
        
        # Test with non-existent property
        non_existent = scraper._extract_property_from_full_json(
            full_json, 
            target_headings=["Non-existent Section"],
            section_types=["Non-existent Type"]
        )
        assert non_existent is None