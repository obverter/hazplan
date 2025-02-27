"""
PubChem scraper module for retrieving comprehensive chemical data from PubChem.

This module uses the PubChem PUG REST API and the PUG View JSON endpoint 
to retrieve extensive chemical information.
"""

import json
import logging
import time
import traceback
from typing import Dict, List, Optional, Union

import requests

from src.scrapers.base_scraper import BaseScraper
from src.utils.cache_manager import CacheManager
from src.utils.helpers import (
    extract_hazard_codes,
    extract_precautionary_codes,
    parse_cas_number,
    parse_physical_property,
    validate_chemical_data,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class PubChemScraper(BaseScraper):
    """
    Scraper for retrieving comprehensive chemical data from PubChem.

    Uses the PubChem PUG REST API and PUG View JSON to comprehensively 
    retrieve chemical properties.
    """

    def __init__(self, use_cache: bool = True, cache_max_age: int = 86400):
        """
        Initialize the PubChem scraper.

        Args:
            use_cache: Whether to use caching for API requests
            cache_max_age: Maximum age for cached responses in seconds (default: 1 day)
        """
        super().__init__(base_url="https://pubchem.ncbi.nlm.nih.gov/rest/pug")
        self.search_url = (
            "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{}/cids/JSON"
        )
        self.properties_url = (
            "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{}/property/{}/JSON"
        )
        self.full_json_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{}/JSON"
        self.ghs_classifications_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{}/JSON?heading=GHS+Classification"
        self.hazards_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{}/JSON?heading=Safety+and+Hazards"

        # Set up caching
        self.use_cache = use_cache
        if use_cache:
            self.cache = CacheManager(max_age=cache_max_age)

        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 2  # seconds

        # Properties to retrieve from PubChem
        self.basic_properties = ",".join([
            "IUPACName",
            "MolecularFormula",
            "MolecularWeight",
            "CanonicalSMILES",
            "IsomericSMILES",
            "InChI",
            "InChIKey",
            "XLogP",
            "ExactMass",
            "MonoisotopicMass",
            "TPSA",
            "Complexity",
            "Charge",
            "HBondDonorCount",
            "HBondAcceptorCount",
            "RotatableBondCount",
            "HeavyAtomCount",
        ])

    def _api_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make an API request with caching and retry functionality.

        Args:
            url: API URL
            params: Optional parameters for the request

        Returns:
            JSON response as a dictionary, or None if request failed
        """
        cache_key = url
        if params:
            cache_key += json.dumps(params, sort_keys=True)

        # Try to get from cache first
        if self.use_cache:
            cached_data = self.cache.get(cache_key)
            if cached_data:
                logger.debug(f"Using cached response for: {url}")
                return cached_data

        # Make the API request with retries
        for attempt in range(1, self.max_retries + 1):
            try:
                # Use the session from the parent BaseScraper class
                if params:
                    response = self.session.get(url, params=params)
                else:
                    response = self.session.get(url)

                response.raise_for_status()
                data = response.json()

                # Cache the response
                if self.use_cache:
                    self.cache.set(cache_key, data)

                return data

            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:  # Too Many Requests
                    # Exponential backoff
                    sleep_time = self.retry_delay * (2 ** (attempt - 1))
                    logger.warning(f"Rate limited. Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                elif response.status_code == 404:  # Not Found
                    logger.warning(f"Resource not found: {url}")
                    return None
                elif attempt < self.max_retries:
                    logger.warning(
                        f"HTTP error {e}. Retrying ({attempt}/{self.max_retries})..."
                    )
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"HTTP error after {self.max_retries} attempts: {e}")
                    return None

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries:
                    logger.warning(
                        f"Request error: {e}. Retrying ({attempt}/{self.max_retries})..."
                    )
                    time.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"Request error after {self.max_retries} attempts: {e}"
                    )
                    return None

            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON response: {e}")
                return None

            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                logger.debug(traceback.format_exc())
                return None

        return None

    def _get_full_json_data(self, cid: str) -> Optional[Dict]:
        """
        Retrieve the full JSON data for a compound by CID.

        Args:
            cid: PubChem Compound ID

        Returns:
            Full JSON data or None if retrieval fails
        """
        try:
            # Check cache first
            if self.use_cache:
                cached_data = self.cache.get(f"full_json_{cid}")
                if cached_data:
                    return cached_data

            # Fetch the full JSON data
            url = self.full_json_url.format(cid)
            data = self._api_request(url)

            # Cache the response
            if self.use_cache and data:
                self.cache.set(f"full_json_{cid}", data)

            return data
        except Exception as e:
            logger.error(f"Error retrieving full JSON for CID {cid}: {str(e)}")
            logger.debug(traceback.format_exc())
            return None

    def _extract_toxicity_data(self, full_json: Dict) -> Dict[str, Optional[str]]:
        """
        Extract toxicity data from the full JSON view.

        Args:
            full_json: Full compound JSON data

        Returns:
            Dictionary containing toxicity information
        """
        # Add this at the beginning of the method
        toxicity_sections = [
            section.get('TOCHeading', '') 
            for section in full_json.get('Record', {}).get('Section', [])
        ]
        print("Toxicity-related sections found:", 
            [s for s in toxicity_sections if any(keyword in s.lower() for keyword in ['toxicity', 'acute', 'health'])])
        # Print the entire structure of the JSON
        with open('full_ethanol_json_structure.txt', 'w') as f:
            def print_structure(obj, indent=0):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        f.write('  ' * indent + str(k) + ':\n')
                        print_structure(v, indent + 1)
                elif isinstance(obj, list):
                    for item in obj:
                        f.write('  ' * indent + '- ')
                        print_structure(item, indent + 1)
                else:
                    f.write('  ' * indent + str(obj) + '\n')
            
            print_structure(full_json)

        toxicity_data = {
            "lc50": None,
            "ld50": None,
            "acute_toxicity_notes": None
        }

        if not full_json or 'Record' not in full_json:
            return toxicity_data

        try:
            # Search all sections recursively
            def search_sections(sections):
                toxicity_info = []
                
                # Extensive list of LD50 and LC50 search keywords
                ld50_keywords = [
                    'LD50', 'lethal dose', 'median lethal dose', 
                    'acute toxicity', 'toxic dose', 'lethal concentration'
                ]
                lc50_keywords = [
                    'LC50', 'lethal concentration', 'median lethal concentration', 
                    'acute inhalation toxicity'
                ]
                
                for section in sections:
                    # Log extremely detailed information about each section
                    print(f"SECTION: {section.get('TOCHeading', 'Unknown')}")
                    print(f"SECTION KEYS: {section.keys()}")
                    
                    # Check information subsections
                    if 'Information' in section:
                        for info in section['Information']:
                            print(f"INFO: {info}")
                            
                            if 'Value' in info and 'StringWithMarkup' in info['Value']:
                                for markup in info['Value']['StringWithMarkup']:
                                    if 'String' in markup:
                                        toxicity_string = markup['String']
                                        print(f"TOXICITY STRING: {toxicity_string}")
                                        
                                        toxicity_info.append(toxicity_string)
                                        
                                        # Look for LD50
                                        if any(keyword.lower() in toxicity_string.lower() for keyword in ld50_keywords):
                                            if not toxicity_data['ld50']:
                                                toxicity_data['ld50'] = toxicity_string
                                                print(f"FOUND LD50: {toxicity_string}")
                                        
                                        # Look for LC50
                                        if any(keyword.lower() in toxicity_string.lower() for keyword in lc50_keywords):
                                            if not toxicity_data['lc50']:
                                                toxicity_data['lc50'] = toxicity_string
                                                print(f"FOUND LC50: {toxicity_string}")
                    
                    # Recursively search subsections
                    if 'Section' in section:
                        sub_toxicity = search_sections(section['Section'])
                        toxicity_info.extend(sub_toxicity)
                
                return toxicity_info

            # Sections to search for toxicity data
            toxicity_sections = [
                'Toxicity Data', 
                'Acute Toxicity', 
                'Toxicological Information', 
                'Acute Effects', 
                'Toxicity',
                'Health Hazards',
                'Acute Exposure',
                'Toxicological Effects',
                'Chronic Toxicity',
                'Toxicity Summary'
            ]

            # Perform the search
            toxicity_sections_found = [
                section for section in full_json['Record'].get('Section', []) 
                if section.get('TOCHeading') in toxicity_sections
            ]
            
            # Print found sections
            print("FOUND TOXICITY SECTIONS:")
            for section in toxicity_sections_found:
                print(section.get('TOCHeading'))
            
            # Extract toxicity information
            toxicity_info = search_sections(toxicity_sections_found)
            
            # Combine toxicity information
            if toxicity_info:
                toxicity_data['acute_toxicity_notes'] = '; '.join(toxicity_info)

            # Print extracted data
            print(f"FINAL TOXICITY DATA: {toxicity_data}")

            return toxicity_data
        
        except Exception as e:
            print(f"ERROR EXTRACTING TOXICITY DATA: {str(e)}")
            print(traceback.format_exc())
            return toxicity_data

    def search_chemical(self, query: str) -> List[Dict[str, str]]:
        """
        Search for a chemical by name or identifier and return a list of results.

        Args:
            query: Chemical name or identifier to search for

        Returns:
            List of dictionaries containing search results
        """
        try:
            # First check if query is a CAS number
            cas_number = parse_cas_number(query)
            if cas_number:
                # Search by CAS number using the synonym search
                logger.info(f"Detected CAS number: {cas_number}")
                url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{cas_number}/cids/JSON"
            else:
                # Search by name
                url = self.search_url.format(query)

            data = self._api_request(url)

            if not data or "IdentifierList" not in data:
                logger.warning(f"No results found for query: {query}")
                return []

            cids = data["IdentifierList"]["CID"]

            # Get basic info for each CID
            results = []
            for cid in cids[:5]:  # Limit to first 5 results for efficiency
                # Get basic properties for each compound
                time.sleep(0.2)  # Be nice to the API
                props = self._get_properties(cid)
                if props:
                    result = {
                        "cid": cid,
                        "name": props.get("IUPACName", "Unknown"),
                        "formula": props.get("MolecularFormula", ""),
                        "molecular_weight": props.get("MolecularWeight", ""),
                    }
                    results.append(result)

            return results
        except Exception as e:
            logger.error(f"Error searching for chemical '{query}': {str(e)}")
            logger.debug(traceback.format_exc())
            return []

    def extract_chemical_data(
        self, identifier: Union[str, Dict[str, str]]
    ) -> Dict[str, any]:
        """
        Extract detailed data for a specific chemical.

        Args:
            identifier: Either a PubChem CID (str) or a result dictionary from search_chemical()

        Returns:
            Dictionary containing the extracted chemical data
        """
        # Get the CID
        if isinstance(identifier, dict):
            cid = identifier.get("cid")
        else:
            cid = identifier

        if not cid:
            logger.error("No CID provided for chemical data extraction")
            return {}

        try:
            # Initialize toxicity data
            toxicity_data = {
                "lc50": None,
                "ld50": None,
                "acute_toxicity_notes": None
            }

            # Get basic properties
            props = self._get_properties(cid)
            if not props:
                return {}

            # Get CAS number
            cas_number = self._get_cas_number(cid)

            # Get GHS classifications
            ghs_data = self._get_ghs_data(cid)

            # Get hazards information
            hazards_data = self._get_hazards_data(cid)

            # Get full JSON data for additional information
            full_json = self._get_full_json_data(cid)
            
            # Log the full JSON data for debugging
            if full_json:
                with open(f"full_json_{cid}.json", "w") as f:
                    json.dump(full_json, f, indent=2)
            
            # Extract toxicity data
            if full_json:
                toxicity_data = self._extract_toxicity_data(full_json)
                
                # Log extracted toxicity data
                logger.info(f"Extracted toxicity data for {cid}: {toxicity_data}")

            # Combine all data
            chemical_data = {
                "cas_number": cas_number,
                "name": props.get("IUPACName", ""),
                "formula": props.get("MolecularFormula", ""),
                "molecular_weight": (
                    float(props.get("MolecularWeight", 0))
                    if props.get("MolecularWeight")
                    else None
                ),
                "canonical_smiles": props.get("CanonicalSMILES", ""),
                "isomeric_smiles": props.get("IsomericSMILES", ""),
                "inchi": props.get("InChI", ""),
                "inchikey": props.get("InChIKey", ""),
                "xlogp": (
                    float(props.get("XLogP"))
                    if props.get("XLogP") is not None
                    else None
                ),
                "exact_mass": (
                    float(props.get("ExactMass")) if props.get("ExactMass") else None
                ),
                "monoisotopic_mass": (
                    float(props.get("MonoisotopicMass"))
                    if props.get("MonoisotopicMass")
                    else None
                ),
                "tpsa": float(props.get("TPSA")) if props.get("TPSA") else None,
                "complexity": (
                    float(props.get("Complexity")) if props.get("Complexity") else None
                ),
                "charge": (
                    int(props.get("Charge"))
                    if props.get("Charge") is not None
                    else None
                ),
                "h_bond_donor_count": (
                    int(props.get("HBondDonorCount"))
                    if props.get("HBondDonorCount") is not None
                    else None
                ),
                "h_bond_acceptor_count": (
                    int(props.get("HBondAcceptorCount"))
                    if props.get("HBondAcceptorCount") is not None
                    else None
                ),
                "rotatable_bond_count": (
                    int(props.get("RotatableBondCount"))
                    if props.get("RotatableBondCount") is not None
                    else None
                ),
                "heavy_atom_count": (
                    int(props.get("HeavyAtomCount"))
                    if props.get("HeavyAtomCount") is not None
                    else None
                ),
                "physical_state": hazards_data.get("physical_state", ""),
                "color": hazards_data.get("color", ""),
                "density": hazards_data.get("density", ""),
                "melting_point": hazards_data.get("melting_point", ""),
                "boiling_point": hazards_data.get("boiling_point", ""),
                "flash_point": hazards_data.get("flash_point", ""),
                "solubility": hazards_data.get("solubility", ""),
                "vapor_pressure": hazards_data.get("vapor_pressure", ""),
                "hazard_statements": ghs_data.get("hazard_statements", ""),
                "precautionary_statements": ghs_data.get(
                    "precautionary_statements", ""
                ),
                "ghs_pictograms": ghs_data.get("pictograms", ""),
                "signal_word": ghs_data.get("signal_word", ""),
                "source_url": f"https://pubchem.ncbi.nlm.nih.gov/compound/{cid}",
                "source_name": "PubChem",
                
                # Add toxicity data
                "lc50": toxicity_data.get("lc50"),
                "ld50": toxicity_data.get("ld50"),
                "acute_toxicity_notes": toxicity_data.get("acute_toxicity_notes"),
            }

            # Extract structured hazard data
            if chemical_data["hazard_statements"]:
                chemical_data["hazard_codes"] = extract_hazard_codes(
                    chemical_data["hazard_statements"]
                )

            if chemical_data["precautionary_statements"]:
                chemical_data["precautionary_codes"] = extract_precautionary_codes(
                    chemical_data["precautionary_statements"]
                )

            # Parse physical properties to extract numeric values and units
            for prop in [
                "density",
                "melting_point",
                "boiling_point",
                "flash_point",
                "vapor_pressure",
            ]:
                if chemical_data.get(prop):
                    value, unit = parse_physical_property(chemical_data[prop])
                    if value is not None:
                        chemical_data[f"{prop}_value"] = value
                        chemical_data[f"{prop}_unit"] = unit

            # Validate the chemical data
            is_valid, errors = validate_chemical_data(chemical_data)
            if not is_valid:
                logger.warning(f"Validation errors for CID {cid}: {', '.join(errors)}")

            return chemical_data
        except Exception as e:
            logger.error(f"Error extracting data for CID {cid}: {str(e)}")
            logger.debug(traceback.format_exc())
            return {}

    def _get_properties(self, cid: str) -> Dict[str, str]:
        """
        Get basic properties for a compound by CID.

        Args:
            cid: PubChem Compound ID

        Returns:
            Dictionary of properties
        """
        url = self.properties_url.format(cid, self.basic_properties)
        data = self._api_request(url)

        if not data or "PropertyTable" not in data:
            return {}

        try:
            props = data["PropertyTable"]["Properties"][0]
            return props
        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing properties for CID {cid}: {str(e)}")
            return {}

    def _get_cas_number(self, cid: str) -> Optional[str]:
        """
        Get CAS registry number for a compound by CID.

        Args:
            cid: PubChem Compound ID

        Returns:
            CAS registry number or None
        """
        synonyms_url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{cid}/synonyms/JSON"
        data = self._api_request(synonyms_url)

        if not data or "InformationList" not in data:
            return None

        try:
            synonyms = data["InformationList"]["Information"][0].get("Synonym", [])

            # Look for CAS number pattern and validate each potential CAS number
            for synonym in synonyms:
                cas_number = parse_cas_number(synonym)
                if cas_number:
                    return cas_number

            return None
        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing synonyms for CID {cid}: {str(e)}")
            return None

    def _get_ghs_data(self, cid: str) -> Dict[str, any]:
        """
        Get GHS classification data for a compound by CID.

        Args:
            cid: PubChem Compound ID

        Returns:
            Dictionary containing GHS classifications
        """
        # Attempt to get data from the specific GHS classification URL
        url = self.ghs_classifications_url.format(cid)
        data = self._api_request(url)

        result = {
            "hazard_statements": "",
            "precautionary_statements": "",
            "pictograms": "",
            "signal_word": "",
        }

        if not data or "Record" not in data or "Section" not in data["Record"]:
            # Try full JSON view as a fallback
            full_json = self._get_full_json_data(cid)
            if not full_json or "Record" not in full_json:
                return result

            data = full_json

        try:
            sections = data["Record"].get("Section", [])

            for section in sections:
                if (
                    "TOCHeading" in section
                    and section["TOCHeading"] == "GHS Classification"
                ):
                    if "Section" not in section:
                        continue

                    for subsection in section["Section"]:
                        if "TOCHeading" not in subsection:
                            continue

                        heading = subsection["TOCHeading"]

                        if heading == "GHS Hazard Statements":
                            statements = []
                            if "Information" in subsection:
                                for info in subsection["Information"]:
                                    if (
                                        "Value" in info
                                        and "StringWithMarkup" in info["Value"]
                                    ):
                                        for markup in info["Value"]["StringWithMarkup"]:
                                            if "String" in markup:
                                                statements.append(markup["String"])
                            result["hazard_statements"] = "; ".join(statements)

                        elif heading == "Precautionary Statement Codes":
                            statements = []
                            if "Information" in subsection:
                                for info in subsection["Information"]:
                                    if (
                                        "Value" in info
                                        and "StringWithMarkup" in info["Value"]
                                    ):
                                        for markup in info["Value"]["StringWithMarkup"]:
                                            if "String" in markup:
                                                statements.append(markup["String"])
                            result["precautionary_statements"] = "; ".join(statements)

                        elif heading == "Pictogram(s)":
                            pictograms = []
                            if "Information" in subsection:
                                for info in subsection["Information"]:
                                    if (
                                        "Value" in info
                                        and "StringWithMarkup" in info["Value"]
                                    ):
                                        for markup in info["Value"]["StringWithMarkup"]:
                                            if "String" in markup:
                                                pictograms.append(markup["String"])
                            result["pictograms"] = "; ".join(pictograms)

                        elif heading == "GHS Signal Word":
                            if (
                                "Information" in subsection
                                and subsection["Information"]
                            ):
                                info = subsection["Information"][0]
                                if (
                                    "Value" in info
                                    and "StringWithMarkup" in info["Value"]
                                ):
                                    markup = info["Value"]["StringWithMarkup"][0]
                                    if "String" in markup:
                                        result["signal_word"] = markup["String"]

            return result
        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing GHS data for CID {cid}: {str(e)}")
            return result

    def _get_hazards_data(self, cid: str) -> Dict[str, str]:
        """
        Get physical properties and hazard data for a compound by CID.

        Args:
            cid: PubChem Compound ID

        Returns:
            Dictionary containing physical properties and hazard data
        """
        # Default result dictionary
        result = {
            "physical_state": "",
            "color": "",
            "density": "",
            "melting_point": "",
            "boiling_point": "",
            "flash_point": "",
            "solubility": "",
            "vapor_pressure": "",
        }

        # Get full JSON data
        full_json = self._get_full_json_data(cid)
        
        if not full_json or 'Record' not in full_json:
            return result

        try:
            # Comprehensive property search
            sections = full_json.get('Record', {}).get('Section', [])
            
            # Recursively search through all sections
            def search_sections(sections):
                properties = {}
                for section in sections:
                    # Log the section for debugging
                    logger.info(f"Examining section: {section.get('TOCHeading', 'Unknown')}")
                    
                    # Check for specific headings
                    headings_map = {
                        "Physical Description": "physical_state",
                        "Color/Form": "color",
                        "Density": "density",
                        "Melting Point": "melting_point", 
                        "Boiling Point": "boiling_point",
                        "Flash Point": "flash_point",
                        "Solubility": "solubility",
                        "Vapor Pressure": "vapor_pressure"
                    }
                    
                    # Check current section
                    section_heading = section.get('TOCHeading', '')
                    if section_heading in headings_map:
                        if 'Information' in section:
                            for info in section['Information']:
                                if 'Value' in info and 'StringWithMarkup' in info['Value']:
                                    for markup in info['Value']['StringWithMarkup']:
                                        if 'String' in markup:
                                            prop_name = headings_map[section_heading]
                                            properties[prop_name] = markup['String']
                    
                    # Recursively search subsections
                    if 'Section' in section:
                        sub_properties = search_sections(section['Section'])
                        properties.update(sub_properties)
                
                return properties

            # Perform the search
            extracted_properties = search_sections(sections)
            
            # Update the result dictionary
            for key, value in extracted_properties.items():
                if value:
                    result[key] = value

            return result
        except Exception as e:
            logger.error(f"Error parsing hazards data for CID {cid}: {str(e)}")
            logger.debug(traceback.format_exc())
            return result

    def _extract_property_from_full_json(
        self, full_json: Dict, target_headings: List[str], 
        section_types: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Extract a specific property from the full JSON data.

        Args:
            full_json: Full compound JSON data
            target_headings: List of target section headings to search
            section_types: Optional list of section types to search

        Returns:
            Extracted property value or None
        """
        if not full_json or 'Record' not in full_json:
            return None

        sections = full_json['Record'].get('Section', [])
        
        logger.info(f"Searching for properties. Target Headings: {target_headings}, Section Types: {section_types}")
        
        for section in sections:
            # Log section details for debugging
            section_heading = section.get('TOCHeading', '')
            section_type = section.get('RecordType', '')
            logger.info(f"Examining section: Heading '{section_heading}', Type '{section_type}'")

            # Check section type if specified
            if (section_types and 
                section_heading not in section_types and 
                section_type not in section_types):
                continue

            # Check section heading
            if section_heading in target_headings:
                # Dive into subsections to find information
                sub_sections = section.get('Section', [])
                for sub_section in sub_sections:
                    # Log subsection details
                    sub_heading = sub_section.get('TOCHeading', '')
                    logger.info(f"Examining subsection: Heading '{sub_heading}'")

                    # Look for specific information
                    if 'Information' in sub_section:
                        for info in sub_section['Information']:
                            if 'Value' in info and 'StringWithMarkup' in info['Value']:
                                # Extract the first string value
                                for markup in info['Value']['StringWithMarkup']:
                                    if 'String' in markup:
                                        logger.info(f"Found property value: {markup['String']}")
                                        return markup['String']

        logger.warning(f"No property found for headings {target_headings}")
        return None

        return None

    def close(self):
        """Close the session and free resources."""
        super().close()

        # Clear expired cache entries if caching is enabled
        if self.use_cache:
            self.cache.clear_expired()

    def clear_cache(self, key: Optional[str] = None):
        """
        Clear the cache.

        Args:
            key: Optional specific cache key to clear. If None, clears all cache.
        """
        if self.use_cache:
            self.cache.clear(key)
            logger.info("Cache cleared")