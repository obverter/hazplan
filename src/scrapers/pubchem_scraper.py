"""
PubChem scraper module for retrieving chemical data from PubChem.

This module uses the PubChem PUG REST API rather than scraping the website directly,
which is more efficient and follows best practices for data retrieval.
"""

import json
import logging
import time
from typing import Dict, List, Optional, Union

import requests

from src.scrapers.base_scraper import BaseScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class PubChemScraper(BaseScraper):
    """
    Scraper for retrieving chemical data from PubChem.

    Uses the PubChem PUG REST API to search for chemicals and retrieve
    their properties. See documentation at:
    https://pubchemdocs.ncbi.nlm.nih.gov/pug-rest
    """

    def __init__(self):
        """Initialize the PubChem scraper."""
        super().__init__(base_url="https://pubchem.ncbi.nlm.nih.gov/rest/pug")
        self.search_url = (
            "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{}/cids/JSON"
        )
        self.properties_url = (
            "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{}/property/{}/JSON"
        )
        self.record_url = (
            "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{}/JSON"
        )
        self.ghs_classifications_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{}/JSON?heading=GHS+Classification"
        self.hazards_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{}/JSON?heading=Safety+and+Hazards"

        # Properties to retrieve from PubChem
        self.basic_properties = ",".join(
            [
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
                "AtomStereoCount",
                "DefinedAtomStereoCount",
                "UndefinedAtomStereoCount",
                "BondStereoCount",
                "DefinedBondStereoCount",
                "UndefinedBondStereoCount",
                "CovalentUnitCount",
                "Volume3D",
                "XStericQuadrupole3D",
                "YStericQuadrupole3D",
                "ZStericQuadrupole3D",
                "FeatureCount3D",
                "FeatureAcceptorCount3D",
                "FeatureDonorCount3D",
                "FeatureAnionCount3D",
                "FeatureCationCount3D",
                "FeatureRingCount3D",
                "FeatureHydrophobeCount3D",
                "ConformerModelRMSD3D",
                "EffectiveRotorCount3D",
                "ConformerCount3D",
            ]
        )

    def search_chemical(self, query: str) -> List[Dict[str, str]]:
        """
        Search for a chemical by name or identifier and return a list of results.

        Args:
            query: Chemical name or identifier to search for

        Returns:
            List of dictionaries containing search results
        """
        try:
            url = self.search_url.format(query)
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()

            if "IdentifierList" not in data:
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
        except requests.exceptions.RequestException as e:
            logger.error(f"Error searching for chemical '{query}': {str(e)}")
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

            # Combine all data
            chemical_data = {
                "cas_number": cas_number,
                "name": props.get("IUPACName", ""),
                "formula": props.get("MolecularFormula", ""),
                "molecular_weight": float(props.get("MolecularWeight", 0)),
                "canonical_smiles": props.get("CanonicalSMILES", ""),
                "isomeric_smiles": props.get("IsomericSMILES", ""),
                "inchi": props.get("InChI", ""),
                "inchikey": props.get("InChIKey", ""),
                "xlogp": props.get("XLogP", ""),
                "exact_mass": props.get("ExactMass", ""),
                "monoisotopic_mass": props.get("MonoisotopicMass", ""),
                "tpsa": props.get("TPSA", ""),
                "complexity": props.get("Complexity", ""),
                "charge": props.get("Charge", ""),
                "h_bond_donor_count": props.get("HBondDonorCount", ""),
                "h_bond_acceptor_count": props.get("HBondAcceptorCount", ""),
                "rotatable_bond_count": props.get("RotatableBondCount", ""),
                "heavy_atom_count": props.get("HeavyAtomCount", ""),
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
            }

            return chemical_data
        except Exception as e:
            logger.error(f"Error extracting data for CID {cid}: {str(e)}")
            return {}

    def _get_properties(self, cid: str) -> Dict[str, str]:
        """
        Get basic properties for a compound by CID.

        Args:
            cid: PubChem Compound ID

        Returns:
            Dictionary of properties
        """
        try:
            url = self.properties_url.format(cid, self.basic_properties)
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()

            if "PropertyTable" not in data:
                return {}

            props = data["PropertyTable"]["Properties"][0]
            return props
        except Exception as e:
            logger.error(f"Error getting properties for CID {cid}: {str(e)}")
            return {}

    def _get_cas_number(self, cid: str) -> Optional[str]:
        """
        Get CAS registry number for a compound by CID.

        Args:
            cid: PubChem Compound ID

        Returns:
            CAS registry number or None
        """
        try:
            synonyms_url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{cid}/synonyms/JSON"
            response = self.session.get(synonyms_url)
            response.raise_for_status()
            data = response.json()

            if "InformationList" not in data:
                return None

            synonyms = data["InformationList"]["Information"][0].get("Synonym", [])

            # Look for CAS number pattern (numbers separated by hyphens)
            for synonym in synonyms:
                parts = synonym.split("-")
                if len(parts) == 3 and all(p.isdigit() for p in parts):
                    return synonym

            return None
        except Exception as e:
            logger.error(f"Error getting CAS number for CID {cid}: {str(e)}")
            return None

    def _get_ghs_data(self, cid: str) -> Dict[str, any]:
        """
        Get GHS classification data for a compound by CID.

        Args:
            cid: PubChem Compound ID

        Returns:
            Dictionary containing GHS classifications
        """
        try:
            url = self.ghs_classifications_url.format(cid)
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()

            result = {
                "hazard_statements": "",
                "precautionary_statements": "",
                "pictograms": "",
                "signal_word": "",
            }

            if "Record" not in data or "Section" not in data["Record"]:
                return result

            sections = data["Record"]["Section"]

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
        except Exception as e:
            logger.error(f"Error getting GHS data for CID {cid}: {str(e)}")
            return {
                "hazard_statements": "",
                "precautionary_statements": "",
                "pictograms": "",
                "signal_word": "",
            }

    def _get_hazards_data(self, cid: str) -> Dict[str, str]:
        """
        Get physical properties and hazard data for a compound by CID.

        Args:
            cid: PubChem Compound ID

        Returns:
            Dictionary containing physical properties and hazard data
        """
        try:
            url = self.hazards_url.format(cid)
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()

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

            if "Record" not in data or "Section" not in data["Record"]:
                return result

            # Function to extract property value
            def extract_property(section_name, property_name):
                for section in data["Record"]["Section"]:
                    if (
                        "TOCHeading" in section
                        and section["TOCHeading"] == section_name
                    ):
                        if "Section" not in section:
                            continue

                        for subsection in section["Section"]:
                            if (
                                "TOCHeading" in subsection
                                and subsection["TOCHeading"] == property_name
                            ):
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
                                            return markup["String"]
                return ""

            # Extract physical properties
            result["physical_state"] = extract_property(
                "Experimental Properties", "Physical Description"
            )
            result["color"] = extract_property("Experimental Properties", "Color/Form")
            result["density"] = extract_property("Experimental Properties", "Density")
            result["melting_point"] = extract_property(
                "Experimental Properties", "Melting Point"
            )
            result["boiling_point"] = extract_property(
                "Experimental Properties", "Boiling Point"
            )
            result["flash_point"] = extract_property(
                "Safety and Hazards", "Flash Point"
            )
            result["solubility"] = extract_property(
                "Experimental Properties", "Solubility"
            )
            result["vapor_pressure"] = extract_property(
                "Experimental Properties", "Vapor Pressure"
            )

            return result
        except Exception as e:
            logger.error(f"Error getting hazards data for CID {cid}: {str(e)}")
            return {
                "physical_state": "",
                "color": "",
                "density": "",
                "melting_point": "",
                "boiling_point": "",
                "flash_point": "",
                "solubility": "",
                "vapor_pressure": "",
            }
