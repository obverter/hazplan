"""
Helper functions for the chemical safety database project.
"""

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union


def parse_cas_number(text: str) -> Optional[str]:
    """
    Extract and validate a CAS registry number from text.

    Args:
        text: Text that may contain a CAS number

    Returns:
        Valid CAS number or None
    """
    if not text:
        return None

    # Try to extract a CAS pattern from the text
    cas_pattern = r"(\d{1,7})-(\d{2})-(\d{1})"
    match = re.search(cas_pattern, text)

    if not match:
        return None

    # Validate the CAS number
    cas_number = match.group(0)
    if is_valid_cas(cas_number):
        return cas_number

    return None


def is_valid_cas(cas_number: str) -> bool:
    """
    Validate a CAS registry number using the checksum digit.

    Args:
        cas_number: CAS number to validate (format: XXXXXXX-YY-Z)

    Returns:
        True if the CAS number is valid, False otherwise
    """
    if not cas_number or not re.match(r"^\d{1,7}-\d{2}-\d{1}$", cas_number):
        return False

    # Split the CAS number
    parts = cas_number.split("-")
    if len(parts) != 3:
        return False

    # Get the check digit
    check_digit = int(parts[2])

    # Calculate the check sum
    digits = parts[0] + parts[1]
    check_sum = sum(int(digits[i]) * (len(digits) - i) for i in range(len(digits)))
    check_sum %= 10

    # Compare
    return check_sum == check_digit


def parse_physical_property(text: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Parse a physical property value and unit from text.

    Args:
        text: Text containing a physical property (e.g., "100.5 °C", "1.2 g/cm³")

    Returns:
        Tuple of (value, unit) where value is a float and unit is a string
    """
    if not text:
        return None, None

    # Pattern for number with optional decimal point followed by whitespace and optional unit
    pattern = r"([-+]?\d*\.?\d+)\s*([^\d\s].*)?"
    match = re.search(pattern, text)

    if not match:
        return None, None

    value = float(match.group(1))
    unit = match.group(2)

    if unit:
        unit = unit.strip()

    return value, unit


def convert_to_standard_unit(
    value: float, unit: str, property_type: str
) -> Tuple[float, str]:
    """
    Convert a physical property to a standard unit.

    Args:
        value: The numerical value
        unit: The original unit
        property_type: Type of property (e.g., 'temperature', 'pressure', 'density')

    Returns:
        Tuple of (converted_value, standard_unit)
    """
    if property_type == "temperature":
        # Convert to Kelvin
        if unit in ["°C", "C"]:
            return value + 273.15, "K"
        elif unit in ["°F", "F"]:
            return (value - 32) * 5 / 9 + 273.15, "K"
        elif unit in ["K"]:
            return value, "K"

    elif property_type == "pressure":
        # Convert to Pascal
        if unit in ["atm"]:
            return value * 101325, "Pa"
        elif unit in ["mmHg", "torr"]:
            return value * 133.322, "Pa"
        elif unit in ["bar"]:
            return value * 100000, "Pa"
        elif unit in ["psi"]:
            return value * 6894.76, "Pa"
        elif unit in ["Pa"]:
            return value, "Pa"

    elif property_type == "density":
        # Convert to g/cm³
        if unit in ["kg/m³"]:
            return value / 1000, "g/cm³"
        elif unit in ["g/cm³", "g/cc", "g/mL"]:
            return value, "g/cm³"

    # If no conversion is available or needed, return the original values
    return value, unit


def extract_hazard_codes(text: str) -> Dict[str, str]:
    """
    Extract GHS hazard codes (H-statements) from text.

    Args:
        text: Text containing hazard statements

    Returns:
        Dictionary mapping hazard codes to descriptions
    """
    if not text:
        return {}

    hazard_pattern = r"(H\d{3}(?:\+H\d{3})*)(?:\s*[:;-]\s*|\s+)(.*?)(?=$|H\d{3}|\n|$)"
    matches = re.finditer(hazard_pattern, text)

    hazards = {}
    for match in matches:
        code = match.group(1)
        description = match.group(2).strip()
        if description:
            hazards[code] = description

    return hazards


def categorize_hazard_statement(code: str) -> str:
    """
    Categorize a GHS hazard statement.

    Args:
        code: GHS hazard code (e.g., 'H200', 'H315')

    Returns:
        Category of the hazard statement ('Physical', 'Health', 'Environmental', or 'Unknown')
    """
    if not code or not code.startswith("H"):
        return "Unknown"

    try:
        num = int(code[1:].split("+")[0])  # Handle combined codes like H315+H319

        if 200 <= num <= 290:
            return "Physical"
        elif 300 <= num <= 373:
            return "Health"
        elif 400 <= num <= 420:
            return "Environmental"
        else:
            return "Unknown"
    except ValueError:
        return "Unknown"


def extract_precautionary_codes(text: str) -> Dict[str, str]:
    """
    Extract GHS precautionary codes (P-statements) from text.

    Args:
        text: Text containing precautionary statements

    Returns:
        Dictionary mapping precautionary codes to descriptions
    """
    if not text:
        return {}

    precautionary_pattern = (
        r"(P\d{3}(?:\+P\d{3})*)(?:\s*[:;-]\s*|\s+)(.*?)(?=$|P\d{3}|\n|$)"
    )
    matches = re.finditer(precautionary_pattern, text)

    precautions = {}
    for match in matches:
        code = match.group(1)
        description = match.group(2).strip()
        if description:
            precautions[code] = description

    return precautions


def normalize_chemical_name(name: str) -> str:
    """
    Normalize a chemical name for consistent searching.

    Args:
        name: Chemical name

    Returns:
        Normalized chemical name
    """
    if not name:
        return ""

    # Convert to lowercase
    normalized = name.lower()

    # Remove common prefixes like "n-", "tert-", etc.
    prefixes = ["n-", "tert-", "sec-", "iso-", "cis-", "trans-"]
    for prefix in prefixes:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :]

    # Remove special characters and extra whitespace
    normalized = re.sub(r"[^\w\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    return normalized


def validate_chemical_data(data: Dict[str, any]) -> Tuple[bool, List[str]]:
    """
    Validate chemical data for required fields and data types.

    Args:
        data: Chemical data dictionary

    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []

    # Required fields
    if not data.get("name"):
        errors.append("Chemical name is required")

    # CAS number validation
    if "cas_number" in data and data["cas_number"]:
        if not is_valid_cas(data["cas_number"]):
            errors.append("Invalid CAS number format")

    # Type validations
    if "molecular_weight" in data and data["molecular_weight"] is not None:
        if (
            not isinstance(data["molecular_weight"], (int, float))
            or data["molecular_weight"] <= 0
        ):
            errors.append("Molecular weight must be a positive number")

    # Check numeric fields
    numeric_fields = [
        "xlogp",
        "exact_mass",
        "monoisotopic_mass",
        "tpsa",
        "complexity",
        "h_bond_donor_count",
        "h_bond_acceptor_count",
        "rotatable_bond_count",
    ]

    for field in numeric_fields:
        if field in data and data[field] is not None:
            try:
                float(data[field])
            except (ValueError, TypeError):
                errors.append(f"{field} must be a number")

    # Check integer fields
    int_fields = [
        "charge",
        "h_bond_donor_count",
        "h_bond_acceptor_count",
        "rotatable_bond_count",
        "heavy_atom_count",
    ]

    for field in int_fields:
        if field in data and data[field] is not None:
            try:
                if int(float(data[field])) != float(data[field]):
                    errors.append(f"{field} must be an integer")
            except (ValueError, TypeError):
                errors.append(f"{field} must be an integer")

    return len(errors) == 0, errors


def format_citation(source_name: str, source_url: str) -> str:
    """
    Format a citation for a data source.

    Args:
        source_name: Name of the source
        source_url: URL of the source

    Returns:
        Formatted citation string
    """
    current_date = datetime.now().strftime("%Y-%m-%d")
    return f"Data retrieved from {source_name} ({source_url}) on {current_date}"
