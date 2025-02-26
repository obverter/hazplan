"""
Tests for helper functions.
"""
import pytest

from src.utils.helpers import (
    parse_cas_number,
    is_valid_cas,
    parse_physical_property,
    convert_to_standard_unit,
    extract_hazard_codes,
    categorize_hazard_statement,
    extract_precautionary_codes,
    normalize_chemical_name,
    validate_chemical_data
)


class TestHelpers:
    """Tests for helper functions."""
    
    def test_parse_cas_number(self):
        """Test parsing CAS numbers from text."""
        # Valid CAS numbers
        assert parse_cas_number("67-64-1") == "67-64-1"
        assert parse_cas_number("CAS: 67-64-1") == "67-64-1"
        assert parse_cas_number("The CAS number is 67-64-1.") == "67-64-1"
        
        # Invalid input
        assert parse_cas_number("") is None
        assert parse_cas_number(None) is None
        assert parse_cas_number("Not a CAS number") is None
        assert parse_cas_number("67-64-X") is None
        
        # Invalid CAS numbers (wrong check digit)
        assert parse_cas_number("67-64-2") is None
    
    def test_is_valid_cas(self):
        """Test CAS number validation."""
        # Valid CAS numbers
        assert is_valid_cas("67-64-1") is True
        assert is_valid_cas("7732-18-5") is True
        assert is_valid_cas("50-00-0") is True
        
        # Invalid CAS numbers
        assert is_valid_cas("") is False
        assert is_valid_cas("67-64-2") is False  # Invalid check digit
        assert is_valid_cas("67-64") is False
        assert is_valid_cas("not-a-cas") is False
    
    def test_parse_physical_property(self):
        """Test parsing physical property values and units."""
        # Temperature values
        assert parse_physical_property("100 °C") == (100.0, "°C")
        assert parse_physical_property("100°C") == (100.0, "°C")
        assert parse_physical_property("-20.5 °C") == (-20.5, "°C")
        
        # Density values
        assert parse_physical_property("1.2 g/cm³") == (1.2, "g/cm³")
        assert parse_physical_property("1.2g/cm³") == (1.2, "g/cm³")
        
        # Pressure values
        assert parse_physical_property("760 mmHg") == (760.0, "mmHg")
        
        # Values without units
        assert parse_physical_property("100") == (100.0, None)
        
        # Invalid input
        assert parse_physical_property("") == (None, None)
        assert parse_physical_property(None) == (None, None)
        assert parse_physical_property("Not a number") == (None, None)
    
    def test_convert_to_standard_unit(self):
        """Test converting physical property values to standard units."""
        # Temperature conversions
        assert convert_to_standard_unit(25, "°C", "temperature") == (298.15, "K")
        assert convert_to_standard_unit(77, "°F", "temperature") == (298.15, "K")
        assert convert_to_standard_unit(298.15, "K", "temperature") == (298.15, "K")
        
        # Pressure conversions
        assert convert_to_standard_unit(1, "atm", "pressure") == (101325, "Pa")
        assert convert_to_standard_unit(760, "mmHg", "pressure") == (101324.72, "Pa")
        
        # Density conversions
        assert convert_to_standard_unit(1000, "kg/m³", "density") == (1.0, "g/cm³")
        
        # No conversion
        assert convert_to_standard_unit(42, "unknown", "unknown") == (42, "unknown")
    
    def test_extract_hazard_codes(self):
        """Test extracting hazard codes from text."""
        # Simple case
        assert extract_hazard_codes("H315: Causes skin irritation") == {"H315": "Causes skin irritation"}
        
        # Multiple codes
        text = "H225: Highly flammable liquid and vapour; H319: Causes serious eye irritation"
        expected = {
            "H225": "Highly flammable liquid and vapour",
            "H319": "Causes serious eye irritation"
        }
        assert extract_hazard_codes(text) == expected
        
        # Combined codes
        assert extract_hazard_codes("H315+H319: Causes skin and eye irritation") == {"H315+H319": "Causes skin and eye irritation"}
        
        # Different separators
        assert extract_hazard_codes("H315 - Causes skin irritation") == {"H315": "Causes skin irritation"}
        assert extract_hazard_codes("H315; Causes skin irritation") == {"H315": "Causes skin irritation"}
        
        # Invalid input
        assert extract_hazard_codes("") == {}
        assert extract_hazard_codes(None) == {}
        assert extract_hazard_codes("Not a hazard statement") == {}
    
    def test_categorize_hazard_statement(self):
        """Test categorizing hazard statements."""
        # Physical hazards
        assert categorize_hazard_statement("H200") == "Physical"
        assert categorize_hazard_statement("H290") == "Physical"
        
        # Health hazards
        assert categorize_hazard_statement("H300") == "Health"
        assert categorize_hazard_statement("H373") == "Health"
        
        # Environmental hazards
        assert categorize_hazard_statement("H400") == "Environmental"
        assert categorize_hazard_statement("H420") == "Environmental"
        
        # Combined codes
        assert categorize_hazard_statement("H315+H319") == "Health"
        
        # Invalid codes
        assert categorize_hazard_statement("") == "Unknown"
        assert categorize_hazard_statement(None) == "Unknown"
        assert categorize_hazard_statement("Not a code") == "Unknown"
        assert categorize_hazard_statement("P100") == "Unknown"
    
    def test_extract_precautionary_codes(self):
        """Test extracting precautionary codes from text."""
        # Simple case
        assert extract_precautionary_codes("P210: Keep away from heat") == {"P210": "Keep away from heat"}
        
        # Multiple codes
        text = "P210: Keep away from heat; P233: Keep container tightly closed"
        expected = {
            "P210": "Keep away from heat",
            "P233": "Keep container tightly closed"
        }
        assert extract_precautionary_codes(text) == expected
        
        # Combined codes
        assert extract_precautionary_codes("P303+P361+P353: IF ON SKIN: Remove immediately all contaminated clothing. Rinse skin with water") == {
            "P303+P361+P353": "IF ON SKIN: Remove immediately all contaminated clothing. Rinse skin with water"
        }
        
        # Different separators
        assert extract_precautionary_codes("P210 - Keep away from heat") == {"P210": "Keep away from heat"}
        
        # Invalid input
        assert extract_precautionary_codes("") == {}
        assert extract_precautionary_codes(None) == {}
        assert extract_precautionary_codes("Not a precautionary statement") == {}
    
    def test_normalize_chemical_name(self):
        """Test normalizing chemical names."""
        # Remove prefixes
        assert normalize_chemical_name("n-Hexane") == "hexane"
        assert normalize_chemical_name("tert-Butyl alcohol") == "butyl alcohol"
        
        # Convert to lowercase
        assert normalize_chemical_name("Acetone") == "acetone"
        assert normalize_chemical_name("SODIUM HYDROXIDE") == "sodium hydroxide"
        
        # Remove special characters
        assert normalize_chemical_name("2,2'-Dichlorodiethyl ether") == "2 2 dichlorodiethyl ether"
        
        # Remove extra whitespace
        assert normalize_chemical_name("Methyl  ethyl   ketone") == "methyl ethyl ketone"
        
        # Invalid input
        assert normalize_chemical_name("") == ""
        assert normalize_chemical_name(None) == ""
    
    def test_validate_chemical_data(self):
        """Test validating chemical data."""
        # Valid data
        valid_data = {
            "name": "Acetone",
            "cas_number": "67-64-1",
            "formula": "C3H6O",
            "molecular_weight": 58.08
        }
        is_valid, errors = validate_chemical_data(valid_data)
        assert is_valid is True
        assert len(errors) == 0
        
        # Missing name
        invalid_data = {
            "cas_number": "67-64-1",
            "formula": "C3H6O",
            "molecular_weight": 58.08
        }
        is_valid, errors = validate_chemical_data(invalid_data)
        assert is_valid is False
        assert "name is required" in errors[0].lower()
        
        # Invalid CAS number
        invalid_data = {
            "name": "Acetone",
            "cas_number": "67-64-2",  # Invalid check digit
            "formula": "C3H6O",
            "molecular_weight": 58.08
        }
        is_valid, errors = validate_chemical_data(invalid_data)
        assert is_valid is False
        assert "cas number" in errors[0].lower()
        
        # Invalid molecular weight
        invalid_data = {
            "name": "Acetone",
            "cas_number": "67-64-1",
            "formula": "C3H6O",
            "molecular_weight": -58.08
        }
        is_valid, errors = validate_chemical_data(invalid_data)
        assert is_valid is False
        assert "molecular weight" in errors[0].lower()
        
        # Invalid numeric field
        invalid_data = {
            "name": "Acetone",
            "formula": "C3H6O",
            "xlogp": "not a number"
        }
        is_valid, errors = validate_chemical_data(invalid_data)
        assert is_valid is False
        assert "xlogp" in errors[0].lower()