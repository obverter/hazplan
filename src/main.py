"""
Main script for the chemical safety database.

This script provides a command-line interface for searching and
retrieving chemical data from PubChem and storing it in a database.
"""

import argparse
import logging
import sys
import time
from pathlib import Path

from src.database.db_manager import DatabaseManager
from src.scrapers.pubchem_scraper import PubChemScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def setup_argparse():
    """Set up command-line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Chemical Safety Database - Search, store, and query chemical data"
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Search command
    search_parser = subparsers.add_parser("search", help="Search for a chemical")
    search_parser.add_argument(
        "query", help="Chemical name or CAS number to search for"
    )
    search_parser.add_argument(
        "--store", action="store_true", help="Store the search results in the database"
    )

    # Query command
    query_parser = subparsers.add_parser("query", help="Query specific chemical information")
    query_parser.add_argument("chemical", help="Chemical name or CAS number")
    query_parser.add_argument(
        "--property", 
        help="Specific property to retrieve", 
        choices=[
            # Existing properties
            "flash_point", "boiling_point", "melting_point", 
            "density", "vapor_pressure", "solubility", 
            "physical_state", "hazard_statements",
            
            # New toxicity-related properties
            "ld50", "lc50", "acute_toxicity_notes"
        ]
    )

    # Import command
    import_parser = subparsers.add_parser("import", help="Import chemicals from a file")
    import_parser.add_argument(
        "file",
        help="Path to a file containing chemical names or CAS numbers (one per line)",
    )

    # Export command
    export_parser = subparsers.add_parser(
        "export", help="Export the database to a CSV file"
    )
    export_parser.add_argument("--output", help="Path to the output CSV file")

    # Count command
    subparsers.add_parser("count", help="Count the number of chemicals in the database")

    return parser


def search_chemical(query, store=False):
    """
    Search for a chemical and display the results.

    Args:
        query: Chemical name or CAS number to search for
        store: Whether to store the search results in the database
    """
    with PubChemScraper() as scraper:
        logger.info(f"Searching for: {query}")
        results = scraper.search_chemical(query)

        if not results:
            logger.info("No results found.")
            return

        logger.info(f"Found {len(results)} results:")
        for i, result in enumerate(results, 1):
            print(f"{i}. {result['name']} (CID: {result['cid']})")
            if "formula" in result and result["formula"]:
                print(f"   Formula: {result['formula']}")
            if "molecular_weight" in result and result["molecular_weight"]:
                print(f"   Molecular Weight: {result['molecular_weight']}")

        if store:
            db_manager = DatabaseManager()
            for result in results:
                logger.info(f"Extracting detailed data for: {result['name']}")
                chemical_data = scraper.extract_chemical_data(result)
                if chemical_data:
                    db_manager.add_chemical(chemical_data)
                    logger.info(f"Stored: {chemical_data.get('name')}")
                else:
                    logger.warning(f"Failed to extract data for: {result['name']}")
                # Be nice to the API
                time.sleep(1)


def query_chemical(chemical, property=None):
    """
    Query a specific chemical's information.

    Args:
        chemical: Chemical name or CAS number
        property: Optional specific property to retrieve
    """
    db_manager = DatabaseManager()
    
    # Expanded search strategies
    search_terms = [chemical]
    
    # Add variations for common chemicals
    chemical_variations = {
        "water": ["water", "oxidane", "H2O"],
        "ethanol": ["ethanol", "ethyl alcohol", "C2H6O"],
        "hydrochloric acid": ["hydrochloric acid", "chlorane", "HCl"]
    }
    
    # Add variations if chemical matches a known variation
    for key, variations in chemical_variations.items():
        if chemical.lower() in [v.lower() for v in variations]:
            search_terms.extend(variations)
    
    # First try searching by name
    results = []
    for term in search_terms:
        results = db_manager.search_chemicals(term)
        if results:
            break
    
    if not results:
        # If no results, try searching by CAS number
        try:
            result = db_manager.get_chemical_by_cas(chemical)
            if result:
                results = [result]
        except Exception:
            pass
    
    if not results:
        logger.error(f"No chemical found matching: {chemical}")
        return
    
    # Use the first result
    chemical_data = results[0]
    
    # Debug: print full chemical data
    print("Full Chemical Data:")
    for key, value in chemical_data.items():
        print(f"{key}: {value}")
    
    if property:
        # If a specific property is requested
        value = chemical_data.get(property, "Not found")
        print(f"\n{chemical_data.get('name', chemical).capitalize()} {property.replace('_', ' ').title()}: {value}")
    else:
        # Print all available information
        print(f"\nChemical Information for: {chemical_data.get('name', 'Unknown')}")
        for key, value in chemical_data.items():
            if value and key not in ['id', 'source_url', 'source_name', 'inchi', 'canonical_smiles', 'isomeric_smiles']:
                print(f"{key.replace('_', ' ').title()}: {value}")


def import_chemicals(file_path):
    """
    Import chemicals from a file containing names or CAS numbers.

    Args:
        file_path: Path to the input file
    """
    path = Path(file_path)
    if not path.exists():
        logger.error(f"File not found: {file_path}")
        return

    try:
        with open(path, "r") as f:
            chemicals = [line.strip() for line in f if line.strip()]
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return

    logger.info(f"Importing {len(chemicals)} chemicals...")

    db_manager = DatabaseManager()
    with PubChemScraper() as scraper:
        for i, chemical in enumerate(chemicals, 1):
            logger.info(f"[{i}/{len(chemicals)}] Processing: {chemical}")

            # Search for the chemical
            results = scraper.search_chemical(chemical)
            if not results:
                logger.warning(f"No results found for: {chemical}")
                continue

            # Get the first result
            result = results[0]
            logger.info(f"Found: {result['name']} (CID: {result['cid']})")

            # Extract detailed data
            logger.info(f"Extracting data for: {result['name']}")
            chemical_data = scraper.extract_chemical_data(result)
            
            if chemical_data:
                # Add to database
                chem_id = db_manager.add_chemical(chemical_data)
                logger.info(f"Stored chemical with ID: {chem_id}")
            else:
                logger.warning(f"Failed to extract data for: {result['name']}")
            
            # Be nice to the API
            if i < len(chemicals):
                time.sleep(1)

    logger.info("Import completed.")


def export_database(output_path=None):
    """
    Export the database to a CSV file.

    Args:
        output_path: Path to the output CSV file
    """
    db_manager = DatabaseManager()
    path = db_manager.export_to_csv(output_path)

    if path:
        logger.info(f"Database exported to: {path}")
    else:
        logger.error("Failed to export database.")


def count_chemicals():
    """Count the number of chemicals in the database."""
    db_manager = DatabaseManager()
    count = db_manager.count_chemicals()
    logger.info(f"Total chemicals in database: {count}")


def main():
    """Main entry point."""
    parser = setup_argparse()
    args = parser.parse_args()

    if args.command == "search":
        search_chemical(args.query, args.store)
    elif args.command == "query":
        query_chemical(args.chemical, args.property)
    elif args.command == "import":
        import_chemicals(args.file)
    elif args.command == "export":
        export_database(args.output)
    elif args.command == "count":
        count_chemicals()
    else:
        parser.print_help()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())