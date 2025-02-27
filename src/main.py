"""
Main script for the chemical safety database.

This script provides a command-line interface for searching and
retrieving chemical data from PubChem and storing it in a database.
"""

import argparse
import logging
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Union

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
    query_parser = subparsers.add_parser(
        "query", help="Query specific chemical information"
    )
    query_parser.add_argument("chemical", help="Chemical name or CAS number")
    query_parser.add_argument(
        "--property",
        help="Specific property to retrieve",
        choices=[
            # Existing properties
            "flash_point",
            "boiling_point",
            "melting_point",
            "density",
            "vapor_pressure",
            "solubility",
            "physical_state",
            "hazard_statements",
            # Toxicity-related properties
            "ld50",
            "lc50",
            "acute_toxicity_notes",
        ],
    )
    query_parser.add_argument(
        "--format",
        help="Output format",
        choices=["text", "json", "csv"],
        default="text",
    )

    # Import command
    import_parser = subparsers.add_parser("import", help="Import chemicals from a file")
    import_parser.add_argument(
        "file",
        help="Path to a file containing chemical names or CAS numbers (one per line)",
    )
    import_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip chemicals that already exist in the database",
    )
    import_parser.add_argument(
        "--update", action="store_true", help="Update existing chemicals with new data"
    )

    # Export command
    export_parser = subparsers.add_parser(
        "export", help="Export the database to a CSV file"
    )
    export_parser.add_argument("--output", help="Path to the output CSV file")
    export_parser.add_argument(
        "--format",
        help="Output format",
        choices=["csv", "json", "excel"],
        default="csv",
    )

    # Count command
    subparsers.add_parser("count", help="Count the number of chemicals in the database")

    # Delete command
    delete_parser = subparsers.add_parser(
        "delete", help="Delete a chemical from the database"
    )
    delete_parser.add_argument("chemical", help="Chemical name or CAS number to delete")
    delete_parser.add_argument(
        "--force", action="store_true", help="Force deletion without confirmation"
    )

    # Version command
    subparsers.add_parser("version", help="Show version information")

    return parser


def extract_ld50_values(text: str) -> Optional[str]:
    """
    Extract LD50 values from text.

    Args:
        text: Text containing LD50 information

    Returns:
        Formatted string with LD50 values or None if none found
    """
    if not text:
        return None

    # Find all instances of LD50 with values
    ld50_pattern = r"LD50.*?(\d+[\d\.]*).*?(mg/kg|g/kg|mg/L|g/L).*?\(([^)]+)\)"
    alternate_pattern = r"LD50\s+(\w+)\s+(\w+)\s+([\d\.]+)\s+(g/[lL]|mg/kg)"

    # Extract all primary matches
    ld50_values = []
    for match in re.finditer(ld50_pattern, text):
        value = match.group(0).strip()
        if value:
            ld50_values.append(value)

    # Extract all alternate format matches
    for match in re.finditer(alternate_pattern, text):
        species = match.group(1)
        route = match.group(2)
        amount = match.group(3)
        unit = match.group(4)
        value = f"LD50 {species} {route} {amount} {unit}"
        if value not in ld50_values:
            ld50_values.append(value)

    if not ld50_values:
        return None

    return "; ".join(ld50_values)


def extract_lc50_values(text: str) -> Optional[str]:
    """
    Extract LC50 values from text.

    Args:
        text: Text containing LC50 information

    Returns:
        Formatted string with LC50 values or None if none found
    """
    if not text:
        return None

    # Find all instances of LC50 with values
    lc50_pattern = (
        r"LC50.*?(\d+[\d\.]*).*?(ppm|mg/[lL]|g/[lL]|mg/m3|g/m3).*?\(([^)]+)\)"
    )
    alternate_pattern = r"LC50\s+(\w+)\s+(\w+)\s+([\d\.]+)\s+(g/cu m|ppm)"

    # Extract all primary matches
    lc50_values = []
    for match in re.finditer(lc50_pattern, text):
        value = match.group(0).strip()
        if value:
            lc50_values.append(value)

    # Extract all alternate format matches
    for match in re.finditer(alternate_pattern, text):
        species = match.group(1)
        route = match.group(2)
        amount = match.group(3)
        unit = match.group(4)
        value = f"LC50 {species} {route} {amount} {unit}"
        if value not in lc50_values:
            lc50_values.append(value)

    if not lc50_values:
        return None

    return "; ".join(lc50_values)


def search_chemical(query, store=False):
    """
    Search for a chemical and display the results.

    Args:
        query: Chemical name or CAS number to search for
        store: Whether to store the search results in the database
    """
    try:
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
                        # Extract toxicity data from acute_toxicity_notes if needed
                        if (
                            "acute_toxicity_notes" in chemical_data
                            and chemical_data["acute_toxicity_notes"]
                        ):
                            notes = chemical_data["acute_toxicity_notes"]

                            # Extract LD50 if not already set
                            if not chemical_data.get("ld50"):
                                ld50_values = extract_ld50_values(notes)
                                if ld50_values:
                                    chemical_data["ld50"] = ld50_values

                            # Extract LC50 if not already set
                            if not chemical_data.get("lc50"):
                                lc50_values = extract_lc50_values(notes)
                                if lc50_values:
                                    chemical_data["lc50"] = lc50_values

                        db_manager.add_chemical(chemical_data)
                        logger.info(f"Stored: {chemical_data.get('name')}")
                    else:
                        logger.warning(f"Failed to extract data for: {result['name']}")
                    # Be nice to the API
                    time.sleep(1)
    except Exception as e:
        logger.error(f"Error during chemical search: {str(e)}")
        raise


def query_chemical(chemical, property=None, output_format="text"):
    """
    Query a specific chemical's information.

    Args:
        chemical: Chemical name or CAS number
        property: Optional specific property to retrieve
        output_format: Format for output data (text, json, csv)
    """
    try:
        db_manager = DatabaseManager()

        # Expanded search strategies
        search_terms = [chemical]

        # Add variations for common chemicals
        chemical_variations = {
            "water": ["water", "oxidane", "H2O"],
            "ethanol": ["ethanol", "ethyl alcohol", "C2H6O"],
            "hydrochloric acid": ["hydrochloric acid", "chlorane", "HCl"],
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
            except Exception as e:
                logger.debug(f"Error in CAS search: {str(e)}")

        if not results:
            logger.error(f"No chemical found matching: {chemical}")
            return

        # Use the first result
        chemical_data = results[0]

        # Extract toxicity data from acute_toxicity_notes if needed
        if (
            property in ["ld50", "lc50"]
            and not chemical_data.get(property)
            and chemical_data.get("acute_toxicity_notes")
        ):
            notes = chemical_data.get("acute_toxicity_notes", "")

            if property == "ld50":
                toxicity_values = extract_ld50_values(notes)
            else:  # lc50
                toxicity_values = extract_lc50_values(notes)

            if toxicity_values:
                chemical_data[property] = toxicity_values

        # If output format is not text, handle accordingly
        if output_format == "json":
            import json

            if property:
                print(json.dumps({property: chemical_data.get(property, "Not found")}))
            else:
                print(json.dumps(chemical_data))
            return
        elif output_format == "csv":
            import csv
            import io

            output = io.StringIO()
            writer = csv.writer(output)
            if property:
                writer.writerow([property, chemical_data.get(property, "Not found")])
            else:
                writer.writerow(chemical_data.keys())
                writer.writerow(chemical_data.values())
            print(output.getvalue())
            return

        # Default text output format
        if property:
            # If a specific property is requested
            value = chemical_data.get(property, "Not found")
            print(
                f"\n{chemical_data.get('name', chemical).capitalize()} {property.replace('_', ' ').title()}: {value}"
            )
        else:
            # Print all available information
            print(f"\nChemical Information for: {chemical_data.get('name', 'Unknown')}")
            print("=" * 40)

            # Group properties by category for better display
            categories = {
                "Identifiers": ["id", "cas_number", "name", "formula"],
                "Physical Properties": [
                    "molecular_weight",
                    "physical_state",
                    "color",
                    "density",
                    "melting_point",
                    "boiling_point",
                    "flash_point",
                    "solubility",
                    "vapor_pressure",
                ],
                "Chemical Properties": [
                    "xlogp",
                    "exact_mass",
                    "monoisotopic_mass",
                    "tpsa",
                    "complexity",
                    "charge",
                    "h_bond_donor_count",
                    "h_bond_acceptor_count",
                    "rotatable_bond_count",
                    "heavy_atom_count",
                ],
                "Toxicity Data": ["ld50", "lc50", "acute_toxicity_notes"],
                "Safety Information": [
                    "hazard_statements",
                    "precautionary_statements",
                    "ghs_pictograms",
                    "signal_word",
                ],
                "Source Information": ["source_url", "source_name"],
                "Computed Values": [
                    "density_value",
                    "density_unit",
                    "melting_point_value",
                    "melting_point_unit",
                    "boiling_point_value",
                    "boiling_point_unit",
                    "flash_point_value",
                    "flash_point_unit",
                    "vapor_pressure_value",
                    "vapor_pressure_unit",
                ],
                "Chemical Identifiers": [
                    "canonical_smiles",
                    "isomeric_smiles",
                    "inchi",
                    "inchikey",
                ],
            }

            # Output each category
            for category, props in categories.items():
                category_data = {
                    key: chemical_data.get(key)
                    for key in props
                    if key in chemical_data and chemical_data.get(key)
                }

                if category_data:
                    print(f"\n{category}:")
                    for key, value in category_data.items():
                        # Limit display length for longer strings
                        if isinstance(value, str) and len(value) > 100:
                            display_value = value[:97] + "..."
                        else:
                            display_value = value

                        # Format key for display
                        display_key = key.replace("_", " ").title()
                        print(f"  {display_key}: {display_value}")
    except Exception as e:
        logger.error(f"Error during chemical query: {str(e)}")
        raise


def import_chemicals(file_path, skip_existing=False, update_existing=False):
    """
    Import chemicals from a file containing names or CAS numbers.

    Args:
        file_path: Path to the input file
        skip_existing: Skip chemicals already in the database
        update_existing: Update existing chemicals with new data
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

            # Check if chemical already exists in the database
            if skip_existing or update_existing:
                existing_records = db_manager.search_chemicals(chemical)
                if existing_records:
                    if skip_existing:
                        logger.info(f"Skipping existing chemical: {chemical}")
                        continue
                    # else: update_existing is True, so we'll continue and update

            # Search for the chemical
            try:
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
                    # Extract toxicity data from acute_toxicity_notes if needed
                    if (
                        "acute_toxicity_notes" in chemical_data
                        and chemical_data["acute_toxicity_notes"]
                    ):
                        notes = chemical_data["acute_toxicity_notes"]

                        # Extract LD50 if not already set
                        if not chemical_data.get("ld50"):
                            ld50_values = extract_ld50_values(notes)
                            if ld50_values:
                                chemical_data["ld50"] = ld50_values

                        # Extract LC50 if not already set
                        if not chemical_data.get("lc50"):
                            lc50_values = extract_lc50_values(notes)
                            if lc50_values:
                                chemical_data["lc50"] = lc50_values

                    # Add to database
                    chem_id = db_manager.add_chemical(chemical_data)
                    logger.info(f"Stored chemical with ID: {chem_id}")
                else:
                    logger.warning(f"Failed to extract data for: {result['name']}")
            except Exception as e:
                logger.error(f"Error processing chemical '{chemical}': {str(e)}")
                continue

            # Be nice to the API - limit rate
            if i < len(chemicals):
                time.sleep(1)

    logger.info("Import completed.")


def export_database(output_path=None, output_format="csv"):
    """
    Export the database to a file.

    Args:
        output_path: Path to the output file
        output_format: Format for output (csv, json, excel)
    """
    db_manager = DatabaseManager()

    # Get all chemicals
    chemicals = db_manager.get_all_chemicals()

    if not chemicals:
        logger.error("No chemicals in database to export.")
        return

    # Define default output path if not provided
    if not output_path:
        project_root = Path(__file__).parent.parent
        data_dir = project_root / "data" / "processed"
        os.makedirs(data_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        filename = f"chemicals_export_{timestamp}.{output_format}"
        output_path = str(data_dir / filename)

    try:
        if output_format == "csv":
            path = db_manager.export_to_csv(output_path)
        elif output_format == "json":
            import json

            with open(output_path, "w") as f:
                json.dump(chemicals, f, indent=2)
            path = output_path
        elif output_format == "excel":
            import pandas as pd

            df = pd.DataFrame(chemicals)
            df.to_excel(output_path, index=False)
            path = output_path
        else:
            logger.error(f"Unsupported output format: {output_format}")
            return

        if path:
            logger.info(f"Database exported to: {path}")
        else:
            logger.error("Failed to export database.")
    except Exception as e:
        logger.error(f"Error exporting database: {str(e)}")


def count_chemicals():
    """Count the number of chemicals in the database."""
    db_manager = DatabaseManager()
    count = db_manager.count_chemicals()
    logger.info(f"Total chemicals in database: {count}")


def delete_chemical(chemical, force=False):
    """
    Delete a chemical from the database.

    Args:
        chemical: Chemical name or CAS number to delete
        force: Skip confirmation if True
    """
    db_manager = DatabaseManager()

    # Search for the chemical
    results = db_manager.search_chemicals(chemical)

    if not results:
        # Try as CAS number
        try:
            result = db_manager.get_chemical_by_cas(chemical)
            if result:
                results = [result]
        except Exception:
            pass

    if not results:
        logger.error(f"No chemical found matching: {chemical}")
        return

    # If multiple results, ask user to confirm
    if len(results) > 1 and not force:
        print(f"Found {len(results)} chemicals matching '{chemical}':")
        for i, result in enumerate(results, 1):
            print(f"{i}. {result.get('name')} (CAS: {result.get('cas_number')})")

        try:
            selection = int(input("Enter number to delete (0 to cancel): "))
            if selection == 0:
                print("Deletion cancelled.")
                return
            if selection < 1 or selection > len(results):
                print("Invalid selection. Deletion cancelled.")
                return
            chemical_data = results[selection - 1]
        except (ValueError, IndexError):
            print("Invalid input. Deletion cancelled.")
            return
    else:
        chemical_data = results[0]

    # Confirm deletion
    if not force:
        confirm = input(
            f"Delete {chemical_data.get('name')} (CAS: {chemical_data.get('cas_number')})? (y/N): "
        )
        if confirm.lower() != "y":
            print("Deletion cancelled.")
            return

    # Delete chemical
    success = db_manager.delete_chemical(chemical_data.get("id"))
    if success:
        logger.info(f"Successfully deleted chemical: {chemical_data.get('name')}")
    else:
        logger.error(f"Failed to delete chemical: {chemical_data.get('name')}")


def show_version():
    """Show version information."""
    version = "1.0.0"  # Update this with your actual version
    print(f"Chemical Safety Database version {version}")
    print("Copyright (c) 2025 Your Organization")
    print("Licensed under the MIT License")


def main():
    """Main entry point."""
    try:
        parser = setup_argparse()
        args = parser.parse_args()

        if args.command == "search":
            search_chemical(args.query, args.store)
        elif args.command == "query":
            output_format = getattr(args, "format", "text")
            query_chemical(args.chemical, args.property, output_format)
        elif args.command == "import":
            skip_existing = getattr(args, "skip_existing", False)
            update_existing = getattr(args, "update", False)
            import_chemicals(args.file, skip_existing, update_existing)
        elif args.command == "export":
            output_format = getattr(args, "format", "csv")
            export_database(args.output, output_format)
        elif args.command == "count":
            count_chemicals()
        elif args.command == "delete":
            delete_chemical(args.chemical, args.force)
        elif args.command == "version":
            show_version()
        else:
            parser.print_help()
            return 1
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback

        logger.debug(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    # Import os here to avoid unused import warning
    import os

    sys.exit(main())
