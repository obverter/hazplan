"""
Main script for the chemical safety database.

This script provides a command-line interface for searching and
retrieving chemical data from PubChem and storing it in a database.
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any

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
    search_parser.add_argument(
        "--limit", type=int, default=5, help="Maximum number of results to display"
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
            # Identification properties
            "cas_number",
            "name",
            "formula",
            "molecular_weight",
            # Physical properties
            "flash_point",
            "boiling_point",
            "melting_point",
            "density",
            "vapor_pressure",
            "solubility",
            "physical_state",
            "color",
            # Safety properties
            "hazard_statements",
            "precautionary_statements",
            "ghs_pictograms",
            "signal_word",
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
    query_parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show detailed information"
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
    import_parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of chemicals to process in a batch",
    )

    # Export command
    export_parser = subparsers.add_parser(
        "export", help="Export the database to a file"
    )
    export_parser.add_argument("--output", help="Path to the output file")
    export_parser.add_argument(
        "--format",
        help="Output format",
        choices=["csv", "json", "excel"],
        default="csv",
    )
    export_parser.add_argument(
        "--filter",
        help="Filter chemical export by property (e.g. 'cas_number=64-17-5' or 'name=ethanol')",
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

    # Update command
    update_parser = subparsers.add_parser(
        "update", help="Update chemical data in the database"
    )
    update_parser.add_argument("chemical", help="Chemical name or CAS number to update")
    update_parser.add_argument(
        "--refresh", action="store_true", help="Fetch fresh data from source"
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
    simple_pattern = r"LD50:\s*([\d\.]+)\s*(mg/kg|g/kg|mg/L|g/L).*?\(([^)]+)\)"

    # Extract all primary matches
    ld50_values = []
    for pattern in [ld50_pattern, alternate_pattern, simple_pattern]:
        for match in re.finditer(pattern, text):
            value = match.group(0).strip()
            if value and value not in ld50_values:
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
    simple_pattern = r"LC50.*?(\d+[\d\.]*)\s*(ppm|mg/[lL]|g/cu m)"

    # Extract all primary matches
    lc50_values = []
    for pattern in [lc50_pattern, alternate_pattern, simple_pattern]:
        for match in re.finditer(pattern, text):
            value = match.group(0).strip()
            if value and value not in lc50_values:
                lc50_values.append(value)

    if not lc50_values:
        return None

    return "; ".join(lc50_values)


def process_chemical_data(chemical_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and enhance raw chemical data.

    Args:
        chemical_data: Dictionary containing chemical data

    Returns:
        Enhanced chemical data dictionary
    """
    if not chemical_data:
        return {}

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

    return chemical_data


def search_chemical(query: str, store: bool = False, limit: int = 5) -> None:
    """
    Search for a chemical and display the results.

    Args:
        query: Chemical name or CAS number to search for
        store: Whether to store the search results in the database
        limit: Maximum number of results to display
    """
    try:
        with PubChemScraper() as scraper:
            logger.info(f"Searching for: {query}")
            results = scraper.search_chemical(query)

            if not results:
                logger.info("No results found.")
                return

            # Limit the number of results to display
            display_results = results[:limit]

            logger.info(
                f"Found {len(results)} results (displaying {len(display_results)}):"
            )
            for i, result in enumerate(display_results, 1):
                print(f"{i}. {result['name']} (CID: {result['cid']})")
                if "formula" in result and result["formula"]:
                    print(f"   Formula: {result['formula']}")
                if "molecular_weight" in result and result["molecular_weight"]:
                    print(f"   Molecular Weight: {result['molecular_weight']}")

            if store:
                db_manager = DatabaseManager()
                for i, result in enumerate(results, 1):
                    logger.info(
                        f"[{i}/{len(results)}] Extracting detailed data for: {result['name']}"
                    )
                    try:
                        chemical_data = scraper.extract_chemical_data(result)

                        if chemical_data:
                            # Process the chemical data to enhance it
                            enhanced_data = process_chemical_data(chemical_data)

                            # Add to database
                            chem_id = db_manager.add_chemical(enhanced_data)
                            if chem_id:
                                logger.info(
                                    f"Stored: {enhanced_data.get('name')} (ID: {chem_id})"
                                )
                            else:
                                logger.warning(
                                    f"Failed to store: {enhanced_data.get('name')}"
                                )
                        else:
                            logger.warning(
                                f"Failed to extract data for: {result['name']}"
                            )
                    except Exception as e:
                        logger.error(
                            f"Error processing chemical {result['name']}: {str(e)}"
                        )

                    # Be nice to the API
                    if i < len(results):
                        time.sleep(1)
    except Exception as e:
        logger.error(f"Error during chemical search: {str(e)}")
        raise


def find_chemical_in_database(
    db_manager: DatabaseManager, chemical: str
) -> Optional[Dict[str, Any]]:
    """
    Find a chemical in the database using various search strategies.

    Args:
        db_manager: DatabaseManager instance
        chemical: Chemical name or CAS number

    Returns:
        Chemical data dictionary or None if not found
    """
    # Expanded search strategies
    search_terms = [chemical]

    # Add variations for common chemicals
    chemical_variations = {
        "water": ["water", "oxidane", "H2O"],
        "ethanol": ["ethanol", "ethyl alcohol", "C2H6O", "alcohol"],
        "hydrochloric acid": ["hydrochloric acid", "chlorane", "HCl"],
        "methanol": ["methanol", "methyl alcohol", "CH3OH", "wood alcohol"],
        "acetone": ["acetone", "propanone", "dimethyl ketone"],
        "benzene": ["benzene", "C6H6"],
    }

    # Add variations if chemical matches a known variation
    for key, variations in chemical_variations.items():
        if chemical.lower() in [v.lower() for v in variations]:
            search_terms.extend(
                [v for v in variations if v.lower() != chemical.lower()]
            )

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
        return None

    # Use the first result
    return results[0]


def query_chemical(
    chemical: str,
    property: Optional[str] = None,
    output_format: str = "text",
    verbose: bool = False,
) -> None:
    """
    Query a specific chemical's information.

    Args:
        chemical: Chemical name or CAS number
        property: Optional specific property to retrieve
        output_format: Format for output data (text, json, csv)
        verbose: Whether to show detailed information
    """
    try:
        db_manager = DatabaseManager()

        # Find the chemical in the database
        chemical_data = find_chemical_in_database(db_manager, chemical)

        if not chemical_data:
            return

        # Process the chemical data to ensure all properties are extracted
        enhanced_data = process_chemical_data(chemical_data)

        # If output format is not text, handle accordingly
        if output_format == "json":
            if property:
                print(json.dumps({property: enhanced_data.get(property, "Not found")}))
            else:
                print(json.dumps(enhanced_data))
            return
        elif output_format == "csv":
            import csv
            import io

            output = io.StringIO()
            writer = csv.writer(output)
            if property:
                writer.writerow([property, enhanced_data.get(property, "Not found")])
            else:
                writer.writerow(enhanced_data.keys())
                writer.writerow(enhanced_data.values())
            print(output.getvalue())
            return

        # Default text output format
        if property:
            # If a specific property is requested
            value = enhanced_data.get(property, "Not found")
            # For better display, modify certain properties
            if (
                property == "acute_toxicity_notes"
                and isinstance(value, str)
                and len(value) > 500
                and not verbose
            ):
                value = value[:500] + "... (use --verbose to see full text)"

            print(
                f"\n{enhanced_data.get('name', chemical).capitalize()} {property.replace('_', ' ').title()}: {value}"
            )
        else:
            # Print all available information
            print(f"\nChemical Information for: {enhanced_data.get('name', 'Unknown')}")
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
                "Toxicity Data": ["ld50", "lc50"],
                "Safety Information": [
                    "hazard_statements",
                    "precautionary_statements",
                    "ghs_pictograms",
                    "signal_word",
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

            # Only show these categories in verbose mode
            verbose_categories = [
                "Chemical Properties",
                "Chemical Identifiers",
                "Computed Values",
            ]

            # Output each category
            for category, props in categories.items():
                if not verbose and category in verbose_categories:
                    continue

                category_data = {
                    key: enhanced_data.get(key)
                    for key in props
                    if key in enhanced_data and enhanced_data.get(key)
                }

                if category_data:
                    print(f"\n{category}:")
                    for key, value in category_data.items():
                        # Limit display length for longer strings
                        if isinstance(value, str) and len(value) > 100 and not verbose:
                            display_value = value[:97] + "..."
                        else:
                            display_value = value

                        # Format key for display
                        display_key = key.replace("_", " ").title()
                        print(f"  {display_key}: {display_value}")

            # Show acute toxicity notes separately, with truncation if needed
            if enhanced_data.get("acute_toxicity_notes"):
                print("\nAcute Toxicity Notes:")
                notes = enhanced_data["acute_toxicity_notes"]
                if len(notes) > 500 and not verbose:
                    print(f"  {notes[:500]}...\n  (use --verbose to see full text)")
                else:
                    print(f"  {notes}")
    except Exception as e:
        logger.error(f"Error during chemical query: {str(e)}")
        raise


def import_chemicals(
    file_path: str,
    skip_existing: bool = False,
    update_existing: bool = False,
    batch_size: int = 10,
) -> None:
    """
    Import chemicals from a file containing names or CAS numbers.

    Args:
        file_path: Path to the input file
        skip_existing: Skip chemicals already in the database
        update_existing: Update existing chemicals with new data
        batch_size: Number of chemicals to process in a batch
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
        # Process chemicals in batches
        for batch_start in range(0, len(chemicals), batch_size):
            batch_end = min(batch_start + batch_size, len(chemicals))
            batch = chemicals[batch_start:batch_end]

            logger.info(
                f"Processing batch {batch_start//batch_size + 1} ({batch_start+1}-{batch_end} of {len(chemicals)})"
            )

            for i, chemical in enumerate(batch, 1):
                item_number = batch_start + i
                logger.info(f"[{item_number}/{len(chemicals)}] Processing: {chemical}")

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
                        # Process the chemical data to enhance it
                        enhanced_data = process_chemical_data(chemical_data)

                        # Add to database
                        chem_id = db_manager.add_chemical(enhanced_data)
                        if chem_id:
                            logger.info(f"Stored chemical with ID: {chem_id}")
                        else:
                            logger.warning(
                                f"Failed to store chemical: {result['name']}"
                            )
                    else:
                        logger.warning(f"Failed to extract data for: {result['name']}")
                except Exception as e:
                    logger.error(f"Error processing chemical '{chemical}': {str(e)}")
                    continue

                # Be nice to the API - limit rate
                if i < len(batch):
                    time.sleep(1)

            # Add a longer delay between batches
            if batch_end < len(chemicals):
                time.sleep(5)

    logger.info("Import completed.")


def export_database(
    output_path: Optional[str] = None,
    output_format: str = "csv",
    filter_expr: Optional[str] = None,
) -> None:
    """
    Export the database to a file.

    Args:
        output_path: Path to the output file
        output_format: Format for output (csv, json, excel)
        filter_expr: Filter expression for chemicals (e.g. 'cas_number=64-17-5')
    """
    db_manager = DatabaseManager()

    # Get all chemicals with optional filtering
    if filter_expr:
        try:
            key, value = filter_expr.split("=", 1)
            key = key.strip()
            value = value.strip()

            chemicals = db_manager.search_chemicals(value)
            if not chemicals:
                logger.error(f"No chemicals found matching filter: {filter_expr}")
                return
        except ValueError:
            logger.error(
                f"Invalid filter expression: {filter_expr}. Use format 'key=value'"
            )
            return
    else:
        chemicals = db_manager.get_all_chemicals()

    if not chemicals:
        logger.error("No chemicals in database to export.")
        return

    # Define default output path if not provided
    if not output_path:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        extension = output_format
        if extension == "excel":
            extension = "xlsx"

        project_root = Path(__file__).parent.parent
        data_dir = project_root / "data" / "processed"
        os.makedirs(data_dir, exist_ok=True)

        filename = f"chemicals_export_{timestamp}.{extension}"
        output_path = str(data_dir / filename)

    try:
        # Process each chemical to ensure all properties are extracted
        enhanced_chemicals = [process_chemical_data(chem) for chem in chemicals]

        if output_format == "csv":
            path = db_manager.export_to_csv(output_path, enhanced_chemicals)
        elif output_format == "json":
            with open(output_path, "w") as f:
                json.dump(enhanced_chemicals, f, indent=2)
            path = output_path
        elif output_format == "excel":
            import pandas as pd

            df = pd.DataFrame(enhanced_chemicals)
            df.to_excel(output_path, index=False)
            path = output_path
        else:
            logger.error(f"Unsupported output format: {output_format}")
            return

        if path:
            logger.info(f"Exported {len(enhanced_chemicals)} chemicals to: {path}")
        else:
            logger.error("Failed to export database.")
    except Exception as e:
        logger.error(f"Error exporting database: {str(e)}")


def count_chemicals() -> None:
    """Count the number of chemicals in the database."""
    db_manager = DatabaseManager()
    count = db_manager.count_chemicals()
    logger.info(f"Total chemicals in database: {count}")


def delete_chemical(chemical: str, force: bool = False) -> None:
    """
    Delete a chemical from the database.

    Args:
        chemical: Chemical name or CAS number to delete
        force: Skip confirmation if True
    """
    db_manager = DatabaseManager()

    # Find the chemical in the database
    chemical_data = find_chemical_in_database(db_manager, chemical)

    if not chemical_data:
        return

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


def update_chemical(chemical: str, refresh: bool = False) -> None:
    """
    Update a chemical in the database.

    Args:
        chemical: Chemical name or CAS number to update
        refresh: Whether to fetch fresh data from the source
    """
    db_manager = DatabaseManager()

    # Find the chemical in the database
    chemical_data = find_chemical_in_database(db_manager, chemical)

    if not chemical_data:
        return

    try:
        if refresh:
            # Fetch fresh data from PubChem
            with PubChemScraper() as scraper:
                logger.info(f"Fetching fresh data for: {chemical_data.get('name')}")

                # Use the CAS number or name to search
                search_term = chemical_data.get("cas_number") or chemical_data.get(
                    "name"
                )
                results = scraper.search_chemical(search_term)

                if not results:
                    logger.error(f"No results found for: {search_term}")
                    return

                # Extract data for the first result
                result = results[0]
                logger.info(f"Found: {result['name']} (CID: {result['cid']})")

                fresh_data = scraper.extract_chemical_data(result)
                if not fresh_data:
                    logger.error(f"Failed to extract data for: {result['name']}")
                    return

                # Process the fresh data
                enhanced_data = process_chemical_data(fresh_data)

                # Update the database
                chem_id = db_manager.add_chemical(enhanced_data)
                logger.info(f"Updated chemical with ID: {chem_id}")
        else:
            # Just re-process existing data
            enhanced_data = process_chemical_data(chemical_data)

            # Update the database
            chem_id = db_manager.add_chemical(enhanced_data)
            logger.info(f"Updated chemical with ID: {chem_id}")

            # Show updated properties
            print(f"\nUpdated properties for {enhanced_data.get('name')}:")
            for key, value in enhanced_data.items():
                if key in ["ld50", "lc50"] and value:
                    print(f"  {key.upper()}: {value}")
    except Exception as e:
        logger.error(f"Error updating chemical '{chemical}': {str(e)}")


def show_version() -> None:
    """Show version information."""
    version = "1.0.0"  # Update this with your actual version
    print(f"Chemical Safety Database version {version}")
    print("Copyright (c) 2025")
    print("Licensed under MIT License")


def main() -> int:
    """Main entry point."""
    try:
        parser = setup_argparse()
        args = parser.parse_args()

        if args.command == "search":
            limit = getattr(args, "limit", 5)
            search_chemical(args.query, args.store, limit)
        elif args.command == "query":
            output_format = getattr(args, "format", "text")
            verbose = getattr(args, "verbose", False)
            query_chemical(args.chemical, args.property, output_format, verbose)
        elif args.command == "import":
            skip_existing = getattr(args, "skip_existing", False)
            update_existing = getattr(args, "update", False)
            batch_size = getattr(args, "batch_size", 10)
            import_chemicals(args.file, skip_existing, update_existing, batch_size)
        elif args.command == "export":
            output_format = getattr(args, "format", "csv")
            filter_expr = getattr(args, "filter", None)
            export_database(args.output, output_format, filter_expr)
        elif args.command == "count":
            count_chemicals()
        elif args.command == "delete":
            delete_chemical(args.chemical, args.force)
        elif args.command == "update":
            refresh = getattr(args, "refresh", False)
            update_chemical(args.chemical, refresh)
        elif args.command == "version":
            show_version()
        else:
            parser.print_help()
            return 1
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback

        logger.debug(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
