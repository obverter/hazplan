"""
Example usage of the chemical safety database.

This script demonstrates how to use the PubChemScraper and DatabaseManager
to search for chemicals, retrieve their data, and store them in a database.
"""
import logging
import time

from src.database.db_manager import DatabaseManager
from src.scrapers.pubchem_scraper import PubChemScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Run the example script."""
    # Initialize the database
    db_manager = DatabaseManager()
    
    # List of chemicals to search for
    chemicals = [
        "acetone",
        "benzene",
        "ethanol",
        "methanol",
        "toluene",
        "chloroform",
        "sulfuric acid",
        "hydrogen peroxide",
        "sodium hydroxide",
        "hydrochloric acid",
        "ammonia",
        "formaldehyde",
    ]
    
    # Search for and store each chemical
    with PubChemScraper() as scraper:
        for chem in chemicals:
            logger.info(f"Searching for: {chem}")
            results = scraper.search_chemical(chem)
            
            if not results:
                logger.warning(f"No results found for: {chem}")
                continue
            
            # Use the first result
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
            time.sleep(1)
    
    # Print database summary
    count = db_manager.count_chemicals()
    logger.info(f"Total chemicals in database: {count}")
    
    # Export to CSV
    csv_path = db_manager.export_to_csv()
    if csv_path:
        logger.info(f"Exported database to: {csv_path}")


if __name__ == "__main__":
    main()