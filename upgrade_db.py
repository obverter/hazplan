# update_toxicity.py
import re
from src.database.db_manager import DatabaseManager
from src.scrapers.pubchem_scraper import PubChemScraper

def extract_ld50_values(text):
    """Extract LD50 values from text."""
    if not text:
        return None
        
    ld50_values = []
    
    patterns = [
        r"LD50:\s*([\d\.]+)\s*(mg/kg|g/kg).*?\(([^)]+)\)", 
        r"LD50\s+(\w+)\s+(\w+)\s+([\d\.]+)\s+(g/[lL]|mg/kg)",
        r"LD50.*?(\d+[\d\.]*).*?(mg/kg|g/kg|mg/L|g/L).*?\(([^)]+)\)"
    ]
    
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            value = match.group(0).strip()
            if value and value not in ld50_values:
                ld50_values.append(value)
    
    if not ld50_values:
        return None
        
    return "; ".join(ld50_values)

def extract_lc50_values(text):
    """Extract LC50 values from text."""
    if not text:
        return None
        
    lc50_values = []
    
    patterns = [
        r"LC50\s+(\w+)\s+(\w+)\s+([\d\.]+)\s+(g/cu m|ppm).*?(\d+)\s*hr",
        r"LC50.*?(\d+[\d\.]*).*?(ppm|mg/[lL]|g/[lL]|mg/m3|g/m3)"
    ]
    
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            value = match.group(0).strip()
            if value and value not in lc50_values:
                lc50_values.append(value)
    
    if not lc50_values:
        return None
        
    return "; ".join(lc50_values)

def update_chemicals():
    """Update all chemicals with toxicity data."""
    db_manager = DatabaseManager()
    scraper = PubChemScraper()
    
    # Get all chemicals
    chemicals = db_manager.get_all_chemicals()
    print(f"Found {len(chemicals)} chemicals in database")
    
    for chemical in chemicals:
        name = chemical.get('name')
        cas = chemical.get('cas_number')
        print(f"Processing {name} (CAS: {cas})")
        
        # Get fresh data from PubChem
        try:
            results = scraper.search_chemical(name or cas)
            if results:
                result = results[0]
                cid = result.get('cid')
                print(f"Found CID: {cid}")
                
                # Get full JSON data
                full_json = scraper._get_full_json_data(cid)
                
                if full_json:
                    # Extract toxicity data
                    toxicity_data = scraper._extract_toxicity_data(full_json)
                    
                    if toxicity_data:
                        # Update chemical with toxicity data
                        chemical['ld50'] = toxicity_data.get('ld50')
                        chemical['lc50'] = toxicity_data.get('lc50')
                        chemical['acute_toxicity_notes'] = toxicity_data.get('acute_toxicity_notes')
                        
                        # Update database
                        db_manager.add_chemical(chemical)
                        print(f"Updated {name} with toxicity data")
                    else:
                        print(f"No toxicity data found for {name}")
                else:
                    print(f"No JSON data found for {name}")
            else:
                print(f"No results found for {name}")
        except Exception as e:
            print(f"Error processing {name}: {str(e)}")
    
    print("Update completed")

if __name__ == "__main__":
    update_chemicals()