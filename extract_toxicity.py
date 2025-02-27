# extract_toxicity.py
import re
import sys
import logging
from src.database.db_manager import DatabaseManager

def extract_ld50_values(text):
    """Extract LD50 values from text."""
    if not text:
        return None
        
    ld50_values = []
    
    # Different LD50 formats to match
    patterns = [
        r"LD50:\s*([\d\.]+)\s*(mg/kg|g/kg).*?\(([^)]+)\)",  # Format: LD50: 5628 mg/kg (Oral, rat)
        r"LD50\s+(\w+)\s+(\w+)\s+([\d\.]+)\s+(g/[lL]|mg/kg)",  # Format: LD50 Mouse iv 2.0 g/L
        r"LD50.*?(\d+[\d\.]*).*?(mg/kg|g/kg|mg/L|g/L).*?\(([^)]+)\)"  # More general pattern
    ]
    
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            value = match.group(0).strip()
            if value and value not in ld50_values:
                ld50_values.append(value)
    
    if not ld50_values:
        return None
        
    return "; ".join(ld50_values)

def update_chemical(chemical_name):
    """Update a chemical's LD50 value."""
    db_manager = DatabaseManager()
    results = db_manager.search_chemicals(chemical_name)
    
    if not results:
        print(f"No chemical found matching: {chemical_name}")
        return
    
    chemical_data = results[0]
    
    # Check if acute_toxicity_notes contains LD50 data
    if 'acute_toxicity_notes' in chemical_data and chemical_data['acute_toxicity_notes']:
        notes = chemical_data['acute_toxicity_notes']
        ld50 = extract_ld50_values(notes)
        
        if ld50:
            # Update the database with the extracted LD50 value
            chemical_data['ld50'] = ld50
            db_manager.add_chemical(chemical_data)
            print(f"Updated {chemical_name} with LD50: {ld50}")
            return True
        else:
            print(f"No LD50 data found in toxicity notes for {chemical_name}")
            return False
    else:
        print(f"No toxicity notes found for {chemical_name}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_toxicity.py <chemical_name>")
        sys.exit(1)
    
    chemical_name = sys.argv[1]
    update_chemical(chemical_name)