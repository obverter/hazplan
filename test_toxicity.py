import re
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

# Connect to the database
db_manager = DatabaseManager()

# Get all chemicals
chemicals = db_manager.get_all_chemicals()
print(f"Found {len(chemicals)} chemicals in database")

for chemical in chemicals:
    # Print all fields to debug what's available
    for key, value in chemical.items():
        if key == 'acute_toxicity_notes' and value:
            print(f"Found toxicity notes for {chemical.get('name')}")
            ld50 = extract_ld50_values(value)
            
            if ld50:
                # Update chemical with LD50 value
                chemical['ld50'] = ld50
                db_manager.add_chemical(chemical)
                print(f"Updated {chemical.get('name')} with LD50: {ld50}")
            else:
                print(f"No LD50 data found in toxicity notes for {chemical.get('name')}")