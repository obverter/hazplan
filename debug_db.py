# debug_database.py
from src.database.db_manager import DatabaseManager

# Connect to the database
db_manager = DatabaseManager()

# Get all chemicals
chemicals = db_manager.get_all_chemicals()
print(f"Found {len(chemicals)} chemicals in database")

for chemical in chemicals:
    print(f"Chemical: {chemical.get('name', 'Unknown')}")
    print(f"Fields: {sorted(chemical.keys())}")
    
    # Check specifically for any field that might contain toxicity data
    toxicity_related_fields = [
        'acute_toxicity_notes', 'toxicity_notes', 'toxicity_data', 
        'ld50', 'lc50', 'acute_toxicity', 'toxicity'
    ]
    
    for field in toxicity_related_fields:
        if field in chemical and chemical[field]:
            print(f"Found data in field '{field}': {chemical[field][:100]}...")
    
    print("-" * 50)