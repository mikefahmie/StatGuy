import os
import requests
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime, timezone

# Load environment variables
load_dotenv()

# Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
CBBD_API_BASE = os.getenv('CBBD_API_BASE', 'https://api.collegebasketballdata.com')
CBBD_API_KEY = os.getenv('CBDB_API_KEY')

# Validate environment variables
if not all([SUPABASE_URL, SUPABASE_KEY, CBBD_API_KEY]):
    raise ValueError("Missing required environment variables")

print(f"✓ Loaded SUPABASE_URL: {SUPABASE_URL}")
print(f"✓ Loaded CBBD_API_KEY: {CBBD_API_KEY[:10]}...")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
print("✓ Supabase client connected")

# API Headers
HEADERS = {
    'accept': 'application/json',
    'Authorization': f'Bearer {CBBD_API_KEY}'
}

def import_venues():
    """Import all venues from CBBD API"""
    print(f"\n{'='*60}")
    print("IMPORTING VENUES")
    print(f"{'='*60}")
    
    # Fetch venues from API
    print("[1/2] Fetching venues from API...")
    url = f"{CBBD_API_BASE}/venues"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        venues_data = response.json()
        
        if not venues_data:
            print("⚠️  No venue data available")
            return
        
        print(f"✓ Received {len(venues_data)} venues from API")
        
    except Exception as e:
        print(f"✗ Error fetching venues: {e}")
        return
    
    # Process and insert venues
    print("[2/2] Inserting venues into database...")
    
    venues_to_insert = []
    errors = []
    
    for venue in venues_data:
        try:
            venue_record = {
                'id': venue['id'],
                'source_id': venue.get('sourceId'),
                'name': venue['name'],
                'city': venue.get('city'),
                'state': venue.get('state'),
                'country': venue.get('country'),
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            venues_to_insert.append(venue_record)
            
        except Exception as e:
            errors.append(f"Venue {venue.get('name', 'unknown')}: {e}")
    
    # Batch insert
    if venues_to_insert:
        try:
            # Use upsert to handle any duplicates
            supabase.table('venues').upsert(
                venues_to_insert,
                on_conflict='id'
            ).execute()
            
            print(f"✅ Successfully inserted {len(venues_to_insert)} venues")
            
        except Exception as e:
            print(f"✗ Error inserting venues: {e}")
            return
    
    # Report results
    print(f"\n{'='*60}")
    print("✅ VENUES IMPORT COMPLETE")
    print(f"{'='*60}")
    print(f"Venues inserted: {len(venues_to_insert):,}")
    print(f"Errors: {len(errors)}")
    
    if errors:
        print(f"\n⚠️  Errors ({len(errors)} total):")
        for error in errors[:10]:
            print(f"  - {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")

if __name__ == "__main__":
    import_venues()