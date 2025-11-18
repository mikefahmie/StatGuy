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

def import_all_rankings():
    """Import all rankings from CBBD API in one call"""
    print(f"\n{'='*60}")
    print("IMPORTING ALL RANKINGS")
    print(f"{'='*60}")
    
    # Fetch ALL rankings from API (no season parameter)
    print("[1/4] Fetching all rankings from API...")
    url = f"{CBBD_API_BASE}/rankings"
    
    try:
        print("  → Making API request (this may take 10-30 seconds)...")
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        rankings_data = response.json()
        
        if not rankings_data:
            print("⚠️  No rankings data available")
            return
        
        print(f"✓ Received {len(rankings_data):,} ranking records from API")
        
    except Exception as e:
        print(f"✗ Error fetching rankings: {e}")
        return
    
    # Analyze the data
    print("\n[2/4] Analyzing data...")
    seasons = set()
    poll_types = set()
    teams = set()
    records_with_poll_date = 0
    
    for record in rankings_data:
        seasons.add(record.get('season'))
        poll_types.add(record.get('pollType'))
        teams.add(record.get('teamId'))
        if record.get('pollDate'):
            records_with_poll_date += 1
    
    print(f"  → Seasons covered: {min(seasons)} to {max(seasons)}")
    print(f"  → Poll types: {', '.join(sorted(poll_types))}")
    print(f"  → Unique teams: {len(teams)}")
    print(f"  → Records with poll_date: {records_with_poll_date:,} ({records_with_poll_date/len(rankings_data)*100:.1f}%)")
    
    # Process rankings and DEDUPLICATE
    print("\n[3/4] Processing and deduplicating rankings...")
    
    # Use a dictionary keyed by the composite primary key to deduplicate
    rankings_dict = {}
    errors = []
    duplicates_found = 0
    
    for rank_data in rankings_data:
        try:
            # Create composite key
            composite_key = (
                rank_data['season'],
                rank_data['seasonType'],
                rank_data['week'],
                rank_data['pollType'],
                rank_data['teamId']
            )
            
            # Parse the poll date (nullable)
            poll_date = None
            if rank_data.get('pollDate'):
                try:
                    # Parse ISO format: "2024-10-14T00:00:00.000Z"
                    poll_date = datetime.fromisoformat(
                        rank_data['pollDate'].replace('Z', '+00:00')
                    ).date().isoformat()
                except Exception as e:
                    # If date parsing fails, leave as None
                    pass
            
            ranking_record = {
                'season': rank_data['season'],
                'season_type': rank_data['seasonType'],
                'week': rank_data['week'],
                'poll_date': poll_date,  # Now nullable
                'poll_type': rank_data['pollType'],
                'team_id': rank_data['teamId'],
                'team': rank_data['team'],
                'conference': rank_data.get('conference'),
                'ranking': rank_data.get('ranking'),  # nullable (others receiving votes)
                'points': rank_data.get('points', 0),
                'first_place_votes': rank_data.get('firstPlaceVotes', 0),
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Check for duplicates
            if composite_key in rankings_dict:
                duplicates_found += 1
                # Keep the one with more data (prefer non-null poll_date, higher points, etc.)
                existing = rankings_dict[composite_key]
                if (poll_date and not existing['poll_date']) or \
                   (rank_data.get('points', 0) > existing['points']):
                    rankings_dict[composite_key] = ranking_record
            else:
                rankings_dict[composite_key] = ranking_record
            
        except Exception as e:
            error_msg = (f"Team {rank_data.get('team', 'unknown')} "
                        f"season {rank_data.get('season')} "
                        f"week {rank_data.get('week')}: {e}")
            errors.append(error_msg)
    
    # Convert dict back to list
    rankings_to_insert = list(rankings_dict.values())
    
    print(f"  → Original records: {len(rankings_data):,}")
    print(f"  → Duplicates found: {duplicates_found:,}")
    print(f"  → Unique records to insert: {len(rankings_to_insert):,}")
    
    # Batch insert in chunks of 1000
    print(f"\n[4/4] Inserting rankings into database...")
    rankings_inserted = 0
    chunk_size = 1000
    
    if rankings_to_insert:
        print(f"  → Inserting in batches of {chunk_size}...")
        
        for i in range(0, len(rankings_to_insert), chunk_size):
            chunk = rankings_to_insert[i:i + chunk_size]
            try:
                supabase.table('rankings').upsert(
                    chunk,
                    on_conflict='season,season_type,week,poll_type,team_id'
                ).execute()
                rankings_inserted += len(chunk)
                
                # Progress update
                progress = min(100, (i + chunk_size) / len(rankings_to_insert) * 100)
                print(f"    → Batch {i//chunk_size + 1}: {rankings_inserted:,}/{len(rankings_to_insert):,} ({progress:.0f}%)")
                
            except Exception as e:
                error_msg = f"Batch insert error at offset {i}: {e}"
                errors.append(error_msg)
                print(f"    ✗ {error_msg}")
    
    # Report results
    print(f"\n{'='*60}")
    print("✅ RANKINGS IMPORT COMPLETE")
    print(f"{'='*60}")
    print(f"API records received:     {len(rankings_data):,}")
    print(f"Duplicates removed:       {duplicates_found:,}")
    print(f"Unique records inserted:  {rankings_inserted:,}")
    print(f"Errors:                   {len(errors)}")
    
    if errors:
        print(f"\n⚠️  Errors ({len(errors)} total):")
        for error in errors[:10]:
            print(f"  - {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")

if __name__ == "__main__":
    import_all_rankings()