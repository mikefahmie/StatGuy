import os
import requests
from dotenv import load_dotenv
from supabase import create_client, Client
import time
from datetime import datetime, timezone

# Load environment variables
load_dotenv()

# Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
CBBD_API_BASE = os.getenv('CBBD_API_BASE', 'https://api.collegebasketballdata.com')
CBBD_API_KEY = os.getenv('CBDB_API_KEY')

# Validate environment variables
if not SUPABASE_URL:
    raise ValueError("SUPABASE_URL not found in environment variables")
if not SUPABASE_KEY:
    raise ValueError("SUPABASE_KEY not found in environment variables")
if not CBBD_API_KEY:
    raise ValueError("CBBD_API_KEY not found in environment variables")

print(f"✓ Loaded SUPABASE_URL: {SUPABASE_URL}")
print(f"✓ Loaded CBBD_API_KEY: {CBBD_API_KEY[:10]}...")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
print("✓ Supabase client connected")

# API Headers with authentication
HEADERS = {
    'accept': 'application/json',
    'Authorization': f'Bearer {CBBD_API_KEY}'
}

def get_all_teams():
    """Fetch all teams from your database"""
    response = supabase.table('teams').select('id, source_id').execute()
    return response.data

def import_roster_for_season(season):
    """Import roster data for all teams in a given season"""
    print(f"\n{'='*60}")
    print(f"SEASON {season}")
    print(f"{'='*60}")
    
    # Fetch ALL rosters for this season in ONE API call
    print(f"[1/3] Fetching roster data from API...")
    url = f"{CBBD_API_BASE}/teams/roster"
    params = {'season': season}
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        all_rosters = response.json()
        
        if not all_rosters:
            print(f"⚠️  No roster data available for season {season}")
            return {
                'season': season,
                'players_updated': 0,
                'rosters_inserted': 0,
                'errors': [f"No roster data available for season {season}"]
            }
        
        print(f"✓ Received {len(all_rosters)} teams from API")
        
    except Exception as e:
        print(f"✗ Error fetching rosters for season {season}: {e}")
        return {
            'season': season,
            'players_updated': 0,
            'rosters_inserted': 0,
            'errors': [f"API error: {e}"]
        }
    
    # Get all teams from database ONCE
    print(f"[2/3] Loading teams from database...")
    all_teams = get_all_teams()
    team_lookup = {str(t['source_id']): t['id'] for t in all_teams if t.get('source_id')}
    print(f"✓ Loaded {len(team_lookup)} Division I teams")
    
    print(f"[3/3] Processing players and roster entries...")
    players_updated = 0
    rosters_inserted = 0
    errors = []
    teams_without_match = []
    
    start_time = time.time()
    last_update = start_time
    
    for idx, roster_data in enumerate(all_rosters, 1):
        # Progress update every 100 teams OR every 10 seconds
        current_time = time.time()
        if idx % 100 == 0 or (current_time - last_update) >= 10:
            elapsed = current_time - start_time
            rate = idx / elapsed if elapsed > 0 else 0
            remaining = (len(all_rosters) - idx) / rate if rate > 0 else 0
            
            print(f"  → {idx}/{len(all_rosters)} teams | "
                  f"Players: {players_updated:,} | "
                  f"Rosters: {rosters_inserted:,} | "
                  f"Errors: {len(errors)} | "
                  f"ETA: {int(remaining)}s")
            last_update = current_time
        
        # Get team identifiers from API response
        team_source_id = str(roster_data.get('teamSourceId'))
        team_name = roster_data.get('team', 'unknown')
        
        # Look up our internal team_id
        team_id = team_lookup.get(team_source_id)
        if not team_id:
            teams_without_match.append(f"{team_name} (source_id: {team_source_id})")
            continue
        
        # Process players for this team
        players = roster_data.get('players', [])
        if not players:
            continue

        for player in players:
            try:
                # Process player
                upsert_player(player, season)
                players_updated += 1
                
                # Process roster entry
                upsert_roster_entry(player, team_id, season)
                rosters_inserted += 1
                    
            except Exception as e:
                error_msg = f"Player {player.get('name', 'unknown')} ({player.get('id', 'unknown')}): {e}"
                errors.append(error_msg)
    
    total_time = time.time() - start_time
    
    print(f"\n{'='*60}")
    print(f"✅ SEASON {season} COMPLETE (took {int(total_time)}s)")
    print(f"{'='*60}")
    print(f"Players updated:          {players_updated:,}")
    print(f"Roster entries inserted:  {rosters_inserted:,}")
    print(f"Teams matched (D-I):      {len(all_rosters) - len(teams_without_match)}")
    print(f"Teams skipped (non D-I):  {len(teams_without_match)}")
    print(f"Errors:                   {len(errors)}")
    
    # Show errors if any
    if errors:
        print(f"\n⚠️  Errors ({len(errors)} total):")
        for error in errors[:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")
    
    return {
        'season': season,
        'players_updated': players_updated,
        'rosters_inserted': rosters_inserted,
        'errors': errors
    }

def upsert_player(player_data, current_season):
    """Update or insert player in players table"""
    athlete_id = player_data['id']
    
    # Check if player exists
    existing = supabase.table('players').select('*').eq('id', athlete_id).execute()
    
    player_update = {
        'name': player_data['name'],
        'first_name': player_data.get('firstName'),
        'last_name': player_data.get('lastName'),
        'position': player_data.get('position'),
        'height': player_data.get('height'),
        'weight': player_data.get('weight'),
        'roster_data_available': True,
        'updated_at': datetime.now(timezone.utc).isoformat()
    }
    
    # Handle hometown data
    hometown = player_data.get('hometown', {})
    if hometown:
        player_update.update({
            'hometown_city': hometown.get('city'),
            'hometown_state': hometown.get('state'),
            'hometown_country': hometown.get('country')
        })
    
    if existing.data:
        # Player exists - update first_season if this is earlier
        existing_player = existing.data[0]
        player_start_season = player_data.get('startSeason', current_season)
        
        player_update['first_season'] = min(
            existing_player['first_season'], 
            player_start_season
        )
        player_update['last_season'] = max(
            existing_player.get('last_season', 0) or 0,
            player_data.get('endSeason', current_season)
        )
        
        # Update existing player
        supabase.table('players').update(player_update).eq('id', athlete_id).execute()
    else:
        # New player - insert
        player_update.update({
            'id': athlete_id,
            'source_id': player_data.get('sourceId'),
            'first_season': player_data.get('startSeason', current_season),
            'last_season': player_data.get('endSeason', current_season),
            'created_at': datetime.now(timezone.utc).isoformat()
        })
        
        supabase.table('players').insert(player_update).execute()
    
    return True

def upsert_roster_entry(player_data, team_id, season):
    """Insert or update roster entry in player_team_rosters"""
    roster_entry = {
        'athlete_id': player_data['id'],
        'team_id': team_id,
        'season': season,
        'jersey': player_data.get('jersey'),
        'position': player_data.get('position'),
        'height': player_data.get('height'),
        'weight': player_data.get('weight')
    }
    
    # Upsert (insert or update if exists)
    supabase.table('player_team_rosters').upsert(
        roster_entry,
        on_conflict='athlete_id,team_id,season'
    ).execute()
    
    return True

def save_progress(results):
    """Save progress to a log file"""
    with open('roster_import_progress.log', 'w') as f:
        f.write(f"Import progress as of {datetime.now()}\n")
        f.write("="*50 + "\n\n")
        for result in results:
            f.write(f"Season {result['season']}:\n")
            f.write(f"  Players: {result['players_updated']}\n")
            f.write(f"  Rosters: {result['rosters_inserted']}\n")
            f.write(f"  Errors: {len(result['errors'])}\n\n")

def run_full_import():
    """Import roster data for all seasons 2005-2026"""
    seasons = range(2005, 2027)  # 2005 through 2026
    all_results = []
    
    print("🏀 Starting full roster import for seasons 2005-2026...")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    for season in seasons:
        result = import_roster_for_season(season)
        all_results.append(result)
        
        # Save progress after each season
        save_progress(all_results)
        
        # Small delay between seasons
        time.sleep(0.5)
    
    print("\n" + "="*60)
    print("🎉 IMPORT COMPLETE!")
    print("="*60)
    
    # Summary statistics
    total_players = sum(r['players_updated'] for r in all_results)
    total_rosters = sum(r['rosters_inserted'] for r in all_results)
    total_errors = sum(len(r['errors']) for r in all_results)
    
    print(f"\nTotal players updated: {total_players:,}")
    print(f"Total roster entries: {total_rosters:,}")
    print(f"Total errors: {total_errors}")
    
    # Show seasons with errors
    if total_errors > 0:
        print("\n❌ Seasons with errors:")
        for result in all_results:
            if result['errors']:
                print(f"  {result['season']}: {len(result['errors'])} errors")
    
    return all_results

if __name__ == "__main__":
    run_full_import()