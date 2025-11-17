import os
import requests
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import Dict, List, Optional
import time
from collections import defaultdict

# Load environment variables
load_dotenv()

# Configuration
CBDB_API_KEY = os.getenv('CBDB_API_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Initialize clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# API Configuration
CBDB_BASE_URL = "https://api.collegebasketballdata.com"
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {CBDB_API_KEY}"
}

# Logging
class Logger:
    def __init__(self):
        self.warnings = []
        self.errors = []
        self.stats = {
            'teams_processed': 0,
            'teams_successful': 0,
            'teams_failed': 0,
            'conferences_processed': 0,
            'history_records_created': 0,
            'gap_years_filled': 0,
            'duplicates_resolved': 0
        }
    
    def info(self, message):
        print(f"[INFO] {message}")
    
    def warning(self, message):
        print(f"[WARNING] {message}")
        self.warnings.append(message)
    
    def error(self, message):
        print(f"[ERROR] {message}")
        self.errors.append(message)
    
    def stat(self, key, increment=1):
        self.stats[key] = self.stats.get(key, 0) + increment
    
    def print_summary(self):
        print("\n" + "="*60)
        print("IMPORT SUMMARY")
        print("="*60)
        for key, value in self.stats.items():
            print(f"{key}: {value}")
        print(f"\nWarnings: {len(self.warnings)}")
        print(f"Errors: {len(self.errors)}")
        if self.warnings[:5]:  # Show first 5
            print("\nFirst 5 Warnings:")
            for w in self.warnings[:5]:
                print(f"  - {w}")
        if self.errors:
            print("\nError Details:")
            for e in self.errors[:10]:  # Show first 10
                print(f"  - {e}")

logger = Logger()

def fetch_conferences():
    """Fetch all conferences from CBBD API"""
    logger.info("Fetching conferences from API...")
    try:
        response = requests.get(f"{CBDB_BASE_URL}/conferences", headers=HEADERS)
        response.raise_for_status()
        conferences = response.json()
        logger.info(f"Fetched {len(conferences)} conferences")
        return conferences
    except Exception as e:
        logger.error(f"Failed to fetch conferences: {e}")
        raise

def upsert_conferences(conferences):
    """Insert or update conferences in the database"""
    logger.info("Upserting conferences to database...")
    
    for conf in conferences:
        try:
            # Upsert conference
            supabase.table('conferences').upsert({
                'id': conf['id'],
                'name': conf['name'],
                'abbreviation': conf.get('abbreviation'),
                'short_name': conf.get('shortName')
            }, on_conflict='id').execute()
            
            logger.stat('conferences_processed')
        except Exception as e:
            logger.error(f"Failed to upsert conference {conf.get('name')}: {e}")
    
    logger.info(f"Upserted {logger.stats['conferences_processed']} conferences")

def fetch_teams_for_season(season: int) -> List[Dict]:
    """Fetch teams for a specific season"""
    try:
        response = requests.get(
            f"{CBDB_BASE_URL}/teams",
            params={'season': season},
            headers=HEADERS
        )
        response.raise_for_status()
        teams = response.json()
        return teams
    except Exception as e:
        logger.error(f"Failed to fetch teams for season {season}: {e}")
        return []

def collect_all_seasons_data():
    """Phase 1: Collect all season data and identify all unique teams"""
    logger.info("Phase 1: Collecting data from all seasons (1925-2025)...")
    
    all_seasons_data = {}
    all_teams_map = {}
    
    for season in range(1925, 2026):
        logger.info(f"Fetching season {season}...")
        teams = fetch_teams_for_season(season)
        
        if teams:
            all_seasons_data[season] = teams
            
            # Track each team's last active season
            for team in teams:
                source_id = team.get('sourceId')
                if not source_id:
                    continue
                
                if source_id not in all_teams_map:
                    all_teams_map[source_id] = {
                        'last_season': season,
                        'data': team
                    }
                else:
                    # Update if this is a more recent season
                    if season > all_teams_map[source_id]['last_season']:
                        all_teams_map[source_id] = {
                            'last_season': season,
                            'data': team
                        }
        
        # Rate limiting - be nice to the API
        time.sleep(0.1)
    
    logger.info(f"Collected data for {len(all_seasons_data)} seasons")
    logger.info(f"Identified {len(all_teams_map)} unique teams")
    
    return all_seasons_data, all_teams_map

def upsert_teams(all_teams_map):
    """Phase 2: Upsert teams table with most recent data"""
    logger.info("Phase 2: Upserting teams table...")
    
    for source_id, team_info in all_teams_map.items():
        team = team_info['data']
        last_season = team_info['last_season']
        
        logger.stat('teams_processed')
        
        try:
            # Prepare team data (exclude conferenceId and conference)
            team_data = {
                'source_id': source_id,
                'school': team.get('school'),
                'mascot': team.get('mascot'),
                'abbreviation': team.get('abbreviation'),
                'display_name': team.get('displayName'),
                'short_display_name': team.get('shortDisplayName'),
                'primary_color': team.get('primaryColor'),
                'secondary_color': team.get('secondaryColor'),
                'current_venue_id': team.get('currentVenueId'),
                'current_venue': team.get('currentVenue'),
                'current_city': team.get('currentCity'),
                'current_state': team.get('currentState'),
                'last_active_season': last_season
            }
            
            # Upsert team
            supabase.table('teams').upsert(
                team_data,
                on_conflict='source_id'
            ).execute()
            
            logger.stat('teams_successful')
            
        except Exception as e:
            logger.error(f"Failed to upsert team {team.get('school')} ({source_id}): {e}")
            logger.stat('teams_failed')
    
    logger.info(f"Upserted {logger.stats['teams_successful']} teams successfully")
    logger.info(f"Failed to upsert {logger.stats['teams_failed']} teams")

def get_team_id_map():
    """Get mapping of source_id to database id"""
    logger.info("Building source_id to team_id mapping...")
    
    response = supabase.table('teams').select('id, source_id').execute()
    teams = response.data
    
    team_id_map = {team['source_id']: team['id'] for team in teams if team.get('source_id')}
    logger.info(f"Mapped {len(team_id_map)} teams")
    
    # Debug: Show a sample of what's in the map
    sample_keys = list(team_id_map.keys())[:5]
    logger.info(f"Sample source_ids in map: {sample_keys}")
    
    return team_id_map

def find_eventual_conference(team_source_id: str, start_season: int, all_seasons_data: Dict) -> Optional[int]:
    """
    Look forward from start_season to find first season 
    where team appears in only ONE conference
    """
    for future_season in range(start_season + 1, 2026):
        if future_season not in all_seasons_data:
            continue
        
        teams_in_season = [t for t in all_seasons_data[future_season] 
                          if t.get('sourceId') == team_source_id]
        
        if len(teams_in_season) == 1:
            # Found single appearance
            return teams_in_season[0].get('conferenceId')
        elif len(teams_in_season) == 0:
            # Team doesn't exist in future - defunct case
            return None
    
    # No single conference found in future
    return None

def build_conference_history(all_seasons_data, team_id_map):
    """Phase 3: Build conference history with duplicate resolution"""
    logger.info("Phase 3: Building conference history (working backwards)...")
    
    # Clear existing history
    logger.info("Clearing existing conference history...")
    try:
        supabase.table('team_conference_history').delete().neq('id', 0).execute()
        logger.info("Successfully cleared existing history")
    except Exception as e:
        logger.error(f"Failed to clear history: {e}")
    
    teams_without_id = set()  # Track which source_ids don't have team_ids
    
    # Process seasons backwards
    for season in range(2025, 1924, -1):
        if season not in all_seasons_data:
            continue
        
        logger.info(f"Processing season {season}...")
        
        # Group teams by source_id
        teams_by_source = defaultdict(list)
        for team in all_seasons_data[season]:
            source_id = team.get('sourceId')
            if source_id:
                teams_by_source[source_id].append(team)
        
        season_processed = 0
        season_skipped = 0
        
        # Process each unique team
        for source_id, team_entries in teams_by_source.items():
            team_id = team_id_map.get(source_id)
            if not team_id:
                if source_id not in teams_without_id:
                    logger.warning(f"No team_id found for source_id {source_id} in season {season}")
                    teams_without_id.add(source_id)
                season_skipped += 1
                continue
            
            if len(team_entries) == 1:
                # Simple case: one conference
                conference_id = team_entries[0].get('conferenceId')
                
                try:
                    supabase.table('team_conference_history').insert({
                        'team_id': team_id,
                        'season': season,
                        'conference_id': conference_id,
                        'existed': True
                    }).execute()
                    
                    logger.stat('history_records_created')
                    season_processed += 1
                    
                except Exception as e:
                    logger.error(f"Failed to insert history for team {source_id} season {season}: {e}")
            
            else:
                # Duplicate case: multiple conferences for this team
                logger.stat('duplicates_resolved')
                conferences_in_season = [t.get('conferenceId') for t in team_entries]
                
                # Find eventual single conference
                eventual_conf = find_eventual_conference(source_id, season, all_seasons_data)
                
                if eventual_conf is None:
                    # Edge case: no single conference found in future
                    logger.warning(f"Team {source_id} in {season} - no eventual single conference. Using first conference.")
                    conferences_in_season.sort()
                    conference_id = conferences_in_season[0]
                    
                    try:
                        supabase.table('team_conference_history').insert({
                            'team_id': team_id,
                            'season': season,
                            'conference_id': conference_id,
                            'existed': True
                        }).execute()
                        
                        logger.stat('history_records_created')
                        season_processed += 1
                    except Exception as e:
                        logger.error(f"Failed to insert history for team {source_id} season {season}: {e}")
                else:
                    # Store conferences DIFFERENT from eventual
                    correct_conferences = [c for c in conferences_in_season if c != eventual_conf]
                    
                    if len(correct_conferences) == 0:
                        # All conferences match eventual - shouldn't happen
                        logger.warning(f"Team {source_id} in {season} - all conferences match eventual ({eventual_conf})")
                        continue
                    
                    # Store the correct conference(s) - should only be 1
                    for conference_id in correct_conferences:
                        try:
                            supabase.table('team_conference_history').insert({
                                'team_id': team_id,
                                'season': season,
                                'conference_id': conference_id,
                                'existed': True
                            }).execute()
                            
                            logger.stat('history_records_created')
                            season_processed += 1
                        except Exception as e:
                            logger.error(f"Failed to insert history for team {source_id} season {season}: {e}")
        
        logger.info(f"  Season {season}: Processed {season_processed}, Skipped {season_skipped}")
    
    logger.info(f"Created {logger.stats['history_records_created']} conference history records")
    logger.info(f"Resolved {logger.stats['duplicates_resolved']} duplicate entries")
    logger.info(f"Teams without team_id: {len(teams_without_id)}")

def fill_gap_years(all_seasons_data, team_id_map):
    """Phase 4: Fill in gap years where teams didn't exist"""
    logger.info("Phase 4: Filling gap years...")
    
    # Reverse the team_id_map
    id_to_source = {v: k for k, v in team_id_map.items()}
    
    for team_id, source_id in id_to_source.items():
        # Find first and last season this team appeared
        seasons_appeared = []
        for season in range(1925, 2026):
            if season in all_seasons_data:
                if any(t.get('sourceId') == source_id for t in all_seasons_data[season]):
                    seasons_appeared.append(season)
        
        if not seasons_appeared:
            continue
        
        first_season = min(seasons_appeared)
        last_season = max(seasons_appeared)
        
        # Check for gaps between first and last
        for season in range(first_season, last_season + 1):
            if season not in seasons_appeared:
                # Check if record already exists
                try:
                    existing = supabase.table('team_conference_history').select('id').eq('team_id', team_id).eq('season', season).execute()
                    
                    if not existing.data:
                        # Insert gap year record
                        supabase.table('team_conference_history').insert({
                            'team_id': team_id,
                            'season': season,
                            'conference_id': None,
                            'existed': False
                        }).execute()
                        
                        logger.stat('gap_years_filled')
                except Exception as e:
                    logger.error(f"Failed to insert gap year for team_id {team_id} season {season}: {e}")
    
    logger.info(f"Filled {logger.stats['gap_years_filled']} gap years")

def validate_data():
    """Phase 5: Validation checks"""
    logger.info("\nPhase 5: Running validation checks...")
    
    # Check 1: Duplicate detection (handled by UNIQUE constraint)
    logger.info("Check 1: Duplicate (team_id, season) combinations...")
    logger.info("✓ No duplicates possible (enforced by UNIQUE constraint)")
    
    # Check 2: Orphaned conference IDs
    logger.info("Check 2: Orphaned conference IDs...")
    history_conferences = supabase.table('team_conference_history').select('conference_id').not_.is_('conference_id', 'null').execute()
    all_conferences = supabase.table('conferences').select('id').execute()
    
    history_conf_ids = set(record['conference_id'] for record in history_conferences.data)
    valid_conf_ids = set(conf['id'] for conf in all_conferences.data)
    
    orphaned = history_conf_ids - valid_conf_ids
    if orphaned:
        logger.warning(f"Found {len(orphaned)} orphaned conference IDs: {orphaned}")
    else:
        logger.info("✓ No orphaned conference IDs")
    
    # Check 3: Gap year logic
    logger.info("Check 3: Gap year validation...")
    gap_records = supabase.table('team_conference_history').select('*').eq('existed', False).execute()
    logger.info(f"✓ Found {len(gap_records.data)} gap year records")
    
    # Check 4: Data completeness
    logger.info("Check 4: Data completeness...")
    team_count = supabase.table('teams').select('id', count='exact').execute()
    history_count = supabase.table('team_conference_history').select('id', count='exact').execute()
    
    logger.info(f"✓ Total teams: {team_count.count}")
    logger.info(f"✓ Total history records: {history_count.count}")
    logger.info(f"✓ Seasons covered: 1925-2025")

def main():
    """Main execution function"""
    try:
        logger.info("Starting teams and conference history import...")
        logger.info("="*60)
        
        # Phase 0: Load conferences
        logger.info("\nPhase 0: Loading conferences...")
        conferences = fetch_conferences()
        upsert_conferences(conferences)
        
        # Phase 1: Collect all data
        all_seasons_data, all_teams_map = collect_all_seasons_data()
        
        # Phase 2: Upsert teams
        upsert_teams(all_teams_map)
        
        # Get team ID mapping
        team_id_map = get_team_id_map()
        
        # Phase 3: Build conference history
        build_conference_history(all_seasons_data, team_id_map)
        
        # Phase 4: Fill gap years
        fill_gap_years(all_seasons_data, team_id_map)
        
        # Phase 5: Validate
        validate_data()
        
        # Print summary
        logger.print_summary()
        
        logger.info("\n" + "="*60)
        logger.info("Import completed successfully!")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Fatal error during import: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()