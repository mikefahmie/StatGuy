import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import glob
import math
from datetime import datetime

# Load environment variables
load_dotenv()

# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env file")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print("🏀 Starting Team Game Stats Import...")
print(f"📊 Connected to Supabase: {SUPABASE_URL}")

# Column mapping: CSV camelCase -> Database snake_case
COLUMN_MAPPING = {
    'gameId': 'game_id',
    'season': 'season',
    'seasonLabel': 'season_label',
    'seasonType': 'season_type',
    'startDate': 'start_date',
    'startTimeTbd': 'start_time_tbd',
    'teamId': 'team_id',
    'team': 'team',
    'opponentId': 'opponent_id',
    'opponent': 'opponent',
    # REMOVED: conference and opponent_conference (data quality issues)
    'neutralSite': 'neutral_site',
    'isHome': 'is_home',
    'conferenceGame': 'conference_game',
    'gameType': 'game_type',
    'notes': 'notes',
    'gameMinutes': 'game_minutes',
    'tournament': 'tournament',
    'teamSeed': 'team_seed',
    'opponentSeed': 'opponent_seed',
    'pace': 'pace',
    
    # Team Stats
    'teamStats_fieldGoals_pct': 'team_field_goals_pct',
    'teamStats_fieldGoals_attempted': 'team_field_goals_attempted',
    'teamStats_fieldGoals_made': 'team_field_goals_made',
    'teamStats_twoPointFieldGoals_pct': 'team_two_point_field_goals_pct',
    'teamStats_twoPointFieldGoals_attempted': 'team_two_point_field_goals_attempted',
    'teamStats_twoPointFieldGoals_made': 'team_two_point_field_goals_made',
    'teamStats_threePointFieldGoals_pct': 'team_three_point_field_goals_pct',
    'teamStats_threePointFieldGoals_attempted': 'team_three_point_field_goals_attempted',
    'teamStats_threePointFieldGoals_made': 'team_three_point_field_goals_made',
    'teamStats_freeThrows_pct': 'team_free_throws_pct',
    'teamStats_freeThrows_attempted': 'team_free_throws_attempted',
    'teamStats_freeThrows_made': 'team_free_throws_made',
    'teamStats_rebounds_total': 'team_rebounds_total',
    'teamStats_rebounds_defensive': 'team_rebounds_defensive',
    'teamStats_rebounds_offensive': 'team_rebounds_offensive',
    'teamStats_turnovers_teamTotal': 'team_turnovers_team_total',
    'teamStats_turnovers_total': 'team_turnovers_total',
    'teamStats_fouls_flagrant': 'team_fouls_flagrant',
    'teamStats_fouls_technical': 'team_fouls_technical',
    'teamStats_fouls_total': 'team_fouls_total',
    'teamStats_points_fastBreak': 'team_points_fast_break',
    'teamStats_points_offTurnovers': 'team_points_off_turnovers',
    'teamStats_points_inPaint': 'team_points_in_paint',
    'teamStats_points_total': 'team_points_total',
    'teamStats_points_largestLead': 'team_points_largest_lead',
    'teamStats_points_byPeriod_0': 'team_points_by_period_0',
    'teamStats_points_byPeriod_1': 'team_points_by_period_1',
    'teamStats_points_byPeriod_2': 'team_points_by_period_2',
    'teamStats_points_byPeriod_3': 'team_points_by_period_3',
    'teamStats_points_byPeriod_4': 'team_points_by_period_4',
    'teamStats_points_byPeriod_5': 'team_points_by_period_5',
    'teamStats_points_byPeriod_6': 'team_points_by_period_6',
    'teamStats_points_byPeriod_7': 'team_points_by_period_7',
    'teamStats_points_byPeriod_8': 'team_points_by_period_8',
    'teamStats_points_byPeriod_9': 'team_points_by_period_9',
    'teamStats_points_byPeriod_10': 'team_points_by_period_10',
    'teamStats_fourFactors_freeThrowRate': 'team_four_factors_free_throw_rate',
    'teamStats_fourFactors_offensiveReboundPct': 'team_four_factors_offensive_rebound_pct',
    'teamStats_fourFactors_effectiveFieldGoalPct': 'team_four_factors_effective_field_goal_pct',
    'teamStats_fourFactors_turnoverRatio': 'team_four_factors_turnover_ratio',
    'teamStats_assists': 'team_assists',
    'teamStats_blocks': 'team_blocks',
    'teamStats_steals': 'team_steals',
    'teamStats_trueShooting': 'team_true_shooting',
    'teamStats_gameScore': 'team_game_score',
    'teamStats_possessions': 'team_possessions',
    'teamStats_rating': 'team_rating',
    
    # Opponent Stats
    'opponentStats_fieldGoals_pct': 'opponent_field_goals_pct',
    'opponentStats_fieldGoals_attempted': 'opponent_field_goals_attempted',
    'opponentStats_fieldGoals_made': 'opponent_field_goals_made',
    'opponentStats_twoPointFieldGoals_pct': 'opponent_two_point_field_goals_pct',
    'opponentStats_twoPointFieldGoals_attempted': 'opponent_two_point_field_goals_attempted',
    'opponentStats_twoPointFieldGoals_made': 'opponent_two_point_field_goals_made',
    'opponentStats_threePointFieldGoals_pct': 'opponent_three_point_field_goals_pct',
    'opponentStats_threePointFieldGoals_attempted': 'opponent_three_point_field_goals_attempted',
    'opponentStats_threePointFieldGoals_made': 'opponent_three_point_field_goals_made',
    'opponentStats_freeThrows_pct': 'opponent_free_throws_pct',
    'opponentStats_freeThrows_attempted': 'opponent_free_throws_attempted',
    'opponentStats_freeThrows_made': 'opponent_free_throws_made',
    'opponentStats_rebounds_total': 'opponent_rebounds_total',
    'opponentStats_rebounds_defensive': 'opponent_rebounds_defensive',
    'opponentStats_rebounds_offensive': 'opponent_rebounds_offensive',
    'opponentStats_turnovers_teamTotal': 'opponent_turnovers_team_total',
    'opponentStats_turnovers_total': 'opponent_turnovers_total',
    'opponentStats_fouls_flagrant': 'opponent_fouls_flagrant',
    'opponentStats_fouls_technical': 'opponent_fouls_technical',
    'opponentStats_fouls_total': 'opponent_fouls_total',
    'opponentStats_points_fastBreak': 'opponent_points_fast_break',
    'opponentStats_points_offTurnovers': 'opponent_points_off_turnovers',
    'opponentStats_points_inPaint': 'opponent_points_in_paint',
    'opponentStats_points_total': 'opponent_points_total',
    'opponentStats_points_largestLead': 'opponent_points_largest_lead',
    'opponentStats_points_byPeriod_0': 'opponent_points_by_period_0',
    'opponentStats_points_byPeriod_1': 'opponent_points_by_period_1',
    'opponentStats_points_byPeriod_2': 'opponent_points_by_period_2',
    'opponentStats_points_byPeriod_3': 'opponent_points_by_period_3',
    'opponentStats_points_byPeriod_4': 'opponent_points_by_period_4',
    'opponentStats_points_byPeriod_5': 'opponent_points_by_period_5',
    'opponentStats_points_byPeriod_6': 'opponent_points_by_period_6',
    'opponentStats_points_byPeriod_7': 'opponent_points_by_period_7',
    'opponentStats_points_byPeriod_8': 'opponent_points_by_period_8',
    'opponentStats_points_byPeriod_9': 'opponent_points_by_period_9',
    'opponentStats_points_byPeriod_10': 'opponent_points_by_period_10',
    'opponentStats_fourFactors_freeThrowRate': 'opponent_four_factors_free_throw_rate',
    'opponentStats_fourFactors_offensiveReboundPct': 'opponent_four_factors_offensive_rebound_pct',
    'opponentStats_fourFactors_effectiveFieldGoalPct': 'opponent_four_factors_effective_field_goal_pct',
    'opponentStats_fourFactors_turnoverRatio': 'opponent_four_factors_turnover_ratio',
    'opponentStats_assists': 'opponent_assists',
    'opponentStats_blocks': 'opponent_blocks',
    'opponentStats_steals': 'opponent_steals',
    'opponentStats_trueShooting': 'opponent_true_shooting',
    'opponentStats_gameScore': 'opponent_game_score',
    'opponentStats_possessions': 'opponent_possessions',
    'opponentStats_rating': 'opponent_rating',
}

# Option to clear existing data
print("\n❓ Clear existing data in team_game_stats table first?")
print("1. Yes - delete all existing stats")
print("2. No - attempt to add to existing data")
clear_choice = input("Enter choice (1 or 2): ").strip()

if clear_choice == "1":
    print("\n🗑️  Clearing existing data...")
    print("   Run this SQL command manually in Supabase:")
    print("   DELETE FROM team_game_stats;")
    print("\n   Press Enter when done...")
    input()

# Find all CSV files in team_game_stats folder
csv_folder = 'team_game_stats'
csv_files = sorted(glob.glob(f'{csv_folder}/*.csv'))

if not csv_files:
    print(f"\n❌ No CSV files found in {csv_folder}/ folder!")
    print(f"   Make sure you have files like: {csv_folder}/2024.csv")
    exit(1)

print(f"\n📂 Found {len(csv_files)} CSV files to import:")
for f in csv_files:
    print(f"   - {os.path.basename(f)}")

# Track overall progress
total_records = 0
total_inserted = 0
failed_files = []
file_summary = []

# Process each CSV file
for csv_file in csv_files:
    filename = os.path.basename(csv_file)
    year = filename.replace('.csv', '')
    
    print(f"\n{'='*60}")
    print(f"📅 Processing {year}...")
    print(f"{'='*60}")
    
    try:
        # Read CSV
        df = pd.read_csv(csv_file, low_memory=False)
        print(f"   ✅ Loaded {len(df)} game-team records from {filename}")
        
        # Rename columns to snake_case (only columns that exist in mapping)
        rename_dict = {k: v for k, v in COLUMN_MAPPING.items() if k in df.columns}
        df = df.rename(columns=rename_dict)
        
        # Drop consolidated byPeriod columns (we use individual period columns instead)
        columns_to_drop = ['teamStats_points_byPeriod', 'opponentStats_points_byPeriod']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')
        
        # Drop conference columns (data quality issues - will handle separately)
        conference_columns = ['conference', 'opponentConference', 'opponent_conference']
        df = df.drop(columns=[col for col in conference_columns if col in df.columns], errors='ignore')
        
        # Remove duplicates - keep first instance of each (game_id, team_id)
        # This handles cases where conference changes caused duplicate rows
        # NOTE: Do this AFTER renaming columns so 'game_id' and 'team_id' exist
        initial_count = len(df)
        df = df.drop_duplicates(subset=['game_id', 'team_id'], keep='first')
        duplicates_removed = initial_count - len(df)
        if duplicates_removed > 0:
            print(f"   🔧 Removed {duplicates_removed} duplicate rows")
        
        # Define which columns are integers vs decimals
        integer_columns = [
            'game_id', 'team_id', 'season', 'opponent_id', 'game_minutes',
            'team_seed', 'opponent_seed',
            # All "made" and "attempted" stats are integers
            'team_field_goals_attempted', 'team_field_goals_made',
            'team_two_point_field_goals_attempted', 'team_two_point_field_goals_made',
            'team_three_point_field_goals_attempted', 'team_three_point_field_goals_made',
            'team_free_throws_attempted', 'team_free_throws_made',
            'team_rebounds_total', 'team_rebounds_defensive', 'team_rebounds_offensive',
            'team_turnovers_team_total', 'team_turnovers_total',
            'team_fouls_flagrant', 'team_fouls_technical', 'team_fouls_total',
            'team_points_fast_break', 'team_points_off_turnovers', 'team_points_in_paint',
            'team_points_total', 'team_points_largest_lead',
            'team_points_by_period_0', 'team_points_by_period_1', 'team_points_by_period_2',
            'team_points_by_period_3', 'team_points_by_period_4', 'team_points_by_period_5',
            'team_points_by_period_6', 'team_points_by_period_7', 'team_points_by_period_8', 
            'team_points_by_period_9', 'team_points_by_period_10',
            'team_assists', 'team_blocks', 'team_steals', 'team_possessions',
            # Opponent stats - same pattern
            'opponent_field_goals_attempted', 'opponent_field_goals_made',
            'opponent_two_point_field_goals_attempted', 'opponent_two_point_field_goals_made',
            'opponent_three_point_field_goals_attempted', 'opponent_three_point_field_goals_made',
            'opponent_free_throws_attempted', 'opponent_free_throws_made',
            'opponent_rebounds_total', 'opponent_rebounds_defensive', 'opponent_rebounds_offensive',
            'opponent_turnovers_team_total', 'opponent_turnovers_total',
            'opponent_fouls_flagrant', 'opponent_fouls_technical', 'opponent_fouls_total',
            'opponent_points_fast_break', 'opponent_points_off_turnovers', 'opponent_points_in_paint',
            'opponent_points_total', 'opponent_points_largest_lead',
            'opponent_points_by_period_0', 'opponent_points_by_period_1', 'opponent_points_by_period_2',
            'opponent_points_by_period_3', 'opponent_points_by_period_4', 'opponent_points_by_period_5',
            'opponent_points_by_period_6', 'opponent_points_by_period_7', 'opponent_points_by_period_8', 
            'opponent_points_by_period_9', 'opponent_points_by_period_10',
            'opponent_assists', 'opponent_blocks', 'opponent_steals', 'opponent_possessions',
        ]
        
        # Clean data types
        for col in df.columns:
            if col in ['team', 'opponent', 'season_label', 'season_type', 'game_type', 'notes', 'tournament', 'start_date']:
                continue  # Skip string columns
            elif col in ['neutral_site', 'is_home', 'conference_game', 'start_time_tbd']:
                # Boolean columns
                df[col] = df[col].fillna(False).astype(bool)
            elif col in integer_columns:
                # Integer columns - use Int64 to handle NaN
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:
                # Decimal columns (percentages, ratings, etc.)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Handle date column
        if 'start_date' in df.columns:
            df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Replace NaN with None - CRITICAL for Supabase
        df = df.replace({float('nan'): None, float('inf'): None, float('-inf'): None})
        
        # Convert to records
        records = df.to_dict('records')
        
        # Double-check: Replace any remaining NaN values
        for record in records:
            for key, value in record.items():
                if value is not None and isinstance(value, float):
                    if math.isnan(value) or math.isinf(value):
                        record[key] = None
        
        total_records += len(records)
        
        # Import in batches
        batch_size = 500  # Optimal for ~12k rows per file
        file_inserted = 0
        file_failed = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            try:
                supabase.table('team_game_stats').insert(batch).execute()
                file_inserted += len(batch)
                total_inserted += len(batch)
                print(f"   ✅ Batch {batch_num}/{total_batches}: Inserted {len(batch)} records")
            except Exception as e:
                print(f"   ⚠️  Batch {batch_num} failed, retrying individually...")
                
                for record in batch:
                    try:
                        supabase.table('team_game_stats').insert([record]).execute()
                        file_inserted += 1
                        total_inserted += 1
                    except Exception as record_error:
                        file_failed += 1
                        game_info = f"Game {record.get('game_id', '?')}, {record.get('team', 'Unknown')}"
                        error_msg = str(record_error)[:80]
                        print(f"      ❌ {game_info}: {error_msg}")
        
        file_summary.append({
            'year': year,
            'total': len(records),
            'inserted': file_inserted,
            'failed': file_failed
        })
        
        print(f"   📊 {year} Summary: {file_inserted} inserted, {file_failed} failed")
        
    except Exception as e:
        print(f"   ❌ Error processing {filename}: {e}")
        failed_files.append({'file': filename, 'error': str(e)})

# Final Summary
print("\n" + "="*60)
print("📊 IMPORT SUMMARY")
print("="*60)
print(f"Total CSV files processed: {len(csv_files)}")
print(f"Total records: {total_records:,}")
print(f"Successfully inserted: {total_inserted:,}")
print(f"Failed: {total_records - total_inserted:,}")

print("\n📅 By Year:")
for summary in file_summary:
    status = "✅" if summary['failed'] == 0 else "⚠️ "
    print(f"   {status} {summary['year']}: {summary['inserted']:,}/{summary['total']:,} records")

if failed_files:
    print(f"\n❌ Files that failed to process:")
    for ff in failed_files:
        print(f"   - {ff['file']}: {ff['error']}")

# Verify in database
print("\n🔍 Verifying import...")
try:
    result = supabase.table('team_game_stats').select('game_id', count='exact').execute()
    db_count = result.count
    print(f"✅ Database now contains {db_count:,} game-team records")
    
    # Show season breakdown (sample first 5 years)
    print("\n📊 Sample records by season:")
    for year in range(2005, 2010):
        count_result = supabase.table('team_game_stats').select('season', count='exact').eq('season', year).execute()
        if count_result.count > 0:
            print(f"   {year}: {count_result.count:,} records")
    print("   ...")
    
except Exception as e:
    print(f"❌ Verification failed: {e}")

print("\n✨ Import complete!")