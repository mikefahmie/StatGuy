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

print("🏀 Starting Player Game Stats Import...")
print(f"📊 Connected to Supabase: {SUPABASE_URL}")

# Column mapping: CSV -> Database snake_case
# Note: Most columns are already snake_case, but some need mapping
COLUMN_MAPPING = {
    'game_id': 'game_id',
    'season': 'season',
    'season_type': 'season_type',
    'start_date': 'start_date',
    'team_id': 'team_id',
    'team': 'team',
    # conference - will be dropped
    'team_seed': 'team_seed',
    'opponent_id': 'opponent_id',
    'opponent': 'opponent',
    # opponent_conference - will be dropped
    'opponent_seed': 'opponent_seed',
    'neutral_site': 'neutral_site',
    'is_home': 'is_home',
    'conference_game': 'conference_game',
    'game_type': 'game_type',
    'game_minutes': 'game_minutes',
    'game_pace': 'game_pace',
    'name': 'name',
    'position': 'position',
    'athleteId': 'athlete_id',  # Note: camelCase in CSV
    'starter': 'starter',
    'ejected': 'ejected',
    'minutes': 'minutes',
    'points': 'points',
    'assists': 'assists',
    'turnovers': 'turnovers',
    'fouls': 'fouls',
    'steals': 'steals',
    'blocks': 'blocks',
    'rebounds_total': 'rebounds_total',
    'rebounds_defensive': 'rebounds_defensive',
    'rebounds_offensive': 'rebounds_offensive',
    # Shooting stats with mixed case in CSV
    'fieldGoals_pct': 'field_goals_pct',
    'fieldGoals_attempted': 'field_goals_attempted',
    'fieldGoals_made': 'field_goals_made',
    'twoPointFieldGoals_pct': 'two_point_field_goals_pct',
    'twoPointFieldGoals_attempted': 'two_point_field_goals_attempted',
    'twoPointFieldGoals_made': 'two_point_field_goals_made',
    'threePointFieldGoals_pct': 'three_point_field_goals_pct',
    'threePointFieldGoals_attempted': 'three_point_field_goals_attempted',
    'threePointFieldGoals_made': 'three_point_field_goals_made',
    'freeThrows_pct': 'free_throws_pct',
    'freeThrows_attempted': 'free_throws_attempted',
    'freeThrows_made': 'free_throws_made',
    # Advanced metrics
    'usage': 'usage',
    'offensiveRating': 'offensive_rating',
    'defensiveRating': 'defensive_rating',
    'netRating': 'net_rating',
    'effectiveFieldGoalPct': 'effective_field_goal_pct',
    'trueShootingPct': 'true_shooting_pct',
    'gameScore': 'game_score',
    'assistsTurnoverRatio': 'assists_turnover_ratio',
    'freeThrowRate': 'free_throw_rate',
    'offensiveReboundPct': 'offensive_rebound_pct',
}

# Option to clear existing data
print("\n❓ Clear existing data in player_game_stats table first?")
print("1. Yes - delete all existing stats")
print("2. No - attempt to add to existing data")
clear_choice = input("Enter choice (1 or 2): ").strip()

if clear_choice == "1":
    print("\n🗑️  Clearing existing data...")
    print("   Run this SQL command manually in Supabase:")
    print("   DELETE FROM player_game_stats;")
    print("\n   Press Enter when done...")
    input()

# Find all CSV files in player_game_stats folder
csv_folder = 'player_game_stats'
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
        print(f"   ✅ Loaded {len(df)} player-game records from {filename}")
        
        # Rename columns to snake_case
        rename_dict = {k: v for k, v in COLUMN_MAPPING.items() if k in df.columns}
        df = df.rename(columns=rename_dict)
        
        # Drop conference columns (data quality issues)
        conference_columns = ['conference', 'opponent_conference']
        df = df.drop(columns=[col for col in conference_columns if col in df.columns], errors='ignore')
        
        # Remove duplicates - keep first instance of each (game_id, athlete_id)
        initial_count = len(df)
        df = df.drop_duplicates(subset=['game_id', 'athlete_id'], keep='first')
        duplicates_removed = initial_count - len(df)
        if duplicates_removed > 0:
            print(f"   🔧 Removed {duplicates_removed} duplicate rows")
        
        # Define integer vs decimal columns
        integer_columns = [
            'game_id', 'athlete_id', 'season', 'team_id', 'opponent_id',
            'team_seed', 'opponent_seed', 'game_minutes', 'game_pace',
            'minutes', 'points', 'assists', 'turnovers', 'fouls', 'steals', 'blocks',
            'field_goals_attempted', 'field_goals_made',
            'two_point_field_goals_attempted', 'two_point_field_goals_made',
            'three_point_field_goals_attempted', 'three_point_field_goals_made',
            'free_throws_attempted', 'free_throws_made',
            'rebounds_total', 'rebounds_defensive', 'rebounds_offensive',
        ]
        
        boolean_columns = ['neutral_site', 'is_home', 'conference_game', 'starter', 'ejected']
        
        # Clean data types
        for col in df.columns:
            if col in ['name', 'team', 'opponent', 'season_type', 'game_type', 'position', 'start_date']:
                continue  # Skip string/date columns
            elif col in boolean_columns:
                # Boolean columns
                df[col] = df[col].fillna(False).astype(bool)
            elif col in integer_columns:
                # Integer columns - more robust conversion
                try:
                    # First convert to numeric, then round, then to Int64
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Round to handle any floating point precision issues
                    df[col] = df[col].round(0)
                    # Convert to nullable integer
                    df[col] = df[col].astype('Int64')
                except Exception as e:
                    print(f"      Warning: Could not convert {col} to integer: {e}")
                    # Leave as float if conversion fails
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                # Decimal columns (ratings, percentages, etc.)
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
        batch_size = 500
        file_inserted = 0
        file_failed = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            try:
                supabase.table('player_game_stats').insert(batch).execute()
                file_inserted += len(batch)
                total_inserted += len(batch)
                print(f"   ✅ Batch {batch_num}/{total_batches}: Inserted {len(batch)} records")
            except Exception as e:
                print(f"   ⚠️  Batch {batch_num} failed, retrying individually...")
                
                for record in batch:
                    try:
                        supabase.table('player_game_stats').insert([record]).execute()
                        file_inserted += 1
                        total_inserted += 1
                    except Exception as record_error:
                        file_failed += 1
                        player_info = f"Game {record.get('game_id', '?')}, {record.get('name', 'Unknown')}"
                        error_msg = str(record_error)[:80]
                        print(f"      ❌ {player_info}: {error_msg}")
        
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
    result = supabase.table('player_game_stats').select('game_id', count='exact').execute()
    db_count = result.count
    print(f"✅ Database now contains {db_count:,} player-game records")
    
    # Show season breakdown (sample)
    print("\n📊 Sample records by season:")
    for year in range(2005, 2010):
        count_result = supabase.table('player_game_stats').select('season', count='exact').eq('season', year).execute()
        if count_result.count > 0:
            print(f"   {year}: {count_result.count:,} records")
    print("   ...")
    
except Exception as e:
    print(f"❌ Verification failed: {e}")

print("\n✨ Import complete!")