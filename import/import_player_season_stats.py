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

print("🏀 Starting Player Season Stats Import...")
print(f"📊 Connected to Supabase: {SUPABASE_URL}")

# Column mapping: CSV camelCase -> Database snake_case
COLUMN_MAPPING = {
    'season': 'season',
    'seasonLabel': 'season_label',
    'teamId': 'team_id',
    'team': 'team',
    # conference - will be dropped
    'athleteId': 'athlete_id',
    'name': 'name',
    'position': 'position',
    'games': 'games',
    'starts': 'starts',
    'minutes': 'minutes',
    'points': 'points',
    'turnovers': 'turnovers',
    'fouls': 'fouls',
    'assists': 'assists',
    'steals': 'steals',
    'blocks': 'blocks',
    'usage': 'usage',
    'offensiveRating': 'offensive_rating',
    'defensiveRating': 'defensive_rating',
    'netRating': 'net_rating',
    'PORPAG': 'porpag',
    'effectiveFieldGoalPct': 'effective_field_goal_pct',
    'trueShootingPct': 'true_shooting_pct',
    'assistsTurnoverRatio': 'assists_turnover_ratio',
    'freeThrowRate': 'free_throw_rate',
    'offensiveReboundPct': 'offensive_rebound_pct',
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
    'rebounds_total': 'rebounds_total',
    'rebounds_defensive': 'rebounds_defensive',
    'rebounds_offensive': 'rebounds_offensive',
    'winShares_totalPer40': 'win_shares_total_per_40',
    'winShares_total': 'win_shares_total',
    'winShares_defensive': 'win_shares_defensive',
    'winShares_offensive': 'win_shares_offensive',
}

# Option to clear existing data
print("\n❓ Clear existing data in player_season_stats table first?")
print("1. Yes - delete all existing stats")
print("2. No - attempt to add to existing data")
clear_choice = input("Enter choice (1 or 2): ").strip()

if clear_choice == "1":
    print("\n🗑️  Clearing existing data...")
    print("   Run this SQL command manually in Supabase:")
    print("   DELETE FROM player_season_stats;")
    print("\n   Press Enter when done...")
    input()

# Find all CSV files in player_season_stats folder
csv_folder = 'player_season_stats'
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
        print(f"   ✅ Loaded {len(df)} player records from {filename}")
        
        # Rename columns to snake_case
        rename_dict = {k: v for k, v in COLUMN_MAPPING.items() if k in df.columns}
        df = df.rename(columns=rename_dict)
        
        # Drop conference column (data quality issues)
        if 'conference' in df.columns:
            df = df.drop(columns=['conference'])
        
        # Remove duplicates - keep first instance of each (season, athlete_id)
        initial_count = len(df)
        df = df.drop_duplicates(subset=['season', 'athlete_id'], keep='first')
        duplicates_removed = initial_count - len(df)
        if duplicates_removed > 0:
            print(f"   🔧 Removed {duplicates_removed} duplicate rows")
        
        # Define integer vs decimal columns
        integer_columns = [
            'season', 'athlete_id', 'team_id', 'games', 'starts', 'minutes',
            'points', 'turnovers', 'fouls', 'assists', 'steals', 'blocks',
            'field_goals_attempted', 'field_goals_made',
            'two_point_field_goals_attempted', 'two_point_field_goals_made',
            'three_point_field_goals_attempted', 'three_point_field_goals_made',
            'free_throws_attempted', 'free_throws_made',
            'rebounds_total', 'rebounds_defensive', 'rebounds_offensive',
        ]
        
        # Clean data types
        for col in df.columns:
            if col in ['name', 'team', 'season_label', 'position']:
                continue  # Skip string columns
            elif col in integer_columns:
                # Integer columns
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:
                # Decimal columns (ratings, percentages, etc.)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
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
                supabase.table('player_season_stats').insert(batch).execute()
                file_inserted += len(batch)
                total_inserted += len(batch)
                print(f"   ✅ Batch {batch_num}/{total_batches}: Inserted {len(batch)} records")
            except Exception as e:
                print(f"   ⚠️  Batch {batch_num} failed, retrying individually...")
                
                for record in batch:
                    try:
                        supabase.table('player_season_stats').insert([record]).execute()
                        file_inserted += 1
                        total_inserted += 1
                    except Exception as record_error:
                        file_failed += 1
                        player_info = f"{record.get('name', 'Unknown')} ({record.get('team', '?')})"
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
    print(f"   {status} {summary['year']}: {summary['inserted']:,}/{summary['total']:,} players")

if failed_files:
    print(f"\n❌ Files that failed to process:")
    for ff in failed_files:
        print(f"   - {ff['file']}: {ff['error']}")

# Verify in database
print("\n🔍 Verifying import...")
try:
    result = supabase.table('player_season_stats').select('season', count='exact').execute()
    db_count = result.count
    print(f"✅ Database now contains {db_count:,} player-season records")
    
    # Show season breakdown (sample)
    print("\n📊 Sample records by season:")
    for year in range(2005, 2010):
        count_result = supabase.table('player_season_stats').select('season', count='exact').eq('season', year).execute()
        if count_result.count > 0:
            print(f"   {year}: {count_result.count:,} players")
    print("   ...")
    
except Exception as e:
    print(f"❌ Verification failed: {e}")

print("\n✨ Import complete!")