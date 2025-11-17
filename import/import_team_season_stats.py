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

print("🏀 Starting Team Season Stats Import...")
print(f"📊 Connected to Supabase: {SUPABASE_URL}")

# Column mapping: CSV camelCase -> Database snake_case
COLUMN_MAPPING = {
    'season': 'season',
    'seasonLabel': 'season_label',
    'teamId': 'team_id',
    'team': 'team',
    'conference': 'conference',
    'games': 'games',
    'wins': 'wins',
    'losses': 'losses',
    'totalMinutes': 'total_minutes',
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
    'teamStats_fourFactors_freeThrowRate': 'team_four_factors_free_throw_rate',
    'teamStats_fourFactors_offensiveReboundPct': 'team_four_factors_offensive_rebound_pct',
    'teamStats_fourFactors_turnoverRatio': 'team_four_factors_turnover_ratio',
    'teamStats_fourFactors_effectiveFieldGoalPct': 'team_four_factors_effective_field_goal_pct',
    'teamStats_assists': 'team_assists',
    'teamStats_blocks': 'team_blocks',
    'teamStats_steals': 'team_steals',
    'teamStats_possessions': 'team_possessions',
    'teamStats_rating': 'team_rating',
    'teamStats_trueShooting': 'team_true_shooting',
    
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
    'opponentStats_fourFactors_freeThrowRate': 'opponent_four_factors_free_throw_rate',
    'opponentStats_fourFactors_offensiveReboundPct': 'opponent_four_factors_offensive_rebound_pct',
    'opponentStats_fourFactors_turnoverRatio': 'opponent_four_factors_turnover_ratio',
    'opponentStats_fourFactors_effectiveFieldGoalPct': 'opponent_four_factors_effective_field_goal_pct',
    'opponentStats_assists': 'opponent_assists',
    'opponentStats_blocks': 'opponent_blocks',
    'opponentStats_steals': 'opponent_steals',
    'opponentStats_possessions': 'opponent_possessions',
    'opponentStats_rating': 'opponent_rating',
    'opponentStats_trueShooting': 'opponent_true_shooting',
}

# Option to clear existing data
print("\n❓ Clear existing data in team_season_stats table first?")
print("1. Yes - delete all existing stats")
print("2. No - attempt to add to existing data")
clear_choice = input("Enter choice (1 or 2): ").strip()

if clear_choice == "1":
    print("\n🗑️  Clearing existing data...")
    try:
        supabase.table('team_season_stats').delete().neq('season', 0).execute()
        print("✅ Table cleared successfully")
    except Exception as e:
        print(f"⚠️  Warning: Could not clear table: {e}")

# Find all CSV files in team_season_stats folder
csv_folder = 'team_season_stats'
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
        df = pd.read_csv(csv_file)
        print(f"   ✅ Loaded {len(df)} teams from {filename}")
        
        # Rename columns to snake_case
        df = df.rename(columns=COLUMN_MAPPING)
        
        # Clean data types for numeric columns
        numeric_cols = [col for col in df.columns if col not in ['team', 'conference', 'season_label']]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Replace NaN with None - must do this AFTER numeric conversion
        # Use fillna(None) which properly converts NaN to None
        df = df.replace({float('nan'): None, float('inf'): None, float('-inf'): None})
        
        # Convert to records
        records = df.to_dict('records')
        
        # Double-check: Replace any remaining NaN values in the dict
        for record in records:
            for key, value in record.items():
                if value is not None and isinstance(value, float):
                    if math.isnan(value) or math.isinf(value):
                        record[key] = None
        total_records += len(records)
        
        # Import in batches
        batch_size = 200
        file_inserted = 0
        file_failed = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            try:
                supabase.table('team_season_stats').insert(batch).execute()
                file_inserted += len(batch)
                total_inserted += len(batch)
                print(f"   ✅ Batch {batch_num}/{total_batches}: Inserted {len(batch)} records")
            except Exception as e:
                print(f"   ⚠️  Batch {batch_num} failed, retrying individually...")
                
                for record in batch:
                    try:
                        supabase.table('team_season_stats').insert([record]).execute()
                        file_inserted += 1
                        total_inserted += 1
                    except Exception as record_error:
                        file_failed += 1
                        print(f"      ❌ Failed: {record.get('team', 'Unknown')} - {str(record_error)[:80]}")
        
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
print(f"Total records: {total_records}")
print(f"Successfully inserted: {total_inserted}")
print(f"Failed: {total_records - total_inserted}")

print("\n📅 By Year:")
for summary in file_summary:
    status = "✅" if summary['failed'] == 0 else "⚠️ "
    print(f"   {status} {summary['year']}: {summary['inserted']}/{summary['total']} teams")

if failed_files:
    print(f"\n❌ Files that failed to process:")
    for ff in failed_files:
        print(f"   - {ff['file']}: {ff['error']}")

# Verify in database
print("\n🔍 Verifying import...")
try:
    result = supabase.table('team_season_stats').select('season', count='exact').execute()
    db_count = result.count
    print(f"✅ Database now contains {db_count} team-season records")
    
    # Show season breakdown
    print("\n📊 Records by season:")
    for year in range(2005, 2026):
        count_result = supabase.table('team_season_stats').select('season', count='exact').eq('season', year).execute()
        if count_result.count > 0:
            print(f"   {year}: {count_result.count} teams")
    
except Exception as e:
    print(f"❌ Verification failed: {e}")

print("\n✨ Import complete!")