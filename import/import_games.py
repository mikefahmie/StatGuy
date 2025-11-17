import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import json
from datetime import datetime

# Load environment variables
load_dotenv()

# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env file")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print("🏀 Starting Statguy.ai Games Import v2...")
print(f"📊 Connected to Supabase: {SUPABASE_URL}")

# Option to clear existing data
print("\n❓ Clear existing data in games table first?")
print("1. Yes - delete all existing games")
print("2. No - attempt to add to existing data")
clear_choice = input("Enter choice (1 or 2): ").strip()

if clear_choice == "1":
    print("\n🗑️  Clearing existing data...")
    try:
        supabase.table('games').delete().neq('id', 0).execute()
        print("✅ Table cleared successfully")
    except Exception as e:
        print(f"⚠️  Warning: Could not clear table: {e}")

# Read CSV
print("\n📂 Reading games.csv...")
df = pd.read_csv('games.csv')
print(f"✅ Loaded {len(df)} games from CSV")

# Better duplicate detection
print("\n🔍 Checking for duplicate IDs in CSV...")
duplicate_count = df['id'].duplicated().sum()
if duplicate_count > 0:
    print(f"⚠️  WARNING: Found {duplicate_count} duplicate IDs in CSV")
    duplicate_ids = df[df['id'].duplicated(keep=False)]['id'].unique()
    print(f"   Unique IDs that appear multiple times: {len(duplicate_ids)}")
    print(f"   First 10 duplicate IDs: {duplicate_ids[:10].tolist()}")
    
    print("\n❓ How to handle duplicates?")
    print("1. Keep FIRST occurrence of each duplicate ID")
    print("2. Keep LAST occurrence of each duplicate ID")
    print("3. Abort import and investigate manually")
    dup_choice = input("Enter choice (1, 2, or 3): ").strip()
    
    if dup_choice == "1":
        df = df.drop_duplicates(subset=['id'], keep='first')
        print(f"✅ Kept first occurrence, removed {duplicate_count} duplicates")
    elif dup_choice == "2":
        df = df.drop_duplicates(subset=['id'], keep='last')
        print(f"✅ Kept last occurrence, removed {duplicate_count} duplicates")
    else:
        print("❌ Import aborted")
        exit(1)
else:
    print("✅ No duplicate IDs found in CSV")

print(f"📦 Proceeding with {len(df)} unique games")

# Clean up numeric columns
print("\n🔧 Cleaning data types...")
integer_cols = [
    'id', 'season', 'venue_id', 
    'home_team_id', 'home_conference_id', 'home_seed', 'home_points',
    'away_team_id', 'away_conference_id', 'away_seed', 'away_points'
]

for col in integer_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

# Handle period_points
print("🏀 Converting period points to JSON...")
for prefix in ['home', 'away']:
    col_name = f'{prefix}_period_points'
    if col_name in df.columns:
        df[col_name] = df[col_name].apply(
            lambda x: json.loads(x) if pd.notna(x) and isinstance(x, str) else None
        )

# Handle boolean columns
print("✅ Converting boolean columns...")
boolean_cols = ['neutral_site', 'conference_game', 'home_winner', 'away_winner']
for col in boolean_cols:
    if col in df.columns:
        df[col] = df[col].fillna(False).astype(bool)

# Handle date column
if 'start_date' in df.columns:
    df['start_date'] = pd.to_datetime(df['start_date']).dt.strftime('%Y-%m-%d')

# Replace NaN with None
df = df.where(pd.notna(df), None)

# Convert to list of dicts
records = df.to_dict('records')
print(f"\n📦 Prepared {len(records)} records for import")

# Setup failure tracking
failed_records = []
failure_log_file = f"import_failures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

# Batch insert with individual retry
print("\n⬆️  Uploading to Supabase...")
batch_size = 1000
total_inserted = 0
failed_batches = 0

for i in range(0, len(records), batch_size):
    batch = records[i:i + batch_size]
    batch_num = (i // batch_size) + 1
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    try:
        result = supabase.table('games').insert(batch).execute()
        total_inserted += len(batch)
        print(f"   ✅ Batch {batch_num}/{total_batches}: Inserted {len(batch)} records")
    except Exception as e:
        # Batch failed - retry individual records
        print(f"   ⚠️  Batch {batch_num}/{total_batches} failed, retrying individually...")
        failed_batches += 1
        batch_success = 0
        batch_failures = 0
        
        for record in batch:
            try:
                supabase.table('games').insert([record]).execute()
                batch_success += 1
                total_inserted += 1
            except Exception as record_error:
                batch_failures += 1
                failed_records.append({
                    'record': record,
                    'error': str(record_error)
                })
        
        print(f"      ✅ Successfully inserted {batch_success}/{len(batch)} records")
        if batch_failures > 0:
            print(f"      ❌ Failed to insert {batch_failures}/{len(batch)} records")

# Save failed records to file
if failed_records:
    print(f"\n💾 Saving {len(failed_records)} failed records to {failure_log_file}...")
    with open(failure_log_file, 'w') as f:
        json.dump(failed_records, f, indent=2, default=str)
    print(f"✅ Failed records saved to: {failure_log_file}")

# Summary
print("\n" + "="*50)
print("📊 IMPORT SUMMARY")
print("="*50)
print(f"Total records in CSV: {len(records)}")
print(f"Successfully inserted: {total_inserted}")
print(f"Failed records: {len(failed_records)}")
print(f"Batches that needed individual retry: {failed_batches}")

if len(failed_records) > 0:
    print(f"\n⚠️  {len(failed_records)} records failed to import")
    print(f"   Review details in: {failure_log_file}")
    print("\n   Top 5 error types:")
    error_types = {}
    for fr in failed_records:
        error_msg = fr['error'][:100]  # First 100 chars
        error_types[error_msg] = error_types.get(error_msg, 0) + 1
    
    for i, (error, count) in enumerate(sorted(error_types.items(), key=lambda x: x[1], reverse=True)[:5]):
        print(f"   {i+1}. {error}... ({count} records)")
else:
    print("\n🎉 All records imported successfully!")

# Verify in database
print("\n🔍 Verifying import...")
try:
    result = supabase.table('games').select('id', count='exact').execute()
    db_count = result.count
    print(f"✅ Database now contains {db_count} games")
    
    if db_count != total_inserted:
        print(f"⚠️  Note: Expected {total_inserted} but found {db_count}")
        print(f"   This may be due to pre-existing data if you chose not to clear the table")
except Exception as e:
    print(f"❌ Verification failed: {e}")

print("\n✨ Import complete!")
if len(failed_records) > 0:
    print(f"\n📋 Next steps:")
    print(f"   1. Review {failure_log_file}")
    print(f"   2. Fix data issues or adjust schema as needed")
    print(f"   3. Re-import failed records if necessary")