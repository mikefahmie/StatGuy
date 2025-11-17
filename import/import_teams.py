import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env file")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print("🏀 Starting Teams Import...")
print(f"📊 Connected to Supabase: {SUPABASE_URL}")

# Option to clear existing data
print("\n❓ Clear existing data in teams table first?")
print("1. Yes - delete all existing teams")
print("2. No - attempt to add to existing data")
clear_choice = input("Enter choice (1 or 2): ").strip()

if clear_choice == "1":
    print("\n🗑️  Clearing existing data...")
    try:
        supabase.table('teams').delete().neq('id', 0).execute()
        print("✅ Table cleared successfully")
    except Exception as e:
        print(f"⚠️  Warning: Could not clear table: {e}")

# Read CSV
print("\n📂 Reading teams.csv...")
df = pd.read_csv('teams.csv')
print(f"✅ Loaded {len(df)} teams from CSV")

# Data validation
print("\n🔍 Validating data...")
print(f"   Total rows: {len(df)}")
print(f"   Unique IDs: {df['id'].nunique()}")
print(f"   Unique team names: {df['team'].nunique()}")
print(f"   Null counts:")
print(f"     - mascot: {df['mascot'].isna().sum()}")
print(f"     - nickname: {df['nickname'].isna().sum()}")
print(f"     - abbreviation: {df['abbreviation'].isna().sum()}")
print(f"     - display_name: {df['display_name'].isna().sum()}")
print(f"     - primary_color: {df['primary_color'].isna().sum()}")
print(f"     - secondary_color: {df['secondary_color'].isna().sum()}")

if df['id'].duplicated().any():
    print("⚠️  WARNING: Duplicate IDs found!")
    print(df[df['id'].duplicated(keep=False)][['id', 'team']])

# Clean data types
print("\n🔧 Cleaning data types...")
df['id'] = df['id'].astype(int)

# Replace NaN with None (converts to NULL in database)
df = df.where(pd.notna(df), None)

# Convert to list of dicts
records = df.to_dict('records')
print(f"\n📦 Prepared {len(records)} teams for import")

# Import in batches
print("\n⬆️  Uploading to Supabase...")
batch_size = 500  # Smaller batches for safety
total_inserted = 0
failed_records = []

for i in range(0, len(records), batch_size):
    batch = records[i:i + batch_size]
    batch_num = (i // batch_size) + 1
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    try:
        result = supabase.table('teams').insert(batch).execute()
        total_inserted += len(batch)
        print(f"   ✅ Batch {batch_num}/{total_batches}: Inserted {len(batch)} teams")
    except Exception as e:
        print(f"   ⚠️  Batch {batch_num}/{total_batches} failed, retrying individually...")
        
        batch_success = 0
        for record in batch:
            try:
                supabase.table('teams').insert([record]).execute()
                batch_success += 1
                total_inserted += 1
            except Exception as record_error:
                failed_records.append({
                    'team': record,
                    'error': str(record_error)
                })
                print(f"      ❌ Failed: {record['team']} - {record_error}")
        
        print(f"      ✅ Successfully inserted {batch_success}/{len(batch)} teams")

# Summary
print("\n" + "="*50)
print("📊 IMPORT SUMMARY")
print("="*50)
print(f"Total records in CSV: {len(records)}")
print(f"Successfully inserted: {total_inserted}")
print(f"Failed records: {len(failed_records)}")

if failed_records:
    print(f"\n❌ {len(failed_records)} teams failed to import:")
    for fr in failed_records[:10]:  # Show first 10
        print(f"   - {fr['team']['team']}: {fr['error'][:80]}")

# Verify in database
print("\n🔍 Verifying import...")
try:
    result = supabase.table('teams').select('id', count='exact').execute()
    db_count = result.count
    print(f"✅ Database now contains {db_count} teams")
    
    # Show a sample
    sample = supabase.table('teams').select('*').limit(10).execute()
    print("\n📋 Sample of imported teams:")
    for team in sample.data:
        mascot = team['mascot'] or '(no mascot)'
        abbr = team['abbreviation'] or '(no abbr)'
        print(f"   {team['id']:4d}. {abbr:10s} - {team['team']:30s} ({mascot})")
    
except Exception as e:
    print(f"❌ Verification failed: {e}")

print("\n✨ Import complete!")