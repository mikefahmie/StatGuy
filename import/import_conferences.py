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

print("🏀 Starting Conferences Import...")
print(f"📊 Connected to Supabase: {SUPABASE_URL}")

# Option to clear existing data
print("\n❓ Clear existing data in conferences table first?")
print("1. Yes - delete all existing conferences")
print("2. No - attempt to add to existing data")
clear_choice = input("Enter choice (1 or 2): ").strip()

if clear_choice == "1":
    print("\n🗑️  Clearing existing data...")
    try:
        supabase.table('conferences').delete().neq('id', 0).execute()
        print("✅ Table cleared successfully")
    except Exception as e:
        print(f"⚠️  Warning: Could not clear table: {e}")

# Read CSV
print("\n📂 Reading conferences.csv...")
df = pd.read_csv('conferences.csv')
print(f"✅ Loaded {len(df)} conferences from CSV")

# Data validation
print("\n🔍 Validating data...")
print(f"   Total rows: {len(df)}")
print(f"   Unique IDs: {df['id'].nunique()}")
print(f"   Unique names: {df['name'].nunique()}")
print(f"   Unique abbreviations: {df['abbreviation'].nunique()}")

if df['id'].duplicated().any():
    print("⚠️  WARNING: Duplicate IDs found!")
    print(df[df['id'].duplicated(keep=False)][['id', 'name']])

# Clean data types
print("\n🔧 Cleaning data types...")
df['id'] = df['id'].astype(int)

# Replace NaN with None
df = df.where(pd.notna(df), None)

# Convert to list of dicts
records = df.to_dict('records')
print(f"\n📦 Prepared {len(records)} conferences for import")

# Since this is a small dataset, import all at once
print("\n⬆️  Uploading to Supabase...")
try:
    result = supabase.table('conferences').insert(records).execute()
    print(f"✅ Successfully inserted {len(records)} conferences")
except Exception as e:
    print(f"❌ Batch insert failed: {e}")
    print("\n⚠️  Retrying with individual inserts...")
    
    success_count = 0
    failed_records = []
    
    for record in records:
        try:
            supabase.table('conferences').insert([record]).execute()
            success_count += 1
        except Exception as record_error:
            failed_records.append({
                'conference': record,
                'error': str(record_error)
            })
            print(f"   ❌ Failed: {record['name']} - {record_error}")
    
    print(f"\n✅ Successfully inserted {success_count}/{len(records)} conferences")
    
    if failed_records:
        print(f"❌ Failed to insert {len(failed_records)} conferences")

# Verify in database
print("\n🔍 Verifying import...")
try:
    result = supabase.table('conferences').select('id', count='exact').execute()
    db_count = result.count
    print(f"✅ Database now contains {db_count} conferences")
    
    # Show a sample
    sample = supabase.table('conferences').select('*').limit(5).execute()
    print("\n📋 Sample of imported conferences:")
    for conf in sample.data:
        print(f"   {conf['id']:2d}. {conf['abbreviation']:10s} - {conf['name']}")
    
except Exception as e:
    print(f"❌ Verification failed: {e}")

print("\n✨ Import complete!")