import os
from dotenv import load_dotenv

load_dotenv()

print("SUPABASE_URL:", os.getenv('SUPABASE_URL'))
print("SUPABASE_KEY:", os.getenv('SUPABASE_KEY')[:20] if os.getenv('SUPABASE_KEY') else None)
print("CBDB_API_KEY:", os.getenv('CBDB_API_KEY')[:20] if os.getenv('CBDB_API_KEY') else None)