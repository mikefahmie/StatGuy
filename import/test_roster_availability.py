# test_roster_availability.py
import os
import requests
from dotenv import load_dotenv
import time

load_dotenv()

CBBD_API_BASE = "https://api.collegebasketballdata.com"
CBBD_API_KEY = os.getenv('CBDB_API_KEY')

HEADERS = {
    'accept': 'application/json',
    'Authorization': f'Bearer {CBBD_API_KEY}'
}

print("Testing roster data availability by season...\n")

for season in range(2005, 2026):
    try:
        response = requests.get(
            f"{CBBD_API_BASE}/teams/roster",
            params={'season': season},
            headers=HEADERS
        )
        response.raise_for_status()
        data = response.json()
        
        if data and len(data) > 0:
            total_players = sum(len(team.get('players', [])) for team in data)
            print(f"✓ {season}: {len(data)} teams, {total_players} players")
        else:
            print(f"✗ {season}: No data")
            
    except Exception as e:
        print(f"✗ {season}: Error - {e}")
    
    time.sleep(0.2)  # Be nice to the API