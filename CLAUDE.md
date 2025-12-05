# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data import pipeline for college basketball statistics. It imports data from CSV files and the College Basketball Data (CBBD) API into a Supabase database. The project handles teams, games, players, rosters, rankings, and various statistical datasets spanning seasons from 2005 to present.

## Environment Setup

All import scripts require environment variables defined in `import/.env`:
- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_KEY`: Your Supabase anon/service key
- `CBDB_API_KEY`: College Basketball Data API key
- `CBBD_API_BASE`: API base URL (https://api.collegebasketballdata.com)

**Note**: The `.env` file is currently tracked in git but should not be. It contains sensitive credentials.

## Running Import Scripts

All import scripts are located in the [import/](import/) directory and follow similar patterns:

```bash
cd import
python import_<entity>.py
```

### Core Data Imports (run in this order)

1. **Teams and Conferences** (foundational data):
   ```bash
   python import_conferences.py  # Import conferences first
   python import_teams.py        # Import teams (references conferences)
   ```

2. **Games**:
   ```bash
   python import_games.py        # Import game data with period-by-period scoring
   ```

3. **Venues**:
   ```bash
   python import_venues.py       # Import venue/arena data
   ```

### Player Data Imports

4. **Rosters** (uses CBBD API):
   ```bash
   python import_rosters.py      # Imports player rosters for all seasons (2005-2026)
   ```
   - Fetches data from CBBD API
   - Updates `players` and `player_team_rosters` tables
   - Handles hometown data, physical attributes
   - Progress saved to `roster_import_progress.log`

5. **Player Statistics** (from CSV files):
   ```bash
   python import_player_season_stats.py           # Season aggregates
   python import_player_game_stats.py             # Game-by-game stats
   python import_player_season_shooting_stats.py  # Shooting percentages by zone
   ```

### Team Statistics Imports

6. **Team Statistics** (from CSV files):
   ```bash
   python import_team_season_stats.py      # Season aggregates
   python import_team_game_stats.py        # Game-by-game stats
   python import_team_shooting_stats.py    # Shooting stats by season
   python import_team_history.py           # Historical team data
   ```

### Rankings

7. **Rankings** (uses CBBD API):
   ```bash
   python import_rankings.py     # Import AP and Coaches Poll rankings
   ```
   - Fetches all rankings in a single API call
   - Deduplicates based on composite key: (season, season_type, week, poll_type, team_id)
   - Handles nullable `poll_date` and `ranking` fields

## Import Script Architecture

All import scripts follow a consistent pattern:

1. **Environment Setup**: Load `.env` and initialize Supabase client
2. **User Prompt**: Ask whether to clear existing data (destructive operation)
3. **Data Loading**:
   - CSV-based imports: Read from files in current directory or subdirectories
   - API-based imports: Fetch from CBBD API with authentication headers
4. **Data Validation**: Check for duplicates, validate types, show data preview
5. **Data Transformation**:
   - Convert column names to snake_case
   - Handle data type conversions (integers, booleans, dates, JSON)
   - Replace NaN/Inf with None for Supabase compatibility
   - Remove duplicates based on composite keys
6. **Batch Import**: Insert in batches (typically 500-1000 records)
7. **Error Handling**: Individual retry on batch failure with detailed logging
8. **Verification**: Query database to confirm import success

## Key Data Structures

### CSV File Organization

Multi-year datasets are organized by year in subdirectories:
- `player_game_stats/2005.csv`, `player_game_stats/2006.csv`, etc.
- `team_season_shooting_stats/2014.csv`, `team_season_shooting_stats/2015.csv`, etc.

Scripts use `glob.glob()` to discover and process all years automatically.

### Column Naming Conventions

- CSV files contain mixed naming: `camelCase`, `snake_case`, and `PascalCase`
- Import scripts map all columns to `snake_case` for database consistency
- Example mappings in [import_player_game_stats.py](import/import_player_game_stats.py#L26-L84)

### Special Data Types

- **JSON columns**: `home_period_points`, `away_period_points` in games (stored as JSONB)
- **Date columns**: Converted to ISO format strings (YYYY-MM-DD)
- **Boolean columns**: Explicitly converted from various representations
- **Nullable integers**: Use pandas `Int64` dtype to preserve NULL values

## Common Patterns

### Duplicate Handling

Duplicates are handled at multiple levels:
- **CSV deduplication**: Drop duplicates before import using composite keys
- **Database upserts**: Use `on_conflict` parameter for idempotent imports
- **In-memory deduplication**: Build dictionary keyed by composite key (rankings)

### Batch Import Strategy

```python
batch_size = 500  # or 1000 for larger datasets
for i in range(0, len(records), batch_size):
    batch = records[i:i + batch_size]
    try:
        supabase.table('table_name').insert(batch).execute()
    except Exception as e:
        # Retry individually on batch failure
        for record in batch:
            try:
                supabase.table('table_name').insert([record]).execute()
            except Exception as record_error:
                # Log individual failures
```

### Data Type Cleaning for Supabase

Critical: Supabase rejects NaN, Inf, and -Inf values. Always clean before insert:

```python
# Replace NaN with None
df = df.replace({float('nan'): None, float('inf'): None, float('-inf'): None})

# For individual records
for record in records:
    for key, value in record.items():
        if value is not None and isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                record[key] = None
```

## Database Schema Notes

- **Primary keys**: Most tables use `id` as primary key
- **Foreign keys**:
  - Teams reference conferences via `conference_id`
  - Games reference teams, venues, conferences
  - Player stats reference `athlete_id` (maps to `players.id`)
  - Rosters use composite key: `(athlete_id, team_id, season)`
- **Source IDs**: Many tables have both `id` (internal) and `source_id` (external API reference)

## API Integration

### CBBD API Authentication

All API calls require Bearer token authentication:

```python
HEADERS = {
    'accept': 'application/json',
    'Authorization': f'Bearer {CBBD_API_KEY}'
}
```

### API-Based Import Scripts

- [import_rosters.py](import/import_rosters.py): Fetches roster data per season, matches teams by `source_id`
- [import_rankings.py](import/import_rankings.py): Fetches all rankings in single call, deduplicates

## Progress Tracking and Logging

- Import scripts print detailed progress with emoji indicators
- Failed records are logged to timestamped JSON files: `import_failures_YYYYMMDD_HHMMSS.json`
- Roster imports save progress to `roster_import_progress.log`
- Scripts show ETAs and processing rates for long-running imports

## Python Environment

The project uses a virtual environment at [import/venv/](import/venv/). Key dependencies:
- `pandas`: CSV processing and data manipulation
- `supabase`: Database client
- `python-dotenv`: Environment variable management
- `requests`: API calls

To activate the virtual environment (from import/ directory):
```bash
# Windows
venv\Scripts\activate
# Unix/Mac
source venv/bin/activate
```
