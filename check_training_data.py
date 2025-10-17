import pandas as pd
import numpy as np

# Check historical game data
print("=" * 70)
print("HISTORICAL GAME DATA ANALYSIS")
print("=" * 70)

df = pd.read_csv('data/raw/games_nba_api.csv')
print(f"\n📊 Total games in dataset: {len(df):,}")

# Find date column
date_col = None
for col in ['date_est', 'date', 'game_date', 'date_utc']:
    if col in df.columns:
        date_col = col
        break

if date_col:
    print(f"📅 Date range: {df[date_col].min()} to {df[date_col].max()}")
else:
    print("📅 Date column not found")

# Check for period data
period_cols = [col for col in df.columns if 'period' in col.lower() or 'quarter' in col.lower() or 'q1' in col.lower() or 'q2' in col.lower()]
print(f"\n🎯 Period-related columns found: {len(period_cols)}")
if period_cols:
    print("   Columns:", period_cols[:10])

# Check for halves data
halves_cols = [col for col in df.columns if 'half' in col.lower() or 'h1' in col.lower() or 'h2' in col.lower()]
print(f"\n🏀 Halves-related columns found: {len(halves_cols)}")
if halves_cols:
    print("   Columns:", halves_cols)

# Check key columns
print(f"\n📋 Key columns present:")
key_cols = ['home_pts', 'visitor_pts', 'home_team', 'visitor_team', 'game_id']
for col in key_cols:
    present = col in df.columns
    print(f"   {col}: {'✅' if present else '❌'}")

# Sample first few columns
print(f"\n📝 All columns ({len(df.columns)} total):")
for i, col in enumerate(df.columns):
    if i < 30:
        print(f"   {i+1}. {col}")
    elif i == 30:
        print(f"   ... ({len(df.columns) - 30} more columns)")
        break

# Check for complete games (with scores)
if 'home_pts' in df.columns and 'visitor_pts' in df.columns:
    complete = df[['home_pts', 'visitor_pts']].notna().all(axis=1).sum()
    print(f"\n✅ Complete games (with scores): {complete:,} ({complete/len(df)*100:.1f}%)")

# Check seasons covered
if 'season_id' in df.columns:
    seasons = df['season_id'].unique()
    print(f"\n📆 Seasons covered: {len(seasons)}")
    print(f"   Seasons: {sorted(seasons)}")
elif 'date_est' in df.columns:
    df['year'] = pd.to_datetime(df['date_est'], errors='coerce').dt.year
    years = df['year'].dropna().unique()
    print(f"\n📆 Years covered: {len(years)}")
    print(f"   Years: {sorted(years)}")

print("\n" + "=" * 70)
