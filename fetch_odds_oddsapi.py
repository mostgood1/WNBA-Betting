"""
Fetch game odds from The Odds API and calculate betting edges
"""

import pandas as pd
import sys
import os
from datetime import datetime

# Add src to path
sys.path.insert(0, 'src')

from nba_betting.odds_api import fetch_game_odds_current, OddsApiConfig

# Get API key from environment
api_key = os.environ.get('ODDS_API_KEY')
if not api_key:
    print("ERROR: ODDS_API_KEY not set in environment")
    sys.exit(1)

# Fetch odds for today
print("\n" + "="*70)
print("FETCHING GAME ODDS FROM THE ODDS API")
print("="*70)

config = OddsApiConfig(api_key=api_key, markets="h2h,spreads,totals")
target_date = datetime(2025, 10, 17)

print(f"\nFetching odds for {target_date.date()}...")
odds_df = fetch_game_odds_current(config, target_date, verbose=True)

if odds_df.empty:
    print("\n⚠️  No odds data returned from API")
    print("   Possible reasons:")
    print("   - No games scheduled for this date")
    print("   - API rate limit reached")
    print("   - Games not yet posted on sportsbooks")
    sys.exit(0)

print(f"\n✅ Fetched {len(odds_df)} odds records")

# Show bookmakers available
bookmakers = odds_df['bookmaker'].unique()
print(f"\nBookmakers: {len(bookmakers)}")
for bk in sorted(bookmakers)[:10]:  # Show first 10
    count = len(odds_df[odds_df['bookmaker'] == bk])
    print(f"  - {bk}: {count} lines")

# Show markets available
markets = odds_df['market'].unique()
print(f"\nMarkets: {', '.join(markets)}")

# Show games
games = odds_df[['home_team', 'away_team']].drop_duplicates()
print(f"\nGames with odds: {len(games)}")
for _, g in games.iterrows():
    print(f"  {g['away_team']} @ {g['home_team']}")

# Save to CSV
output_file = f"data/processed/game_odds_oddsapi_2025-10-17.csv"
odds_df.to_csv(output_file, index=False)
print(f"\n✅ Saved odds to: {output_file}")

# Now create a summary by game and market
print("\n" + "="*70)
print("ODDS SUMMARY BY GAME")
print("="*70)

# Pivot to get consensus lines
for _, game in games.iterrows():
    home = game['home_team']
    away = game['away_team']
    
    game_odds = odds_df[(odds_df['home_team'] == home) & (odds_df['away_team'] == away)]
    
    print(f"\n{away} @ {home}")
    print("-" * 60)
    
    # Spreads
    spreads = game_odds[game_odds['market'] == 'spreads']
    if not spreads.empty:
        home_spreads = spreads[spreads['outcome_name'] == home]
        if not home_spreads.empty:
            avg_spread = home_spreads['point'].mean()
            avg_price = home_spreads['price'].mean()
            print(f"  Spread (home): {avg_spread:+.1f} @ {avg_price:.0f} (avg of {len(home_spreads)} books)")
    
    # Totals
    totals = game_odds[game_odds['market'] == 'totals']
    if not totals.empty:
        over_lines = totals[totals['outcome_name'] == 'Over']
        if not over_lines.empty:
            avg_total = over_lines['point'].mean()
            avg_price = over_lines['price'].mean()
            print(f"  Total: {avg_total:.1f} @ {avg_price:.0f} (avg of {len(over_lines)} books)")
    
    # Moneyline (h2h)
    h2h = game_odds[game_odds['market'] == 'h2h']
    if not h2h.empty:
        home_ml = h2h[h2h['outcome_name'] == home]
        away_ml = h2h[h2h['outcome_name'] == away]
        if not home_ml.empty and not away_ml.empty:
            print(f"  Moneyline: {away} {away_ml['price'].mean():.0f} / {home} {home_ml['price'].mean():.0f}")

print("\n" + "="*70)
print("NEXT STEP: Compare to your predictions")
print("="*70)
print("\nTo calculate edges, compare:")
print("  Your predictions: data/processed/predictions_2025-10-17.csv")
print("  Market odds: data/processed/game_odds_oddsapi_2025-10-17.csv")
print("\n")
