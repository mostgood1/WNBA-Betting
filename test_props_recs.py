import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent
d = "2025-10-17"

# Load edges file
edges_p = BASE_DIR / "data" / "processed" / f"props_edges_{d}.csv"
print(f"Loading: {edges_p}")
print(f"Exists: {edges_p.exists()}")

if edges_p.exists():
    df = pd.read_csv(edges_p)
    print(f"\nTotal rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    
    # Check for player_name column
    if "player_name" in df.columns:
        unique_players = df['player_name'].nunique()
        print(f"\nUnique players: {unique_players}")
        print(f"\nFirst 5 players:")
        print(df['player_name'].unique()[:5])
        
        # Group by player
        grouped = df.groupby('player_name')
        print(f"\nPlayer groups: {len(grouped)}")
        
        # Sample one player's data
        first_player = df['player_name'].iloc[0]
        player_data = df[df['player_name'] == first_player]
        print(f"\n{first_player} has {len(player_data)} props:")
        print(player_data[['stat', 'line', 'edge', 'ev']].to_string(index=False))
    else:
        print("\n❌ No 'player_name' column found!")
        print(f"Available columns: {list(df.columns)}")
