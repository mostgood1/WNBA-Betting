"""Check feature data types"""
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from nba_betting.features_enhanced import build_features_enhanced, get_enhanced_feature_columns

# Load games
games = pd.read_csv('data/raw/games_nba_api.csv').tail(5)
df = build_features_enhanced(games, include_advanced_stats=True, include_injuries=True)
cols = get_enhanced_feature_columns()

print(f"\nFeature columns: {len(cols)}")
print("\nData types:")
for col in cols:
    dtype = df[col].dtype
    print(f"  {col}: {dtype}")
    if dtype == 'object':
        print(f"    Sample values: {df[col].head(3).tolist()}")
