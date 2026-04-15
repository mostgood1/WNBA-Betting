"""
Enhanced feature engineering with injury data and advanced statistics.
Extends base features with pace, efficiency, and injury impact.
"""

from __future__ import annotations
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Optional

from .config import paths
from .scrapers import BasketballReferenceScraper, NBAInjuryDatabase


def add_advanced_stats_features(df: pd.DataFrame, season: int = 2025) -> pd.DataFrame:
    """
    Add pace, efficiency, and Four Factors features to games DataFrame.
    
    Args:
        df: Games DataFrame with home_team and visitor_team columns
        season: NBA season year
    
    Returns:
        DataFrame with additional advanced stats features
    """
    # Try to load cached advanced stats
    stats_file = paths.data_processed / f"team_advanced_stats_{season}.csv"
    
    if stats_file.exists():
        stats_df = pd.read_csv(stats_file)
        print(f"Loaded advanced stats from {stats_file}")
    else:
        # Fetch from Basketball Reference
        print(f"Fetching advanced stats for season {season}...")
        scraper = BasketballReferenceScraper()
        stats_df = scraper.get_team_stats(season)
        
        if not stats_df.empty:
            stats_df.to_csv(stats_file, index=False)
            print(f"Saved advanced stats to {stats_file}")
        else:
            print("No advanced stats available, skipping...")
            return df
    
    # Merge stats for home team
    df = df.merge(
        stats_df,
        left_on='home_team',
        right_on='team',
        how='left',
        suffixes=('', '_home_adv')
    )
    
    adv_cols = [
        'pace',
        'off_rtg',
        'def_rtg',
        'efg_pct',
        'tov_pct',
        'orb_pct',
        'ft_rate',
        # Optional add-ons (present in some cached advanced-stats builds)
        'fg3a_rate',
        'fg3_pct',
        'ts_pct',
        'ast_per_100',
    ]

    # Rename home team columns
    for col in adv_cols:
        if col in df.columns:
            df[f'home_{col}'] = df[col]
            df.drop(columns=[col], inplace=True)
    
    # Remove duplicate team column
    if 'team' in df.columns:
        df.drop(columns=['team'], inplace=True)
    
    # Merge stats for visitor team
    df = df.merge(
        stats_df,
        left_on='visitor_team',
        right_on='team',
        how='left',
        suffixes=('', '_visitor_adv')
    )
    
    # Rename visitor team columns
    for col in adv_cols:
        if col in df.columns:
            df[f'visitor_{col}'] = df[col]
            df.drop(columns=[col], inplace=True)
    
    # Remove duplicate team column
    if 'team' in df.columns:
        df.drop(columns=['team'], inplace=True)
    
    # Calculate differential features
    if 'home_pace' in df.columns and 'visitor_pace' in df.columns:
        df['pace_diff'] = df['home_pace'] - df['visitor_pace']
        df['combined_pace'] = (df['home_pace'] + df['visitor_pace']) / 2
    
    if 'home_off_rtg' in df.columns and 'home_def_rtg' in df.columns:
        df['home_net_rtg'] = df['home_off_rtg'] - df['home_def_rtg']
        df['visitor_net_rtg'] = df['visitor_off_rtg'] - df['visitor_def_rtg']
        df['net_rtg_diff'] = df['home_net_rtg'] - df['visitor_net_rtg']

    # Optional differentials
    for c in ['fg3a_rate', 'fg3_pct', 'ts_pct', 'ast_per_100']:
        hc = f'home_{c}'
        vc = f'visitor_{c}'
        if hc in df.columns and vc in df.columns:
            df[f'{c}_diff'] = df[hc] - df[vc]
    
    print(f"Added {len([c for c in df.columns if 'pace' in c or 'rtg' in c or 'efg' in c])} advanced stats features")
    
    return df


def add_injury_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add injury impact features to games DataFrame.
    
    Args:
        df: Games DataFrame with home_team, visitor_team, and date columns
    
    Returns:
        DataFrame with additional injury features
    """
    # Load injury database
    injury_file = paths.root / "data" / "raw" / "injuries.csv"
    
    if not injury_file.exists():
        print("No injury data found. Run: python -m nba_betting.cli fetch-injuries")
        # Add empty columns
        for prefix in ['home', 'visitor']:
            df[f'{prefix}_injuries_out'] = 0
            df[f'{prefix}_injuries_questionable'] = 0
            df[f'{prefix}_injuries_total'] = 0
            df[f'{prefix}_injury_impact'] = 0.0
        df['injury_differential'] = 0.0
        return df
    
    injury_df = pd.read_csv(injury_file)
    injury_df['date'] = pd.to_datetime(injury_df['date'])
    print(f"Loaded {len(injury_df)} injury records")
    
    # Initialize injury feature columns
    for prefix in ['home', 'visitor']:
        df[f'{prefix}_injuries_out'] = 0
        df[f'{prefix}_injuries_questionable'] = 0
        df[f'{prefix}_injuries_total'] = 0
        df[f'{prefix}_injury_impact'] = 0.0
    
    # Ensure date column is datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    # Calculate injury features for each game
    for idx, row in df.iterrows():
        game_date = pd.to_datetime(row['date'])
        
        # Get injuries within 3 days of game (current injury status)
        date_window = injury_df[
            (injury_df['date'] >= game_date - pd.Timedelta(days=3)) &
            (injury_df['date'] <= game_date + pd.Timedelta(days=1))
        ]
        
        # Home team injuries
        home_injuries = date_window[date_window['team'] == row['home_team']]
        if not home_injuries.empty:
            out_count = len(home_injuries[home_injuries['status'] == 'OUT'])
            q_count = len(home_injuries[home_injuries['status'] == 'QUESTIONABLE'])
            d_count = len(home_injuries[home_injuries['status'] == 'DOUBTFUL'])
            
            df.at[idx, 'home_injuries_out'] = out_count
            df.at[idx, 'home_injuries_questionable'] = q_count
            df.at[idx, 'home_injuries_total'] = len(home_injuries)
            df.at[idx, 'home_injury_impact'] = out_count * 1.0 + d_count * 0.5 + q_count * 0.3
        
        # Visitor team injuries
        visitor_injuries = date_window[date_window['team'] == row['visitor_team']]
        if not visitor_injuries.empty:
            out_count = len(visitor_injuries[visitor_injuries['status'] == 'OUT'])
            q_count = len(visitor_injuries[visitor_injuries['status'] == 'QUESTIONABLE'])
            d_count = len(visitor_injuries[visitor_injuries['status'] == 'DOUBTFUL'])
            
            df.at[idx, 'visitor_injuries_out'] = out_count
            df.at[idx, 'visitor_injuries_questionable'] = q_count
            df.at[idx, 'visitor_injuries_total'] = len(visitor_injuries)
            df.at[idx, 'visitor_injury_impact'] = out_count * 1.0 + d_count * 0.5 + q_count * 0.3
    
    # Calculate differential (negative = home more injured)
    df['injury_differential'] = df['home_injury_impact'] - df['visitor_injury_impact']
    
    print(f"Added injury features for {len(df)} games")
    print(f"   Average home injuries: {df['home_injuries_total'].mean():.2f}")
    print(f"   Average visitor injuries: {df['visitor_injuries_total'].mean():.2f}")
    
    return df


def build_features_enhanced(games: pd.DataFrame, 
                           include_advanced_stats: bool = True,
                           include_injuries: bool = True,
                           season: int = 2025) -> pd.DataFrame:
    """
    Build enhanced features including base features + advanced stats + injuries.
    
    Args:
        games: Raw games DataFrame
        include_advanced_stats: Whether to add pace/efficiency features
        include_injuries: Whether to add injury features
        season: NBA season year
    
    Returns:
        DataFrame with all features
    """
    from .features import build_features
    
    print("\n" + "="*70)
    print("ENHANCED FEATURE ENGINEERING")
    print("="*70)
    
    # Step 1: Build base features (ELO, rest, form, schedule)
    print("\n[1/3] Building base features (ELO, rest, form, schedule)...")
    df = build_features(games)
    base_feature_count = len(df.columns)
    print(f"Built {base_feature_count} base features")
    
    # Convert feature columns to standard numeric types (no pandas nullable types)
    # This ensures compatibility with ONNX models
    # Skip non-feature columns like team names, dates, etc.
    non_feature_cols = ['home_team', 'visitor_team', 'date', 'game_id', 'season', 
                       'home_pts', 'visitor_pts', 'home_score', 'visitor_score']
    
    for col in df.columns:
        if col in non_feature_cols:
            continue  # Skip metadata columns
        
        if df[col].dtype == 'object':
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        elif df[col].dtype == 'Int64':  # Pandas nullable integer
            df[col] = df[col].fillna(0).astype('int64')
        elif df[col].dtype == 'boolean':  # Pandas nullable boolean
            df[col] = df[col].fillna(False).astype('int64')
    
    # Step 2: Add advanced statistics
    if include_advanced_stats:
        print("\n[2/3] Adding advanced statistics (pace, efficiency, Four Factors)...")
        df = add_advanced_stats_features(df, season=season)
        adv_feature_count = len(df.columns) - base_feature_count
        print(f"Added {adv_feature_count} advanced stat features")
    else:
        print("\n[2/3] Skipping advanced statistics (disabled)")
    
    # Step 3: Add injury features
    if include_injuries:
        print("\n[3/3] Adding injury impact features...")
        df = add_injury_features(df)
        injury_feature_count = len(df.columns) - base_feature_count - (adv_feature_count if include_advanced_stats else 0)
        print(f"Added {injury_feature_count} injury features")
    else:
        print("\n[3/3] Skipping injury features (disabled)")
    
    total_features = len(df.columns)
    print("\n" + "="*70)
    print(f"TOTAL FEATURES: {total_features}")
    print("="*70 + "\n")
    
    return df


def get_enhanced_feature_columns() -> list[str]:
    """
    Return list of all feature column names for enhanced model training.
    
    Returns:
        List of feature column names
    """
    base_features = [
        "elo_diff",
        "home_rest_days", "visitor_rest_days",
        "home_b2b", "visitor_b2b",
        "home_form_off_5", "home_form_def_5",
        "visitor_form_off_5", "visitor_form_def_5",
        "home_form_margin_5", "visitor_form_margin_5", "form_margin_diff",
        "home_games_last3", "visitor_games_last3",
        "home_games_last5", "visitor_games_last5",
        "home_games_last7", "visitor_games_last7",
        "home_3in4", "visitor_3in4",
        "home_4in6", "visitor_4in6",
        "home_season_game_number", "visitor_season_game_number",
        "season_game_number_diff", "season_day_number", "season_progress",
        "rest_advantage",
    ]
    
    advanced_features = [
        "home_pace", "visitor_pace", "pace_diff", "combined_pace",
        "home_off_rtg", "visitor_off_rtg",
        "home_def_rtg", "visitor_def_rtg",
        "home_net_rtg", "visitor_net_rtg", "net_rtg_diff",
        "home_efg_pct", "visitor_efg_pct",
        "home_tov_pct", "visitor_tov_pct",
        "home_orb_pct", "visitor_orb_pct",
        "home_ft_rate", "visitor_ft_rate",
    ]
    
    injury_features = [
        "home_injuries_out", "visitor_injuries_out",
        "home_injuries_questionable", "visitor_injuries_questionable",
        "home_injuries_total", "visitor_injuries_total",
        "home_injury_impact", "visitor_injury_impact",
        "injury_differential",
    ]
    
    return base_features + advanced_features + injury_features


# Example usage
if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    # Load raw games
    games_file = paths.data_raw / "games_nba_api.csv"
    if games_file.exists():
        print(f"Loading games from {games_file}...")
        games = pd.read_csv(games_file)
        
        # Rename columns to match expected format
        if 'home_team_tri' in games.columns:
            games = games.rename(columns={'home_team_tri': 'home_team', 'visitor_team_tri': 'visitor_team'})
        if 'date_est' in games.columns:
            games = games.rename(columns={'date_est': 'date'})
        if 'home_score' in games.columns:
            games = games.rename(columns={'home_score': 'home_pts', 'visitor_score': 'visitor_pts'})
        
        # Build enhanced features
        df = build_features_enhanced(games, include_advanced_stats=True, include_injuries=True)
        
        # Show feature summary
        print("\nFeature Summary:")
        print(f"   Total games: {len(df)}")
        print(f"   Total features: {len(df.columns)}")
        print(f"   Missing values: {df.isnull().sum().sum()}")
        
        # Show sample
        feature_cols = get_enhanced_feature_columns()
        available_features = [f for f in feature_cols if f in df.columns]
        print(f"\nAvailable features: {len(available_features)}/{len(feature_cols)}")
        print("\nSample (first game):")
        print(df[available_features].head(1).T)
        
    else:
        print(f"Games file not found: {games_file}")
