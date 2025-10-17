"""
Calculate betting edges by comparing model predictions to market odds
"""

import pandas as pd
import numpy as np
import sys

# Add src to path
sys.path.insert(0, 'src')

def american_to_prob(american_odds):
    """Convert American odds to implied probability"""
    if american_odds > 0:
        return 100 / (american_odds + 100)
    else:
        return abs(american_odds) / (abs(american_odds) + 100)

def prob_to_american(prob):
    """Convert probability to American odds"""
    if prob >= 0.5:
        return -(prob * 100) / (1 - prob)
    else:
        return ((1 - prob) * 100) / prob

print("\n" + "="*70)
print("CALCULATING BETTING EDGES")
print("="*70)

# Load predictions
preds = pd.read_csv('data/processed/predictions_2025-10-17.csv')
print(f"\n✅ Loaded {len(preds)} predictions")

# Load market odds
odds = pd.read_csv('data/processed/game_odds_2025-10-17.csv')
print(f"✅ Loaded {len(odds)} market lines from {odds['bookmaker'].iloc[0]}")

# Merge predictions with odds
merged = preds.merge(
    odds,
    on=['home_team', 'visitor_team'],
    how='inner'
)

# Check columns after merge
print(f"\nColumns available: {list(merged.columns)}")

print(f"\n✅ Matched {len(merged)} games with both predictions and odds")

print("\n" + "="*70)
print("EDGE ANALYSIS")
print("="*70)

edges = []

for _, game in merged.iterrows():
    home = game['home_team']
    away = game['visitor_team']
    
    # Our predictions
    pred_win_prob = game['home_win_prob']
    pred_margin = game['pred_margin']
    pred_total = game['pred_total']
    
    # Market lines (using _y suffix from merge, or checking both sources)
    market_spread = game.get('home_spread_y', game.get('home_spread', None))
    market_total = game.get('total_y', game.get('total', None))
    home_ml = game.get('home_ml_y', game.get('home_ml', None))
    
    # Calculate implied probabilities from market
    market_win_prob = american_to_prob(home_ml)
    
    print(f"\n{away} @ {home}")
    print("-" * 60)
    
    # WIN PROBABILITY EDGE
    win_prob_edge = pred_win_prob - market_win_prob
    print(f"\nWin Probability:")
    print(f"  Model: {pred_win_prob:.1%}")
    print(f"  Market: {market_win_prob:.1%} (ML: {home_ml:+.0f})")
    print(f"  Edge: {win_prob_edge:+.1%}")
    
    # Betting recommendation for moneyline
    if abs(win_prob_edge) > 0.05:  # 5% edge threshold
        if win_prob_edge > 0:
            print(f"  >>> BET HOME: {home} ML {home_ml:+.0f}")
            edges.append({
                'game': f"{away} @ {home}",
                'bet_type': 'Moneyline',
                'recommendation': f'{home} ML {home_ml:+.0f}',
                'edge': win_prob_edge,
                'model_value': pred_win_prob,
                'market_value': market_win_prob
            })
        else:
            away_ml = game.get('away_ml_y', game.get('away_ml', None))
            print(f"  >>> BET AWAY: {away} ML {away_ml:+.0f}")
            edges.append({
                'game': f"{away} @ {home}",
                'bet_type': 'Moneyline',
                'recommendation': f'{away} ML {away_ml:+.0f}',
                'edge': abs(win_prob_edge),
                'model_value': 1 - pred_win_prob,
                'market_value': 1 - market_win_prob
            })
    
    # SPREAD EDGE
    spread_diff = pred_margin - market_spread
    print(f"\nSpread:")
    print(f"  Model: {pred_margin:+.1f}")
    print(f"  Market: {market_spread:+.1f}")
    print(f"  Difference: {spread_diff:+.1f} points")
    
    # If model predicts better performance than market, bet on that side
    if abs(spread_diff) > 3.0:  # 3-point threshold
        if spread_diff > 0:
            # Model thinks home team will beat spread
            print(f"  >>> BET HOME: {home} {market_spread:+.1f} (-110)")
            edges.append({
                'game': f"{away} @ {home}",
                'bet_type': 'Spread',
                'recommendation': f'{home} {market_spread:+.1f}',
                'edge': abs(spread_diff),
                'model_value': pred_margin,
                'market_value': market_spread
            })
        else:
            # Model thinks away team will beat spread
            away_spread = game.get('away_spread_y', game.get('away_spread', -market_spread))
            print(f"  >>> BET AWAY: {away} {away_spread:+.1f} (-110)")
            edges.append({
                'game': f"{away} @ {home}",
                'bet_type': 'Spread',
                'recommendation': f'{away} {away_spread:+.1f}',
                'edge': abs(spread_diff),
                'model_value': pred_margin,
                'market_value': market_spread
            })
    
    # TOTAL EDGE
    total_diff = pred_total - market_total
    print(f"\nTotal:")
    print(f"  Model: {pred_total:.1f}")
    print(f"  Market: {market_total:.1f}")
    print(f"  Difference: {total_diff:+.1f} points")
    
    # If difference is significant, bet over/under
    if abs(total_diff) > 5.0:  # 5-point threshold
        if total_diff > 0:
            print(f"  >>> BET OVER: {market_total:.1f} (-110)")
            edges.append({
                'game': f"{away} @ {home}",
                'bet_type': 'Total',
                'recommendation': f'Over {market_total:.1f}',
                'edge': total_diff,
                'model_value': pred_total,
                'market_value': market_total
            })
        else:
            print(f"  >>> BET UNDER: {market_total:.1f} (-110)")
            edges.append({
                'game': f"{away} @ {home}",
                'bet_type': 'Total',
                'recommendation': f'Under {market_total:.1f}',
                'edge': abs(total_diff),
                'model_value': pred_total,
                'market_value': market_total
            })

# Summary
print("\n" + "="*70)
print(f"BETTING RECOMMENDATIONS ({len(edges)} edges found)")
print("="*70)

if edges:
    edges_df = pd.DataFrame(edges)
    edges_df = edges_df.sort_values('edge', ascending=False)
    
    print(f"\n{len(edges)} betting opportunities with significant edge:\n")
    for i, edge in edges_df.iterrows():
        print(f"{i+1}. {edge['game']}")
        print(f"   {edge['bet_type']}: {edge['recommendation']}")
        edge_display = f"{edge['edge']:.1%}" if edge['bet_type'] == 'Moneyline' else f"{edge['edge']:.1f} pts"
        print(f"   Edge: {edge_display}")
        print(f"   Model: {edge['model_value']:.2f} | Market: {edge['market_value']:.2f}")
        print()
    
    # Save recommendations
    output_file = 'data/processed/edges_2025-10-17.csv'
    edges_df.to_csv(output_file, index=False)
    print(f"✅ Saved recommendations to: {output_file}")
else:
    print("\n⚠️  No significant edges found")
    print("   Try adjusting thresholds:")
    print("   - Win probability: Currently 5%")
    print("   - Spread: Currently 3 points")
    print("   - Total: Currently 5 points")

print("\n" + "="*70)
print("IMPORTANT NOTES")
print("="*70)
print("\n⚠️  Model Calibration:")
print("   - Your model shows 0% and 100% win probabilities")
print("   - This suggests overconfidence - use with caution")
print("   - Focus on spread and total predictions for now")
print("   - Consider recalibrating probabilities after collecting data")

print("\n📊 Betting Strategy:")
print("   - Day 1: Shadow mode recommended (don't bet, just track)")
print("   - Wait 5-7 days to validate accuracy")
print("   - Start with small stakes once validated")
print("   - Never bet more than 1-2% per game initially")

print("\n")
