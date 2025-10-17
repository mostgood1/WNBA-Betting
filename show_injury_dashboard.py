"""
Visualize injury impact across NBA teams.
Shows which teams are most affected by injuries.
"""

import pandas as pd
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from nba_betting.scrapers import ESPNInjuryScraper

def show_injury_dashboard():
    """Display injury impact dashboard."""
    
    # Load injury data
    try:
        df = pd.read_csv('data/raw/injuries.csv')
    except FileNotFoundError:
        print("❌ No injury data found. Run: python -m nba_betting.cli fetch-injuries")
        return
    
    print("\n" + "="*70)
    print(" 🏥 NBA INJURY REPORT DASHBOARD ".center(70))
    print("="*70)
    
    print(f"\n📊 OVERVIEW:")
    print(f"   Total Injuries: {len(df)}")
    print(f"   Teams Affected: {df['team'].nunique()}/30")
    print(f"   Last Updated: {df['date'].iloc[0]}")
    
    print(f"\n📋 INJURY STATUS BREAKDOWN:")
    status_counts = df['status'].value_counts()
    for status, count in status_counts.items():
        print(f"   {status}: {count}")
    
    print(f"\n🏀 TEAMS WITH MOST INJURIES:")
    team_counts = df.groupby('team').size().sort_values(ascending=False).head(10)
    for i, (team, count) in enumerate(team_counts.items(), 1):
        bar = "█" * count
        print(f"   {i:2d}. {team}: {bar} ({count})")
    
    # Calculate impact scores
    print(f"\n⚠️  INJURY IMPACT SCORES (Higher = More Impact):")
    scraper = ESPNInjuryScraper()
    
    impacts = []
    for team in df['team'].unique():
        impact = scraper.get_team_injury_impact(team)
        impacts.append({
            'team': team,
            'total': impact['total_injuries'],
            'out': impact['out_count'],
            'questionable': impact['questionable_count'],
            'impact_score': impact['impact_score'],
        })
    
    impact_df = pd.DataFrame(impacts).sort_values('impact_score', ascending=False).head(15)
    
    for i, row in enumerate(impact_df.itertuples(), 1):
        impact_bar = "🔴" * int(row.impact_score) if row.impact_score > 0 else "🟢"
        print(f"   {i:2d}. {row.team}: {impact_bar} (Score: {row.impact_score:.1f} | OUT: {row.out}, Q: {row.questionable})")
    
    print(f"\n✅ HEALTHIEST TEAMS (Fewest Injuries):")
    healthy = df.groupby('team').size().sort_values().head(5)
    for team, count in healthy.items():
        print(f"   {team}: {count} injuries")
    
    print("\n" + "="*70)
    print("💡 TIP: Teams with high impact scores may underperform expectations")
    print("="*70 + "\n")


if __name__ == "__main__":
    show_injury_dashboard()
