"""
ESPN Injury Report scraper for NBA teams.
Tracks injuries, questionable players, and impact on team strength.
"""

import time
from typing import List, Dict, Optional
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd


class ESPNInjuryScraper:
    """Scrapes NBA injury reports from ESPN."""
    
    BASE_URL = "https://www.espn.com/nba/injuries"
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # Player impact weights (rough estimate)
    POSITION_IMPACT = {
        'starter': 1.0,
        'key_reserve': 0.6,
        'reserve': 0.3,
        'deep_bench': 0.1,
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
    
    def get_all_injuries(self) -> pd.DataFrame:
        """
        Scrape current injury report for all NBA teams.
        
        Returns:
            DataFrame with columns:
            - team: Team abbreviation
            - player: Player name
            - status: OUT, QUESTIONABLE, DOUBTFUL, DAY-TO-DAY
            - injury: Injury description
            - date: Report date
        """
        try:
            response = self.session.get(self.BASE_URL, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            injuries = []
            
            # Find all team injury sections
            injury_sections = soup.find_all('div', class_='ResponsiveTable')
            
            for section in injury_sections:
                # Get team name
                team_header = section.find_previous('div', class_='Table__Title')
                if not team_header:
                    continue
                
                team_name = team_header.get_text(strip=True)
                team_abbr = self._get_team_abbreviation(team_name)
                
                # Parse injury table
                table = section.find('table')
                if not table:
                    continue
                
                rows = table.find_all('tr')[1:]  # Skip header
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        player = cols[0].get_text(strip=True)
                        status = cols[1].get_text(strip=True).upper()
                        injury = cols[2].get_text(strip=True)
                        
                        injuries.append({
                            'team': team_abbr,
                            'player': player,
                            'status': status,
                            'injury': injury,
                            'date': datetime.now().strftime('%Y-%m-%d'),
                        })
            
            return pd.DataFrame(injuries)
            
        except Exception as e:
            print(f"Error fetching ESPN injury data: {e}")
            return pd.DataFrame()
    
    def get_team_injury_impact(self, team: str) -> Dict[str, any]:
        """
        Calculate injury impact for a specific team.
        
        Args:
            team: Team abbreviation
        
        Returns:
            Dict with:
            - total_injuries: Total number of injured players
            - out_count: Players ruled out
            - questionable_count: Questionable players
            - impact_score: Weighted impact (0-10 scale, higher = more impactful injuries)
        """
        injuries = self.get_all_injuries()
        
        if injuries.empty:
            return {
                'total_injuries': 0,
                'out_count': 0,
                'questionable_count': 0,
                'doubtful_count': 0,
                'impact_score': 0.0,
            }
        
        team_injuries = injuries[injuries['team'] == team]
        
        out_count = len(team_injuries[team_injuries['status'] == 'OUT'])
        questionable = len(team_injuries[team_injuries['status'] == 'QUESTIONABLE'])
        doubtful = len(team_injuries[team_injuries['status'] == 'DOUBTFUL'])
        
        # Calculate impact score (simplified - would need player importance data)
        # For now, OUT = 1.0, DOUBTFUL = 0.5, QUESTIONABLE = 0.3
        impact_score = (
            out_count * 1.0 +
            doubtful * 0.5 +
            questionable * 0.3
        )
        
        return {
            'total_injuries': len(team_injuries),
            'out_count': out_count,
            'questionable_count': questionable,
            'doubtful_count': doubtful,
            'impact_score': float(impact_score),
        }
    
    def get_matchup_injury_differential(self, home_team: str, 
                                       away_team: str) -> Dict[str, float]:
        """
        Compare injury situations for both teams in a matchup.
        
        Args:
            home_team: Home team abbreviation
            away_team: Away team abbreviation
        
        Returns:
            Dict with home/away impact scores and differential
        """
        home_impact = self.get_team_injury_impact(home_team)
        away_impact = self.get_team_injury_impact(away_team)
        
        differential = home_impact['impact_score'] - away_impact['impact_score']
        
        return {
            'home_injury_impact': home_impact['impact_score'],
            'away_injury_impact': away_impact['impact_score'],
            'injury_differential': differential,  # Negative = home more injured
            'home_out': home_impact['out_count'],
            'away_out': away_impact['out_count'],
        }
    
    def _get_team_abbreviation(self, team_name: str) -> str:
        """Convert ESPN team name to standard abbreviation."""
        
        team_map = {
            'Atlanta Hawks': 'ATL',
            'Boston Celtics': 'BOS',
            'Brooklyn Nets': 'BKN',
            'Charlotte Hornets': 'CHA',
            'Chicago Bulls': 'CHI',
            'Cleveland Cavaliers': 'CLE',
            'Dallas Mavericks': 'DAL',
            'Denver Nuggets': 'DEN',
            'Detroit Pistons': 'DET',
            'Golden State Warriors': 'GSW',
            'Houston Rockets': 'HOU',
            'Indiana Pacers': 'IND',
            'LA Clippers': 'LAC',
            'Los Angeles Clippers': 'LAC',
            'LA Lakers': 'LAL',
            'Los Angeles Lakers': 'LAL',
            'Memphis Grizzlies': 'MEM',
            'Miami Heat': 'MIA',
            'Milwaukee Bucks': 'MIL',
            'Minnesota Timberwolves': 'MIN',
            'New Orleans Pelicans': 'NOP',
            'New York Knicks': 'NYK',
            'Oklahoma City Thunder': 'OKC',
            'Orlando Magic': 'ORL',
            'Philadelphia 76ers': 'PHI',
            'Phoenix Suns': 'PHX',
            'Portland Trail Blazers': 'POR',
            'Sacramento Kings': 'SAC',
            'San Antonio Spurs': 'SAS',
            'Toronto Raptors': 'TOR',
            'Utah Jazz': 'UTA',
            'Washington Wizards': 'WAS',
        }
        
        return team_map.get(team_name, team_name)


class NBAInjuryDatabase:
    """Manages injury data with historical tracking."""
    
    def __init__(self, filepath: str = "data/raw/injuries.csv"):
        self.filepath = filepath
        self.scraper = ESPNInjuryScraper()
    
    def update_injuries(self) -> pd.DataFrame:
        """Fetch latest injuries and append to database."""
        new_injuries = self.scraper.get_all_injuries()
        
        if new_injuries.empty:
            print("No new injury data fetched")
            return pd.DataFrame()
        
        # Load existing data
        try:
            existing = pd.read_csv(self.filepath)
            # Append new data (avoiding duplicates)
            combined = pd.concat([existing, new_injuries], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=['team', 'player', 'date', 'status'],
                keep='last'
            )
        except FileNotFoundError:
            combined = new_injuries
        
        # Save updated database
        combined.to_csv(self.filepath, index=False)
        print(f"Saved {len(combined)} injury records to {self.filepath}")
        
        return combined
    
    def get_injury_features(self, team: str, date: str) -> Dict[str, float]:
        """
        Get injury features for a team on a specific date.
        
        Args:
            team: Team abbreviation
            date: Date in YYYY-MM-DD format
        
        Returns:
            Dict with injury-related features
        """
        try:
            df = pd.read_csv(self.filepath)
            # Filter to date and team
            team_injuries = df[(df['team'] == team) & (df['date'] == date)]
            
            if team_injuries.empty:
                return {
                    'injuries_out': 0,
                    'injuries_questionable': 0,
                    'injuries_total': 0,
                    'injury_impact': 0.0,
                }
            
            out = len(team_injuries[team_injuries['status'] == 'OUT'])
            questionable = len(team_injuries[team_injuries['status'] == 'QUESTIONABLE'])
            doubtful = len(team_injuries[team_injuries['status'] == 'DOUBTFUL'])
            
            impact = out * 1.0 + doubtful * 0.5 + questionable * 0.3
            
            return {
                'injuries_out': out,
                'injuries_questionable': questionable,
                'injuries_total': len(team_injuries),
                'injury_impact': float(impact),
            }
            
        except Exception as e:
            print(f"Error getting injury features: {e}")
            return {
                'injuries_out': 0,
                'injuries_questionable': 0,
                'injuries_total': 0,
                'injury_impact': 0.0,
            }


# Example usage
if __name__ == "__main__":
    scraper = ESPNInjuryScraper()
    
    print("Fetching injury report...")
    injuries = scraper.get_all_injuries()
    print(f"Found {len(injuries)} injury reports")
    print(injuries.head(10))
    
    print("\nTeam injury impacts:")
    for team in injuries['team'].unique()[:5]:
        impact = scraper.get_team_injury_impact(team)
        print(f"{team}: {impact}")
    
    print("\nMatchup differential (BKN @ TOR):")
    diff = scraper.get_matchup_injury_differential('TOR', 'BKN')
    print(diff)
    
    print("\nTesting injury database...")
    db = NBAInjuryDatabase()
    db.update_injuries()
