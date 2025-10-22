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
try:
    from ..teams import to_tricode as _to_tri
except Exception:
    def _to_tri(x: str) -> str:
        return (x or '').strip().upper()


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
            - status: OUT, QUESTIONABLE, DOUBTFUL, DAY-TO-DAY, etc.
            - injury: Injury description
            - date: Report date (YYYY-MM-DD)
        """
        try:
            response = self.session.get(self.BASE_URL, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            injuries: List[Dict[str, str]] = []

            # ESPN often renders each team's injuries as a titled table block.
            # Pair the nearest preceding team title (Table__Title) with each ResponsiveTable.
            sections = soup.find_all('div', class_='ResponsiveTable')
            for section in sections:
                # Find closest preceding team title
                team_header = section.find_previous(lambda tag: tag.name in ('div', 'h2', 'h3') and (
                    ('Table__Title' in (tag.get('class') or [])) or 'Injuries' in tag.get_text('')
                ))
                if not team_header:
                    continue
                team_name_raw = team_header.get_text(strip=True)
                team_abbr = self._get_team_abbreviation(team_name_raw)

                table = section.find('table')
                if not table:
                    continue

                rows = table.find_all('tr')
                if not rows:
                    continue

                # Build header map from first row (th or td)
                header_cells = [c.get_text(strip=True).upper() for c in rows[0].find_all(['th', 'td'])]
                # Fallback if header row is not present: try to infer by column count
                has_header = any(h for h in header_cells)

                def idx_of(names: List[str], default: Optional[int] = None) -> Optional[int]:
                    for n in names:
                        for i, h in enumerate(header_cells):
                            if n in h:
                                return i
                    return default

                player_idx = idx_of(['PLAYER', 'NAME'], 0)
                # ESPN sometimes has columns: PLAYER | POS | DATE | INJURY | STATUS | RETURN
                status_idx = idx_of(['STATUS'], None)
                injury_idx = idx_of(['INJURY', 'DETAIL'], None)
                date_idx = idx_of(['DATE', 'UPDATED'], None)

                # If header couldn't be read (no <th>), assume a common layout by count
                # [PLAYER, POS, DATE, INJURY, STATUS, ...]
                if not has_header or status_idx is None or injury_idx is None:
                    # Infer by column count of second row
                    sample_cols = rows[1].find_all('td') if len(rows) > 1 else []
                    n = len(sample_cols)
                    if n >= 5:
                        player_idx = 0
                        # POS -> 1, DATE -> 2, INJURY -> 3, STATUS -> 4
                        date_idx = 2 if date_idx is None else date_idx
                        injury_idx = 3 if injury_idx is None else injury_idx
                        status_idx = 4 if status_idx is None else status_idx
                    elif n >= 3:
                        # Legacy: [PLAYER, STATUS, INJURY]
                        player_idx = 0
                        status_idx = 1 if status_idx is None else status_idx
                        injury_idx = 2 if injury_idx is None else injury_idx

                # Process data rows
                data_rows = rows[1:] if has_header else rows
                for row in data_rows:
                    cols = row.find_all('td')
                    if not cols:
                        continue
                    try:
                        player = cols[player_idx].get_text(strip=True) if player_idx is not None and player_idx < len(cols) else ''
                        status = cols[status_idx].get_text(strip=True).upper() if status_idx is not None and status_idx < len(cols) else ''
                        injury = cols[injury_idx].get_text(strip=True) if injury_idx is not None and injury_idx < len(cols) else ''
                        # Normalize common status short-hands from ESPN
                        status_norm = status.upper()
                        # Some pages render empty status but put it in injury/notes; try to extract OUT/QUESTIONABLE keywords
                        if not status_norm and injury:
                            txt = injury.upper()
                            for key in ['OUT', 'QUESTIONABLE', 'DOUBTFUL', 'DAY-TO-DAY', 'DTD', 'SUSPENDED', 'INACTIVE', 'REST']:
                                if key in txt:
                                    status_norm = key
                                    break
                        date_str = datetime.now().strftime('%Y-%m-%d')
                        injuries.append({
                            'team': team_abbr,
                            'player': player,
                            'status': status_norm,
                            'injury': injury,
                            'date': date_str,
                        })
                    except Exception:
                        continue

            df = pd.DataFrame(injuries)
            # Basic cleanup: drop empties and duplicates
            if not df.empty:
                df = df[df['player'].astype(str).str.len() > 0]
                df = df[df['team'].astype(str).str.len() > 0]
                df = df.drop_duplicates(subset=['team', 'player', 'date', 'status'], keep='last')
            return df

        except Exception as e:
            print(f"Error fetching ESPN injury data: {e}")
            return pd.DataFrame()


class RotowireInjuryScraper:
    """Scrapes NBA injury reports from RotoWire.

    Uses a resilient header-detection approach to parse the central injury page.
    """
    BASE_URL = "https://www.rotowire.com/basketball/injury-report.php"
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

    def get_all_injuries(self) -> pd.DataFrame:
        try:
            html = None
            # 1) Try simple GET
            try:
                resp = self.session.get(self.BASE_URL, timeout=20)
                resp.raise_for_status()
                html = resp.text
            except Exception:
                html = None

            rows = self._parse_html(html) if html else []
            # 2) If no rows, try headless rendering
            if not rows:
                rendered = self._fetch_html_headless()
                if rendered:
                    rows = self._parse_html(rendered)

            df = pd.DataFrame(rows)
            if not df.empty:
                df = df[df['player'].astype(str).str.len() > 0]
                df = df[df['team'].astype(str).str.len() > 0]
                df = df.drop_duplicates(subset=['team','player','date','status'], keep='last')
            return df
        except Exception as e:
            print(f"Error fetching Rotowire injury data: {e}")
            return pd.DataFrame()

    def _parse_html(self, html: Optional[str]) -> List[Dict[str, str]]:
        if not html:
            return []
        soup = BeautifulSoup(html, 'lxml')
        tables = soup.find_all('table')
        out_rows: List[Dict[str, str]] = []

        def idx_of(header_cells: List[str], names: List[str], default: Optional[int] = None) -> Optional[int]:
            for n in names:
                for i, h in enumerate(header_cells):
                    if n in h:
                        return i
            return default

        for tbl in tables:
            rows = tbl.find_all('tr')
            if not rows:
                continue
            header_cells_raw = rows[0].find_all(['th','td'])
            header_cells = [c.get_text(strip=True).upper() for c in header_cells_raw]
            # Heuristic: must contain at least PLAYER and STATUS or INJURY
            if not any('PLAYER' in h for h in header_cells):
                continue
            if not any(('STATUS' in h) or ('INJURY' in h) for h in header_cells):
                continue

            player_idx = idx_of(header_cells, ['PLAYER','NAME'], 0)
            status_idx = idx_of(header_cells, ['STATUS'], None)
            injury_idx = idx_of(header_cells, ['INJURY','DETAIL','NOTES'], None)
            team_idx = idx_of(header_cells, ['TEAM'], None)

            data_rows = rows[1:]
            for r in data_rows:
                cols = r.find_all('td')
                if not cols:
                    continue
                try:
                    player = cols[player_idx].get_text(strip=True) if player_idx is not None and player_idx < len(cols) else ''
                    status = cols[status_idx].get_text(strip=True).upper() if status_idx is not None and status_idx < len(cols) else ''
                    injury = cols[injury_idx].get_text(strip=True) if injury_idx is not None and injury_idx < len(cols) else ''
                    team_raw = cols[team_idx].get_text(strip=True) if team_idx is not None and team_idx < len(cols) else ''
                    team = _to_tri(team_raw)
                    if (not team) or (len(team) != 3):
                        # Try to infer team from player cell if it contains team badge/name like "LeBron James (LAL)"
                        ptxt = cols[player_idx].get_text(' ', strip=True) if player_idx is not None and player_idx < len(cols) else ''
                        maybe = ptxt.split('(')[-1].split(')')[0] if '(' in ptxt and ')' in ptxt else ''
                        team = _to_tri(maybe) if maybe else team

                    status_norm = status.upper()
                    if not status_norm and injury:
                        itxt = injury.upper()
                        for key in ['OUT','QUESTIONABLE','DOUBTFUL','DAY-TO-DAY','DTD','SUSPENDED','INACTIVE','REST']:
                            if key in itxt:
                                status_norm = key; break
                    date_str = datetime.now().strftime('%Y-%m-%d')
                    out_rows.append({
                        'team': team,
                        'player': player,
                        'status': status_norm,
                        'injury': injury,
                        'date': date_str,
                    })
                except Exception:
                    continue
        return out_rows

    def _fetch_html_headless(self) -> Optional[str]:
        """Render Rotowire in a headless browser (Selenium) and return HTML, or None on failure."""
        try:
            # Lazy imports so Selenium is optional
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from webdriver_manager.chrome import ChromeDriverManager

            options = Options()
            # Chrome 109+ supports --headless=new for better behavior
            options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument(f"--user-agent={self.HEADERS.get('User-Agent')}")

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            try:
                driver.set_page_load_timeout(30)
                driver.get(self.BASE_URL)
                # Wait for a table with header cells containing Player/Status
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'table'))
                )
                html = driver.page_source
                return html
            finally:
                driver.quit()
        except Exception as e:
            print(f"Headless Rotowire fetch failed: {e}")
            return None
    
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
        # Prefer Rotowire; fallback to ESPN
        combined_new = pd.DataFrame()
        try:
            rw = RotowireInjuryScraper()
            rw_df = rw.get_all_injuries()
            if not rw_df.empty:
                combined_new = rw_df
        except Exception as e:
            print(f"Rotowire scrape failed: {e}")
        try:
            espn_df = self.scraper.get_all_injuries()
            if not espn_df.empty:
                combined_new = pd.concat([combined_new, espn_df], ignore_index=True) if not combined_new.empty else espn_df
        except Exception as e:
            print(f"ESPN scrape failed: {e}")

        if combined_new.empty:
            print("No new injury data fetched from either source")
            return pd.DataFrame()
        
        # Load existing data
        try:
            existing = pd.read_csv(self.filepath)
            # Append new data (avoiding duplicates)
            combined = pd.concat([existing, combined_new], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=['team', 'player', 'date', 'status'],
                keep='last'
            )
        except FileNotFoundError:
            combined = combined_new
        
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
