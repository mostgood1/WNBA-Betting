"""NBA injuries scrapers.

Primary goal for the pipeline is *availability gating* for a given date.

- Preferred source: NBA official injury report PDFs from official.nba.com.
- Fallbacks: Rotowire + ESPN HTML pages.
"""

import io
import re
import time
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pypdf import PdfReader

from ..config import paths
from ..roster_files import pick_rosters_file
try:
    from ..teams import to_tricode as _to_tri
except Exception:
    def _to_tri(x: str) -> str:
        return (x or '').strip().upper()


def _season_for_date_str(date_str: str) -> str | None:
    try:
        d = datetime.strptime(str(date_str), "%Y-%m-%d")
    except Exception:
        return None
    season_year = d.year if d.month >= 7 else (d.year - 1)
    return f"{season_year}-{(season_year + 1) % 100:02d}"


def _norm_player_name_key(name: str) -> str:
    try:
        s = str(name or "").strip().lower()
    except Exception:
        s = ""
    if not s:
        return ""
    try:
        import unicodedata as _ud

        s = _ud.normalize("NFKD", s)
        s = s.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    toks = [t for t in s.split(" ") if t and t not in {"jr", "sr", "ii", "iii", "iv", "v"}]
    return " ".join(toks)


def _roster_name_to_team_map(date_str: str | None) -> dict[str, str]:
    season = _season_for_date_str(str(date_str or "").strip()) if date_str else None
    roster_file = pick_rosters_file(paths.data_processed, season=season)
    if roster_file is None or not roster_file.exists():
        return {}

    try:
        rdf = pd.read_csv(roster_file)
    except Exception:
        return {}
    if rdf is None or rdf.empty:
        return {}

    cols = {str(c).upper(): c for c in rdf.columns}
    name_col = cols.get("PLAYER") or cols.get("PLAYER_NAME")
    team_col = cols.get("TEAM_ABBREVIATION")
    if not name_col or not team_col:
        return {}

    out: dict[str, str] = {}
    for _, row in rdf[[name_col, team_col]].dropna().iterrows():
        key = _norm_player_name_key(row.get(name_col))
        if not key:
            continue
        tri = str(_to_tri(str(row.get(team_col) or "")) or "").strip().upper()
        if len(tri) == 3:
            out[key] = tri
    return out


def _apply_roster_team_corrections(df: pd.DataFrame, *, date_str: str | None) -> pd.DataFrame:
    if df is None or df.empty or "player" not in df.columns or "team" not in df.columns:
        return df

    roster_map = _roster_name_to_team_map(date_str)
    if not roster_map:
        return df

    out = df.copy()
    out["_player_key"] = out["player"].astype(str).map(_norm_player_name_key)
    out["team"] = out["_player_key"].map(lambda key: roster_map.get(str(key) or "")).where(
        out["_player_key"].astype(str).str.len() > 0,
        other=None,
    ).fillna(out["team"])
    out["team"] = out["team"].astype(str).map(lambda value: str(_to_tri(str(value) or "") or "").strip().upper())
    out = out[out["team"].astype(str).str.len() == 3].copy()
    return out.drop(columns=["_player_key"], errors="ignore")

# Robust team name -> abbreviation mapper (used by ESPN parser)
def _map_team_name_to_abbr(team_name: str) -> str:
    """Convert a full ESPN team name to a standard three-letter abbreviation.

    Falls back to the original string if no mapping is found.
    """
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
    return team_map.get((team_name or '').strip(), (team_name or '').strip())


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
                # Use module-level mapper to avoid attribute issues if class helper is missing
                team_abbr = _map_team_name_to_abbr(team_name_raw)
                # Harden: if header is noisy (e.g., page-wide "Team Injuries...") try to extract a real team name/tri
                KNOWN_TRIS = {
                    'ATL','BOS','BKN','CHA','CHI','CLE','DAL','DEN','DET','GSW','HOU','IND','LAC','LAL','MEM','MIA','MIL','MIN','NOP','NYK','OKC','ORL','PHI','PHX','POR','SAC','SAS','TOR','UTA','WAS'
                }
                if (not team_abbr) or (len(team_abbr) != 3) or (team_abbr.upper() not in KNOWN_TRIS):
                    txt = (team_name_raw or '').strip()
                    # Try to spot a known full team name inside the text
                    name_map = {
                        'Atlanta Hawks': 'ATL','Boston Celtics':'BOS','Brooklyn Nets':'BKN','Charlotte Hornets':'CHA','Chicago Bulls':'CHI','Cleveland Cavaliers':'CLE','Dallas Mavericks':'DAL','Denver Nuggets':'DEN','Detroit Pistons':'DET','Golden State Warriors':'GSW','Houston Rockets':'HOU','Indiana Pacers':'IND','LA Clippers':'LAC','Los Angeles Clippers':'LAC','LA Lakers':'LAL','Los Angeles Lakers':'LAL','Memphis Grizzlies':'MEM','Miami Heat':'MIA','Milwaukee Bucks':'MIL','Minnesota Timberwolves':'MIN','New Orleans Pelicans':'NOP','New York Knicks':'NYK','Oklahoma City Thunder':'OKC','Orlando Magic':'ORL','Philadelphia 76ers':'PHI','Phoenix Suns':'PHX','Portland Trail Blazers':'POR','Sacramento Kings':'SAC','San Antonio Spurs':'SAS','Toronto Raptors':'TOR','Utah Jazz':'UTA','Washington Wizards':'WAS'
                    }
                    found = None
                    for full, tri in name_map.items():
                        if full in txt:
                            found = tri; break
                    # Or a parenthetical tri like (LAL)
                    if not found and '(' in txt and ')' in txt:
                        maybe = txt.split('(')[-1].split(')')[0].strip().upper()
                        if maybe in KNOWN_TRIS:
                            found = maybe
                    team_abbr = found or ''
                # If still not a valid tri code, skip this section entirely
                if not team_abbr or (team_abbr.upper() not in KNOWN_TRIS):
                    continue

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
                        # Normalize common status; strip positional junk
                        POS = {'G','F','C','PG','SG','SF','PF'}
                        status_norm = (status or '').upper().strip()
                        if status_norm in POS:
                            status_norm = ''
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

                    POS = {'G','F','C','PG','SG','SF','PF'}
                    status_norm = (status or '').upper().strip()
                    if status_norm in POS:
                        status_norm = ''
                    if not status_norm and injury:
                        itxt = injury.upper()
                        for key in ['OUT','QUESTIONABLE','DOUBTFUL','DAY-TO-DAY','DTD','SUSPENDED','INACTIVE','REST']:
                            if key in itxt:
                                status_norm = key; break
                    date_str = datetime.now().strftime('%Y-%m-%d')
                    if team and len(team) == 3:
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
        fp = Path(filepath)
        if not fp.is_absolute() and fp.parts and fp.parts[0].lower() == "data":
            fp = paths.data_root / Path(*fp.parts[1:])
        self.filepath = fp
        self.scraper = ESPNInjuryScraper()

    def update_injuries(self, date_str: Optional[str] = None) -> pd.DataFrame:
        """Fetch latest injuries and append to database.

        If date_str is provided, attempts to fetch the NBA official injury report for that date.
        """
        if not date_str:
            date_str = datetime.now().strftime("%Y-%m-%d")

        # Prefer NBA official for the target date; only use fallbacks if official is unavailable.
        combined_new = pd.DataFrame()
        official_ok = False

        try:
            off = NBAOfficialInjuryReportScraper()
            off_df = off.get_injuries_for_date(date_str)
            if not off_df.empty:
                combined_new = off_df
                official_ok = True
        except Exception as e:
            print(f"NBA official injury report fetch failed: {e}")

        if not official_ok:
            try:
                rw = RotowireInjuryScraper()
                rw_df = rw.get_all_injuries()
                if not rw_df.empty:
                    combined_new = (
                        pd.concat([combined_new, rw_df], ignore_index=True)
                        if not combined_new.empty
                        else rw_df
                    )
            except Exception as e:
                print(f"Rotowire scrape failed: {e}")
            try:
                espn_df = self.scraper.get_all_injuries()
                if not espn_df.empty:
                    combined_new = (
                        pd.concat([combined_new, espn_df], ignore_index=True)
                        if not combined_new.empty
                        else espn_df
                    )
            except Exception as e:
                print(f"ESPN scrape failed: {e}")

        if combined_new.empty:
            print("No new injury data fetched from either source")
            return pd.DataFrame()

        # Ensure a deterministic date column for availability gating.
        try:
            combined_new = combined_new.copy()
            if "date" not in combined_new.columns:
                combined_new["date"] = str(date_str)
            else:
                combined_new["date"] = combined_new["date"].fillna("").astype(str)
                # If upstream returned an empty date, force to date_str.
                combined_new["date"] = combined_new["date"].where(
                    combined_new["date"].astype(str).str.len() > 0, other=str(date_str)
                )
        except Exception:
            pass

        # Load existing data; for a date-scoped run, replace that day's rows.
        try:
            existing = pd.read_csv(self.filepath)
            if not existing.empty and "date" in existing.columns:
                existing = existing[existing["date"].astype(str) != str(date_str)].copy()
            # Append new data (we will re-normalize and then de-duplicate by team/player/date)
            combined = pd.concat([existing, combined_new], ignore_index=True)
        except FileNotFoundError:
            combined = combined_new

        # Normalize statuses across entire set to remove positional leakage and infer from notes where possible
        if not combined.empty:
            POS = {"G", "F", "C", "PG", "SG", "SF", "PF"}
            for c in ("team", "player", "status", "injury", "date"):
                if c in combined.columns:
                    combined[c] = combined[c].astype(str)
            # Normalize team to tri-codes; drop rows with invalid/unrecognized teams
            try:
                from ..teams import to_tricode as _to_tri
            except Exception:
                def _to_tri(x: str) -> str:
                    return (x or "").strip().upper()
            combined["team"] = combined["team"].map(lambda x: _to_tri(str(x)))
            combined = combined[combined["team"].astype(str).str.len() == 3]
            status_norm = combined.get("status", "").astype(str).str.upper().str.strip()
            status_norm = status_norm.where(~status_norm.isin(POS), other="")
            # Infer from injury text when empty
            inj_txt = combined.get("injury", "").astype(str).str.upper()
            needs_infer = status_norm.eq("") & inj_txt.notna()
            infer_vals = []
            for need, txt in zip(needs_infer.tolist(), inj_txt.tolist()):
                if not need:
                    infer_vals.append(None)
                    continue
                val = None
                for key in [
                    "OUT",
                    "QUESTIONABLE",
                    "DOUBTFUL",
                    "PROBABLE",
                    "DAY-TO-DAY",
                    "DTD",
                    "SUSPENDED",
                    "INACTIVE",
                    "REST",
                ]:
                    if key in txt:
                        val = key
                        break
                infer_vals.append(val)
            import numpy as _np
            infer_series = pd.Series(infer_vals, index=status_norm.index)
            status_norm = status_norm.where(~needs_infer, other=infer_series.fillna(""))
            combined["status"] = status_norm
            combined = _apply_roster_team_corrections(combined, date_str=date_str)
            # De-duplicate by team/player/date (keep last = prefer newest scrape in file order)
            if "date" in combined.columns:
                combined["_row"] = _np.arange(len(combined))
                combined = combined.sort_values(["date", "_row"]).drop(columns=["_row"])
            combined = combined.drop_duplicates(subset=["team", "player", "date"], keep="last")

        # Save updated database
        try:
            Path(self.filepath).parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        combined.to_csv(self.filepath, index=False)
        print(f"Saved {len(combined)} injury records to {self.filepath}")
        return combined

    def get_injury_features(self, team: str, date: str) -> Dict[str, float]:
        """Get injury features for a team on a specific date."""
        try:
            df = pd.read_csv(self.filepath)
            team_injuries = df[(df["team"] == team) & (df["date"] == date)]

            if team_injuries.empty:
                return {
                    "injuries_out": 0,
                    "injuries_questionable": 0,
                    "injuries_total": 0,
                    "injury_impact": 0.0,
                }

            out = len(team_injuries[team_injuries["status"] == "OUT"])
            questionable = len(team_injuries[team_injuries["status"] == "QUESTIONABLE"])
            doubtful = len(team_injuries[team_injuries["status"] == "DOUBTFUL"])

            impact = out * 1.0 + doubtful * 0.5 + questionable * 0.3

            return {
                "injuries_out": int(out),
                "injuries_questionable": int(questionable),
                "injuries_total": int(len(team_injuries)),
                "injury_impact": float(impact),
            }

        except Exception as e:
            print(f"Error getting injury features: {e}")
            return {
                "injuries_out": 0,
                "injuries_questionable": 0,
                "injuries_total": 0,
                "injury_impact": 0.0,
            }


class NBAOfficialInjuryReportScraper:
    """Fetches NBA official injury report PDF and parses it into injuries rows.

    Source is official.nba.com, which links to PDFs hosted at ak-static.cms.nba.com.

    Output schema matches the historical injuries database used by the rest of the repo:
    team (tricode), player ("First Last"), status (OUT/QUESTIONABLE/...), injury (free text), date (YYYY-MM-DD).
    """

    SEASON_PAGE_TMPL = "https://official.nba.com/nba-injury-report-{season}-season/"

    _KNOWN_TRIS = {
        "ATL",
        "BOS",
        "BKN",
        "CHA",
        "CHI",
        "CLE",
        "DAL",
        "DEN",
        "DET",
        "GSW",
        "HOU",
        "IND",
        "LAC",
        "LAL",
        "MEM",
        "MIA",
        "MIL",
        "MIN",
        "NOP",
        "NYK",
        "OKC",
        "ORL",
        "PHI",
        "PHX",
        "POR",
        "SAC",
        "SAS",
        "TOR",
        "UTA",
        "WAS",
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
            }
        )

    @staticmethod
    def _season_slug_for_date(date_str: str) -> str:
        """Return season slug like '2025-26' for a YYYY-MM-DD date."""
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d")
            season_year = d.year if d.month >= 7 else (d.year - 1)
            season = f"{season_year}-{(season_year + 1) % 100:02d}"
            return season
        except Exception:
            # Best-effort fallback to current season
            now = datetime.now()
            season_year = now.year if now.month >= 7 else (now.year - 1)
            return f"{season_year}-{(season_year + 1) % 100:02d}"

    @staticmethod
    def _parse_report_time_key(url: str) -> int:
        """Return minutes since midnight for the report timestamp in a PDF URL."""
        # Injury-Report_YYYY-MM-DD_HH_MMAM.pdf
        m = re.search(r"Injury-Report_\d{4}-\d{2}-\d{2}_(\d{2})_(\d{2})(AM|PM)\.pdf", str(url))
        if not m:
            return -1
        hh = int(m.group(1))
        mm = int(m.group(2))
        ap = m.group(3)
        if hh == 12:
            hh = 0
        if ap == "PM":
            hh += 12
        return hh * 60 + mm

    def _latest_pdf_url_for_date(self, date_str: str) -> Optional[str]:
        season = self._season_slug_for_date(date_str)
        page_url = self.SEASON_PAGE_TMPL.format(season=season)
        try:
            r = self.session.get(page_url, timeout=20)
            if not r.ok:
                return None
            soup = BeautifulSoup(r.text, "html.parser")
            hrefs = [a.get("href") for a in soup.find_all("a") if a.get("href")]
            needle = f"Injury-Report_{date_str}_"
            pdfs = [h for h in hrefs if isinstance(h, str) and needle in h and h.lower().endswith(".pdf")]
            if not pdfs:
                return None
            # Normalize protocol-relative/relative links (the page tends to use absolute, but be safe)
            fixed = []
            for h in pdfs:
                if h.startswith("//"):
                    fixed.append("https:" + h)
                elif h.startswith("/"):
                    fixed.append("https://official.nba.com" + h)
                else:
                    fixed.append(h)
            fixed = sorted(fixed, key=self._parse_report_time_key)
            return fixed[-1]
        except Exception:
            return None

    def _download_pdf(self, url: str) -> Optional[bytes]:
        try:
            # PDF host is ak-static.cms.nba.com and is generally accessible.
            r = self.session.get(url, timeout=30)
            if not r.ok:
                return None
            ct = (r.headers.get("content-type") or "").lower()
            if "pdf" not in ct and not url.lower().endswith(".pdf"):
                return None
            return r.content
        except Exception:
            return None

    def _parse_pdf_rows(self, pdf_bytes: bytes, *, date_str: str) -> pd.DataFrame:
        """Parse PDF bytes into rows.

        The PDF text extraction is token-like (often one word per line). The injury report
        uses a row-based table where Matchup/Team may be omitted for subsequent player rows.
        This parser is stateful: it carries the current matchup and team forward until the next
        explicit matchup/team token appears.
        """

        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            text = "\n".join((p.extract_text() or "") for p in reader.pages)
        except Exception:
            return pd.DataFrame()

        toks = [t.strip() for t in text.splitlines() if t and str(t).strip()]
        if not toks:
            return pd.DataFrame()

        try:
            from ..teams import normalize_team, to_tricode
        except Exception:

            def to_tricode(x: str) -> str:
                return (x or "").strip().upper()

            def normalize_team(x: str) -> str:
                return x

        DATE_RE = re.compile(r"\d{2}/\d{2}/\d{4}")
        TIME_RE = re.compile(r"\d{2}:\d{2}")
        MATCHUP_RE = re.compile(r"[A-Z]{2,3}@[A-Z]{2,3}")
        STATUS_TOKS = {"Out", "Questionable", "Doubtful", "Probable"}

        def norm_status(s: str) -> str:
            s = str(s or "").strip()
            if not s:
                return ""
            m = {
                "Out": "OUT",
                "Questionable": "QUESTIONABLE",
                "Doubtful": "DOUBTFUL",
                "Probable": "PROBABLE",
            }
            return m.get(s, s.upper())

        def match_team_at(pos: int) -> tuple[Optional[str], int]:
            for n in (4, 3, 2, 1):
                if pos + n > len(toks):
                    continue
                s = " ".join(toks[pos : pos + n]).strip()
                if not s:
                    continue
                tri = (to_tricode(normalize_team(s)) or "").strip().upper()
                if tri in self._KNOWN_TRIS:
                    return tri, n
            return None, 0

        def looks_like_player_start(pos: int) -> tuple[bool, int]:
            """Return (is_player, comma_pos) where comma_pos is index of token ending with ',' for last-name part."""
            if pos >= len(toks):
                return False, -1
            t0 = toks[pos]
            if t0.endswith(","):
                return True, pos
            if pos + 1 < len(toks) and toks[pos + 1].endswith(","):
                # Allow multi-token last names like "Moore Jr.," or "Van Vleet,"
                # but avoid false positives on headers.
                if t0 not in {"Game", "Date", "Time", "Matchup", "Team", "Player", "Name", "Current", "Status", "Reason"}:
                    return True, pos + 1
            return False, -1

        out_rows: list[dict[str, str]] = []
        current_team: Optional[str] = None
        current_matchup: Optional[str] = None

        i = 0
        while i < len(toks):
            t = toks[i]

            # Skip boilerplate/header tokens commonly repeated at page breaks.
            if t in {"Injury", "Report:", "Page", "of", "Game", "Date", "Time", "Matchup", "Team", "Player", "Name", "Current", "Status", "Reason", "(ET)"}:
                i += 1
                continue
            if TIME_RE.fullmatch(t) or DATE_RE.fullmatch(t):
                i += 1
                continue
            if t in {"AM", "PM"}:
                i += 1
                continue
            if t.isdigit():
                # Page numbers and other counters
                i += 1
                continue

            # Track matchup boundaries.
            if MATCHUP_RE.fullmatch(t):
                current_matchup = t
                current_team = None
                i += 1
                continue

            # Team names can appear repeatedly; update state.
            tri, n_team = match_team_at(i)
            if tri and n_team > 0:
                current_team = tri
                i += n_team
                continue

            # Player row start
            is_player, comma_pos = looks_like_player_start(i)
            if not is_player:
                i += 1
                continue

            # Parse "Last[, suffix], First ... <Status> <Reason...>"
            last_tokens = toks[i : comma_pos + 1]
            last_tokens = [lt.rstrip(",").strip() for lt in last_tokens if lt and lt.strip()]
            j = comma_pos + 1
            if j >= len(toks):
                break
            # Find the next status token; stop early if we hit a matchup/team boundary.
            st_j: Optional[int] = None
            while j < len(toks):
                tj = toks[j]
                if tj in STATUS_TOKS:
                    st_j = j
                    break
                if MATCHUP_RE.fullmatch(tj):
                    break
                tri2, n2 = match_team_at(j)
                if tri2 and n2 > 0:
                    break
                if DATE_RE.fullmatch(tj) or TIME_RE.fullmatch(tj) or tj in {"(ET)", "Injury", "Report:", "Page", "of"}:
                    break
                j += 1
            if st_j is None or not current_team:
                # Can't safely emit a row without a status/team.
                i = comma_pos + 1
                continue

            first_tokens = [ft.strip() for ft in toks[comma_pos + 1 : st_j] if ft and ft.strip()]
            if not first_tokens:
                i = st_j + 1
                continue
            player = (" ".join(first_tokens) + " " + " ".join(last_tokens)).strip()
            status = norm_status(toks[st_j])

            # Collect reason tokens until the next player/team/matchup boundary.
            k = st_j + 1
            reason_tokens: list[str] = []
            while k < len(toks):
                tk = toks[k]
                if MATCHUP_RE.fullmatch(tk) or DATE_RE.fullmatch(tk) or TIME_RE.fullmatch(tk):
                    break
                tri3, n3 = match_team_at(k)
                if tri3 and n3 > 0:
                    break
                is_next_player, _ = looks_like_player_start(k)
                if is_next_player:
                    break
                # Page-break boilerplate shouldn't become part of the reason.
                if tk in {"Injury", "Report:", "Page", "of"}:
                    break
                reason_tokens.append(tk)
                k += 1
            reason = re.sub(r"\s+", " ", " ".join(reason_tokens)).strip()

            out_rows.append(
                {
                    "team": current_team,
                    "player": player,
                    "status": status,
                    "injury": reason,
                    "date": str(date_str),
                }
            )

            i = k

        df = pd.DataFrame(out_rows)
        if df.empty:
            return df
        df["team"] = df["team"].astype(str).str.upper().str.strip()
        df["player"] = df["player"].astype(str).str.strip()
        df["status"] = df["status"].astype(str).str.upper().str.strip()
        df["injury"] = df.get("injury", "").astype(str)
        df["date"] = df.get("date", str(date_str)).astype(str)
        df = df[df["team"].isin(sorted(self._KNOWN_TRIS))].copy()
        df = df[df["player"].astype(str).str.len() > 0].copy()
        df = df.drop_duplicates(subset=["team", "player", "date"], keep="last")
        return df

    def get_injuries_for_date(self, date_str: str) -> pd.DataFrame:
        url = self._latest_pdf_url_for_date(date_str)
        if not url:
            return pd.DataFrame()
        pdf = self._download_pdf(url)
        if not pdf:
            return pd.DataFrame()
        return self._parse_pdf_rows(pdf, date_str=date_str)


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
