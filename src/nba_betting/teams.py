TEAM_ALIASES = {
    # Common aliases -> Basketball-Reference display names
    "la lakers": "Los Angeles Lakers",
    "l.a. lakers": "Los Angeles Lakers",
    "lakers": "Los Angeles Lakers",
    "gsw": "Golden State Warriors",
    "warriors": "Golden State Warriors",
    "clippers": "Los Angeles Clippers",
    "la clippers": "Los Angeles Clippers",
    "l.a. clippers": "Los Angeles Clippers",
    "boston": "Boston Celtics",
    "celtics": "Boston Celtics",
    # NBA abbreviations -> full names
    "atl": "Atlanta Hawks",
    "bos": "Boston Celtics",
    "bkn": "Brooklyn Nets",
    "cha": "Charlotte Hornets",
    "chi": "Chicago Bulls",
    "cle": "Cleveland Cavaliers",
    "dal": "Dallas Mavericks",
    "den": "Denver Nuggets",
    "det": "Detroit Pistons",
    "gsw": "Golden State Warriors",
    "hou": "Houston Rockets",
    "ind": "Indiana Pacers",
    "lac": "Los Angeles Clippers",
    "lal": "Los Angeles Lakers",
    "mem": "Memphis Grizzlies",
    "mia": "Miami Heat",
    "mil": "Milwaukee Bucks",
    "min": "Minnesota Timberwolves",
    "nop": "New Orleans Pelicans",
    "no": "New Orleans Pelicans",
    "nyk": "New York Knicks",
    "okc": "Oklahoma City Thunder",
    "orl": "Orlando Magic",
    "phi": "Philadelphia 76ers",
    "phx": "Phoenix Suns",
    "por": "Portland Trail Blazers",
    "sac": "Sacramento Kings",
    "sas": "San Antonio Spurs",
    "sa": "San Antonio Spurs",
    "tor": "Toronto Raptors",
    "uta": "Utah Jazz",
    "utah": "Utah Jazz",
    "was": "Washington Wizards",
}


def normalize_team(name: str) -> str:
    key = (name or "").strip().lower()
    return TEAM_ALIASES.get(key, name)

# Minimal tricode map; used by exporters to emit standardized team abbreviations.
_NAME_TO_TRI = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}

def to_tricode(name: str) -> str:
    """Best-effort conversion of a team string to an NBA tricode.

    - Accepts aliases and abbreviations via normalize_team
    - Falls back to uppercasing a 3-letter key if it looks like a tricode
    - Returns the input uppercased if no mapping found
    """
    if not name:
        return ""
    norm = normalize_team(name)
    tri = _NAME_TO_TRI.get(norm)
    if tri:
        return tri
    # If already looks like a tricode, return upper
    s = str(name).strip().upper()
    if len(s) == 3:
        return s
    return s
