TEAM_ALIASES = {
    # Common aliases -> canonical WNBA display names
    "atl": "Atlanta Dream",
    "atlanta": "Atlanta Dream",
    "dream": "Atlanta Dream",
    "chi": "Chicago Sky",
    "chicago": "Chicago Sky",
    "sky": "Chicago Sky",
    "conn": "Connecticut Sun",
    "con": "Connecticut Sun",
    "connecticut": "Connecticut Sun",
    "sun": "Connecticut Sun",
    "dal": "Dallas Wings",
    "dallas": "Dallas Wings",
    "wings": "Dallas Wings",
    "gs": "Golden State Valkyries",
    "gsv": "Golden State Valkyries",
    "golden state": "Golden State Valkyries",
    "valkyries": "Golden State Valkyries",
    "ind": "Indiana Fever",
    "indiana": "Indiana Fever",
    "fever": "Indiana Fever",
    "las": "Los Angeles Sparks",
    "la": "Los Angeles Sparks",
    "los angeles": "Los Angeles Sparks",
    "sparks": "Los Angeles Sparks",
    "lv": "Las Vegas Aces",
    "lva": "Las Vegas Aces",
    "las vegas": "Las Vegas Aces",
    "aces": "Las Vegas Aces",
    "min": "Minnesota Lynx",
    "minnesota": "Minnesota Lynx",
    "lynx": "Minnesota Lynx",
    "ny": "New York Liberty",
    "nyl": "New York Liberty",
    "new york": "New York Liberty",
    "liberty": "New York Liberty",
    "phx": "Phoenix Mercury",
    "pho": "Phoenix Mercury",
    "phoenix": "Phoenix Mercury",
    "mercury": "Phoenix Mercury",
    "sea": "Seattle Storm",
    "seattle": "Seattle Storm",
    "storm": "Seattle Storm",
    "por": "Portland Fire",
    "portland": "Portland Fire",
    "fire": "Portland Fire",
    "tor": "Toronto Tempo",
    "toronto": "Toronto Tempo",
    "tempo": "Toronto Tempo",
    "was": "Washington Mystics",
    "wsh": "Washington Mystics",
    "washington": "Washington Mystics",
    "mystics": "Washington Mystics",
}

# Also allow case-insensitive matching on already-canonical display names.
_CANONICAL_BY_LOWER = {v.lower(): v for v in TEAM_ALIASES.values()}


def normalize_team(name: str) -> str:
    raw = (name or "").strip()
    key = raw.lower()
    if key in TEAM_ALIASES:
        return TEAM_ALIASES[key]
    if key in _CANONICAL_BY_LOWER:
        return _CANONICAL_BY_LOWER[key]
    return raw

# Minimal tricode map; used by exporters to emit standardized team abbreviations.
_NAME_TO_TRI = {
    "Atlanta Dream": "ATL",
    "Chicago Sky": "CHI",
    "Connecticut Sun": "CON",
    "Dallas Wings": "DAL",
    "Golden State Valkyries": "GSV",
    "Indiana Fever": "IND",
    "Las Vegas Aces": "LVA",
    "Los Angeles Sparks": "LAS",
    "Minnesota Lynx": "MIN",
    "New York Liberty": "NYL",
    "Phoenix Mercury": "PHX",
    "Portland Fire": "POR",
    "Seattle Storm": "SEA",
    "Toronto Tempo": "TOR",
    "Washington Mystics": "WSH",
}

TEAM_TRICODES = tuple(sorted(set(_NAME_TO_TRI.values())))

def to_tricode(name: str) -> str:
    """Best-effort conversion of a team string to a WNBA tricode.

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
