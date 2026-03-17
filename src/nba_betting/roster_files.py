from __future__ import annotations

from pathlib import Path

import pandas as pd


_EXPECTED_TEAM_CODES: tuple[str, ...] = (
    "ATL",
    "BKN",
    "BOS",
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
)


def expected_roster_team_codes() -> list[str]:
    return list(_EXPECTED_TEAM_CODES)


def roster_file_team_set(path: Path | str | None) -> set[str]:
    if path is None:
        return set()
    try:
        roster_path = Path(path)
    except Exception:
        return set()
    try:
        if not roster_path.exists():
            return set()
        df = pd.read_csv(roster_path, usecols=["TEAM_ABBREVIATION"])
        if not isinstance(df, pd.DataFrame) or df.empty:
            return set()
        vals = df["TEAM_ABBREVIATION"].dropna().astype(str).str.upper().str.strip()
        return {v for v in vals.tolist() if v}
    except Exception:
        return set()


def roster_file_team_count(path: Path | str | None) -> int:
    return int(len(roster_file_team_set(path)))


def roster_file_missing_teams(path: Path | str | None, expected: list[str] | tuple[str, ...] | None = None) -> list[str]:
    expected_set = {str(v).strip().upper() for v in (expected or _EXPECTED_TEAM_CODES) if str(v).strip()}
    if not expected_set:
        return []
    return sorted(expected_set - roster_file_team_set(path))


def roster_file_is_complete(
    path: Path | str | None,
    *,
    expected: list[str] | tuple[str, ...] | None = None,
    min_team_count: int | None = None,
) -> bool:
    teams = roster_file_team_set(path)
    if not teams:
        return False

    expected_set = {str(v).strip().upper() for v in (expected or _EXPECTED_TEAM_CODES) if str(v).strip()}
    if min_team_count is None:
        min_team_count = len(expected_set) if expected_set else len(_EXPECTED_TEAM_CODES)
    try:
        min_team_count = max(1, int(min_team_count))
    except Exception:
        min_team_count = len(_EXPECTED_TEAM_CODES)

    if expected_set:
        return len(teams & expected_set) >= min_team_count
    return len(teams) >= min_team_count


def pick_rosters_file(processed_dir: Path | str, season: str | None = None) -> Path | None:
    processed = Path(processed_dir)
    candidates: list[Path] = []
    seen: set[Path] = set()

    def _add(path: Path) -> None:
        if path.exists() and path not in seen:
            seen.add(path)
            candidates.append(path)

    if season:
        _add(processed / f"rosters_{season}.csv")
        start_year = str(season).split("-", 1)[0].strip()
        if start_year:
            _add(processed / f"rosters_{start_year}.csv")
            for path in sorted(processed.glob(f"rosters_{start_year}*.csv")):
                _add(path)

    if not candidates:
        for path in sorted(processed.glob("rosters_*.csv")):
            _add(path)

    if not candidates:
        return None

    candidates.sort(
        key=lambda p: (roster_file_team_count(p), p.stat().st_mtime if p.exists() else 0),
        reverse=True,
    )
    return candidates[0]