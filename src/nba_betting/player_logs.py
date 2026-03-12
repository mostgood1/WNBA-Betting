from __future__ import annotations

from datetime import date as _date
from datetime import timedelta
import time
from typing import Iterable, List

import pandas as pd

from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.library import http as nba_http

from .boxscores import _boxscore_from_espn, _espn_scoreboard, _espn_to_tri
from .config import paths


def _configure_stats_headers() -> None:
    try:
        nba_http.STATS_HEADERS.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": "https://www.nba.com",
                "Referer": "https://www.nba.com/stats/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                "Connection": "keep-alive",
            }
        )
    except Exception:
        pass


def _write_player_logs(df: pd.DataFrame) -> pd.DataFrame:
    out_dir = paths.data_processed
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(out_dir / "player_logs.parquet", index=False)
    except Exception:
        pass
    df.to_csv(out_dir / "player_logs.csv", index=False)
    return df


def _season_from_game_date(value: object) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    season_start = int(ts.year) if int(ts.month) >= 7 else (int(ts.year) - 1)
    return f"{season_start}-{(season_start + 1) % 100:02d}"


def _player_logs_from_boxscores_frame(hist: pd.DataFrame) -> pd.DataFrame:
    if hist is None or hist.empty:
        return pd.DataFrame()

    cols = {c.upper(): c for c in hist.columns}

    def _col(*names: str) -> str | None:
        for name in names:
            key = name.upper()
            if key in cols:
                return cols[key]
        return None

    gid_col = _col("GAME_ID", "GAMEID", "game_id", "gameId")
    date_col = _col("GAME_DATE", "DATE", "date")
    pid_col = _col("PLAYER_ID", "PERSONID", "player_id", "personId")
    pname_col = _col("PLAYER_NAME", "PLAYER")
    first_col = _col("FIRSTNAME", "FIRST_NAME", "firstName")
    last_col = _col("FAMILYNAME", "LASTNAME", "LAST_NAME", "familyName")
    team_col = _col("TEAM_ABBREVIATION", "TEAMTRICODE", "teamTricode")
    season_col = _col("SEASON")

    if not gid_col or not date_col or not pid_col or not team_col:
        return pd.DataFrame()

    fallback = pd.DataFrame(index=hist.index)
    fallback["GAME_ID"] = hist[gid_col].astype(str).str.strip()
    fallback["GAME_DATE"] = pd.to_datetime(hist[date_col], errors="coerce")
    fallback["PLAYER_ID"] = pd.to_numeric(hist[pid_col], errors="coerce")
    fallback["TEAM_ABBREVIATION"] = hist[team_col].astype(str).str.strip().str.upper()

    if pname_col:
        fallback["PLAYER_NAME"] = hist[pname_col].astype(str).str.strip()
    elif first_col and last_col:
        fallback["PLAYER_NAME"] = (
            hist[first_col].fillna("").astype(str).str.strip()
            + " "
            + hist[last_col].fillna("").astype(str).str.strip()
        ).str.strip()
    else:
        fallback["PLAYER_NAME"] = ""

    numeric_cols = {
        "MIN": ["MIN"],
        "PTS": ["PTS", "points"],
        "REB": ["REB", "reboundsTotal"],
        "AST": ["AST", "assists"],
        "STL": ["STL", "steals"],
        "BLK": ["BLK", "blocks"],
        "TOV": ["TOV", "turnovers"],
        "FG3M": ["FG3M", "threePointersMade"],
        "FG3A": ["FG3A", "threePointersAttempted"],
        "FGA": ["FGA", "fieldGoalsAttempted"],
        "FGM": ["FGM", "fieldGoalsMade"],
        "FTA": ["FTA", "freeThrowsAttempted"],
        "FTM": ["FTM", "freeThrowsMade"],
        "OREB": ["OREB", "reboundsOffensive"],
        "DREB": ["DREB", "reboundsDefensive"],
        "PF": ["PF", "foulsPersonal"],
        "PLUS_MINUS": ["PLUS_MINUS", "plusMinusPoints", "PLUSMINUSPOINTS"],
    }
    for out_col, candidates in numeric_cols.items():
        src_col = _col(*candidates)
        if src_col:
            fallback[out_col] = pd.to_numeric(hist[src_col], errors="coerce")
        else:
            fallback[out_col] = 0.0

    if season_col:
        fallback["SEASON"] = hist[season_col].astype(str).str.strip()
    else:
        fallback["SEASON"] = fallback["GAME_DATE"].map(_season_from_game_date)

    fallback = fallback[
        fallback["GAME_DATE"].notna()
        & fallback["PLAYER_ID"].notna()
        & (fallback["GAME_ID"].str.len() > 0)
        & (fallback["TEAM_ABBREVIATION"].str.len() > 0)
        & (fallback["PLAYER_NAME"].str.len() > 0)
    ].copy()
    if fallback.empty:
        return fallback

    fallback["PLAYER_ID"] = fallback["PLAYER_ID"].astype(int)
    fallback = fallback.drop_duplicates(subset=["GAME_ID", "PLAYER_ID"], keep="last")
    fallback = fallback.sort_values(["GAME_DATE", "GAME_ID", "PLAYER_ID"], kind="stable").reset_index(drop=True)
    return fallback


def _fallback_player_logs_from_boxscores_history() -> pd.DataFrame:
    hist_parquet = paths.data_processed / "boxscores_history.parquet"
    hist_csv = paths.data_processed / "boxscores_history.csv"
    try:
        if hist_parquet.exists():
            hist = pd.read_parquet(hist_parquet)
        elif hist_csv.exists():
            hist = pd.read_csv(hist_csv)
        else:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

    fallback = _player_logs_from_boxscores_frame(hist)
    if not fallback.empty:
        fallback.attrs["source"] = "boxscores_history"
    return fallback


def _current_season_str(today: _date | None = None) -> str:
    ref = today or _date.today()
    season_start = ref.year if ref.month >= 7 else (ref.year - 1)
    return f"{season_start}-{(season_start + 1) % 100:02d}"


def _fallback_player_logs_from_recent_espn_boxscores(
    seasons: Iterable[str],
    *,
    lookback_days: int = 35,
    rate_delay: float = 0.1,
) -> pd.DataFrame:
    requested = {str(season or "").strip() for season in seasons if str(season or "").strip()}
    if not requested or _current_season_str() not in requested:
        return pd.DataFrame()

    end_date = _date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(0, int(lookback_days) - 1))
    frames: List[pd.DataFrame] = []
    cur = start_date
    while cur <= end_date:
        date_str = cur.isoformat()
        try:
            scoreboard = _espn_scoreboard(date_str)
        except Exception:
            scoreboard = {}
        events = scoreboard.get("events") if isinstance(scoreboard, dict) else None
        if isinstance(events, list):
            for event in events:
                try:
                    comps = (event or {}).get("competitions") or []
                    if not comps:
                        continue
                    competitors = (comps[0] or {}).get("competitors") or []
                    home = next((team for team in competitors if str((team or {}).get("homeAway") or "").strip().lower() == "home"), None)
                    away = next((team for team in competitors if str((team or {}).get("homeAway") or "").strip().lower() == "away"), None)
                    if not home or not away:
                        continue

                    home_tri = _espn_to_tri(str((((home or {}).get("team") or {}).get("abbreviation")) or "").strip())
                    away_tri = _espn_to_tri(str((((away or {}).get("team") or {}).get("abbreviation")) or "").strip())
                    if not home_tri or not away_tri:
                        continue

                    event_id = str((event or {}).get("id") or "").strip()
                    game_id = f"espn_{event_id}" if event_id else f"espn_{date_str}_{away_tri}_{home_tri}"
                    df = _boxscore_from_espn(date_str=date_str, game_id=game_id, home_tri=home_tri, away_tri=away_tri)
                    if df is None or df.empty:
                        continue
                    df = df.copy()
                    df["date"] = date_str
                    frames.append(df)
                except Exception:
                    continue
                if rate_delay > 0:
                    time.sleep(rate_delay)
        cur += timedelta(days=1)

    if not frames:
        return pd.DataFrame()

    fallback = _player_logs_from_boxscores_frame(pd.concat(frames, ignore_index=True))
    if not fallback.empty:
        fallback.attrs["source"] = "espn_recent_boxscores"
    return fallback


def _fetch_season_player_logs(season: str, max_attempts: int = 3) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            _configure_stats_headers()
            res = leaguegamelog.LeagueGameLog(
                season=season,
                season_type_all_star="Regular Season",
                player_or_team_abbreviation="P",
                counter=0,
                timeout=45,
            )

            df = pd.DataFrame()
            try:
                nd = res.get_normalized_dict() or {}
                df = pd.DataFrame(nd.get("LeagueGameLog", []))
            except Exception:
                df = pd.DataFrame()

            if df.empty:
                try:
                    frames = res.get_data_frames() or []
                    if frames:
                        df = frames[0]
                except Exception:
                    df = pd.DataFrame()

            if df is not None and not df.empty:
                out = df.copy()
                out["SEASON"] = season
                return out

            last_error = RuntimeError(f"LeagueGameLog returned no rows for {season}")
        except Exception as exc:
            last_error = exc

        if attempt < max_attempts:
            time.sleep(min(2.0 * attempt, 6.0))

    if last_error is None:
        raise RuntimeError(f"LeagueGameLog failed for {season}")
    raise RuntimeError(f"LeagueGameLog failed for {season}: {last_error}") from last_error


def fetch_player_logs(seasons: Iterable[str]) -> pd.DataFrame:
    """Fetch league-wide player game logs for given seasons and save to processed.

    seasons: iterable of season strings like ['2023-24','2024-25','2025-26']
    Returns concatenated DataFrame.
    """
    frames: List[pd.DataFrame] = []
    errors: List[str] = []
    for season in seasons:
        season_str = str(season or "").strip()
        if not season_str:
            continue
        try:
            frames.append(_fetch_season_player_logs(season_str))
        except Exception as exc:
            errors.append(str(exc))

    if frames:
        out = pd.concat(frames, ignore_index=True)
        return _write_player_logs(out)

    fallback = _fallback_player_logs_from_boxscores_history()
    if fallback is not None and not fallback.empty:
        return _write_player_logs(fallback)

    fallback = _fallback_player_logs_from_recent_espn_boxscores(seasons)
    if fallback is not None and not fallback.empty:
        return _write_player_logs(fallback)

    if errors:
        raise RuntimeError("; ".join(errors))
    raise RuntimeError("LeagueGameLog returned no rows for all requested seasons")
