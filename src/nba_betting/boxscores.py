from __future__ import annotations

import json
import time
from datetime import datetime as _dt
from pathlib import Path
from typing import Any, List, Optional, Tuple

import pandas as pd
import requests

from .config import paths


def _tri_to_espn(tri: str) -> str:
    s = str(tri or "").strip().upper()
    fix = {
        "GSW": "GS",
        "NOP": "NO",
        "NYK": "NY",
        "UTA": "UTAH",
        "WAS": "WSH",
        "SAS": "SA",
        "PHX": "PHO",
    }
    return fix.get(s, s)


def _espn_to_tri(abbr: str) -> str:
    s = str(abbr or "").strip().upper()
    fix = {
        "GS": "GSW",
        "NO": "NOP",
        "NY": "NYK",
        "UTAH": "UTA",
        "WSH": "WAS",
        "SA": "SAS",
        "PHO": "PHX",
    }
    return fix.get(s, s)


def _espn_cache_dir() -> Path:
    d = paths.data_processed / "_espn_cache"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, data: dict[str, Any]) -> None:
    try:
        path.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        return


def _http_get_json(url: str, timeout: int = 18) -> dict[str, Any]:
    try:
        r = requests.get(
            url,
            headers={"Accept": "application/json", "User-Agent": "nba-betting/1.0"},
            timeout=int(timeout),
        )
        if not r.ok:
            return {}
        j = r.json()
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}


def _espn_scoreboard(date_str: str) -> dict[str, Any]:
    # ESPN expects YYYYMMDD
    ymd = str(date_str).replace("-", "")
    cache = _espn_cache_dir() / f"scoreboard_{ymd}.json"
    if cache.exists():
        jd = _read_json(cache)
        if jd:
            return jd
    url = f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"
    jd = _http_get_json(url, timeout=18)
    if jd:
        _write_json(cache, jd)
    return jd


def _espn_summary(event_id: str, *, force: bool = False) -> dict[str, Any]:
    cache = _espn_cache_dir() / f"summary_{str(event_id).strip()}.json"
    if (not force) and cache.exists():
        jd = _read_json(cache)
        if jd:
            return jd
    url = f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={event_id}"
    jd = _http_get_json(url, timeout=18)
    if jd:
        _write_json(cache, jd)
    return jd


def _espn_event_id_for_matchup(date_str: str, home_tri: str, away_tri: str) -> Optional[str]:
    sb = _espn_scoreboard(date_str)
    events = sb.get("events") if isinstance(sb, dict) else None
    if not isinstance(events, list):
        return None
    h = _tri_to_espn(home_tri)
    a = _tri_to_espn(away_tri)
    for e in events:
        try:
            comps = (e or {}).get("competitions") or []
            if not comps:
                continue
            c0 = comps[0] or {}
            teams = c0.get("competitors") or []
            if len(teams) < 2:
                continue
            home = next((t for t in teams if str((t or {}).get("homeAway")) == "home"), None)
            away = next((t for t in teams if str((t or {}).get("homeAway")) == "away"), None)
            if not home or not away:
                continue
            hab = str(((home.get("team") or {}).get("abbreviation")) or "").strip().upper()
            aab = str(((away.get("team") or {}).get("abbreviation")) or "").strip().upper()
            if hab == h and aab == a:
                return str((e or {}).get("id") or "").strip() or None
        except Exception:
            continue
    return None


def _parse_made_att(s: Any) -> tuple[int, int]:
    try:
        t = str(s or "").strip()
        if "-" not in t:
            v = int(float(t))
            return v, v
        a, b = t.split("-", 1)
        return int(float(a)), int(float(b))
    except Exception:
        return 0, 0


def _to_float(x: Any) -> float:
    try:
        return float(pd.to_numeric(x, errors="coerce"))
    except Exception:
        return 0.0


def _parse_minutes(x: Any) -> float:
    """Parse minutes from ESPN/NBA formats.

    ESPN sometimes uses 'MM:SS' strings; nba_api is often a number.
    """
    try:
        s = str(x or "").strip()
        if not s:
            return 0.0
        if ":" in s:
            mm, ss = s.split(":", 1)
            m = float(pd.to_numeric(mm, errors="coerce") or 0)
            sec = float(pd.to_numeric(ss, errors="coerce") or 0)
            return max(0.0, m + sec / 60.0)
        return float(pd.to_numeric(s, errors="coerce") or 0)
    except Exception:
        return 0.0


def _to_int(x: Any) -> int:
    try:
        return int(pd.to_numeric(x, errors="coerce"))
    except Exception:
        return 0


def _boxscore_from_espn(date_str: str, game_id: str, home_tri: str, away_tri: str) -> pd.DataFrame:
    event_id = _espn_event_id_for_matchup(date_str, home_tri=home_tri, away_tri=away_tri)
    if not event_id:
        return pd.DataFrame()
    summ = _espn_summary(event_id)
    box = (summ or {}).get("boxscore") or {}
    teams = box.get("players") or []
    if not isinstance(teams, list) or not teams:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for tp in teams:
        team = (tp or {}).get("team") or {}
        team_ab = _espn_to_tri(str(team.get("abbreviation") or "").strip())
        stats_groups = (tp or {}).get("statistics") or []
        if not isinstance(stats_groups, list) or not stats_groups:
            continue
        g0 = stats_groups[0] or {}
        labels = g0.get("labels") or []
        athletes = g0.get("athletes") or []
        if not isinstance(labels, list) or not isinstance(athletes, list):
            continue

        for a in athletes:
            if not isinstance(a, dict):
                continue
            athlete = a.get("athlete") or {}
            name = str(athlete.get("displayName") or "").strip()
            if not name:
                continue

            pid = str(athlete.get("id") or "").strip()
            first = str(athlete.get("firstName") or "").strip()
            last = str(athlete.get("lastName") or "").strip()
            if (not first or not last) and name:
                parts = name.split()
                if len(parts) >= 2:
                    first = first or parts[0]
                    last = last or " ".join(parts[1:])

            starter = bool(a.get("starter"))
            pos = (athlete.get("position") or {}).get("abbreviation")
            stats = a.get("stats") or []
            if not isinstance(stats, list):
                stats = []
            m: dict[str, Any] = {str(lbl): (stats[i] if i < len(stats) else None) for i, lbl in enumerate(labels)}

            fgm, fga = _parse_made_att(m.get("FG"))
            fg3m, fg3a = _parse_made_att(m.get("3PT"))
            ftm, fta = _parse_made_att(m.get("FT"))

            # Keep raw minutes for downstream code that expects MM:SS strings.
            minutes_raw = str(m.get("MIN") or "").strip()

            # Derive percent columns to loosely match nba_api BoxScoreTraditionalV3 columns.
            fg_pct = (float(fgm) / float(fga)) if fga else 0.0
            fg3_pct = (float(fg3m) / float(fg3a)) if fg3a else 0.0
            ft_pct = (float(ftm) / float(fta)) if fta else 0.0

            rows.append(
                {
                    # Normalized columns (preferred)
                    "game_id": str(game_id),
                    "TEAM_ABBREVIATION": str(team_ab),
                    "PLAYER_ID": int(pid) if pid.isdigit() else None,
                    "PLAYER_NAME": name,
                    "STARTER": starter,
                    "START_POSITION": str(pos or "") if starter else "",
                    "MIN": _parse_minutes(minutes_raw),
                    "PTS": _to_int(m.get("PTS")),
                    "REB": _to_int(m.get("REB")),
                    "AST": _to_int(m.get("AST")),
                    "TOV": _to_int(m.get("TO")),
                    "STL": _to_int(m.get("STL")),
                    "BLK": _to_int(m.get("BLK")),
                    "PF": _to_int(m.get("PF")),
                    "PLUS_MINUS": _to_int(m.get("+/-")),
                    "OREB": _to_int(m.get("OREB")),
                    "DREB": _to_int(m.get("DREB")),
                    "FGM": int(fgm),
                    "FGA": int(fga),
                    "FG3M": int(fg3m),
                    "FG3A": int(fg3a),
                    "FTM": int(ftm),
                    "FTA": int(fta),
                    "DNP": bool(a.get("didNotPlay")) or False,
                    "DNP_REASON": str(a.get("reason") or "").strip(),

                    # nba_api-like columns (compat for older readers)
                    "gameId": str(game_id),
                    "teamTricode": str(team_ab),
                    "personId": int(pid) if pid.isdigit() else None,
                    "firstName": first,
                    "familyName": last,
                    "position": str(pos or ""),
                    "comment": str(a.get("reason") or "").strip() if bool(a.get("didNotPlay")) else "",
                    "minutes": minutes_raw,
                    "fieldGoalsMade": int(fgm),
                    "fieldGoalsAttempted": int(fga),
                    "fieldGoalsPercentage": float(fg_pct),
                    "threePointersMade": int(fg3m),
                    "threePointersAttempted": int(fg3a),
                    "threePointersPercentage": float(fg3_pct),
                    "freeThrowsMade": int(ftm),
                    "freeThrowsAttempted": int(fta),
                    "freeThrowsPercentage": float(ft_pct),
                    "reboundsOffensive": _to_int(m.get("OREB")),
                    "reboundsDefensive": _to_int(m.get("DREB")),
                    "reboundsTotal": _to_int(m.get("REB")),
                    "assists": _to_int(m.get("AST")),
                    "steals": _to_int(m.get("STL")),
                    "blocks": _to_int(m.get("BLK")),
                    "turnovers": _to_int(m.get("TO")),
                    "foulsPersonal": _to_int(m.get("PF")),
                    "points": _to_int(m.get("PTS")),
                    "plusMinusPoints": _to_float(m.get("+/-")),

                    "source": "espn",
                }
            )

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df


def _normalize_boxscore_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a per-game boxscore frame into a stable schema.

    Output columns (when available):
    - game_id, gameId
    - TEAM_ABBREVIATION
    - PLAYER_ID, PLAYER_NAME
    - MIN, PTS, REB, AST, TOV, STL, BLK, OREB, DREB, PF
    - FGM, FGA, FG3M, FG3A, FTM, FTA
    - PLUS_MINUS
    - STARTER, START_POSITION
    - source
    """
    if df is None or df.empty:
        return pd.DataFrame()

    out: dict[str, Any] = {}
    cols_u = {c.upper(): c for c in df.columns}

    def _get(*names: str) -> Optional[str]:
        for n in names:
            key = n.upper()
            if key in cols_u:
                return cols_u[key]
        return None

    # Identify common input variants
    gid_c = _get("game_id", "gameid", "GAME_ID")
    team_c = _get("TEAM_ABBREVIATION", "TEAMTRICODE", "TEAM_ABBR", "teamTricode")
    pid_c = _get("PLAYER_ID", "PERSONID", "player_id", "personId")
    pname_c = _get("PLAYER_NAME", "PLAYER")
    first_c = _get("FIRSTNAME", "firstName")
    last_c = _get("FAMILYNAME", "LASTNAME", "familyName")
    min_c = _get("MIN", "minutes", "MINUTES")

    def _series_int(col: Optional[str]) -> pd.Series:
        if not col or col not in df.columns:
            return pd.Series([0] * len(df))
        return pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    def _series_float(col: Optional[str]) -> pd.Series:
        if not col or col not in df.columns:
            return pd.Series([0.0] * len(df))
        return pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

    out["game_id"] = df[gid_c].astype(str).str.strip() if gid_c else None
    out["gameId"] = out["game_id"]
    out["TEAM_ABBREVIATION"] = df[team_c].astype(str).str.strip().str.upper() if team_c else None

    if pid_c:
        out["PLAYER_ID"] = pd.to_numeric(df[pid_c], errors="coerce")
    else:
        out["PLAYER_ID"] = None

    if pname_c:
        out["PLAYER_NAME"] = df[pname_c].astype(str).str.strip()
    elif first_c and last_c:
        out["PLAYER_NAME"] = (
            df[first_c].astype(str).str.strip() + " " + df[last_c].astype(str).str.strip()
        ).str.strip()
    else:
        out["PLAYER_NAME"] = None

    # Minutes
    if min_c and min_c in df.columns:
        out["MIN"] = df[min_c].apply(_parse_minutes)
    else:
        out["MIN"] = 0.0

    # Stats: prefer normalized names if present; else map nba_api-like
    out["PTS"] = _series_int(_get("PTS", "points"))
    out["REB"] = _series_int(_get("REB", "reboundsTotal"))
    out["AST"] = _series_int(_get("AST", "assists"))
    out["STL"] = _series_int(_get("STL", "steals"))
    out["BLK"] = _series_int(_get("BLK", "blocks"))
    out["TOV"] = _series_int(_get("TOV", "turnovers"))
    out["OREB"] = _series_int(_get("OREB", "reboundsOffensive"))
    out["DREB"] = _series_int(_get("DREB", "reboundsDefensive"))
    out["PF"] = _series_int(_get("PF", "foulsPersonal"))

    out["FGM"] = _series_int(_get("FGM", "fieldGoalsMade"))
    out["FGA"] = _series_int(_get("FGA", "fieldGoalsAttempted"))
    out["FG3M"] = _series_int(_get("FG3M", "threePointersMade"))
    out["FG3A"] = _series_int(_get("FG3A", "threePointersAttempted"))
    out["FTM"] = _series_int(_get("FTM", "freeThrowsMade"))
    out["FTA"] = _series_int(_get("FTA", "freeThrowsAttempted"))
    out["PLUS_MINUS"] = _series_float(_get("PLUS_MINUS", "plusMinusPoints", "PLUSMINUSPOINTS"))

    starter_c = _get("STARTER")
    out["STARTER"] = df[starter_c].astype(bool) if starter_c else False
    sp_c = _get("START_POSITION")
    out["START_POSITION"] = df[sp_c].astype(str) if sp_c else ""

    src_c = _get("source")
    out["source"] = df[src_c].astype(str) if src_c else "nba_api"

    out_df = pd.DataFrame(out)
    out_df = out_df.dropna(subset=["game_id", "TEAM_ABBREVIATION", "PLAYER_NAME"], how="any")
    return out_df


def _cdn_games_for_date(date_str: str) -> List[dict[str, Any]]:
    """Minimal scoreboard from NBA CDN: returns [{gameId, home, away, statusText, status}]."""
    try:
        target = _dt.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return []
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.nba.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
    }
    try:
        ymd = target.strftime("%Y%m%d")
        u_day = f"https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{ymd}.json"
        r = requests.get(u_day, headers=headers, timeout=20)
        if not r.ok:
            return []
        j = r.json() or {}
        games = (j.get("scoreboard") or {}).get("games") or []
        out: list[dict[str, Any]] = []
        for g in games:
            out.append(
                {
                    "gameId": str(g.get("gameId") or "").strip(),
                    "home": str(((g.get("homeTeam") or {}).get("teamTricode")) or "").upper(),
                    "away": str(((g.get("awayTeam") or {}).get("teamTricode")) or "").upper(),
                    "statusText": g.get("gameStatusText"),
                    "status": g.get("gameStatus"),
                }
            )
        return out
    except Exception:
        return []


def _scoreboard_games(date_str: str) -> pd.DataFrame:
    try:
        from nba_api.stats.endpoints import scoreboardv2
        sb = scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=30)
        nd = sb.get_normalized_dict()
        gh = pd.DataFrame(nd.get("GameHeader", []))
        return gh
    except Exception:
        return pd.DataFrame()


def _nba_gid_to_tricodes(date_str: str) -> dict[str, tuple[str, str]]:
    """Map NBA gameId -> (home_tricode, away_tricode) using ScoreboardV2.

    This is used to support ESPN fallbacks even when NBA CDN scoreboard data is missing.
    """
    try:
        from nba_api.stats.endpoints import scoreboardv2

        sb = scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=30)
        nd = sb.get_normalized_dict() or {}
        gh = pd.DataFrame(nd.get("GameHeader", []))
        ls = pd.DataFrame(nd.get("LineScore", []))
        if gh.empty or ls.empty:
            return {}

        # teamId -> tricode from LineScore
        team_id_col = "TEAM_ID" if "TEAM_ID" in ls.columns else None
        tri_col = "TEAM_ABBREVIATION" if "TEAM_ABBREVIATION" in ls.columns else None
        gid_col = "GAME_ID" if "GAME_ID" in ls.columns else None
        if not team_id_col or not tri_col or not gid_col:
            return {}

        teamid_to_tri: dict[int, str] = {}
        for _, r in ls.iterrows():
            try:
                tid = int(r[team_id_col])
                tri = str(r[tri_col] or "").strip().upper()
                if tri:
                    teamid_to_tri[tid] = tri
            except Exception:
                continue

        out: dict[str, tuple[str, str]] = {}
        if "GAME_ID" not in gh.columns or "HOME_TEAM_ID" not in gh.columns or "VISITOR_TEAM_ID" not in gh.columns:
            return {}
        for _, r in gh.iterrows():
            try:
                gid = str(r["GAME_ID"] or "").strip()
                if not gid:
                    continue
                home_tid = int(r["HOME_TEAM_ID"])
                away_tid = int(r["VISITOR_TEAM_ID"])
                home_tri = teamid_to_tri.get(home_tid, "")
                away_tri = teamid_to_tri.get(away_tid, "")
                if home_tri and away_tri:
                    out[gid] = (home_tri, away_tri)
            except Exception:
                continue
        return out
    except Exception:
        return {}


def _fetch_boxscore_for_game(game_id: str, rate_delay: float = 0.35) -> pd.DataFrame:
    """Fetch BoxScoreTraditionalV3 for a single game id. Returns empty DataFrame on failure."""
    try:
        from nba_api.stats.endpoints import boxscoretraditionalv3
        gid = str(game_id)
        resp = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=gid, timeout=30)
        # Prefer normalized dict, but some environments require data_frames fallback
        try:
            nd = resp.get_normalized_dict() or {}
            df = pd.DataFrame(nd.get("PlayerStats", []))
            if df is None or df.empty:
                frames = resp.get_data_frames() or []
                if frames:
                    df = frames[0]
        except Exception:
            frames = resp.get_data_frames() or []
            df = frames[0] if frames else pd.DataFrame()
        if df is None or df.empty:
            return pd.DataFrame()
        df["game_id"] = gid
        return df
    except Exception:
        return pd.DataFrame()


def fetch_boxscores_for_date(date_str: str, only_final: bool = True, rate_delay: float = 0.35) -> Tuple[pd.DataFrame, List[str]]:
    """Fetch boxscores for all games on a date and write per-game and combined CSVs.

    Returns (combined_df, game_ids)
    """
    # Use CDN scoreboard to map gameId -> (home, away) even if nba_api is down.
    cdn_games = _cdn_games_for_date(date_str)
    gid_to_teams: dict[str, tuple[str, str]] = {}
    for g in cdn_games:
        gid = str(g.get("gameId") or "").strip()
        if not gid:
            continue
        gid_to_teams[gid] = (str(g.get("home") or "").upper(), str(g.get("away") or "").upper())

    # If nba_api scoreboard is available, use it to fill in any missing mappings.
    if not gid_to_teams:
        gid_to_teams.update(_nba_gid_to_tricodes(date_str))

    gh = _scoreboard_games(date_str)
    game_ids: List[str] = []
    if gh is not None and not gh.empty:
        cols = {c.upper(): c for c in gh.columns}
        gid_col = cols.get("GAME_ID") or cols.get("GAMECODE")
        status_col = cols.get("GAME_STATUS_ID") or cols.get("GAMESTATUSID") or cols.get("GAME_STATUS_TEXT")
        if gid_col:
            for _, r in gh.iterrows():
                try:
                    gid = str(r[gid_col])
                    if not gid:
                        continue
                    if only_final and status_col is not None:
                        try:
                            st = r[status_col]
                            if str(st).isdigit() and int(st) < 3:
                                continue
                            if isinstance(st, str) and st.strip().lower() not in ("final", "final/ot", "final/2ot", "final/3ot"):
                                continue
                        except Exception:
                            pass
                    game_ids.append(gid)
                except Exception:
                    continue

        # Ensure we have team mappings for any gameIds we discovered via nba_api.
        if game_ids and (not gid_to_teams or any(gid not in gid_to_teams for gid in game_ids)):
            gid_to_teams.update(_nba_gid_to_tricodes(date_str))
    # Fallback if nba_api scoreboard is unavailable.
    if not game_ids and cdn_games:
        for g in cdn_games:
            gid = str(g.get("gameId") or "").strip()
            if not gid:
                continue
            if only_final:
                st = str(g.get("statusText") or "").strip().lower()
                if st and ("final" not in st):
                    continue
            game_ids.append(gid)
    out_dir = paths.data_processed / "boxscores"
    out_dir.mkdir(parents=True, exist_ok=True)
    frames_norm: List[pd.DataFrame] = []
    for gid in game_ids:
        df = _fetch_boxscore_for_game(gid, rate_delay=rate_delay)
        if df is None or df.empty:
            # ESPN fallback
            home_tri, away_tri = gid_to_teams.get(gid, ("", ""))
            if home_tri and away_tri:
                df = _boxscore_from_espn(date_str=date_str, game_id=gid, home_tri=home_tri, away_tri=away_tri)
            if df is None or df.empty:
                continue
        # Always write the raw per-game frame for debugging/tool compatibility.
        df.to_csv(out_dir / f"boxscore_{gid}.csv", index=False)
        norm = _normalize_boxscore_df(df)
        if norm is not None and not norm.empty:
            frames_norm.append(norm)
        time.sleep(rate_delay)
    if frames_norm:
        combo = pd.concat(frames_norm, ignore_index=True)
        combo["date"] = date_str
        combo.to_csv(paths.data_processed / f"boxscores_{date_str}.csv", index=False)
        return combo, game_ids
    return pd.DataFrame(), game_ids


def update_boxscores_history_for_date(date_str: str, include_live: bool = False, rate_delay: float = 0.35) -> dict[str, Any]:
    """Fetch boxscores for a date and append into a durable history file.

    Writes:
    - data/processed/boxscores_history.parquet (preferred)
    - data/processed/boxscores_history.csv (fallback)
    """
    df, gids = fetch_boxscores_for_date(date_str, only_final=(not include_live), rate_delay=rate_delay)
    hist_parquet = paths.data_processed / "boxscores_history.parquet"
    hist_csv = paths.data_processed / "boxscores_history.csv"

    def _read_hist() -> pd.DataFrame:
        if hist_parquet.exists():
            try:
                return pd.read_parquet(hist_parquet)
            except Exception:
                pass
        if hist_csv.exists():
            try:
                return pd.read_csv(hist_csv)
            except Exception:
                pass
        return pd.DataFrame()

    hist = _read_hist()
    if df is None or df.empty:
        return {
            "date": date_str,
            "games": len(gids),
            "rows": 0,
            "history_rows": 0 if hist is None else int(len(hist)),
        }

    cur = df.copy()
    # De-dupe keys (prefer player_id if present)
    key_cols = [c for c in ["game_id", "PLAYER_ID"] if c in cur.columns]
    if len(key_cols) < 2:
        key_cols = [c for c in ["game_id", "TEAM_ABBREVIATION", "PLAYER_NAME"] if c in cur.columns]

    combo = pd.concat([hist, cur], ignore_index=True) if hist is not None and not hist.empty else cur
    if "date" in combo.columns:
        combo["date"] = pd.to_datetime(combo["date"], errors="coerce")
    if key_cols:
        # Keep the latest record by date
        combo = combo.sort_values(["date"], kind="stable")
        combo = combo.drop_duplicates(subset=key_cols, keep="last")

    # Write back
    wrote = None
    try:
        combo.to_parquet(hist_parquet, index=False)
        wrote = str(hist_parquet)
    except Exception:
        try:
            combo.to_csv(hist_csv, index=False)
            wrote = str(hist_csv)
        except Exception:
            wrote = None

    return {
        "date": date_str,
        "games": len(gids),
        "rows": int(len(df)),
        "history_rows": int(len(combo)),
        "wrote": wrote,
    }


def backfill_boxscores(start_date: str, end_date: str, only_final: bool = True, rate_delay: float = 0.35) -> pd.DataFrame:
    """Backfill boxscores for a date range [start..end] inclusive. Skips dates already fetched."""
    import datetime as _dt
    try:
        s = _dt.datetime.strptime(start_date, "%Y-%m-%d").date()
        e = _dt.datetime.strptime(end_date, "%Y-%m-%d").date()
        if e < s:
            s, e = e, s
    except Exception:
        return pd.DataFrame()
    out_frames: List[pd.DataFrame] = []
    cur = s
    while cur <= e:
        d = cur.isoformat()
        out_path = paths.data_processed / f"boxscores_{d}.csv"
        if out_path.exists():
            cur += _dt.timedelta(days=1)
            continue
        df, _ = fetch_boxscores_for_date(d, only_final=only_final, rate_delay=rate_delay)
        if df is not None and not df.empty:
            out_frames.append(df)
        cur += _dt.timedelta(days=1)
    if out_frames:
        return pd.concat(out_frames, ignore_index=True)
    return pd.DataFrame()
