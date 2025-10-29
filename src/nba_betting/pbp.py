from __future__ import annotations

import time
import pandas as pd
from typing import List, Tuple

from .config import paths
import requests
from datetime import datetime as _dt, date as _date


def _scoreboard_games(date_str: str) -> pd.DataFrame:
    try:
        from nba_api.stats.endpoints import scoreboardv2
        sb = scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=30)
        nd = sb.get_normalized_dict()
        gh = pd.DataFrame(nd.get("GameHeader", []))
        return gh
    except Exception:
        return pd.DataFrame()


def _fetch_pbp_for_game(game_id: str, rate_delay: float = 0.35) -> pd.DataFrame:
    """Fetch PlayByPlayV3 for a single game id. Returns empty DataFrame on failure."""
    try:
        from nba_api.stats.endpoints import playbyplayv3
        gid = str(game_id)
        resp = playbyplayv3.PlayByPlayV3(game_id=gid, start_period=1, end_period=14, timeout=30)
        # Some environments return empty normalized dict; fallback to data_frames
        try:
            nd = resp.get_normalized_dict() or {}
            df = pd.DataFrame(nd.get("PlayByPlay", []))
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
        time.sleep(rate_delay)
        return df
    except Exception:
        return pd.DataFrame()


def fetch_pbp_for_date(date_str: str, only_final: bool = True, rate_delay: float = 0.35) -> Tuple[pd.DataFrame, List[str]]:
    """Fetch play-by-play logs for all games on a date.

    - date_str: YYYY-MM-DD (US/Eastern by Scoreboard semantics)
    - only_final: when True, fetch only games that appear finalized on the scoreboard
    - rate_delay: seconds to sleep between API calls

    Writes per-game CSV under data/processed/pbp/pbp_<gameId>.csv and a combined data/processed/pbp_<date>.csv
    Returns (combined_df, game_ids)
    """
    gh = _scoreboard_games(date_str)
    if gh is None or gh.empty:
        # Fallback to CDN-based fetch if nba_api scoreboard is unavailable
        combo_cdn, gids_cdn = fetch_pbp_for_date_cdn(date_str, only_final=only_final, rate_delay=rate_delay)
        return combo_cdn, gids_cdn
    cols = {c.upper(): c for c in gh.columns}
    gid_col = cols.get("GAME_ID") or cols.get("GAMECODE")
    status_col = cols.get("GAME_STATUS_ID") or cols.get("GAMESTATUSID") or cols.get("GAME_STATUS_TEXT")
    if not gid_col:
        return pd.DataFrame(), []
    # Select games
    games = gh.copy()
    game_ids: List[str] = []
    for _, r in games.iterrows():
        try:
            gid = str(r[gid_col])
            if not gid:
                continue
            if only_final and status_col is not None:
                try:
                    st = r[status_col]
                    # ScoreboardV2: GAME_STATUS_ID 1=Scheduled, 2=In-Progress, 3=Final
                    if str(st).isdigit() and int(st) < 3:
                        continue
                    if isinstance(st, str) and st.strip().lower() not in ("final", "final/ot", "final/2ot", "final/3ot"):
                        # If textual, skip non-final statuses
                        continue
                except Exception:
                    pass
            game_ids.append(gid)
        except Exception:
            continue
    # Fetch PBP per game
    out_dir = paths.data_processed / "pbp"
    out_dir.mkdir(parents=True, exist_ok=True)
    frames: List[pd.DataFrame] = []
    for gid in game_ids:
        df = _fetch_pbp_for_game(gid, rate_delay=rate_delay)
        if df is None or df.empty:
            continue
        # Write per-game CSV
        try:
            (out_dir / f"pbp_{gid}.csv").write_text("", encoding="utf-8")  # ensure file exists before to_csv
        except Exception:
            pass
        df.to_csv(out_dir / f"pbp_{gid}.csv", index=False)
        frames.append(df)
    if frames:
        combo = pd.concat(frames, ignore_index=True)
        combo["date"] = date_str
        combo.to_csv(paths.data_processed / f"pbp_{date_str}.csv", index=False)
        return combo, game_ids
    return pd.DataFrame(), game_ids


def backfill_pbp(start_date: str, end_date: str, only_final: bool = True, rate_delay: float = 0.35) -> pd.DataFrame:
    """Backfill PBP for a date range [start..end] inclusive. Skips dates that already have a combined file.

    Returns concatenated DataFrame of all fetched rows (may be large).
    """
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
        out_path = paths.data_processed / f"pbp_{d}.csv"
        if out_path.exists():
            cur += _dt.timedelta(days=1)
            continue
        df, _ = fetch_pbp_for_date(d, only_final=only_final, rate_delay=rate_delay)
        if df is not None and not df.empty:
            out_frames.append(df)
        cur += _dt.timedelta(days=1)
    if out_frames:
        return pd.concat(out_frames, ignore_index=True)
    return pd.DataFrame()


# -------------------- CDN fallback implementation --------------------

def _cdn_games_for_date(date_str: str) -> List[dict]:
    """Return list of games dicts for a date using NBA public CDN.

    For today: use liveData todaysScoreboard_00.json
    Otherwise: filter static scheduleLeagueV2_1.json by date
    """
    try:
        target = _dt.strptime(date_str, "%Y-%m-%d").date()
        today = _date.today()
        headers = {"Accept":"application/json","User-Agent":"nba-betting/1.0"}
        if target == today:
            u = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
            r = requests.get(u, headers=headers, timeout=15)
            if r.ok:
                j = r.json() or {}
                games = (j.get("scoreboard") or {}).get("games") or []
                # normalize to minimal shape
                out = []
                for g in games:
                    out.append({
                        "gameId": str(g.get("gameId") or "").strip(),
                        "home": ((g.get("homeTeam") or {}).get("teamTricode") or "").upper(),
                        "away": ((g.get("awayTeam") or {}).get("teamTricode") or "").upper(),
                        "statusText": g.get("gameStatusText"),
                        "status": g.get("gameStatus")
                    })
                return out
        # else static schedule
        u = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
        r = requests.get(u, headers=headers, timeout=20)
        if not r.ok:
            return []
        j = r.json() or {}
        league = j.get("leagueSchedule") or {}
        game_dates = league.get("gameDates") or []
        out = []
        for gd in game_dates:
            gd_raw = gd.get("gameDate")
            gd_norm = None
            if gd_raw:
                s = str(gd_raw)
                # schedule uses 'MM/DD/YYYY 00:00:00' format
                try:
                    gd_norm = _dt.strptime(s, "%m/%d/%Y %H:%M:%S").date().isoformat()
                except Exception:
                    gd_norm = s.split("T")[0]
            if gd_norm != date_str:
                continue
            for g in (gd.get("games") or []):
                out.append({
                    "gameId": str(g.get("gameId") or "").strip(),
                    "home": ((g.get("homeTeam") or {}).get("teamTricode") or "").upper(),
                    "away": ((g.get("awayTeam") or {}).get("teamTricode") or "").upper(),
                    "statusText": g.get("gameStatusText"),
                    "status": g.get("gameStatus")
                })
        return out
    except Exception:
        return []


def _fetch_pbp_cdn_for_game(game_id: str, rate_delay: float = 0.2) -> pd.DataFrame:
    """Fetch PBP for a game using NBA CDN playbyplay JSON and return a normalized DataFrame.

    Produces columns compatible with downstream helpers: period, clock, description, game_id, and best-effort name/id fields.
    """
    try:
        headers = {"Accept":"application/json","User-Agent":"nba-betting/1.0"}
        gid = str(game_id)
        u = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_00_{gid}.json"
        r = requests.get(u, headers=headers, timeout=15)
        if not r.ok:
            # try S3 URL variant
            u2 = f"https://nba-prod-us-east-1-mediaops-stats.s3.amazonaws.com/NBA/liveData/playbyplay/playbyplay_00_{gid}.json"
            r = requests.get(u2, headers=headers, timeout=15)
            if not r.ok:
                return pd.DataFrame()
        j = r.json() or {}
        acts = ((j.get("game") or {}).get("actions") or [])
        rows = []
        for a in acts:
            try:
                period = int(a.get("period", 0))
            except Exception:
                period = None
            clock = a.get("clock") or a.get("timeRemaining")
            desc = a.get("description")
            if not desc:
                # synthesize brief text
                parts = []
                at = a.get("actionType") or a.get("shotType")
                st = a.get("subType") or a.get("shotSubType")
                res = a.get("shotResult")
                nm = a.get("playerName") or a.get("playerNameI")
                if at: parts.append(str(at))
                if st: parts.append(str(st))
                if res: parts.append(str(res))
                if nm: parts.append(str(nm))
                desc = " ".join(parts)
            rows.append({
                "period": period,
                "clock": clock,
                "description": desc,
                "player1_name": a.get("playerName") or a.get("playerNameI"),
                "player1_id": a.get("personId") or a.get("playerId"),
                "teamTricode": a.get("teamTricode") or a.get("teamTricodeHome") or a.get("teamTricodeAway"),
                "game_id": gid,
            })
        df = pd.DataFrame(rows)
        time.sleep(rate_delay)
        return df
    except Exception:
        return pd.DataFrame()


def fetch_pbp_for_date_cdn(date_str: str, only_final: bool = True, rate_delay: float = 0.2) -> Tuple[pd.DataFrame, List[str]]:
    games = _cdn_games_for_date(date_str)
    if not games:
        return pd.DataFrame(), []
    # filter only finals when status present and only_final requested
    gids = []
    for g in games:
        gid = g.get("gameId")
        if not gid:
            continue
        if only_final and g.get("status") is not None:
            try:
                if int(g.get("status")) != 3:
                    # 3 == Final
                    continue
            except Exception:
                st = str(g.get("statusText") or "").lower()
                if st and ("final" not in st):
                    continue
        gids.append(str(gid))
    out_dir = paths.data_processed / "pbp"
    out_dir.mkdir(parents=True, exist_ok=True)
    frames: List[pd.DataFrame] = []
    for gid in gids:
        df = _fetch_pbp_cdn_for_game(gid, rate_delay=rate_delay)
        if df is None or df.empty:
            continue
        df.to_csv(out_dir / f"pbp_{gid}.csv", index=False)
        frames.append(df)
    if frames:
        combo = pd.concat(frames, ignore_index=True)
        combo["date"] = date_str
        combo.to_csv(paths.data_processed / f"pbp_{date_str}.csv", index=False)
        return combo, gids
    return pd.DataFrame(), gids
