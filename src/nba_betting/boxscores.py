from __future__ import annotations

import time
import pandas as pd
from typing import List, Tuple

from .config import paths


def _scoreboard_games(date_str: str) -> pd.DataFrame:
    try:
        from nba_api.stats.endpoints import scoreboardv2
        sb = scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=30)
        nd = sb.get_normalized_dict()
        gh = pd.DataFrame(nd.get("GameHeader", []))
        return gh
    except Exception:
        return pd.DataFrame()


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
        time.sleep(rate_delay)
        return df
    except Exception:
        return pd.DataFrame()


def fetch_boxscores_for_date(date_str: str, only_final: bool = True, rate_delay: float = 0.35) -> Tuple[pd.DataFrame, List[str]]:
    """Fetch boxscores for all games on a date and write per-game and combined CSVs.

    Returns (combined_df, game_ids)
    """
    gh = _scoreboard_games(date_str)
    if gh is None or gh.empty:
        return pd.DataFrame(), []
    cols = {c.upper(): c for c in gh.columns}
    gid_col = cols.get("GAME_ID") or cols.get("GAMECODE")
    status_col = cols.get("GAME_STATUS_ID") or cols.get("GAMESTATUSID") or cols.get("GAME_STATUS_TEXT")
    if not gid_col:
        return pd.DataFrame(), []
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
                    if str(st).isdigit() and int(st) < 3:
                        continue
                    if isinstance(st, str) and st.strip().lower() not in ("final", "final/ot", "final/2ot", "final/3ot"):
                        continue
                except Exception:
                    pass
            game_ids.append(gid)
        except Exception:
            continue
    out_dir = paths.data_processed / "boxscores"
    out_dir.mkdir(parents=True, exist_ok=True)
    frames: List[pd.DataFrame] = []
    for gid in game_ids:
        df = _fetch_boxscore_for_game(gid, rate_delay=rate_delay)
        if df is None or df.empty:
            continue
        try:
            (out_dir / f"boxscore_{gid}.csv").write_text("", encoding="utf-8")
        except Exception:
            pass
        df.to_csv(out_dir / f"boxscore_{gid}.csv", index=False)
        frames.append(df)
    if frames:
        combo = pd.concat(frames, ignore_index=True)
        combo["date"] = date_str
        combo.to_csv(paths.data_processed / f"boxscores_{date_str}.csv", index=False)
        return combo, game_ids
    return pd.DataFrame(), game_ids


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
