from __future__ import annotations

import time
from datetime import datetime as _dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .boxscores import (
    _cdn_games_for_date,
    _espn_event_id_for_matchup,
    _espn_scoreboard,
    _espn_summary,
    _espn_to_tri,
    _nba_gid_to_tricodes,
)
from .config import paths


def _clock_to_seconds_remaining(clock: Any) -> Optional[int]:
    """Parse ESPN clock dict/string into seconds remaining in period."""
    try:
        if isinstance(clock, dict):
            s = str(clock.get("displayValue") or "").strip()
        else:
            s = str(clock or "").strip()
        if not s:
            return None
        if ":" not in s:
            v = int(float(s))
            return max(0, v)
        mm, ss = s.split(":", 1)
        m = int(float(mm))
        sec = int(float(ss))
        sec = max(0, min(59, sec))
        return max(0, m * 60 + sec)
    except Exception:
        return None


def _team_id_to_tricode_from_summary(summary: dict[str, Any]) -> Dict[str, str]:
    try:
        comp = ((summary.get("header") or {}).get("competitions") or [None])[0] or {}
        comps = comp.get("competitors") or []
        out: Dict[str, str] = {}
        for c in comps:
            team = (c or {}).get("team") or {}
            tid = str(team.get("id") or "").strip()
            ab = str(team.get("abbreviation") or "").strip().upper()
            if tid and ab:
                out[tid] = _espn_to_tri(ab)
        return out
    except Exception:
        return {}


def _home_away_tricodes_from_summary(summary: dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    try:
        comp = ((summary.get("header") or {}).get("competitions") or [None])[0] or {}
        comps = comp.get("competitors") or []
        home_tri = None
        away_tri = None
        for c in comps:
            team = (c or {}).get("team") or {}
            ab = str(team.get("abbreviation") or "").strip().upper()
            tri = _espn_to_tri(ab) if ab else None
            ha = str((c or {}).get("homeAway") or "").strip().lower()
            if tri and ha == "home":
                home_tri = tri
            elif tri and ha == "away":
                away_tri = tri
        return home_tri, away_tri
    except Exception:
        return None, None


def _parse_substitution(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (enter_name, exit_name) if this looks like an ESPN substitution text."""
    try:
        t = str(text or "").strip()
        if not t:
            return None, None
        # Typical: "X enters the game for Y."
        if " enters the game for " in t:
            a, b = t.split(" enters the game for ", 1)
            b = b.rstrip(".").strip()
            return a.strip(), b.strip()
        return None, None
    except Exception:
        return None, None


def fetch_pbp_espn_for_date(
    date_str: str,
    only_final: bool = True,
    rate_delay: float = 0.25,
    *,
    force_scoreboard: bool = False,
) -> Tuple[pd.DataFrame, List[str]]:
    """Fetch play-by-play for all games on a date from ESPN summary endpoint.

    Writes per-game files under data/processed/pbp_espn/pbp_espn_<gameId>.csv
    and a combined per-date file data/processed/pbp_espn_<date>.csv

    Returns (combined_df, game_ids)
    """
    # Determine slate gameIds + (home, away) tricodes.
    gid_to_teams: dict[str, tuple[str, str]] = {}

    cdn_games = _cdn_games_for_date(date_str)
    for g in cdn_games:
        gid = str(g.get("gameId") or "").strip()
        if not gid:
            continue
        gid_to_teams[gid] = (str(g.get("home") or "").upper(), str(g.get("away") or "").upper())

    if not gid_to_teams:
        gid_to_teams.update(_nba_gid_to_tricodes(date_str))

    # If still empty, we can fall back to ESPN scoreboard itself (event IDs only).
    game_ids: List[str] = sorted(gid_to_teams.keys())

    out_dir = paths.data_processed / "pbp_espn"
    out_dir.mkdir(parents=True, exist_ok=True)

    frames: List[pd.DataFrame] = []

    # If we have no NBA gids (rare), fall back to ESPN event list and write event-scoped rows.
    if not game_ids:
        sb = _espn_scoreboard(date_str, force=bool(force_scoreboard))
        events = sb.get("events") if isinstance(sb, dict) else None
        if not isinstance(events, list) or not events:
            return pd.DataFrame(), []
        for e in events:
            eid = str((e or {}).get("id") or "").strip()
            if not eid:
                continue
            summ = _espn_summary(eid)
            try:
                plays = summ.get("plays") if isinstance(summ, dict) else None
                if (not isinstance(plays, list)) or (not plays):
                    summ = _espn_summary(eid, force=True)
                    plays = summ.get("plays") if isinstance(summ, dict) else None
                if not isinstance(plays, list) or not plays:
                    continue
                team_map = _team_id_to_tricode_from_summary(summ)
                home_tri, away_tri = _home_away_tricodes_from_summary(summ)
                rows = []
                for p in plays:
                    pt = (p.get("type") or {}).get("text")
                    period = (p.get("period") or {}).get("number")
                    clock = p.get("clock")
                    sec_rem = _clock_to_seconds_remaining(clock)
                    team_id = str((p.get("team") or {}).get("id") or "").strip()
                    team_tri = team_map.get(team_id)
                    enter, exit = _parse_substitution(str(p.get("text") or ""))
                    parts = p.get("participants") or []
                    pid_in = None
                    pid_out = None
                    try:
                        if isinstance(parts, list) and len(parts) >= 2:
                            pid_in = str(((parts[0] or {}).get("athlete") or {}).get("id") or "").strip() or None
                            pid_out = str(((parts[1] or {}).get("athlete") or {}).get("id") or "").strip() or None
                    except Exception:
                        pid_in = None
                        pid_out = None
                    rows.append(
                        {
                            "date": date_str,
                            "event_id": eid,
                            "game_id": None,
                            "home_tri": home_tri,
                            "away_tri": away_tri,
                            "play_id": str(p.get("id") or ""),
                            "sequence": p.get("sequenceNumber"),
                            "period": period,
                            "clock": (clock.get("displayValue") if isinstance(clock, dict) else str(clock or "")),
                            "clock_sec_remaining": sec_rem,
                            "home_score": p.get("homeScore"),
                            "away_score": p.get("awayScore"),
                            "score_value": p.get("scoreValue"),
                            "scoring_play": p.get("scoringPlay"),
                            "type": str(pt or ""),
                            "text": str(p.get("text") or ""),
                            "team": team_tri,
                            "enter_player_id": pid_in,
                            "exit_player_id": pid_out,
                            "enter_player_name": enter,
                            "exit_player_name": exit,
                            "source": "espn",
                        }
                    )
                df = pd.DataFrame(rows)
                if not df.empty:
                    df.to_csv(out_dir / f"pbp_espn_event_{eid}.csv", index=False)
                    frames.append(df)
            finally:
                # Always sleep between ESPN summary calls to reduce rate-limit / empty-play failures.
                if rate_delay and float(rate_delay) > 0:
                    time.sleep(rate_delay)
        if frames:
            combo = pd.concat(frames, ignore_index=True)
            combo.to_csv(paths.data_processed / f"pbp_espn_{date_str}.csv", index=False)
            return combo, []
        return pd.DataFrame(), []

    # Standard: iterate NBA gameIds, resolve ESPN eventId via matchup.
    for gid in game_ids:
        home_tri, away_tri = gid_to_teams.get(gid, ("", ""))
        if not home_tri or not away_tri:
            continue

        # If only_final requested and CDN provides status, honor it.
        if only_final and cdn_games:
            g = next((x for x in cdn_games if str(x.get("gameId") or "").strip() == gid), None)
            if g is not None:
                st = str(g.get("statusText") or "").strip().lower()
                if st and ("final" not in st):
                    continue

        eid = _espn_event_id_for_matchup(date_str, home_tri=home_tri, away_tri=away_tri, force_scoreboard=bool(force_scoreboard))
        if not eid:
            continue

        summ = _espn_summary(eid)
        try:
            plays = summ.get("plays") if isinstance(summ, dict) else None
            if (not isinstance(plays, list)) or (not plays):
                summ = _espn_summary(eid, force=True)
                plays = summ.get("plays") if isinstance(summ, dict) else None
            if not isinstance(plays, list) or not plays:
                continue

            team_map = _team_id_to_tricode_from_summary(summ)

            rows: List[dict[str, Any]] = []
            for p in plays:
                pt = (p.get("type") or {}).get("text")
                period = (p.get("period") or {}).get("number")
                clock = p.get("clock")
                sec_rem = _clock_to_seconds_remaining(clock)
                team_id = str((p.get("team") or {}).get("id") or "").strip()
                team_tri = team_map.get(team_id)

                enter, exit = _parse_substitution(str(p.get("text") or ""))
                parts = p.get("participants") or []
                pid_in = None
                pid_out = None
                try:
                    if isinstance(parts, list) and len(parts) >= 2 and str(pt or "").lower() == "substitution":
                        pid_in = str(((parts[0] or {}).get("athlete") or {}).get("id") or "").strip() or None
                        pid_out = str(((parts[1] or {}).get("athlete") or {}).get("id") or "").strip() or None
                except Exception:
                    pid_in = None
                    pid_out = None

                rows.append(
                    {
                        "date": date_str,
                        "game_id": gid,
                        "event_id": eid,
                        "home_tri": home_tri,
                        "away_tri": away_tri,
                        "play_id": str(p.get("id") or ""),
                        "sequence": p.get("sequenceNumber"),
                        "period": period,
                        "clock": (clock.get("displayValue") if isinstance(clock, dict) else str(clock or "")),
                        "clock_sec_remaining": sec_rem,
                        "home_score": p.get("homeScore"),
                        "away_score": p.get("awayScore"),
                        "score_value": p.get("scoreValue"),
                        "scoring_play": p.get("scoringPlay"),
                        "type": str(pt or ""),
                        "text": str(p.get("text") or ""),
                        "team": team_tri,
                        "enter_player_id": pid_in,
                        "exit_player_id": pid_out,
                        "enter_player_name": enter,
                        "exit_player_name": exit,
                        "source": "espn",
                    }
                )

            df = pd.DataFrame(rows)
            if df is None or df.empty:
                continue
            df.to_csv(out_dir / f"pbp_espn_{gid}.csv", index=False)
            frames.append(df)
        finally:
            # Always sleep between ESPN summary calls to reduce rate-limit / empty-play failures.
            if rate_delay and float(rate_delay) > 0:
                time.sleep(rate_delay)

    if frames:
        combo = pd.concat(frames, ignore_index=True)
        combo.to_csv(paths.data_processed / f"pbp_espn_{date_str}.csv", index=False)
        return combo, game_ids

    return pd.DataFrame(), game_ids


def update_pbp_espn_history_for_date(date_str: str, include_live: bool = False, rate_delay: float = 0.25) -> dict[str, Any]:
    """Fetch ESPN PBP for a date and append to a durable history file."""
    df, gids = fetch_pbp_espn_for_date(date_str, only_final=(not include_live), rate_delay=rate_delay)

    hist_parquet = paths.data_processed / "pbp_espn_history.parquet"
    hist_csv = paths.data_processed / "pbp_espn_history.csv"

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

    combo = pd.concat([hist, df], ignore_index=True) if hist is not None and not hist.empty else df

    # Best-effort dedupe by unique play_id within event.
    key_cols = [c for c in ["event_id", "play_id"] if c in combo.columns]
    if "date" in combo.columns:
        combo["date"] = pd.to_datetime(combo["date"], errors="coerce")
    if key_cols:
        combo = combo.sort_values(["date"], kind="stable")
        combo = combo.drop_duplicates(subset=key_cols, keep="last")

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


def backfill_pbp_espn_history(
    start_date: str,
    end_date: str,
    finals_only: bool = True,
    rate_delay: float = 0.25,
    skip_if_daily_exists: bool = True,
) -> pd.DataFrame:
    """Backfill ESPN PBP history over a date range.

    Returns a DataFrame of per-day summaries.
    """
    try:
        s = pd.to_datetime(start_date).date()
        e = pd.to_datetime(end_date).date()
    except Exception:
        return pd.DataFrame()

    if e < s:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for d in pd.date_range(s, e, freq="D"):
        ds = d.date().isoformat()
        daily = paths.data_processed / f"pbp_espn_{ds}.csv"
        if skip_if_daily_exists and daily.exists():
            rows.append({"date": ds, "skipped": True, "reason": "daily_exists"})
            continue
        info = update_pbp_espn_history_for_date(ds, include_live=(not finals_only), rate_delay=rate_delay)
        info["skipped"] = False
        rows.append(info)
    return pd.DataFrame(rows)
