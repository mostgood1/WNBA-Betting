from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from .boxscores import _espn_event_id_for_matchup, _espn_summary, _nba_gid_to_tricodes
from .config import paths
from .pbp_espn import _clock_to_seconds_remaining, _parse_substitution, _team_id_to_tricode_from_summary


REGULATION_PERIOD_SECONDS = 12 * 60
OT_PERIOD_SECONDS = 5 * 60


def _period_seconds(period: int) -> int:
    return REGULATION_PERIOD_SECONDS if int(period) <= 4 else OT_PERIOD_SECONDS


def _summary_team_players(summary: dict[str, Any]) -> list[dict[str, Any]]:
    box = (summary or {}).get("boxscore") or {}
    players = box.get("players") or []
    return players if isinstance(players, list) else []


def _player_id_name_maps(summary: dict[str, Any]) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Return (id->name, team_tricode->list[player_ids])."""
    id_to_name: dict[str, str] = {}
    team_to_ids: dict[str, list[str]] = {}

    for sec in _summary_team_players(summary):
        team = (sec.get("team") or {})
        tid = str(team.get("id") or "").strip()
        tri = str(team.get("abbreviation") or "").strip().upper()
        if tid:
            tri = tri
        if not tri:
            continue

        stats = sec.get("statistics") or []
        if not isinstance(stats, list) or not stats:
            continue
        athletes = (stats[0] or {}).get("athletes") or []
        if not isinstance(athletes, list):
            continue

        team_ids: list[str] = []
        for a in athletes:
            ath = (a or {}).get("athlete") or {}
            pid = str(ath.get("id") or "").strip()
            nm = str(ath.get("displayName") or ath.get("shortName") or "").strip()
            if pid:
                team_ids.append(pid)
                if pid not in id_to_name and nm:
                    id_to_name[pid] = nm

        if team_ids:
            team_to_ids[tri] = team_ids

    return id_to_name, team_to_ids


def _starters_by_team(summary: dict[str, Any]) -> dict[str, list[str]]:
    """Return team_tricode -> list of 5 starter player IDs."""
    out: dict[str, list[str]] = {}

    for sec in _summary_team_players(summary):
        team = (sec.get("team") or {})
        tri = str(team.get("abbreviation") or "").strip().upper()
        if not tri:
            continue

        stats = sec.get("statistics") or []
        if not isinstance(stats, list) or not stats:
            continue
        athletes = (stats[0] or {}).get("athletes") or []
        if not isinstance(athletes, list):
            continue

        starters: list[str] = []
        bench: list[str] = []
        for a in athletes:
            ath = (a or {}).get("athlete") or {}
            pid = str(ath.get("id") or "").strip()
            if not pid:
                continue
            if bool(a.get("starter")):
                starters.append(pid)
            else:
                bench.append(pid)

        # ESPN should provide 5 starters, but be defensive.
        if len(starters) >= 5:
            out[tri] = starters[:5]
        elif starters:
            out[tri] = starters + bench[: max(0, 5 - len(starters))]

    return out


def _plays_to_pbp_df(date_str: str, game_id: str, event_id: str, summary: dict[str, Any]) -> pd.DataFrame:
    plays = (summary or {}).get("plays") or []
    if not isinstance(plays, list) or not plays:
        return pd.DataFrame()

    team_map = _team_id_to_tricode_from_summary(summary)

    rows: list[dict[str, Any]] = []
    for p in plays:
        pt = (p.get("type") or {}).get("text")
        period = (p.get("period") or {}).get("number")
        clock = p.get("clock")
        sec_rem = _clock_to_seconds_remaining(clock)
        team_id = str((p.get("team") or {}).get("id") or "").strip()
        team_tri = team_map.get(team_id)

        text = str(p.get("text") or "")
        enter_name, exit_name = _parse_substitution(text)

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

        abs_time = None
        try:
            if period is not None and sec_rem is not None:
                abs_time = _compute_abs_time(int(period), int(sec_rem))
        except Exception:
            abs_time = None

        rows.append(
            {
                "date": date_str,
                "game_id": game_id,
                "event_id": event_id,
                "play_id": str(p.get("id") or ""),
                "sequence": p.get("sequenceNumber"),
                "period": period,
                "clock": (clock.get("displayValue") if isinstance(clock, dict) else str(clock or "")),
                "clock_sec_remaining": sec_rem,
                "abs_time_sec": abs_time,
                "type": str(pt or ""),
                "text": text,
                "team": team_tri,
                "enter_player_id": pid_in,
                "exit_player_id": pid_out,
                "enter_player_name": enter_name,
                "exit_player_name": exit_name,
                "scoring_play": bool(p.get("scoringPlay")) if "scoringPlay" in p else None,
                "shooting_play": bool(p.get("shootingPlay")) if "shootingPlay" in p else None,
                "score_value": p.get("scoreValue"),
                "points_attempted": p.get("pointsAttempted"),
                "participant1_id": (str(((parts[0] or {}).get("athlete") or {}).get("id") or "").strip() if isinstance(parts, list) and len(parts) >= 1 else None),
                "participant2_id": (str(((parts[1] or {}).get("athlete") or {}).get("id") or "").strip() if isinstance(parts, list) and len(parts) >= 2 else None),
                "source": "espn",
            }
        )

    return pd.DataFrame(rows)


def _safe_int(x: Any) -> Optional[int]:
    try:
        v = int(pd.to_numeric(x, errors="coerce"))
        return v
    except Exception:
        return None


def _compute_abs_time(period: int, clock_sec_remaining: Optional[int]) -> Optional[int]:
    if period is None:
        return None
    p = int(period)
    if clock_sec_remaining is None:
        return None

    # elapsed within this period
    el = _period_seconds(p) - int(clock_sec_remaining)
    el = max(0, el)

    base = 0
    if p <= 4:
        base = (p - 1) * REGULATION_PERIOD_SECONDS
    else:
        base = 4 * REGULATION_PERIOD_SECONDS + (p - 5) * OT_PERIOD_SECONDS

    return int(base + el)


def _ensure_five(lineup: list[str], roster: list[str]) -> list[str]:
    uniq = [x for x in lineup if x]
    # preserve order, unique
    seen: set[str] = set()
    out: list[str] = []
    for x in uniq:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    # fill if short
    for pid in roster:
        if len(out) >= 5:
            break
        if pid and pid not in seen:
            seen.add(pid)
            out.append(pid)
    # trim if long
    return out[:5]


def build_team_stints(
    pbp: pd.DataFrame,
    team: str,
    starters: list[str],
    roster: list[str],
) -> pd.DataFrame:
    """Build lineup stints for a single team from substitution events."""
    if pbp is None or pbp.empty:
        return pd.DataFrame()

    df = pbp.copy()
    if "type" not in df.columns:
        return pd.DataFrame()

    df = df[df["type"].astype(str).str.lower() == "substitution"].copy()
    df = df[df.get("team").astype(str).str.upper() == str(team).upper()].copy()
    if df.empty:
        return pd.DataFrame()

    df["period"] = pd.to_numeric(df.get("period"), errors="coerce")
    df["clock_sec_remaining"] = pd.to_numeric(df.get("clock_sec_remaining"), errors="coerce")
    df = df.dropna(subset=["period", "clock_sec_remaining"]).copy()
    if df.empty:
        return pd.DataFrame()

    df["abs_time_sec"] = df.apply(lambda r: _compute_abs_time(int(r["period"]), int(r["clock_sec_remaining"])), axis=1)
    df = df.dropna(subset=["abs_time_sec"]).copy()

    sort_cols = ["abs_time_sec"]
    if "sequence" in df.columns:
        sort_cols.append("sequence")
    df = df.sort_values(sort_cols, ascending=True, kind="stable")

    # Determine game end from plays periods
    max_period = int(df["period"].max())
    game_end = 4 * REGULATION_PERIOD_SECONDS
    if max_period > 4:
        game_end += (max_period - 4) * OT_PERIOD_SECONDS

    cur_lineup = _ensure_five(list(starters), roster)
    last_t = 0
    stints: list[dict[str, Any]] = []

    def _push(t0: int, t1: int, lineup: list[str]) -> None:
        if t1 <= t0:
            return
        stints.append(
            {
                "team": str(team).upper(),
                "start_sec": int(t0),
                "end_sec": int(t1),
                "duration_sec": int(t1 - t0),
                "lineup_player_ids": ";".join(lineup),
            }
        )

    for _, r in df.iterrows():
        t = int(r["abs_time_sec"])
        _push(last_t, t, cur_lineup)

        pid_in = str(r.get("enter_player_id") or "").strip() or None
        pid_out = str(r.get("exit_player_id") or "").strip() or None

        if pid_out and pid_out in cur_lineup:
            cur_lineup = [x for x in cur_lineup if x != pid_out]
        if pid_in and pid_in not in cur_lineup:
            cur_lineup = cur_lineup + [pid_in]

        cur_lineup = _ensure_five(cur_lineup, roster)
        last_t = t

    _push(last_t, game_end, cur_lineup)

    out = pd.DataFrame(stints)
    if out.empty:
        return out

    # Attach period column for convenience.
    def _period_for_abs(t: int) -> int:
        if t < 4 * REGULATION_PERIOD_SECONDS:
            return int(t // REGULATION_PERIOD_SECONDS) + 1
        t2 = t - 4 * REGULATION_PERIOD_SECONDS
        return 5 + int(t2 // OT_PERIOD_SECONDS)

    out["period"] = out["start_sec"].map(lambda t: _period_for_abs(int(t)))
    return out


def merge_on_court_segments(home: pd.DataFrame, away: pd.DataFrame) -> pd.DataFrame:
    """Merge team stints into shared on-court segments (both lineups fixed)."""
    if home is None or home.empty or away is None or away.empty:
        return pd.DataFrame()

    hi = 0
    ai = 0
    rows: list[dict[str, Any]] = []

    while hi < len(home) and ai < len(away):
        h = home.iloc[hi]
        a = away.iloc[ai]
        s = int(max(int(h["start_sec"]), int(a["start_sec"])))
        e = int(min(int(h["end_sec"]), int(a["end_sec"])))
        if e > s:
            rows.append(
                {
                    "start_sec": s,
                    "end_sec": e,
                    "duration_sec": int(e - s),
                    "home_lineup_player_ids": str(h.get("lineup_player_ids") or ""),
                    "away_lineup_player_ids": str(a.get("lineup_player_ids") or ""),
                }
            )
        if int(h["end_sec"]) <= int(a["end_sec"]):
            hi += 1
        else:
            ai += 1

    return pd.DataFrame(rows)


def compute_pair_minutes(segments: pd.DataFrame, team: str, side: str = "home") -> pd.DataFrame:
    """Compute within-team pair minutes together from merged segments."""
    if segments is None or segments.empty:
        return pd.DataFrame()

    col = "home_lineup_player_ids" if str(side).lower() == "home" else "away_lineup_player_ids"
    if col not in segments.columns:
        return pd.DataFrame()

    acc: dict[tuple[str, str], dict[str, Any]] = {}

    for _, r in segments.iterrows():
        dur = float(r.get("duration_sec") or 0)
        if dur <= 0:
            continue
        ids = [x for x in str(r.get(col) or "").split(";") if x]
        if len(ids) < 2:
            continue
        ids = sorted(set(ids))
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                k = (ids[i], ids[j])
                if k not in acc:
                    acc[k] = {"team": str(team).upper(), "player1_id": ids[i], "player2_id": ids[j], "sec_together": 0.0, "segments": 0}
                acc[k]["sec_together"] += dur
                acc[k]["segments"] += 1

    rows = list(acc.values())
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["min_together"] = out["sec_together"].astype(float) / 60.0
    out = out.sort_values(["min_together"], ascending=False, kind="stable")
    return out


def join_plays_to_segments(pbp: pd.DataFrame, segments: pd.DataFrame) -> pd.DataFrame:
    """Attach the active 5v5 lineups to each play via abs_time_sec."""
    if pbp is None or pbp.empty or segments is None or segments.empty:
        return pd.DataFrame()

    if "abs_time_sec" not in pbp.columns:
        return pd.DataFrame()
    if not {"start_sec", "end_sec", "home_lineup_player_ids", "away_lineup_player_ids"}.issubset(set(segments.columns)):
        return pd.DataFrame()

    plays = pbp.copy()
    plays["abs_time_sec"] = pd.to_numeric(plays["abs_time_sec"], errors="coerce")
    plays = plays.dropna(subset=["abs_time_sec"]).copy()
    if plays.empty:
        return pd.DataFrame()

    seg = segments.sort_values(["start_sec"], kind="stable").reset_index(drop=True)
    seg["start_sec"] = pd.to_numeric(seg["start_sec"], errors="coerce")
    seg["end_sec"] = pd.to_numeric(seg["end_sec"], errors="coerce")

    plays = plays.sort_values(["abs_time_sec"], kind="stable").reset_index(drop=True)

    out_rows: list[dict[str, Any]] = []
    j = 0
    for _, r in plays.iterrows():
        t = int(r["abs_time_sec"])
        while j < len(seg) and int(seg.loc[j, "end_sec"]) <= t:
            j += 1
        if j >= len(seg):
            break
        s0 = int(seg.loc[j, "start_sec"])
        e0 = int(seg.loc[j, "end_sec"])
        if not (s0 <= t < e0):
            # try nearby (rare boundary issues)
            found = False
            for k in (j - 1, j + 1):
                if 0 <= k < len(seg):
                    s1 = int(seg.loc[k, "start_sec"])
                    e1 = int(seg.loc[k, "end_sec"])
                    if s1 <= t < e1:
                        j = k
                        s0, e0 = s1, e1
                        found = True
                        break
            if not found:
                continue

        d = r.to_dict()
        d["home_lineup_player_ids"] = str(seg.loc[j, "home_lineup_player_ids"] or "")
        d["away_lineup_player_ids"] = str(seg.loc[j, "away_lineup_player_ids"] or "")
        out_rows.append(d)

    return pd.DataFrame(out_rows)


def fetch_rotations_for_game(date_str: str, game_id: str, home_tri: str, away_tri: str) -> dict[str, Any]:
    """Build stints + merged segments + pair minutes for a single NBA gameId."""
    eid = _espn_event_id_for_matchup(date_str, home_tri=home_tri, away_tri=away_tri)
    if not eid:
        return {"date": date_str, "game_id": game_id, "event_id": None, "error": "no_event"}

    summ = _espn_summary(eid)
    starters = _starters_by_team(summ)
    id_to_name, roster_map = _player_id_name_maps(summ)

    pbp = _plays_to_pbp_df(date_str, game_id=game_id, event_id=eid, summary=summ)
    if pbp is None or pbp.empty:
        # ESPN sometimes returns/caches a pre-game summary without plays.
        # Force a refresh before giving up.
        summ = _espn_summary(eid, force=True)
        starters = _starters_by_team(summ)
        id_to_name, roster_map = _player_id_name_maps(summ)
        pbp = _plays_to_pbp_df(date_str, game_id=game_id, event_id=eid, summary=summ)
        if pbp is None or pbp.empty:
            return {"date": date_str, "game_id": game_id, "event_id": eid, "error": "no_plays"}

    h0 = str(home_tri).upper()
    a0 = str(away_tri).upper()

    home_starters = starters.get(h0) or []
    away_starters = starters.get(a0) or []
    home_roster = roster_map.get(h0) or []
    away_roster = roster_map.get(a0) or []

    home_stints = build_team_stints(pbp, team=h0, starters=home_starters, roster=home_roster)
    away_stints = build_team_stints(pbp, team=a0, starters=away_starters, roster=away_roster)

    segments = merge_on_court_segments(home_stints, away_stints)

    if segments is not None and not segments.empty:
        segments["home_team"] = h0
        segments["away_team"] = a0

    plays_ctx = join_plays_to_segments(pbp, segments)

    if plays_ctx is not None and not plays_ctx.empty:
        plays_ctx["home_team"] = h0
        plays_ctx["away_team"] = a0

    home_pairs = compute_pair_minutes(segments, team=h0, side="home")
    away_pairs = compute_pair_minutes(segments, team=a0, side="away")

    def _attach_names(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        df = df.copy()
        df["player1_name"] = df["player1_id"].map(lambda x: id_to_name.get(str(x), ""))
        df["player2_name"] = df["player2_id"].map(lambda x: id_to_name.get(str(x), ""))
        return df

    home_pairs = _attach_names(home_pairs)
    away_pairs = _attach_names(away_pairs)

    for d in [home_stints, away_stints, segments, home_pairs, away_pairs, plays_ctx]:
        if d is not None and not d.empty:
            d["date"] = date_str
            d["game_id"] = str(game_id)
            d["event_id"] = str(eid)

    return {
        "date": date_str,
        "game_id": str(game_id),
        "event_id": str(eid),
        "home": h0,
        "away": a0,
        "home_stints": home_stints,
        "away_stints": away_stints,
        "segments": segments,
        "home_pairs": home_pairs,
        "away_pairs": away_pairs,
        "plays_ctx": plays_ctx,
    }


def update_rotations_history_for_date(date_str: str, rate_delay: float = 0.25) -> dict[str, Any]:
    """Fetch ESPN rotations for all games on date and append to history datasets."""
    gid_map = _nba_gid_to_tricodes(date_str)
    if not gid_map:
        return {"date": date_str, "games": 0, "rows_stints": 0, "rows_pairs": 0, "error": "no_games"}

    out_dir = paths.data_processed / "rotations_espn"
    out_dir.mkdir(parents=True, exist_ok=True)

    stints_frames: list[pd.DataFrame] = []
    pairs_frames: list[pd.DataFrame] = []
    plays_frames: list[pd.DataFrame] = []

    failures: list[dict[str, Any]] = []

    for gid, (home, away) in sorted(gid_map.items()):
        r: dict[str, Any] = {}
        last_err: Optional[str] = None
        # ESPN endpoints can be flaky; retry a few times with light backoff.
        for attempt in range(1, 4):
            r = fetch_rotations_for_game(date_str, game_id=gid, home_tri=home, away_tri=away)
            if not r.get("error"):
                last_err = None
                break
            last_err = str(r.get("error"))
            time.sleep(max(rate_delay, 0.15) * attempt)

        if last_err is not None:
            failures.append(
                {
                    "date": date_str,
                    "game_id": str(gid),
                    "event_id": str(r.get("event_id") or "") or None,
                    "home": home,
                    "away": away,
                    "error": last_err,
                }
            )
            time.sleep(rate_delay)
            continue

        # write per-game
        (r["home_stints"]).to_csv(out_dir / f"stints_home_{gid}.csv", index=False)
        (r["away_stints"]).to_csv(out_dir / f"stints_away_{gid}.csv", index=False)
        (r["segments"]).to_csv(out_dir / f"segments_{gid}.csv", index=False)

        hp = r["home_pairs"]
        ap = r["away_pairs"]
        all_pairs = pd.concat([hp, ap], ignore_index=True) if (hp is not None and ap is not None) else (hp or ap)
        if all_pairs is not None and not all_pairs.empty:
            all_pairs.to_csv(out_dir / f"pairs_{gid}.csv", index=False)

        stints_frames.append(pd.concat([r["home_stints"], r["away_stints"]], ignore_index=True))
        if all_pairs is not None and not all_pairs.empty:
            pairs_frames.append(all_pairs)

        pc = r.get("plays_ctx")
        if isinstance(pc, pd.DataFrame) and (not pc.empty):
            plays_frames.append(pc)

        time.sleep(rate_delay)

    stints = pd.concat(stints_frames, ignore_index=True) if stints_frames else pd.DataFrame()
    pairs = pd.concat(pairs_frames, ignore_index=True) if pairs_frames else pd.DataFrame()
    plays_ctx = pd.concat(plays_frames, ignore_index=True) if plays_frames else pd.DataFrame()

    st_hist_pq = paths.data_processed / "rotation_stints_history.parquet"
    st_hist_csv = paths.data_processed / "rotation_stints_history.csv"
    pr_hist_pq = paths.data_processed / "pair_minutes_history.parquet"
    pr_hist_csv = paths.data_processed / "pair_minutes_history.csv"

    pc_hist_pq = paths.data_processed / "play_context_history.parquet"
    pc_hist_csv = paths.data_processed / "play_context_history.csv"

    def _append_hist(df: pd.DataFrame, pq: Path, csv: Path, key_cols: list[str]) -> tuple[Optional[str], int]:
        if df is None or df.empty:
            return None, 0
        hist = None
        if pq.exists():
            try:
                hist = pd.read_parquet(pq)
            except Exception:
                hist = None
        if hist is None and csv.exists():
            try:
                hist = pd.read_csv(csv)
            except Exception:
                hist = None
        combo = pd.concat([hist, df], ignore_index=True) if hist is not None and not hist.empty else df
        if key_cols:
            try:
                combo = combo.drop_duplicates(subset=key_cols, keep="last")
            except Exception:
                pass
        wrote = None
        try:
            combo.to_parquet(pq, index=False)
            wrote = str(pq)
        except Exception:
            try:
                combo.to_csv(csv, index=False)
                wrote = str(csv)
            except Exception:
                wrote = None
        return wrote, int(len(combo))

    wrote_st, st_rows = _append_hist(stints, st_hist_pq, st_hist_csv, key_cols=["event_id", "team", "start_sec", "end_sec", "lineup_player_ids"])
    wrote_pr, pr_rows = _append_hist(pairs, pr_hist_pq, pr_hist_csv, key_cols=["event_id", "team", "player1_id", "player2_id"])
    wrote_pc, pc_rows = _append_hist(plays_ctx, pc_hist_pq, pc_hist_csv, key_cols=["event_id", "play_id"])

    out: dict[str, Any] = {
        "date": date_str,
        "games": int(len(gid_map)),
        "rows_stints": 0 if stints is None else int(len(stints)),
        "rows_pairs": 0 if pairs is None else int(len(pairs)),
        "rows_plays_ctx": 0 if plays_ctx is None else int(len(plays_ctx)),
        "history_stints_rows": st_rows,
        "history_pairs_rows": pr_rows,
        "history_plays_ctx_rows": pc_rows,
        "wrote_stints": wrote_st,
        "wrote_pairs": wrote_pr,
        "wrote_plays_ctx": wrote_pc,
    }

    if failures:
        try:
            pd.DataFrame(failures).to_csv(out_dir / f"rotations_failures_{date_str}.csv", index=False)
            out["failures"] = int(len(failures))
            out["failures_file"] = str(out_dir / f"rotations_failures_{date_str}.csv")
        except Exception:
            out["failures"] = int(len(failures))

    return out
