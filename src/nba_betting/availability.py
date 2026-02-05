from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import json
from pathlib import Path

import pandas as pd

from .config import paths
from .league_status import build_league_status


@dataclass(frozen=True)
class DressedCheckResult:
    date: str
    ok: bool
    dressed_players_path: Path
    summary_path: Path
    summary: dict[str, Any]


def _coerce_bool(x: object) -> object:
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return x
    except Exception:
        pass
    s = str(x).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return None


def build_and_check_dressed_players(
    date_str: str,
    *,
    min_dressed_per_team: int = 8,
    min_total_roster_per_team: int = 10,
    fail_on_error: bool = True,
) -> DressedCheckResult:
    """Build a best-effort 'expected dressed to play' list for a date.

    This is intentionally conservative and deterministic, using only internal artifacts:
    - current rosters (from `fetch-rosters`) + league-wide resolution
    - injury designations (from `fetch-injuries` -> data/raw/injuries.csv + overrides)

    Output:
    - data/processed/dressed_players_<date>.csv
    - data/processed/dressed_summary_<date>.json

    The sim engine and props pipelines already consume `playing_today` from `league_status`.
    This tool provides a *first-step gate* that can fail the daily update if the player pool
    looks obviously wrong (common during trade deadline / roster churn).
    """

    ls_path = paths.data_processed / f"league_status_{date_str}.csv"
    if ls_path.exists():
        try:
            ls = pd.read_csv(ls_path)
        except Exception:
            ls = build_league_status(date_str)
    else:
        ls = build_league_status(date_str)

    if ls is None or ls.empty:
        dressed = pd.DataFrame(columns=["date", "team", "player_id", "player_name", "injury_status", "playing_today", "team_on_slate"])
        summary = {
            "date": date_str,
            "ok": False,
            "reason": "league_status_empty",
            "rows_league_status": 0,
        }
    else:
        ls = ls.copy()
        # Normalize expected columns
        for c in ("team", "injury_status", "player_name"):
            if c in ls.columns:
                ls[c] = ls[c].astype(str).fillna("").str.upper().str.strip() if c != "player_name" else ls[c].astype(str).fillna("").str.strip()
        if "playing_today" in ls.columns:
            ls["playing_today"] = ls["playing_today"].map(_coerce_bool)
        if "team_on_slate" in ls.columns:
            ls["team_on_slate"] = ls["team_on_slate"].map(_coerce_bool)

        # Focus only on slate teams.
        if "team_on_slate" in ls.columns:
            slate = ls[ls["team_on_slate"] == True].copy()  # noqa: E712
        else:
            # If missing, best-effort: treat all teams as slate.
            slate = ls.copy()
            slate["team_on_slate"] = True

        # Expected dressed: not explicitly false.
        if "playing_today" in slate.columns:
            dressed = slate[slate["playing_today"] != False].copy()  # noqa: E712
        else:
            dressed = slate.copy()
            dressed["playing_today"] = None

        dressed.insert(0, "date", date_str)

        keep = [
            "date",
            "team",
            "player_id",
            "player_name",
            "injury_status",
            "playing_today",
            "team_on_slate",
        ]
        dressed = dressed[[c for c in keep if c in dressed.columns]].copy()

        # Summary checks
        issues: list[str] = []
        team_counts: dict[str, Any] = {}
        try:
            grouped = slate.groupby("team", dropna=False)
            for team, g in grouped:
                tri = str(team or "").strip().upper()
                if not tri:
                    continue

                total = int(len(g))
                if "playing_today" in g.columns:
                    dressed_n = int((g["playing_today"] != False).sum())  # noqa: E712
                else:
                    dressed_n = total

                team_counts[tri] = {
                    "roster_rows": total,
                    "expected_dressed_rows": dressed_n,
                }

                if total < int(min_total_roster_per_team):
                    issues.append(f"team_roster_thin:{tri}:{total}")
                if dressed_n < int(min_dressed_per_team):
                    issues.append(f"team_dressed_thin:{tri}:{dressed_n}")
        except Exception:
            pass

        # Duplicate player_id across teams among expected dressed is a strong sign of stale roster mapping.
        try:
            if {"player_id", "team"}.issubset(set(dressed.columns)):
                tmp = dressed.copy()
                tmp["player_id"] = pd.to_numeric(tmp["player_id"], errors="coerce")
                tmp = tmp.dropna(subset=["player_id"])
                dup = tmp.groupby("player_id")["team"].nunique()
                bad = dup[dup > 1]
                if not bad.empty:
                    issues.append(f"duplicate_player_id_across_teams:{int(len(bad))}")
        except Exception:
            pass

        summary = {
            "date": date_str,
            "rows_league_status": int(len(ls)),
            "rows_slate": int(len(slate)),
            "rows_expected_dressed": int(len(dressed)),
            "min_dressed_per_team": int(min_dressed_per_team),
            "min_total_roster_per_team": int(min_total_roster_per_team),
            "team_counts": team_counts,
            "issues": issues,
        }
        summary["ok"] = bool(len(issues) == 0)

    dressed_path = paths.data_processed / f"dressed_players_{date_str}.csv"
    summary_path = paths.data_processed / f"dressed_summary_{date_str}.json"
    try:
        dressed.to_csv(dressed_path, index=False)
    except Exception:
        # Don't fail writing output if we can at least provide summary.
        pass

    try:
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    except Exception:
        pass

    ok = bool(summary.get("ok"))
    if (not ok) and fail_on_error:
        raise RuntimeError(f"Dressed-to-play check failed: issues={summary.get('issues')}")

    return DressedCheckResult(
        date=date_str,
        ok=ok,
        dressed_players_path=dressed_path,
        summary_path=summary_path,
        summary=summary,
    )
