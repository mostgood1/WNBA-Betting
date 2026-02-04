from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


def _season_for_date(date_str: str) -> str:
    d = pd.to_datetime(date_str, errors="coerce")
    if pd.isna(d):
        raise ValueError(f"invalid date: {date_str}")
    d = d.date()
    start_year = d.year if d.month >= 7 else d.year - 1
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def _pick_rosters_file(processed: Path, season: str) -> Path | None:
    preferred = processed / f"rosters_{season}.csv"
    if preferred.exists():
        return preferred
    # fallback: any rosters_*.csv, newest
    files = sorted(processed.glob("rosters_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _mtime_date(p: Path):
    try:
        return datetime.fromtimestamp(p.stat().st_mtime).date()
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Audit that daily rosters are fresh and that league_status team assignments agree "
            "with processed season rosters (trade-day guardrail)."
        )
    )
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--season", default=None, help="Season string like 2025-26 (auto from date if omitted)")
    ap.add_argument("--fail-if-stale", action="store_true", help="Fail if rosters file mtime < date")
    ap.add_argument(
        "--max-mismatches",
        type=int,
        default=0,
        help="Allowed mismatches before failing (default 0)",
    )
    args = ap.parse_args()

    date_str = str(args.date).strip()
    season = str(args.season).strip() if args.season else _season_for_date(date_str)

    processed = Path("data/processed")
    league_path = processed / f"league_status_{date_str}.csv"
    if not league_path.exists():
        raise SystemExit(f"missing {league_path}")

    rosters_path = _pick_rosters_file(processed, season)
    if rosters_path is None or not rosters_path.exists():
        raise SystemExit(f"missing rosters file for season={season} under {processed}")

    # Freshness check
    rosters_mtime = _mtime_date(rosters_path)
    target_date = pd.to_datetime(date_str, errors="coerce").date()
    stale = bool(rosters_mtime is not None and rosters_mtime < target_date)

    league = pd.read_csv(league_path)
    rosters = pd.read_csv(rosters_path)

    # Normalize / validate columns
    lc = {c.upper(): c for c in league.columns}
    rc = {c.upper(): c for c in rosters.columns}

    l_pid = lc.get("PLAYER_ID")
    l_team = lc.get("TEAM") or lc.get("TEAM_ABBREVIATION") or lc.get("TEAM_TRI")
    l_on = lc.get("TEAM_ON_SLATE")

    if not l_pid or not l_team:
        raise SystemExit(f"league_status missing required columns: PLAYER_ID + TEAM (have {list(league.columns)})")

    r_pid = rc.get("PLAYER_ID")
    r_team = rc.get("TEAM_ABBREVIATION")
    if not r_pid or not r_team:
        raise SystemExit(f"rosters missing required columns: PLAYER_ID + TEAM_ABBREVIATION (have {list(rosters.columns)})")

    league = league.copy()
    league[l_pid] = pd.to_numeric(league[l_pid], errors="coerce")
    league[l_team] = league[l_team].astype(str).str.upper().str.strip()

    rosters = rosters.copy()
    rosters[r_pid] = pd.to_numeric(rosters[r_pid], errors="coerce")
    rosters[r_team] = rosters[r_team].astype(str).str.upper().str.strip()

    # Focus audit on players whose teams matter for today
    if l_on and l_on in league.columns:
        on = league[l_on].astype(str).str.lower().isin({"true", "1", "yes", "y"})
        league = league[on].copy()

    league = league.dropna(subset=[l_pid])
    rosters = rosters.dropna(subset=[r_pid])

    # Duplicates by player_id happen (two-way/transactions). Only treat as fatal if the same
    # player_id appears under multiple distinct teams.
    dup = rosters[rosters.duplicated(subset=[r_pid], keep=False)].copy()
    dup_players = int(dup[r_pid].nunique()) if not dup.empty else 0
    multi_team_dups = 0
    try:
        if not dup.empty:
            tmp = dup[[r_pid, r_team]].dropna().copy()
            tmp[r_pid] = pd.to_numeric(tmp[r_pid], errors="coerce")
            tmp[r_team] = tmp[r_team].astype(str).str.upper().str.strip()
            counts = tmp.groupby(r_pid)[r_team].nunique(dropna=True)
            multi_team_dups = int((counts > 1).sum())
    except Exception:
        multi_team_dups = 0

    # Build player_id -> set(teams) from rosters.
    roster_pid_to_teams: dict[int, set[str]] = {}
    try:
        tmp = rosters[[r_pid, r_team]].dropna().copy()
        tmp[r_pid] = pd.to_numeric(tmp[r_pid], errors="coerce")
        tmp[r_team] = tmp[r_team].astype(str).str.upper().str.strip()
        tmp = tmp.dropna(subset=[r_pid])
        for _, rr in tmp.iterrows():
            try:
                pid = int(rr[r_pid])
                tri = str(rr[r_team] or "").strip().upper()
                if pid and tri:
                    roster_pid_to_teams.setdefault(pid, set()).add(tri)
            except Exception:
                continue
    except Exception:
        roster_pid_to_teams = {}

    league_small = league[[l_pid, l_team]].copy()
    league_small.rename(columns={l_pid: "player_id", l_team: "team_league"}, inplace=True)

    league_small["team_league"] = league_small["team_league"].fillna("").astype(str).str.upper().str.strip()

    # A mismatch is only when league_status team is not among the roster teams for that player_id.
    mism_rows = []
    for _, rr in league_small.iterrows():
        try:
            pid = int(rr["player_id"])
        except Exception:
            continue
        league_team = str(rr.get("team_league") or "").strip().upper()
        teams = roster_pid_to_teams.get(pid)
        if not teams:
            continue
        if league_team and league_team not in teams:
            mism_rows.append({"player_id": pid, "team_league": league_team, "team_roster_teams": sorted(list(teams))})

    mism = pd.DataFrame(mism_rows)

    out = {
        "date": date_str,
        "season": season,
        "league_status": str(league_path),
        "rosters": str(rosters_path),
        "rosters_mtime_date": None if rosters_mtime is None else rosters_mtime.isoformat(),
        "stale": stale,
        "on_slate_rows_checked": int(len(league_small)),
        "mismatches_n": int(len(mism)),
        "duplicate_roster_player_ids": dup_players,
        "multi_team_duplicate_player_ids": multi_team_dups,
        "mismatch_examples": (mism.head(25).to_dict(orient="records") if not mism.empty else []),
        "ran_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    print(json.dumps(out, indent=2))

    # Multi-team duplicates are common around trades/transactions; report but do not fail.
    if args.fail_if_stale and stale:
        raise SystemExit(4)
    if int(len(mism)) > int(args.max_mismatches):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
