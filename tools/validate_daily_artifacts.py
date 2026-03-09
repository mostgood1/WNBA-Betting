"""Validate daily produced artifacts.

This is used by scripts/daily_update.ps1 to ensure key outputs exist. Some
artifacts, such as game odds and ESPN rotations coverage, are produced via
best-effort blocks and can be downgraded to warnings by configuration.

Exit codes:
  0: OK
  3: Missing required artifacts (when fail-on-missing enabled)
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def _exists_nonempty(path: Path) -> bool:
    try:
        return path.exists() and path.stat().st_size > 0
    except Exception:
        return bool(path.exists())


def _parse_bool(v: object, default: bool = False) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s == "":
        return default
    return s in {"1", "true", "yes", "y"}


def _rotations_status(proc: Path, date_yesterday: str | None) -> tuple[int | None, int, list[str], list[str], str | None]:
    rot_dir = proc / "rotations_espn"
    rot_expected: int | None = None
    rot_have = 0
    rot_missing_gids: list[str] = []
    rot_excused_no_event_gids: list[str] = []
    rot_error: str | None = None

    if not date_yesterday:
        return rot_expected, rot_have, rot_missing_gids, rot_excused_no_event_gids, rot_error

    try:
        from nba_betting.boxscores import _nba_gid_to_tricodes

        gid_map = _nba_gid_to_tricodes(str(date_yesterday)) or {}
        gids = sorted([str(g).strip() for g in gid_map.keys() if str(g).strip()])

        # If ESPN returns no event for a game, rotations cannot be fetched.
        excused: set[str] = set()
        try:
            import csv

            failures_fp = rot_dir / f"rotations_failures_{str(date_yesterday)}.csv"
            if failures_fp.exists() and failures_fp.stat().st_size > 0:
                with failures_fp.open("r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        gid = str((row or {}).get("game_id") or "").strip()
                        err = str((row or {}).get("error") or "").strip().lower()
                        if gid and err == "no_event":
                            excused.add(gid)
        except Exception:
            excused = set()

        rot_excused_no_event_gids = sorted(excused)
        rot_expected = len([g for g in gids if g not in excused])

        for gid in gids:
            if gid in excused:
                continue
            hp = rot_dir / f"stints_home_{gid}.csv"
            ap = rot_dir / f"stints_away_{gid}.csv"
            if _exists_nonempty(hp) and _exists_nonempty(ap):
                rot_have += 1
            else:
                rot_missing_gids.append(gid)

    except Exception as e:
        rot_error = str(e)

    return rot_expected, rot_have, rot_missing_gids, rot_excused_no_event_gids, rot_error


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", default=os.environ.get("REPO_ROOT", "."), help="Repo root path")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--yesterday", default=None, help="YYYY-MM-DD")

    ap.add_argument("--fail-on-missing", action="store_true", default=_parse_bool(os.environ.get("FAIL_ON_MISSING"), True))
    ap.add_argument("--require-odds", action="store_true", default=_parse_bool(os.environ.get("REQUIRE_ODDS"), True))
    ap.add_argument("--require-smartsim", action="store_true", default=_parse_bool(os.environ.get("REQUIRE_SMARTSIM"), True))
    ap.add_argument(
        "--require-rotations",
        action="store_true",
        default=_parse_bool(os.environ.get("REQUIRE_ROTATIONS"), True),
        help="Require ESPN rotations stints coverage (uses a minimum coverage threshold)",
    )
    ap.add_argument(
        "--rotations-min-coverage",
        type=float,
        default=float(os.environ.get("ROTATIONS_MIN_COVERAGE", "0.70") or "0.70"),
        help="Minimum fraction of yesterday's games that must have rotations stints present",
    )

    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    proc = repo_root / "data" / "processed"

    date_str = str(args.date)
    date_yesterday = str(args.yesterday) if args.yesterday else None

    pred = proc / f"predictions_{date_str}.csv"
    props = proc / f"props_predictions_{date_str}.csv"
    odds = proc / f"game_odds_{date_str}.csv"

    slate_games: int | None = None
    try:
        import pandas as pd

        if _exists_nonempty(pred):
            df = pd.read_csv(pred)
            if df is not None and not df.empty:
                if {"home_team", "visitor_team"}.issubset(set(df.columns)):
                    slate_games = int(df[["home_team", "visitor_team"]].drop_duplicates().shape[0])
                else:
                    slate_games = int(len(df))
    except Exception:
        slate_games = None

    smart = sorted(proc.glob(f"smart_sim_{date_str}_*.json"))
    smart_count = len(smart)

    rot_expected, rot_have, rot_missing_gids, rot_excused_no_event_gids, rot_error = _rotations_status(proc, date_yesterday)

    missing: list[str] = []
    warnings: list[str] = []

    # --- Sanity checks: minutes vs player stats (props predictions) ---
    try:
        import pandas as pd
        import numpy as np

        if _exists_nonempty(props):
            dfp = pd.read_csv(props)
            if dfp is not None and not dfp.empty:
                # Choose the best available minutes signal.
                min_candidates = [c for c in ["pred_min", "roll10_min", "roll5_min", "roll3_min", "lag1_min"] if c in dfp.columns]
                min_col = min_candidates[0] if min_candidates else None
                # Prefer the most-populated column.
                if min_candidates:
                    best = None
                    best_n = -1
                    best_sum = -1.0
                    for c in min_candidates:
                        v = pd.to_numeric(dfp[c], errors="coerce")
                        ok = v[np.isfinite(v) & (v > 0)]
                        n_ok = int(ok.shape[0])
                        s_ok = float(ok.sum()) if n_ok > 0 else 0.0
                        if (n_ok > best_n) or (n_ok == best_n and s_ok > best_sum):
                            best = c
                            best_n = n_ok
                            best_sum = s_ok
                    min_col = best or min_col

                if min_col:
                    mins = pd.to_numeric(dfp[min_col], errors="coerce").fillna(0.0).astype(float)
                    dfp = dfp.copy()
                    dfp["__min__"] = mins.clip(lower=0.0, upper=48.0)

                    # Restrict to players expected on slate if present.
                    if "team_on_slate" in dfp.columns:
                        try:
                            tos = dfp["team_on_slate"].astype(str).str.lower().str.strip()
                            dfp = dfp[tos.isin(["true", "1", "yes", "y"])].copy()
                        except Exception:
                            pass
                    if "playing_today" in dfp.columns:
                        try:
                            pt = dfp["playing_today"].astype(str).str.lower().str.strip()
                            dfp = dfp[~pt.isin(["false", "0", "no", "n"])].copy()
                        except Exception:
                            pass

                    # Team-level minutes coverage: top-10 should roughly cover a full game.
                    if "team" in dfp.columns and not dfp.empty:
                        tmp = dfp.copy()
                        tmp["team"] = tmp["team"].astype(str).str.upper().str.strip()
                        grp = tmp.groupby("team", as_index=False)
                        for _, g in grp:
                            team = str(g["team"].iloc[0] or "").strip()
                            top = g.sort_values("__min__", ascending=False).head(10)
                            top_sum = float(top["__min__"].sum())
                            starters20 = int((top["__min__"] >= 20.0).sum())
                            if top_sum < 180.0:
                                warnings.append(f"minutes_sanity: team {team} top10 minutes sum low ({top_sum:.1f}) using {min_col}")
                            if starters20 < 5:
                                warnings.append(f"minutes_sanity: team {team} has <5 players at 20+ min (found {starters20}) using {min_col}")

                    # Player-level: suspicious combos.
                    if "pred_pts" in dfp.columns:
                        pts = pd.to_numeric(dfp["pred_pts"], errors="coerce").fillna(0.0).astype(float)
                        dfp["__pts__"] = pts
                        bad = dfp[(dfp["__min__"] > 0) & (dfp["__min__"] < 8.0) & (dfp["__pts__"] >= 10.0)]
                        if not bad.empty:
                            # only report a small sample
                            names = []
                            for _, r in bad.head(6).iterrows():
                                nm = str(r.get("player_name") or "").strip()
                                team = str(r.get("team") or "").strip()
                                names.append(f"{nm}({team})")
                            warnings.append(f"minutes_sanity: {len(bad)} players have <8 min but >=10 pred_pts (sample: {', '.join(names)})")
    except Exception:
        # Best-effort: do not fail daily artifacts validation.
        pass

    if not _exists_nonempty(pred):
        missing.append(pred.name)
    if not _exists_nonempty(props):
        missing.append(props.name)

    odds_ok = _exists_nonempty(odds)
    if not odds_ok:
        if args.require_odds:
            missing.append(odds.name)
        else:
            warnings.append(f"optional artifact missing: {odds.name}")

    if args.require_smartsim and slate_games is not None and slate_games > 0 and smart_count < max(1, slate_games):
        missing.append(f"smart_sim_{date_str}_*.json ({smart_count}/{slate_games})")

    cov = None
    if rot_expected is None:
        msg = "rotations_espn stints (could not determine game ids)"
        if args.require_rotations:
            missing.append(msg)
        elif date_yesterday:
            warnings.append(f"optional artifact unavailable: {msg}")
    elif rot_expected > 0:
        cov = float(rot_have) / float(rot_expected)
        thr = float(args.rotations_min_coverage)
        thr = max(0.0, min(1.0, thr))
        if cov + 1e-12 < thr:
            msg = f"rotations_espn stints ({rot_have}/{rot_expected})"
            if args.require_rotations:
                missing.append(msg)
            else:
                warnings.append(f"optional artifact partial: {msg}")
        elif rot_have < rot_expected:
            warnings.append(f"rotations_espn stints partial ({rot_have}/{rot_expected})")

    out = {
        "date": date_str,
        "yesterday": date_yesterday,
        "predictions_ok": _exists_nonempty(pred),
        "props_predictions_ok": _exists_nonempty(props),
        "game_odds_ok": _exists_nonempty(odds),
        "slate_games": slate_games,
        "smart_sim_count": smart_count,
        "rotations_dir_exists": bool((proc / "rotations_espn").exists()),
        "rotations_expected_games_yesterday": rot_expected,
        "rotations_have_games_yesterday": rot_have,
        "rotations_min_coverage": float(args.rotations_min_coverage),
        "rotations_coverage_yesterday": cov,
        "rotations_missing_game_ids_yesterday": rot_missing_gids[:50],
        "rotations_excused_no_event_game_ids_yesterday": rot_excused_no_event_gids[:50],
        "rotations_error": rot_error,
        "require_odds": bool(args.require_odds),
        "require_smartsim": bool(args.require_smartsim),
        "require_rotations_espn": bool(args.require_rotations),
        "warnings": warnings,
        "missing": missing,
        "ok": (len(missing) == 0),
    }

    try:
        fp = proc / f"daily_artifacts_{date_str}.json"
        fp.write_text(json.dumps(out, indent=2), encoding="utf-8")
    except Exception:
        pass

    print(json.dumps(out, indent=2))

    if args.fail_on_missing and missing:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
