from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROC = ROOT / "data" / "processed"


def _parse_bool(v: object, default: bool = False) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s == "":
        return default
    return s in {"1", "true", "yes", "y"}


def _date_range(start: str, end: str) -> list[str]:
    s = pd.to_datetime(start, errors="coerce")
    e = pd.to_datetime(end, errors="coerce")
    if pd.isna(s) or pd.isna(e):
        raise ValueError(f"invalid start/end: {start}..{end}")
    s = s.date()
    e = e.date()
    if s > e:
        s, e = e, s
    out: list[str] = []
    d = s
    while d <= e:
        out.append(str(d))
        d = (pd.Timestamp(d) + pd.Timedelta(days=1)).date()
    return out


def _exists_nonempty(fp: Path) -> bool:
    try:
        return fp.exists() and fp.stat().st_size > 0
    except Exception:
        return bool(fp.exists())


def _safe_num_series(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce")


def _norm_bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    s = df[col]
    if s.dtype == bool:
        return s
    return s.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def _candidate_cols(all_cols: list[str], patterns: list[str]) -> list[str]:
    cols = [c for c in all_cols]
    out: list[str] = []
    low = {c.lower(): c for c in cols}
    for p in patterns:
        if p.lower() in low:
            out.append(low[p.lower()])
    return out


def _detect_expected_minutes_cols(cols: list[str]) -> list[str]:
    # Broad-but-safe: anything that clearly looks like a minutes projection.
    wanted = {
        "expected_min",
        "expected_minutes",
        "exp_min",
        "proj_min",
        "projected_min",
        "minutes_proj",
        "mins_proj",
        "minutes_projection",
        "starter_min",
    }
    out: list[str] = []
    for c in cols:
        cl = c.lower().strip()
        if cl in wanted:
            out.append(c)
            continue
        if "expected" in cl and "min" in cl:
            out.append(c)
            continue
        if "project" in cl and "min" in cl:
            out.append(c)
            continue
    return sorted(list(dict.fromkeys(out)))


def _detect_starter_cols(cols: list[str]) -> list[str]:
    wanted = {"is_starter", "starter", "starting", "starting_lineup", "in_starting_lineup"}
    out: list[str] = []
    for c in cols:
        cl = c.lower().strip()
        if cl in wanted:
            out.append(c)
            continue
        if "starter" in cl:
            out.append(c)
            continue
        if cl.startswith("is_") and "start" in cl:
            out.append(c)
            continue
    return sorted(list(dict.fromkeys(out)))


def _detect_asof_ts_cols(cols: list[str]) -> list[str]:
    out: list[str] = []
    for c in cols:
        cl = c.lower().strip()
        if cl in {"asof_ts", "asof_timestamp", "asof_datetime", "asof_time"}:
            out.append(c)
            continue
        if cl.startswith("asof_") and ("ts" in cl or "time" in cl or "stamp" in cl):
            out.append(c)
    return sorted(list(dict.fromkeys(out)))


@dataclass
class DateGap:
    date: str

    props_predictions_path: str
    props_predictions_ok: bool
    predictions_path: str
    predictions_ok: bool
    game_odds_path: str
    game_odds_ok: bool

    injuries_counts_path: str
    injuries_counts_ok: bool
    league_status_path: str
    league_status_ok: bool
    roster_audit_path: str
    roster_audit_ok: bool

    props_rows: int
    props_rows_focus: int
    props_cols: list[str]

    pregame_expected_minutes_path: str
    pregame_expected_minutes_ok: bool
    pregame_expected_minutes_rows: int
    pregame_expected_minutes_cols: list[str]
    pregame_expected_minutes_team_cov_mean: float | None
    pregame_expected_minutes_asof_ts_max: str | None

    minutes_cols_present: list[str]
    minutes_cols_coverage: dict[str, float]

    expected_minutes_cols_present: list[str]
    starter_cols_present: list[str]
    asof_ts_cols_present: list[str]

    injury_status_nonempty_frac: float | None
    injury_status_dtd_frac: float | None


def audit_date(date_str: str, processed: Path) -> DateGap:
    ds = str(date_str).strip()
    props_fp = processed / f"props_predictions_{ds}.csv"
    pred_fp = processed / f"predictions_{ds}.csv"
    odds_fp = processed / f"game_odds_{ds}.csv"

    inj_fp = processed / f"injuries_counts_{ds}.json"
    ls_fp = processed / f"league_status_{ds}.csv"
    ra_fp = processed / f"roster_audit_{ds}.csv"

    props_ok = _exists_nonempty(props_fp)

    props_rows = 0
    props_rows_focus = 0
    props_cols: list[str] = []
    pem_rows = 0
    pem_cols: list[str] = []
    pem_team_cov_mean: float | None = None
    pem_asof_ts_max: str | None = None
    minutes_cols_present: list[str] = []
    minutes_cols_coverage: dict[str, float] = {}
    expected_minutes_cols_present: list[str] = []
    starter_cols_present: list[str] = []
    asof_ts_cols_present: list[str] = []
    injury_status_nonempty_frac: float | None = None
    injury_status_dtd_frac: float | None = None

    pem_fp = processed / f"pregame_expected_minutes_{ds}.csv"
    pem_ok = _exists_nonempty(pem_fp)
    if pem_ok:
        try:
            pem = pd.read_csv(pem_fp)
        except Exception:
            pem = pd.DataFrame()

        if pem is not None and (not pem.empty):
            pem_rows = int(len(pem))
            pem_cols = [str(c) for c in list(pem.columns)]

            # Rough coverage: mean fraction of rows with positive expected minutes per team.
            try:
                team_col = "team_tri" if "team_tri" in pem.columns else ("team" if "team" in pem.columns else None)
                min_col = None
                for c in ["exp_min_mean", "expected_min", "expected_minutes", "exp_minutes", "proj_min", "projected_min"]:
                    if c in pem.columns:
                        min_col = c
                        break
                if team_col and min_col:
                    tmp = pem[[team_col, min_col]].copy()
                    tmp[team_col] = tmp[team_col].astype(str).str.upper().str.strip()
                    v = pd.to_numeric(tmp[min_col], errors="coerce")
                    tmp["_ok"] = (v.notna()) & (v > 0) & (v < 60)
                    grp = tmp.groupby(team_col)["_ok"].mean()
                    if len(grp) > 0:
                        pem_team_cov_mean = float(grp.mean())
            except Exception:
                pem_team_cov_mean = None

            # Freshness: max as-of timestamp if present.
            try:
                asof_col = None
                for c in ["exp_asof_ts", "asof_ts", "asof_timestamp", "asof_datetime"]:
                    if c in pem.columns:
                        asof_col = c
                        break
                if asof_col:
                    ts = pd.to_datetime(pem[asof_col], errors="coerce")
                    if ts.notna().any():
                        pem_asof_ts_max = str(ts.max())
            except Exception:
                pem_asof_ts_max = None

    if props_ok:
        try:
            df = pd.read_csv(props_fp)
        except Exception:
            df = pd.DataFrame()

        if df is not None and (not df.empty):
            props_rows = int(len(df))
            props_cols = [str(c) for c in list(df.columns)]

            focus = df.copy()
            if "team_on_slate" in focus.columns:
                try:
                    focus = focus[_norm_bool_series(focus, "team_on_slate")].copy()
                except Exception:
                    pass
            if "playing_today" in focus.columns:
                try:
                    focus = focus[_norm_bool_series(focus, "playing_today")].copy()
                except Exception:
                    pass
            props_rows_focus = int(len(focus))

            # Minutes signals used by the sim today.
            minutes_candidates = [c for c in ["pred_min", "roll10_min", "roll5_min", "roll3_min", "lag1_min"] if c in focus.columns]
            minutes_cols_present = minutes_candidates
            for c in minutes_candidates:
                try:
                    v = _safe_num_series(focus, c)
                    ok = v.notna() & (v > 0) & (v < 60)
                    denom = max(1, int(len(focus)))
                    minutes_cols_coverage[c] = float(ok.sum()) / float(denom)
                except Exception:
                    minutes_cols_coverage[c] = 0.0

            expected_minutes_cols_present = _detect_expected_minutes_cols(props_cols)
            starter_cols_present = _detect_starter_cols(props_cols)
            asof_ts_cols_present = _detect_asof_ts_cols(props_cols)

            if "injury_status" in focus.columns and len(focus) > 0:
                try:
                    inj = focus["injury_status"].fillna("").astype(str).str.strip()
                    injury_status_nonempty_frac = float((inj != "").mean())
                    injury_status_dtd_frac = float((inj.str.upper() == "DAY-TO-DAY").mean())
                except Exception:
                    injury_status_nonempty_frac = None
                    injury_status_dtd_frac = None

    return DateGap(
        date=ds,
        props_predictions_path=str(props_fp),
        props_predictions_ok=props_ok,
        predictions_path=str(pred_fp),
        predictions_ok=_exists_nonempty(pred_fp),
        game_odds_path=str(odds_fp),
        game_odds_ok=_exists_nonempty(odds_fp),
        injuries_counts_path=str(inj_fp),
        injuries_counts_ok=_exists_nonempty(inj_fp),
        league_status_path=str(ls_fp),
        league_status_ok=_exists_nonempty(ls_fp),
        roster_audit_path=str(ra_fp),
        roster_audit_ok=_exists_nonempty(ra_fp),
        props_rows=props_rows,
        props_rows_focus=props_rows_focus,
        props_cols=props_cols,
        pregame_expected_minutes_path=str(pem_fp),
        pregame_expected_minutes_ok=pem_ok,
        pregame_expected_minutes_rows=pem_rows,
        pregame_expected_minutes_cols=pem_cols,
        pregame_expected_minutes_team_cov_mean=pem_team_cov_mean,
        pregame_expected_minutes_asof_ts_max=pem_asof_ts_max,
        minutes_cols_present=minutes_cols_present,
        minutes_cols_coverage=minutes_cols_coverage,
        expected_minutes_cols_present=expected_minutes_cols_present,
        starter_cols_present=starter_cols_present,
        asof_ts_cols_present=asof_ts_cols_present,
        injury_status_nonempty_frac=injury_status_nonempty_frac,
        injury_status_dtd_frac=injury_status_dtd_frac,
    )


def _mean_or_none(vals: list[float]) -> float | None:
    vv = [float(x) for x in vals if x is not None]
    if not vv:
        return None
    return float(sum(vv)) / float(len(vv))


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Audit sim-engine data gaps over a date range by inspecting data/processed daily artifacts. "
            "Reports file existence and pregame-signal column coverage (expected minutes, starters, as-of timestamp)."
        )
    )
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--processed", default=str(PROC), help="Processed dir (default: data/processed)")
    ap.add_argument("--out", default=None, help="Optional JSON output path")
    ap.add_argument("--out-csv", default=None, help="Optional CSV output path (one row per date)")
    ap.add_argument(
        "--fail-on-missing-expected-minutes",
        action="store_true",
        help="Exit non-zero if expected-minutes columns are missing for any date with props_predictions present",
    )
    ap.add_argument(
        "--fail-on-missing-league-status",
        action="store_true",
        help="Exit non-zero if league_status_<date>.csv is missing for any date in range",
    )
    args = ap.parse_args()

    processed = Path(str(args.processed)).resolve()
    dates = _date_range(str(args.start), str(args.end))

    per_date: list[dict[str, Any]] = []
    missing_expected_minutes_dates: list[str] = []
    missing_league_status_dates: list[str] = []
    missing_pregame_expected_minutes_dates: list[str] = []

    props_ok_n = 0
    league_status_ok_n = 0
    injuries_counts_ok_n = 0
    roster_audit_ok_n = 0
    pem_ok_n = 0

    pem_team_cov_means: list[float] = []

    injury_status_nonempty_fracs: list[float] = []
    injury_status_dtd_fracs: list[float] = []

    minutes_cov_accum: dict[str, list[float]] = {"pred_min": [], "roll10_min": [], "roll5_min": [], "roll3_min": [], "lag1_min": []}

    for ds in dates:
        rep = audit_date(ds, processed)
        per_date.append(
            {
                "date": rep.date,
                "props_predictions_ok": rep.props_predictions_ok,
                "predictions_ok": rep.predictions_ok,
                "game_odds_ok": rep.game_odds_ok,
                "injuries_counts_ok": rep.injuries_counts_ok,
                "league_status_ok": rep.league_status_ok,
                "roster_audit_ok": rep.roster_audit_ok,
                "props_rows": rep.props_rows,
                "props_rows_focus": rep.props_rows_focus,
                "pregame_expected_minutes_ok": rep.pregame_expected_minutes_ok,
                "pregame_expected_minutes_rows": rep.pregame_expected_minutes_rows,
                "pregame_expected_minutes_cols": rep.pregame_expected_minutes_cols,
                "pregame_expected_minutes_team_cov_mean": rep.pregame_expected_minutes_team_cov_mean,
                "pregame_expected_minutes_asof_ts_max": rep.pregame_expected_minutes_asof_ts_max,
                "minutes_cols_present": rep.minutes_cols_present,
                "minutes_cols_coverage": rep.minutes_cols_coverage,
                "expected_minutes_cols_present": rep.expected_minutes_cols_present,
                "starter_cols_present": rep.starter_cols_present,
                "asof_ts_cols_present": rep.asof_ts_cols_present,
                "injury_status_nonempty_frac": rep.injury_status_nonempty_frac,
                "injury_status_dtd_frac": rep.injury_status_dtd_frac,
            }
        )

        if rep.props_predictions_ok:
            props_ok_n += 1
            if not rep.expected_minutes_cols_present:
                missing_expected_minutes_dates.append(rep.date)
            if not rep.pregame_expected_minutes_ok:
                missing_pregame_expected_minutes_dates.append(rep.date)
            if rep.injury_status_nonempty_frac is not None:
                injury_status_nonempty_fracs.append(float(rep.injury_status_nonempty_frac))
            if rep.injury_status_dtd_frac is not None:
                injury_status_dtd_fracs.append(float(rep.injury_status_dtd_frac))
            for k, v in (rep.minutes_cols_coverage or {}).items():
                if k in minutes_cov_accum:
                    minutes_cov_accum[k].append(float(v))

        if rep.pregame_expected_minutes_ok:
            pem_ok_n += 1
            if rep.pregame_expected_minutes_team_cov_mean is not None:
                pem_team_cov_means.append(float(rep.pregame_expected_minutes_team_cov_mean))

        if rep.league_status_ok:
            league_status_ok_n += 1
        else:
            missing_league_status_dates.append(rep.date)

        if rep.injuries_counts_ok:
            injuries_counts_ok_n += 1
        if rep.roster_audit_ok:
            roster_audit_ok_n += 1

    # Global artifacts (existence only)
    global_artifacts = {
        "rotation_stints_history_ok": _exists_nonempty(processed / "rotation_stints_history.csv"),
        "rotation_priors_first_bench_sub_in_ok": _exists_nonempty(processed / "rotation_priors_first_bench_sub_in.csv"),
        "rotations_espn_dir_exists": bool((processed / "rotations_espn").exists()),
        "schedule_ok": _exists_nonempty(processed / "schedule_2025_26.csv") or _exists_nonempty(processed / "schedule_2025_26.json"),
    }

    minutes_cov_mean = {k: _mean_or_none(v) for k, v in minutes_cov_accum.items() if v}

    summary = {
        "start": str(args.start),
        "end": str(args.end),
        "processed": str(processed),
        "days": len(dates),
        "props_predictions_days": props_ok_n,
        "league_status_days": league_status_ok_n,
        "injuries_counts_days": injuries_counts_ok_n,
        "roster_audit_days": roster_audit_ok_n,
        "pregame_expected_minutes_days": pem_ok_n,
        "missing_expected_minutes_dates": missing_expected_minutes_dates[:50],
        "missing_expected_minutes_days": len(missing_expected_minutes_dates),
        "missing_pregame_expected_minutes_dates": missing_pregame_expected_minutes_dates[:50],
        "missing_pregame_expected_minutes_days": len(missing_pregame_expected_minutes_dates),
        "pregame_expected_minutes_team_cov_mean": _mean_or_none(pem_team_cov_means),
        "missing_league_status_dates": missing_league_status_dates[:50],
        "missing_league_status_days": len(missing_league_status_dates),
        "injury_status_nonempty_frac_mean": _mean_or_none(injury_status_nonempty_fracs),
        "injury_status_dtd_frac_mean": _mean_or_none(injury_status_dtd_fracs),
        "minutes_cols_coverage_mean": minutes_cov_mean,
        "global_artifacts": global_artifacts,
        "ran_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    out_obj = {"summary": summary, "per_date": per_date}
    s = json.dumps(out_obj, ensure_ascii=False, indent=2)
    print(s)

    if args.out:
        Path(str(args.out)).write_text(s, encoding="utf-8")

    if args.out_csv:
        try:
            pd.DataFrame(per_date).to_csv(Path(str(args.out_csv)), index=False)
        except Exception:
            pass

    if args.fail_on_missing_expected_minutes and missing_expected_minutes_dates:
        return 2
    if args.fail_on_missing_league_status and missing_league_status_dates:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
