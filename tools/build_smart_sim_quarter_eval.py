"""Build smart_sim_quarter_eval_*.csv with period (quarters/halves) calibration columns.

This script joins:
- SmartSim outputs: data/processed/smart_sim_<date>_<HOME>_<AWAY>.json
- Actuals (preferred): data/processed/smart_sim_quarter_eval_*_pbp_*.csv
    (these are PBP/ESPN-derived quarter + half points)
- Actuals (fallback): data/raw/games_nba_api.csv (if it contains quarter/half points)

And produces a wide CSV under data/processed/ with, per period:
- *_home_pred, *_away_pred, *_total_pred, *_margin_pred
- *_home_act, *_away_act, *_total_act, *_margin_act
- *_over_p, *_over_y (when market_total exists)
- *_cover_p, *_cover_y (when market_home_spread exists)

This is designed as the data source for `nba_betting.cli calibrate-period-probs`.

Usage:
  python tools/build_smart_sim_quarter_eval.py --start 2026-01-01 --end 2026-01-24
  python tools/build_smart_sim_quarter_eval.py --end 2026-01-24 --days 45
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
PROC = REPO_ROOT / "data" / "processed"
RAW = REPO_ROOT / "data" / "raw"


def _pbp_actuals_files() -> list[Path]:
    """Find processed files that contain period actuals derived from PBP.

    We intentionally support multiple naming schemes so that daily automation can
    write per-day or rolling-range actuals files and this builder will aggregate
    them into a single actuals table.
    """
    pats = [
        # Preferred: actuals-only files built from ESPN PBP
        "smart_sim_quarter_eval_*_pbp_*actuals*.csv",
        # Historical/legacy: PBP-based eval exports that include *_home_act fields
        "smart_sim_quarter_eval_*_pbp_*.csv",
        "smart_sim_quarter_eval_*_pbp.csv",
        "smart_sim_quarter_eval_*_espn*.csv",
    ]
    cands: list[Path] = []
    for pat in pats:
        cands += list(PROC.glob(pat))
    # Unique + stable order
    uniq = {str(p): p for p in cands if p.exists()}
    files = list(uniq.values())
    try:
        files = sorted(files, key=lambda p: p.stat().st_mtime)
    except Exception:
        files = sorted(files)
    return files


def _num(x: Any) -> float | None:
    try:
        v = float(x)
        if np.isfinite(v):
            return float(v)
        return None
    except Exception:
        return None


def _clamp01(x: Any, default: float = 0.5) -> float:
    try:
        v = float(x)
        if not np.isfinite(v):
            return float(default)
        return float(max(0.0, min(1.0, v)))
    except Exception:
        return float(default)


def _logloss(p: float, y: float) -> float:
    p = float(max(1e-6, min(1.0 - 1e-6, float(p))))
    y = float(max(0.0, min(1.0, float(y))))
    return float(-(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))


def _period_keys() -> list[str]:
    return ["q1", "q2", "q3", "q4", "h1", "h2"]


def _derive_half_points(row: pd.Series) -> tuple[float | None, float | None, float | None, float | None]:
    """Return (home_h1, home_h2, away_h1, away_h2) with best-effort fallbacks."""
    # Prefer explicit columns if present
    hh1 = _num(row.get("home_h1"))
    hh2 = _num(row.get("home_h2"))
    ah1 = _num(row.get("visitor_h1"))
    ah2 = _num(row.get("visitor_h2"))

    # Alternate naming from processed PBP evals
    if hh1 is None:
        hh1 = _num(row.get("h1_home_act"))
    if hh2 is None:
        hh2 = _num(row.get("h2_home_act"))
    if ah1 is None:
        ah1 = _num(row.get("h1_away_act"))
    if ah2 is None:
        ah2 = _num(row.get("h2_away_act"))

    # Fallback from quarters
    hq1 = _num(row.get("home_q1")); hq2 = _num(row.get("home_q2"))
    hq3 = _num(row.get("home_q3")); hq4 = _num(row.get("home_q4"))
    aq1 = _num(row.get("visitor_q1")); aq2 = _num(row.get("visitor_q2"))
    aq3 = _num(row.get("visitor_q3")); aq4 = _num(row.get("visitor_q4"))

    if hq1 is None:
        hq1 = _num(row.get("q1_home_act"))
    if hq2 is None:
        hq2 = _num(row.get("q2_home_act"))
    if hq3 is None:
        hq3 = _num(row.get("q3_home_act"))
    if hq4 is None:
        hq4 = _num(row.get("q4_home_act"))
    if aq1 is None:
        aq1 = _num(row.get("q1_away_act"))
    if aq2 is None:
        aq2 = _num(row.get("q2_away_act"))
    if aq3 is None:
        aq3 = _num(row.get("q3_away_act"))
    if aq4 is None:
        aq4 = _num(row.get("q4_away_act"))

    if hh1 is None and (hq1 is not None or hq2 is not None):
        hh1 = float((hq1 or 0.0) + (hq2 or 0.0))
    if hh2 is None and (hq3 is not None or hq4 is not None):
        hh2 = float((hq3 or 0.0) + (hq4 or 0.0))
    if ah1 is None and (aq1 is not None or aq2 is not None):
        ah1 = float((aq1 or 0.0) + (aq2 or 0.0))
    if ah2 is None and (aq3 is not None or aq4 is not None):
        ah2 = float((aq3 or 0.0) + (aq4 or 0.0))

    return hh1, hh2, ah1, ah2


def _load_actuals_from_processed_pbp_eval(start: str, end: str) -> pd.DataFrame:
    files = _pbp_actuals_files()
    if not files:
        return pd.DataFrame()

    want = {
        "date",
        "home_tri",
        "away_tri",
        "game_id",
        # actuals-only naming
        "q1_home_act",
        "q1_away_act",
        "q2_home_act",
        "q2_away_act",
        "q3_home_act",
        "q3_away_act",
        "q4_home_act",
        "q4_away_act",
        "h1_home_act",
        "h1_away_act",
        "h2_home_act",
        "h2_away_act",
        # raw naming
        "home_q1",
        "home_q2",
        "home_q3",
        "home_q4",
        "visitor_q1",
        "visitor_q2",
        "visitor_q3",
        "visitor_q4",
        "home_h1",
        "home_h2",
        "visitor_h1",
        "visitor_h2",
    }

    parts: list[pd.DataFrame] = []
    for fp in files:
        try:
            df = pd.read_csv(fp, usecols=lambda c: c in want)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        if "date" not in df.columns or "home_tri" not in df.columns or "away_tri" not in df.columns:
            continue
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df[(df["date"].astype(str) >= start) & (df["date"].astype(str) <= end)].copy()
        if df.empty:
            continue
        parts.append(df)

    if not parts:
        return pd.DataFrame()

    df = pd.concat(parts, ignore_index=True)
    df["home_tri"] = df["home_tri"].astype(str).str.upper().str.strip()
    df["away_tri"] = df["away_tri"].astype(str).str.upper().str.strip()

    # Standardize naming to the raw loader conventions used by _period_act.
    # PBP actuals files store quarter/half points as q*_home_act/q*_away_act.
    ren: dict[str, str] = {}
    for qn in (1, 2, 3, 4):
        if f"q{qn}_home_act" in df.columns:
            ren[f"q{qn}_home_act"] = f"home_q{qn}"
        if f"q{qn}_away_act" in df.columns:
            ren[f"q{qn}_away_act"] = f"visitor_q{qn}"
    if "h1_home_act" in df.columns:
        ren["h1_home_act"] = "home_h1"
    if "h1_away_act" in df.columns:
        ren["h1_away_act"] = "visitor_h1"
    if "h2_home_act" in df.columns:
        ren["h2_home_act"] = "home_h2"
    if "h2_away_act" in df.columns:
        ren["h2_away_act"] = "visitor_h2"
    if ren:
        df = df.rename(columns=ren)

    for c in [
        "home_q1",
        "home_q2",
        "home_q3",
        "home_q4",
        "visitor_q1",
        "visitor_q2",
        "visitor_q3",
        "visitor_q4",
        "home_h1",
        "home_h2",
        "visitor_h1",
        "visitor_h2",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Deduplicate (later files win due to concat order)
    try:
        df = df.sort_values(["date", "home_tri", "away_tri"]).drop_duplicates(
            subset=["date", "home_tri", "away_tri"], keep="last"
        )
    except Exception:
        pass

    return df


def _load_actuals_from_raw_nba_api(start: str, end: str) -> pd.DataFrame:
    gp = RAW / "games_nba_api.csv"
    if not gp.exists():
        return pd.DataFrame()

    try:
        df = pd.read_csv(gp)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df[(df["date"].astype(str) >= start) & (df["date"].astype(str) <= end)].copy()
    if df.empty:
        return df

    # Map to tricodes
    try:
        from nba_betting.teams import normalize_team, to_tricode

        df["home_tri"] = df["home_team"].astype(str).map(normalize_team).map(to_tricode)
        df["away_tri"] = df["visitor_team"].astype(str).map(normalize_team).map(to_tricode)
    except Exception:
        # Best effort fallback: if home_team already a tri
        df["home_tri"] = df["home_team"].astype(str).str.upper().str.strip()
        df["away_tri"] = df["visitor_team"].astype(str).str.upper().str.strip()

    # Coerce numeric quarter columns
    for c in [
        "home_q1", "home_q2", "home_q3", "home_q4",
        "visitor_q1", "visitor_q2", "visitor_q3", "visitor_q4",
        "home_h1", "home_h2", "visitor_h1", "visitor_h2",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date", "home_tri", "away_tri"], how="any")
    df["home_tri"] = df["home_tri"].astype(str).str.upper().str.strip()
    df["away_tri"] = df["away_tri"].astype(str).str.upper().str.strip()

    return df


def _load_actuals(start: str, end: str) -> pd.DataFrame:
    # Prefer processed PBP/ESPN-derived actuals when present (includes 2026 windows).
    df = _load_actuals_from_processed_pbp_eval(start, end)
    if df is not None and not df.empty:
        return df
    # Fallback to raw nba_api games file (if it includes period scoring).
    return _load_actuals_from_raw_nba_api(start, end)


def _find_sim_file(date_str: str, home_tri: str, away_tri: str) -> Path | None:
    # Preferred filename convention
    p = PROC / f"smart_sim_{date_str}_{home_tri}_{away_tri}.json"
    if p.exists():
        return p

    # Fallback search (rare mismatches)
    cands = list(PROC.glob(f"smart_sim_{date_str}_*.json"))
    for fp in cands:
        try:
            obj = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        h = str(obj.get("home") or "").upper().strip()
        a = str(obj.get("away") or "").upper().strip()
        if h == home_tri and a == away_tri:
            return fp
    return None


def _period_act(row: pd.Series, period: str) -> tuple[float | None, float | None]:
    """Return (home_pts, away_pts) for the period."""
    if period.startswith("q"):
        qn = int(period[1:])
        return _num(row.get(f"home_q{qn}")), _num(row.get(f"visitor_q{qn}"))
    if period == "h1":
        hh1, _, ah1, _ = _derive_half_points(row)
        return hh1, ah1
    if period == "h2":
        _, hh2, _, ah2 = _derive_half_points(row)
        return hh2, ah2
    return None, None


def build_eval(start: str, end: str) -> pd.DataFrame:
    actuals = _load_actuals(start, end)
    if actuals is None or actuals.empty:
        return pd.DataFrame()

    # Index actuals by (date, home_tri, away_tri)
    idx: dict[tuple[str, str, str], pd.Series] = {}
    for _, r in actuals.iterrows():
        k = (str(r.get("date")), str(r.get("home_tri")), str(r.get("away_tri")))
        if k not in idx:
            idx[k] = r

    rows: list[dict[str, Any]] = []

    for ds in pd.date_range(pd.to_datetime(start), pd.to_datetime(end), freq="D").strftime("%Y-%m-%d"):
        sim_files = sorted(PROC.glob(f"smart_sim_{ds}_*.json"))
        if not sim_files:
            continue

        for fp in sim_files:
            try:
                obj = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:
                continue

            home = str(obj.get("home") or "").upper().strip()
            away = str(obj.get("away") or "").upper().strip()
            if not home or not away:
                continue

            # Actual row
            arow = idx.get((ds, home, away))

            rec: dict[str, Any] = {
                "date": ds,
                "home_tri": home,
                "away_tri": away,
                "game_id": _num(obj.get("game_id")),
                "use_pbp": bool((obj.get("mode") or {}).get("use_pbp", obj.get("use_pbp", False))),
            }

            periods = obj.get("periods") if isinstance(obj.get("periods"), dict) else {}

            for p in _period_keys():
                pobj = periods.get(p) if isinstance(periods, dict) else None
                if not isinstance(pobj, dict):
                    pobj = {}

                # Pred
                rec[f"{p}_home_pred"] = _num(pobj.get("home_mean"))
                rec[f"{p}_away_pred"] = _num(pobj.get("away_mean"))
                rec[f"{p}_total_pred"] = _num(pobj.get("total_mean"))
                rec[f"{p}_margin_pred"] = _num(pobj.get("margin_mean"))

                # Actual
                h_act = a_act = None
                if arow is not None:
                    h_act, a_act = _period_act(arow, p)
                rec[f"{p}_home_act"] = h_act
                rec[f"{p}_away_act"] = a_act
                rec[f"{p}_total_act"] = (h_act + a_act) if (h_act is not None and a_act is not None) else None
                rec[f"{p}_margin_act"] = (h_act - a_act) if (h_act is not None and a_act is not None) else None

                # Errors (abs)
                def _abs_err(a: float | None, b: float | None) -> float | None:
                    if a is None or b is None:
                        return None
                    return float(abs(float(a) - float(b)))

                rec[f"{p}_home_abs_err"] = _abs_err(rec[f"{p}_home_act"], rec[f"{p}_home_pred"])
                rec[f"{p}_away_abs_err"] = _abs_err(rec[f"{p}_away_act"], rec[f"{p}_away_pred"])
                rec[f"{p}_total_abs_err"] = _abs_err(rec[f"{p}_total_act"], rec[f"{p}_total_pred"])
                rec[f"{p}_margin_abs_err"] = _abs_err(rec[f"{p}_margin_act"], rec[f"{p}_margin_pred"])

                # Prob markets (raw preferred)
                n_sims = int(obj.get("n_sims") or 0) if obj.get("n_sims") is not None else 0

                # Totals over
                total_line = _num(pobj.get("market_total"))
                p_over = _num(pobj.get("p_total_over_raw"))
                if p_over is None:
                    p_over = _num(pobj.get("p_total_over"))

                rec[f"{p}_over_n"] = int(n_sims) if total_line is not None else 0
                rec[f"{p}_over_p"] = _clamp01(p_over, default=np.nan) if (p_over is not None and total_line is not None) else None

                y_over = None
                if total_line is not None and rec.get(f"{p}_total_act") is not None:
                    try:
                        delta = float(rec[f"{p}_total_act"]) - float(total_line)
                        if abs(delta) < 1e-9:
                            y_over = None
                        else:
                            y_over = 1.0 if delta > 0 else 0.0
                    except Exception:
                        y_over = None
                rec[f"{p}_over_y"] = y_over

                if rec.get(f"{p}_over_p") is not None and y_over is not None:
                    pp = float(rec[f"{p}_over_p"])
                    yy = float(y_over)
                    rec[f"{p}_over_brier"] = float((pp - yy) ** 2)
                    rec[f"{p}_over_logloss"] = _logloss(pp, yy)
                else:
                    rec[f"{p}_over_brier"] = None
                    rec[f"{p}_over_logloss"] = None

                # Spread/cover
                spread_line = _num(pobj.get("market_home_spread"))
                p_cover = _num(pobj.get("p_home_cover_raw"))
                if p_cover is None:
                    p_cover = _num(pobj.get("p_home_cover"))

                rec[f"{p}_cover_n"] = int(n_sims) if spread_line is not None else 0
                rec[f"{p}_cover_p"] = _clamp01(p_cover, default=np.nan) if (p_cover is not None and spread_line is not None) else None

                y_cover = None
                if spread_line is not None and rec.get(f"{p}_margin_act") is not None:
                    try:
                        # home covers if margin + home_spread > 0
                        delta = float(rec[f"{p}_margin_act"]) + float(spread_line)
                        if abs(delta) < 1e-9:
                            y_cover = None
                        else:
                            y_cover = 1.0 if delta > 0 else 0.0
                    except Exception:
                        y_cover = None
                rec[f"{p}_cover_y"] = y_cover

                if rec.get(f"{p}_cover_p") is not None and y_cover is not None:
                    pp = float(rec[f"{p}_cover_p"])
                    yy = float(y_cover)
                    rec[f"{p}_cover_brier"] = float((pp - yy) ** 2)
                    rec[f"{p}_cover_logloss"] = _logloss(pp, yy)
                else:
                    rec[f"{p}_cover_brier"] = None
                    rec[f"{p}_cover_logloss"] = None

            rows.append(rec)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Normalize columns ordering: meta first, then periods.
    meta = ["date", "home_tri", "away_tri", "game_id", "use_pbp"]
    cols = [c for c in meta if c in df.columns]
    for p in _period_keys():
        cols += [
            f"{p}_home_pred", f"{p}_away_pred", f"{p}_total_pred", f"{p}_margin_pred",
            f"{p}_home_act", f"{p}_away_act", f"{p}_total_act", f"{p}_margin_act",
            f"{p}_home_abs_err", f"{p}_away_abs_err", f"{p}_total_abs_err", f"{p}_margin_abs_err",
            f"{p}_over_n", f"{p}_cover_n",
            f"{p}_over_y", f"{p}_over_p", f"{p}_over_brier", f"{p}_over_logloss",
            f"{p}_cover_y", f"{p}_cover_p", f"{p}_cover_brier", f"{p}_cover_logloss",
        ]
    cols = [c for c in cols if c in df.columns]
    df = df.reindex(columns=cols)

    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, required=False, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, required=True, help="End date YYYY-MM-DD")
    ap.add_argument("--days", type=int, required=False, help="If --start not provided, use (end - days + 1)")
    ap.add_argument("--out", type=str, required=False, help="Output path (default under data/processed)")
    args = ap.parse_args()

    end = pd.to_datetime(args.end).strftime("%Y-%m-%d")
    if args.start:
        start = pd.to_datetime(args.start).strftime("%Y-%m-%d")
    else:
        days = int(args.days or 45)
        start = (pd.to_datetime(end) - pd.Timedelta(days=days - 1)).strftime("%Y-%m-%d")

    out_path = Path(args.out) if args.out else (PROC / f"smart_sim_quarter_eval_{start}_{end}.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = build_eval(start=start, end=end)
    if df is None or df.empty:
        print({"ok": False, "rows": 0, "start": start, "end": end, "out": str(out_path)})
        # Still write an empty file for determinism
        pd.DataFrame([]).to_csv(out_path, index=False)
        return 0

    df.to_csv(out_path, index=False)
    print({"ok": True, "rows": int(len(df)), "start": start, "end": end, "out": str(out_path)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
