import argparse
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
import json
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"

# Ensure local package imports work when running as a script.
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from nba_betting.teams import to_tricode  # noqa: E402
from nba_betting.sim.quarters import TeamContext, GameInputs, simulate_quarters_analytic  # noqa: E402


def _num(x):
    try:
        v = pd.to_numeric(x, errors="coerce")
        return float(v) if np.isfinite(v) else None
    except Exception:
        return None


def _logloss(p: np.ndarray, y: np.ndarray) -> float:
    p = np.asarray(p, dtype=float)
    y = np.asarray(y, dtype=float)
    m = np.isfinite(p) & np.isfinite(y)
    if int(m.sum()) == 0:
        return float("nan")
    pp = np.clip(p[m], 1e-6, 1.0 - 1e-6)
    yy = y[m]
    return float(np.mean(-(yy * np.log(pp) + (1.0 - yy) * np.log(1.0 - pp))))


def _iter_dates(start: datetime, end: datetime):
    n = int((end.date() - start.date()).days)
    for i in range(n + 1):
        yield (start + timedelta(days=i)).date().isoformat()


def _load_day(ds: str) -> pd.DataFrame | None:
    pred_p = PROCESSED / f"predictions_{ds}.csv"
    odds_p = PROCESSED / f"game_odds_{ds}.csv"
    rec_p = PROCESSED / f"recon_games_{ds}.csv"
    if not (pred_p.exists() and odds_p.exists() and rec_p.exists()):
        return None
    try:
        pred = pd.read_csv(pred_p)
        odds = pd.read_csv(odds_p)
        rec = pd.read_csv(rec_p)
    except Exception:
        return None

    # Tricodes for robust joining.
    def _tri(x):
        try:
            return to_tricode(str(x)).strip().upper()
        except Exception:
            return str(x).strip().upper()

    for df, h, a in ((pred, "home_team", "visitor_team"), (odds, "home_team", "visitor_team")):
        if h in df.columns:
            df["home_tri"] = df[h].astype(str).map(_tri)
        if a in df.columns:
            df["away_tri"] = df[a].astype(str).map(_tri)

    if "home_tri" not in rec.columns and "home_team" in rec.columns:
        rec["home_tri"] = rec["home_team"].astype(str).map(_tri)
    if "away_tri" not in rec.columns and "visitor_team" in rec.columns:
        rec["away_tri"] = rec["visitor_team"].astype(str).map(_tri)
    if "home_tri" in rec.columns:
        rec["home_tri"] = rec["home_tri"].astype(str).str.upper().str.strip()
    if "away_tri" in rec.columns:
        rec["away_tri"] = rec["away_tri"].astype(str).str.upper().str.strip()

    # Normalize expected columns for predictions.
    pred = pred.copy()
    if "totals" not in pred.columns and "pred_total" in pred.columns:
        pred["totals"] = pred["pred_total"]
    if "spread_margin" not in pred.columns and "pred_margin" in pred.columns:
        pred["spread_margin"] = pred["pred_margin"]

    need_pred_cols = {"home_tri", "away_tri", "totals", "spread_margin"}
    need_odds_cols = {"home_tri", "away_tri", "total", "home_spread"}
    need_rec_cols = {"home_tri", "away_tri", "home_pts", "visitor_pts"}
    if not need_pred_cols.issubset(set(pred.columns)):
        return None
    if not need_odds_cols.issubset(set(odds.columns)):
        return None
    if not need_rec_cols.issubset(set(rec.columns)):
        return None

    m = pred.merge(odds, on=["home_tri", "away_tri"], how="inner", suffixes=("_pred", "_odds"))
    m = m.merge(rec, on=["home_tri", "away_tri"], how="inner", suffixes=("", "_rec"))
    if m.empty:
        return None

    m["date"] = ds
    return m


def collect_training(days: int, end_date: datetime) -> pd.DataFrame:
    start = end_date - timedelta(days=days)
    rows = []
    for ds in _iter_dates(start, end_date):
        m = _load_day(ds)
        if m is None or m.empty:
            continue
        rows.append(m)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def evaluate_grid(df: pd.DataFrame, step: float) -> pd.DataFrame:
    def _col_or_default(col: str, default: float) -> pd.Series:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(default)
        # Create a per-row default series (so downstream ops are vectorized).
        return pd.Series([default] * len(df), index=df.index, dtype=float)

    # Precompute observed outcomes.
    home_pts = pd.to_numeric(df["home_pts"], errors="coerce")
    away_pts = pd.to_numeric(df["visitor_pts"], errors="coerce")
    actual_margin = (home_pts - away_pts).to_numpy(dtype=float)
    actual_total = (home_pts + away_pts).to_numpy(dtype=float)

    total_line = pd.to_numeric(df["total"], errors="coerce").to_numpy(dtype=float)
    home_spread = pd.to_numeric(df["home_spread"], errors="coerce").to_numpy(dtype=float)

    y_ml = (actual_margin > 0).astype(float)
    y_cover = (actual_margin + home_spread > 0).astype(float)
    y_over = (actual_total > total_line).astype(float)

    # Pull model baselines needed to build TeamContexts.
    pred_total = pd.to_numeric(df["totals"], errors="coerce").to_numpy(dtype=float)
    pred_margin = pd.to_numeric(df["spread_margin"], errors="coerce").to_numpy(dtype=float)

    home_pace = _col_or_default("home_pace", 98.0).to_numpy(dtype=float)
    away_pace = _col_or_default("away_pace", 98.0).to_numpy(dtype=float)

    # Ratings fields are optional.
    home_def = _col_or_default("home_def_rating", 112.0).to_numpy(dtype=float)
    away_def = _col_or_default("away_def_rating", 112.0).to_numpy(dtype=float)

    # Implied team means from model totals/margins.
    home_mu_model = 0.5 * (pred_total + pred_margin)
    away_mu_model = 0.5 * (pred_total - pred_margin)

    # Convert means + pace -> off_ratings.
    home_off = (home_mu_model / np.maximum(1e-6, home_pace)) * 100.0
    away_off = (away_mu_model / np.maximum(1e-6, away_pace)) * 100.0

    # Team names are not used for math; keep tricodes.
    home_team = df["home_tri"].astype(str).to_numpy()
    away_team = df["away_tri"].astype(str).to_numpy()

    total_ws = np.round(np.arange(0.0, 1.0 + 1e-9, step), 4)
    margin_ws = np.round(np.arange(0.0, 1.0 + 1e-9, step), 4)

    out_rows = []
    n = int(len(df))

    # Cache per-row base TeamContexts so we only vary weights.
    base_ctx = []
    for i in range(n):
        base_ctx.append(
            (
                TeamContext(team=str(home_team[i]), pace=float(home_pace[i]), off_rating=float(home_off[i]), def_rating=float(home_def[i])),
                TeamContext(team=str(away_team[i]), pace=float(away_pace[i]), off_rating=float(away_off[i]), def_rating=float(away_def[i])),
            )
        )

    for tw in total_ws:
        for mw in margin_ws:
            p_ml = np.full(n, np.nan, dtype=float)
            p_cover = np.full(n, np.nan, dtype=float)
            p_over = np.full(n, np.nan, dtype=float)

            for i in range(n):
                # Require finite market lines.
                if not np.isfinite(total_line[i]) or not np.isfinite(home_spread[i]):
                    continue
                hc, ac = base_ctx[i]
                inp = GameInputs(
                    date=str(df.iloc[i].get("date") or ""),
                    home=hc,
                    away=ac,
                    market_total=float(total_line[i]),
                    market_home_spread=float(home_spread[i]),
                    blend_total_market_w=float(tw),
                    blend_margin_market_w=float(mw),
                )
                summ = simulate_quarters_analytic(inp)
                pr = summ.probs or {}
                p_ml[i] = float(pr.get("p_home_ml", np.nan))
                p_cover[i] = float(pr.get("p_home_cover", np.nan))
                p_over[i] = float(pr.get("p_total_over", np.nan))

            ll_ml = _logloss(p_ml, y_ml)
            ll_ats = _logloss(p_cover, y_cover)
            ll_tot = _logloss(p_over, y_over)
            # Combined: average of available markets.
            parts = [x for x in [ll_ml, ll_ats, ll_tot] if np.isfinite(x)]
            ll_all = float(np.mean(parts)) if parts else float("nan")
            out_rows.append(
                {
                    "total_w": float(tw),
                    "margin_w": float(mw),
                    "n": int(np.isfinite(y_ml).sum()),
                    "logloss_ml": ll_ml,
                    "logloss_ats": ll_ats,
                    "logloss_total": ll_tot,
                    "logloss_all": ll_all,
                }
            )

    return pd.DataFrame(out_rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="Backtest and tune market blend weights for quarter sim")
    ap.add_argument("--train-days", type=int, default=60, help="Days before end to train")
    ap.add_argument("--end", type=str, help="End date YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--step", type=float, default=0.05, help="Grid step for weights")
    ap.add_argument("--write-default", action="store_true", help="Write best weights to data/processed/quarters_blend_weights.json")
    args = ap.parse_args()

    end = datetime.strptime(args.end, "%Y-%m-%d") if args.end else (datetime.today() - timedelta(days=1))
    train = collect_training(int(args.train_days), end)
    if train is None or train.empty:
        print("NO_TRAIN_DATA")
        return 0

    grid = evaluate_grid(train, float(args.step))
    if grid.empty:
        print("NO_GRID")
        return 0

    # Pick best combined weights.
    best = grid.sort_values(["logloss_all"], ascending=True).iloc[0].to_dict()
    meta = {
        "total_w": float(best["total_w"]),
        "margin_w": float(best["margin_w"]),
        "train_days": int(args.train_days),
        "end": end.date().isoformat(),
        "rows": int(len(train)),
        "step": float(args.step),
        "best_logloss_all": float(best["logloss_all"]),
        "best_logloss_ml": float(best.get("logloss_ml")) if best.get("logloss_ml") is not None else None,
        "best_logloss_ats": float(best.get("logloss_ats")) if best.get("logloss_ats") is not None else None,
        "best_logloss_total": float(best.get("logloss_total")) if best.get("logloss_total") is not None else None,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }

    out_csv = PROCESSED / "quarters_blend_grid.csv"
    out_json = PROCESSED / "quarters_blend_tuning.json"
    grid.to_csv(out_csv, index=False)
    out_json.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    if bool(args.write_default):
        (PROCESSED / "quarters_blend_weights.json").write_text(json.dumps({"total_w": meta["total_w"], "margin_w": meta["margin_w"], "trained": meta}, indent=2), encoding="utf-8")
        print(f"OK best total_w={meta['total_w']:.3f} margin_w={meta['margin_w']:.3f} (WROTE quarters_blend_weights.json)")
    else:
        print(f"OK best total_w={meta['total_w']:.3f} margin_w={meta['margin_w']:.3f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
