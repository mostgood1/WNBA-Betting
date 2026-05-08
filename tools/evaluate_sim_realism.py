from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from nba_betting.config import paths
from nba_betting.league import LEAGUE
from nba_betting.sim.quarters import GameInputs, TeamContext, sample_quarter_scores, simulate_quarters


def _to_float(x: Any) -> float | None:
    try:
        v = float(x)
        return v if np.isfinite(v) else None
    except Exception:
        return None


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: date, end: date) -> list[date]:
    out: list[date] = []
    d = start
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


def _quantiles(x: np.ndarray, qs: tuple[float, ...] = (0.1, 0.25, 0.5, 0.75, 0.9)) -> dict[str, float]:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return {f"q{int(q*100):02d}": float("nan") for q in qs}
    vals = np.quantile(x, qs)
    return {f"q{int(q*100):02d}": float(v) for q, v in zip(qs, vals)}


def _safe_mean(x: pd.Series) -> float:
    try:
        v = pd.to_numeric(x, errors="coerce")
        return float(v.mean())
    except Exception:
        return float("nan")


def _find_latest_recon_date(processed_dir: Path) -> date | None:
    files = sorted(processed_dir.glob("recon_games_*.csv"))
    if not files:
        return None
    # names are YYYY-MM-DD; sort lexicographically works
    last = files[-1].stem.replace("recon_games_", "")
    try:
        return _parse_date(last)
    except Exception:
        return None


def _load_predictions(processed_dir: Path, d: date) -> pd.DataFrame | None:
    fp = processed_dir / f"predictions_{d.isoformat()}.csv"
    if not fp.exists():
        return None
    try:
        df = pd.read_csv(fp)
        return df if isinstance(df, pd.DataFrame) and not df.empty else None
    except Exception:
        return None


def _load_recon(processed_dir: Path, d: date) -> pd.DataFrame | None:
    fp = processed_dir / f"recon_games_{d.isoformat()}.csv"
    if not fp.exists():
        return None
    try:
        df = pd.read_csv(fp)
        return df if isinstance(df, pd.DataFrame) and not df.empty else None
    except Exception:
        return None


def _load_game_odds(processed_dir: Path, d: date) -> pd.DataFrame | None:
    fp = processed_dir / f"game_odds_{d.isoformat()}.csv"
    if not fp.exists():
        return None
    try:
        df = pd.read_csv(fp)
        return df if isinstance(df, pd.DataFrame) and not df.empty else None
    except Exception:
        return None


def _build_context_from_row(row: pd.Series) -> tuple[TeamContext, TeamContext, float | None, float | None]:
    home = str(row.get("home_team") or "").strip()
    away = str(row.get("visitor_team") or "").strip()

    pred_total = (
        _to_float(row.get("pred_total"))
        or _to_float(row.get("totals"))
        or _to_float(row.get("total_pred"))
    )
    pred_margin = (
        _to_float(row.get("pred_margin"))
        or _to_float(row.get("spread_margin"))
        or _to_float(row.get("margin_pred"))
    )

    home_pace = _to_float(row.get("home_pace")) or float(LEAGUE.baseline_pace)
    away_pace = _to_float(row.get("away_pace")) or float(LEAGUE.baseline_pace)

    home_mu_implied = None
    away_mu_implied = None
    if pred_total is not None and pred_margin is not None:
        home_mu_implied = 0.5 * (pred_total + pred_margin)
        away_mu_implied = 0.5 * (pred_total - pred_margin)

    def _rating_from_mu(mu: float | None, pace: float) -> float:
        if mu is None:
            return float(LEAGUE.baseline_off_rating)
        try:
            return float((mu / max(1e-6, pace)) * 100.0)
        except Exception:
            return float(LEAGUE.baseline_off_rating)

    home_off = _to_float(row.get("home_off_rating")) or _rating_from_mu(home_mu_implied, home_pace)
    away_off = _to_float(row.get("away_off_rating")) or _rating_from_mu(away_mu_implied, away_pace)
    home_def = _to_float(row.get("home_def_rating")) or float(LEAGUE.baseline_def_rating)
    away_def = _to_float(row.get("away_def_rating")) or float(LEAGUE.baseline_def_rating)

    home_ctx = TeamContext(team=home, pace=float(home_pace), off_rating=float(home_off), def_rating=float(home_def), injuries_out=0, back_to_back=False)
    away_ctx = TeamContext(team=away, pace=float(away_pace), off_rating=float(away_off), def_rating=float(away_def), injuries_out=0, back_to_back=False)

    market_total = _to_float(row.get("total"))
    market_home_spread = _to_float(row.get("home_spread"))

    return home_ctx, away_ctx, market_total, market_home_spread


def main() -> int:
    ap = argparse.ArgumentParser(description="Backtest sim realism vs actual results")
    ap.add_argument("--start", type=str, default=None, help="Start date (YYYY-MM-DD)")
    ap.add_argument("--end", type=str, default=None, help="End date (YYYY-MM-DD). Default: latest recon date")
    ap.add_argument("--days", type=int, default=30, help="If start not provided, evaluate last N days ending at --end")
    ap.add_argument("--n-samples", type=int, default=2000, help="Quarter sim samples per game")
    ap.add_argument("--seed", type=int, default=1, help="RNG seed base")
    ap.add_argument("--out-csv", type=str, default=None, help="Output CSV path. Default: data/processed/sim_realism_games_<start>_<end>.csv")
    ap.add_argument("--out-json", type=str, default=None, help="Output JSON path. Default: data/processed/sim_realism_summary_<start>_<end>.json")
    args = ap.parse_args()

    processed_dir = paths.data_processed
    latest = _find_latest_recon_date(processed_dir)
    if latest is None:
        raise SystemExit("No recon_games_*.csv found in data/processed")

    end_d = _parse_date(args.end) if args.end else latest
    start_d = _parse_date(args.start) if args.start else (end_d - timedelta(days=int(args.days) - 1))
    if start_d > end_d:
        start_d, end_d = end_d, start_d

    rows: list[dict[str, Any]] = []
    missing: list[str] = []

    for d in _daterange(start_d, end_d):
        recon = _load_recon(processed_dir, d)
        preds = _load_predictions(processed_dir, d)
        if recon is None or preds is None:
            missing.append(d.isoformat())
            continue

        # Fill market lines from game_odds_<date>.csv when predictions is missing them.
        odds = _load_game_odds(processed_dir, d)
        if odds is not None and not odds.empty:
            try:
                odds = odds.copy()
                odds["home_team"] = odds.get("home_team").astype(str).str.strip()
                odds["visitor_team"] = odds.get("visitor_team").astype(str).str.strip()
                keep = [c for c in ["date", "home_team", "visitor_team", "home_spread", "total"] if c in odds.columns]
                odds = odds[keep].copy() if keep else odds.iloc[0:0]
                if not odds.empty:
                    preds = preds.copy()
                    preds["home_team"] = preds.get("home_team").astype(str).str.strip()
                    preds["visitor_team"] = preds.get("visitor_team").astype(str).str.strip()
                    preds = preds.merge(odds, on=["date", "home_team", "visitor_team"], how="left", suffixes=("", "_odds"))
                    for col in ("home_spread", "total"):
                        if col in preds.columns and f"{col}_odds" in preds.columns:
                            a = pd.to_numeric(preds[col], errors="coerce")
                            b = pd.to_numeric(preds[f"{col}_odds"], errors="coerce")
                            preds[col] = a.where(a.notna(), b)
                    preds = preds.drop(columns=[c for c in preds.columns if c.endswith("_odds")])
            except Exception:
                pass

        # Standardize join keys
        for df in (recon, preds):
            df["home_team"] = df.get("home_team").astype(str).str.strip()
            df["visitor_team"] = df.get("visitor_team").astype(str).str.strip()

        m = recon.merge(preds, on=["date", "home_team", "visitor_team"], how="inner", suffixes=("_recon", ""))
        if m.empty:
            continue

        for gi, r in m.iterrows():
            home_pts = _to_float(r.get("home_pts"))
            away_pts = _to_float(r.get("visitor_pts"))
            if home_pts is None or away_pts is None:
                continue
            actual_total = float(home_pts + away_pts)
            actual_margin = float(home_pts - away_pts)
            actual_home_win = 1.0 if actual_margin > 0 else 0.0

            home_ctx, away_ctx, market_total, market_home_spread = _build_context_from_row(r)
            inp = GameInputs(date=d.isoformat(), home=home_ctx, away=away_ctx, market_total=market_total, market_home_spread=market_home_spread)

            try:
                summary = simulate_quarters(inp, n_samples=int(args.n_samples))
                rng = np.random.default_rng(int(args.seed) + int(d.strftime("%Y%m%d")) + int(gi))
                hq, aq = sample_quarter_scores(summary.quarters, n_samples=int(args.n_samples), rng=rng, round_to_int=True)
                home_final = hq.sum(axis=1)
                away_final = aq.sum(axis=1)
                total = home_final + away_final
                margin = home_final - away_final
            except Exception as e:
                rows.append(
                    {
                        "date": d.isoformat(),
                        "home_team": home_ctx.team,
                        "visitor_team": away_ctx.team,
                        "error": str(e),
                    }
                )
                continue

            q_total = _quantiles(total)
            q_margin = _quantiles(margin)
            p_home_win = float(np.mean(margin > 0))

            sim_total_mu = float(np.mean(total))
            sim_margin_mu = float(np.mean(margin))

            sd_total = float(np.std(total, ddof=0))
            sd_margin = float(np.std(margin, ddof=0))
            z_total = (sim_total_mu - actual_total) / max(1e-6, sd_total)
            z_margin = (sim_margin_mu - actual_margin) / max(1e-6, sd_margin)

            # Coverage
            cover_80_total = float(q_total["q10"] <= actual_total <= q_total["q90"]) if np.isfinite(q_total["q10"]) else float("nan")
            cover_50_total = float(q_total["q25"] <= actual_total <= q_total["q75"]) if np.isfinite(q_total["q25"]) else float("nan")
            cover_80_margin = float(q_margin["q10"] <= actual_margin <= q_margin["q90"]) if np.isfinite(q_margin["q10"]) else float("nan")
            cover_50_margin = float(q_margin["q25"] <= actual_margin <= q_margin["q75"]) if np.isfinite(q_margin["q25"]) else float("nan")

            # Errors
            total_mae = abs(sim_total_mu - actual_total)
            margin_mae = abs(sim_margin_mu - actual_margin)
            brier = (p_home_win - actual_home_win) ** 2

            # Baselines: market total and spread
            market_margin = None
            market_total_abs_err = None
            market_margin_abs_err = None
            sim_vs_market_total_abs_err = None
            sim_vs_market_margin_abs_err = None
            if market_total is not None:
                market_total_abs_err = abs(float(market_total) - actual_total)
                sim_vs_market_total_abs_err = abs(sim_total_mu - float(market_total))
            if market_home_spread is not None:
                market_margin = float(-market_home_spread)
                market_margin_abs_err = abs(market_margin - actual_margin)
                sim_vs_market_margin_abs_err = abs(sim_margin_mu - market_margin)

            rows.append(
                {
                    "date": d.isoformat(),
                    "home_team": home_ctx.team,
                    "visitor_team": away_ctx.team,
                    "home_pts": float(home_pts),
                    "visitor_pts": float(away_pts),
                    "actual_total": actual_total,
                    "actual_margin": actual_margin,
                    "market_total": market_total,
                    "market_home_spread": market_home_spread,
                    "market_margin": market_margin,
                    "sim_total_mu": sim_total_mu,
                    "sim_margin_mu": sim_margin_mu,
                    "p_home_win": p_home_win,
                    "total_mae": total_mae,
                    "margin_mae": margin_mae,
                    "brier": brier,
                    "sd_total": sd_total,
                    "sd_margin": sd_margin,
                    "z_total": float(z_total),
                    "z_margin": float(z_margin),
                    "cover80_total": cover_80_total,
                    "cover50_total": cover_50_total,
                    "cover80_margin": cover_80_margin,
                    "cover50_margin": cover_50_margin,
                    "iqr_total": float(q_total["q75"] - q_total["q25"]),
                    "iqr_margin": float(q_margin["q75"] - q_margin["q25"]),
                    **{f"total_{k}": v for k, v in q_total.items()},
                    **{f"margin_{k}": v for k, v in q_margin.items()},
                    "market_total_abs_err": market_total_abs_err,
                    "market_margin_abs_err": market_margin_abs_err,
                    "sim_vs_market_total_abs_err": sim_vs_market_total_abs_err,
                    "sim_vs_market_margin_abs_err": sim_vs_market_margin_abs_err,
                }
            )

    out_df = pd.DataFrame(rows)
    if out_df.empty:
        raise SystemExit("No rows evaluated (missing files or no merges)")

    # Aggregate summary
    def _mean(df: pd.DataFrame, col: str) -> float:
        if col not in df.columns:
            return float("nan")
        return _safe_mean(df[col])

    df_ok = out_df[out_df.get("error").isna()] if "error" in out_df.columns else out_df
    df_mkt_total = df_ok[df_ok.get("market_total").notna()] if "market_total" in df_ok.columns else df_ok.iloc[0:0]
    df_mkt_spread = df_ok[df_ok.get("market_home_spread").notna()] if "market_home_spread" in df_ok.columns else df_ok.iloc[0:0]

    summary = {
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "days_requested": int(args.days),
        "n_samples": int(args.n_samples),
        "rows": int(len(out_df)),
        "rows_with_error": int(pd.to_numeric(out_df.get("error").notna(), errors="coerce").sum()) if "error" in out_df.columns else 0,
        "missing_dates": missing,
        "rows_ok": int(len(df_ok)),
        "rows_with_market_total": int(len(df_mkt_total)),
        "rows_with_market_spread": int(len(df_mkt_spread)),
        "metrics": {
            "total_mae": _mean(df_ok, "total_mae"),
            "margin_mae": _mean(df_ok, "margin_mae"),
            "brier": _mean(df_ok, "brier"),
            "cover80_total": _mean(df_ok, "cover80_total"),
            "cover50_total": _mean(df_ok, "cover50_total"),
            "cover80_margin": _mean(df_ok, "cover80_margin"),
            "cover50_margin": _mean(df_ok, "cover50_margin"),
            "iqr_total": _mean(df_ok, "iqr_total"),
            "iqr_margin": _mean(df_ok, "iqr_margin"),
            "mean_abs_z_total": float(pd.to_numeric(df_ok.get("z_total"), errors="coerce").abs().mean()),
            "mean_abs_z_margin": float(pd.to_numeric(df_ok.get("z_margin"), errors="coerce").abs().mean()),

            "market_total_abs_err": _mean(df_mkt_total, "market_total_abs_err"),
            "sim_total_mae_on_market_total_games": _mean(df_mkt_total, "total_mae"),
            "sim_vs_market_total_abs_err": _mean(df_mkt_total, "sim_vs_market_total_abs_err"),

            "market_margin_abs_err": _mean(df_mkt_spread, "market_margin_abs_err"),
            "sim_margin_mae_on_market_spread_games": _mean(df_mkt_spread, "margin_mae"),
            "sim_vs_market_margin_abs_err": _mean(df_mkt_spread, "sim_vs_market_margin_abs_err"),
        },
    }

    out_csv = Path(args.out_csv) if args.out_csv else (processed_dir / f"sim_realism_games_{start_d.isoformat()}_{end_d.isoformat()}.csv")
    out_json = Path(args.out_json) if args.out_json else (processed_dir / f"sim_realism_summary_{start_d.isoformat()}_{end_d.isoformat()}.json")
    out_df.to_csv(out_csv, index=False)
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(json.dumps({"out_csv": str(out_csv), "out_json": str(out_json), "summary": summary}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
