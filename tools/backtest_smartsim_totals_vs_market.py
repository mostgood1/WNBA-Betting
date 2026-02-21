import argparse
import glob
import json
import os
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd


@dataclass(frozen=True)
class TotalsRow:
    sim_date: str
    matchup: str
    n_sims: int | None
    sim_total_mean: float
    market_total: float
    diff: float
    total_p10: float | None
    total_p50: float | None
    total_p90: float | None
    p_total_over: float | None
    actual_total: float | None
    odds_total: float | None
    file: str


def _norm_team_name(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() or ch.isspace() else "" for ch in str(s or "")).strip()


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError("end must be >= start")
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def _load_rows_for_date(sim_date: str, processed_dir: str) -> list[TotalsRow]:
    pattern = os.path.join(processed_dir, f"smart_sim_{sim_date}_*.json")
    paths = sorted(glob.glob(pattern))
    rows: list[TotalsRow] = []

    # Actuals (if present)
    actuals_path = os.path.join(processed_dir, f"recon_games_{sim_date}.csv")
    actuals_idx: dict[tuple[str, str], float] = {}
    names_idx: dict[tuple[str, str], tuple[str, str]] = {}
    if os.path.exists(actuals_path):
        try:
            rg = pd.read_csv(actuals_path)
            for _, rr in rg.iterrows():
                away = str(rr.get("away_tri") or rr.get("visitor_tri") or rr.get("away") or rr.get("visitor") or "").strip().upper()
                home = str(rr.get("home_tri") or rr.get("home") or "").strip().upper()
                ta = rr.get("total_actual")
                if away and home and ta is not None and not pd.isna(ta):
                    actuals_idx[(away, home)] = float(ta)
                vt = rr.get("visitor_team") or rr.get("away_team") or rr.get("visitor")
                ht = rr.get("home_team") or rr.get("home")
                if away and home and vt and ht:
                    names_idx[(away, home)] = (str(vt), str(ht))
        except Exception:
            actuals_idx = {}
            names_idx = {}

    # Odds totals (if present)
    odds_path = os.path.join(processed_dir, f"game_odds_{sim_date}.csv")
    odds_idx: dict[tuple[str, str], float] = {}
    if os.path.exists(odds_path):
        try:
            go = pd.read_csv(odds_path)
            for _, rr in go.iterrows():
                ht = rr.get("home_team")
                vt = rr.get("visitor_team")
                tot = rr.get("total")
                if ht and vt and tot is not None and not pd.isna(tot):
                    odds_idx[(_norm_team_name(str(vt)), _norm_team_name(str(ht)))] = float(tot)
        except Exception:
            odds_idx = {}

    for path in paths:
        with open(path, "r", encoding="utf-8") as f:
            j = json.load(f)

        score = j.get("score") or {}
        market = j.get("market") or {}

        sim_total = score.get("total_mean")
        market_total = market.get("market_total")
        if sim_total is None or market_total is None:
            continue

        home = j.get("home")
        away = j.get("away")
        matchup = f"{away}@{home}"

        tq = score.get("total_q") or {}
        total_p10 = tq.get("p10")
        total_p50 = tq.get("p50")
        total_p90 = tq.get("p90")
        p_total_over = score.get("p_total_over")
        away_u = str(away).upper()
        home_u = str(home).upper()
        actual_total = actuals_idx.get((away_u, home_u))
        odds_total = None
        nm = names_idx.get((away_u, home_u))
        if nm:
            vname, hname = nm
            odds_total = odds_idx.get((_norm_team_name(vname), _norm_team_name(hname)))

        rows.append(
            TotalsRow(
                sim_date=sim_date,
                matchup=matchup,
                n_sims=j.get("n_sims"),
                sim_total_mean=float(sim_total),
                market_total=float(market_total),
                diff=float(sim_total) - float(market_total),
                total_p10=(float(total_p10) if total_p10 is not None else None),
                total_p50=(float(total_p50) if total_p50 is not None else None),
                total_p90=(float(total_p90) if total_p90 is not None else None),
                p_total_over=(float(p_total_over) if p_total_over is not None else None),
                actual_total=actual_total,
                odds_total=odds_total,
                file=os.path.basename(path),
            )
        )

    return rows


def _summarize(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "games": 0,
            "mae": float("nan"),
            "rmse": float("nan"),
            "bias": float("nan"),
            "within3": float("nan"),
            "within5": float("nan"),
            "within7": float("nan"),
        }

    err = df["diff"].astype(float)
    return {
        "games": int(len(df)),
        "mae": float(err.abs().mean()),
        "rmse": float((err.pow(2).mean()) ** 0.5),
        "bias": float(err.mean()),
        "within3": float((err.abs() <= 3).mean() * 100),
        "within5": float((err.abs() <= 5).mean() * 100),
        "within7": float((err.abs() <= 7).mean() * 100),
    }


def _summarize_vs_odds(df: pd.DataFrame) -> dict:
    dfo = df[df["odds_total"].notna()].copy()
    if dfo.empty:
        return {
            "games": 0,
            "mae": float("nan"),
            "rmse": float("nan"),
            "bias": float("nan"),
        }
    err = (dfo["sim_total_mean"].astype(float) - dfo["odds_total"].astype(float))
    return {
        "games": int(len(dfo)),
        "mae": float(err.abs().mean()),
        "rmse": float((err.pow(2).mean()) ** 0.5),
        "bias": float(err.mean()),
    }


def _summarize_odds_vs_actual(df: pd.DataFrame) -> dict:
    dfo = df[df["odds_total"].notna() & df["actual_total"].notna()].copy()
    if dfo.empty:
        return {
            "games": 0,
            "mae": float("nan"),
            "rmse": float("nan"),
            "bias": float("nan"),
        }
    err = (dfo["odds_total"].astype(float) - dfo["actual_total"].astype(float))
    return {
        "games": int(len(dfo)),
        "mae": float(err.abs().mean()),
        "rmse": float((err.pow(2).mean()) ** 0.5),
        "bias": float(err.mean()),
    }


def _summarize_actual(df: pd.DataFrame) -> dict:
    dfa = df[df["actual_total"].notna()].copy()
    if dfa.empty:
        return {
            "games": 0,
            "mae": float("nan"),
            "rmse": float("nan"),
            "bias": float("nan"),
            "cov_p10_p90": float("nan"),
            "tail_low": float("nan"),
            "tail_high": float("nan"),
            "avg_width_p10_p90": float("nan"),
            "brier_over": float("nan"),
            "logloss_over": float("nan"),
        }

    err = (dfa["sim_total_mean"].astype(float) - dfa["actual_total"].astype(float))

    # Interval coverage
    cov_mask = (
        dfa["total_p10"].notna()
        & dfa["total_p90"].notna()
        & (dfa["actual_total"].astype(float) >= dfa["total_p10"].astype(float))
        & (dfa["actual_total"].astype(float) <= dfa["total_p90"].astype(float))
    )
    tail_low_mask = dfa["total_p10"].notna() & (dfa["actual_total"].astype(float) < dfa["total_p10"].astype(float))
    tail_high_mask = dfa["total_p90"].notna() & (dfa["actual_total"].astype(float) > dfa["total_p90"].astype(float))
    width_mask = dfa["total_p10"].notna() & dfa["total_p90"].notna()
    avg_width = float((dfa.loc[width_mask, "total_p90"].astype(float) - dfa.loc[width_mask, "total_p10"].astype(float)).mean()) if width_mask.any() else float("nan")

    # Over probability calibration vs closing total
    calib = dfa[dfa["p_total_over"].notna() & dfa["market_total"].notna()].copy()
    brier = float("nan")
    logloss = float("nan")
    if not calib.empty:
        y = (calib["actual_total"].astype(float) > calib["market_total"].astype(float)).astype(float)
        p = calib["p_total_over"].astype(float).clip(1e-12, 1.0 - 1e-12)
        # exclude pushes exactly on the line
        push = (calib["actual_total"].astype(float) == calib["market_total"].astype(float))
        if (~push).any():
            yy = y[~push]
            pp = p[~push]
            brier = float(((pp - yy) ** 2).mean())
            logloss = float((-(yy * pp.apply(math.log) + (1.0 - yy) * (1.0 - pp).apply(math.log))).mean())

    return {
        "games": int(len(dfa)),
        "mae": float(err.abs().mean()),
        "rmse": float((err.pow(2).mean()) ** 0.5),
        "bias": float(err.mean()),
        "cov_p10_p90": float(cov_mask.mean() * 100.0) if len(dfa) else float("nan"),
        "tail_low": float(tail_low_mask.mean() * 100.0) if len(dfa) else float("nan"),
        "tail_high": float(tail_high_mask.mean() * 100.0) if len(dfa) else float("nan"),
        "avg_width_p10_p90": avg_width,
        "brier_over": brier,
        "logloss_over": logloss,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Backtest SmartSim totals vs embedded market totals.")
    ap.add_argument("--end", default=None, help="End date YYYY-MM-DD (default: today)")
    ap.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to include ending at --end (default: 7)",
    )
    ap.add_argument(
        "--processed-dir",
        default=os.path.join("data", "processed"),
        help="Processed directory containing smart_sim_*.json files",
    )
    args = ap.parse_args()

    end = _parse_date(args.end) if args.end else date.today()
    days = int(args.days)
    start = end - timedelta(days=days - 1)

    all_rows: list[TotalsRow] = []
    missing_dates: list[str] = []

    for d in _daterange(start, end):
        ds = d.isoformat()
        rows = _load_rows_for_date(ds, args.processed_dir)
        if not rows:
            missing_dates.append(ds)
        all_rows.extend(rows)

    df = pd.DataFrame([r.__dict__ for r in all_rows])

    overall = _summarize(df)
    print(
        f"SmartSim totals vs embedded market_total ({start.isoformat()}..{end.isoformat()}) "
        f"games={overall['games']} MAE={overall['mae']:.2f} RMSE={overall['rmse']:.2f} "
        f"bias={overall['bias']:+.2f} within±3={overall['within3']:.1f}% "
        f"within±5={overall['within5']:.1f}% within±7={overall['within7']:.1f}%"
    )

    overall_vs_odds = _summarize_vs_odds(df)
    if overall_vs_odds["games"] > 0:
        print(
            f"SmartSim totals vs ODDS total ({start.isoformat()}..{end.isoformat()}) "
            f"games={overall_vs_odds['games']} MAE={overall_vs_odds['mae']:.2f} RMSE={overall_vs_odds['rmse']:.2f} bias={overall_vs_odds['bias']:+.2f}"
        )

    overall_odds_actual = _summarize_odds_vs_actual(df)
    if overall_odds_actual["games"] > 0:
        print(
            f"ODDS total vs ACTUAL ({start.isoformat()}..{end.isoformat()}) "
            f"games={overall_odds_actual['games']} MAE={overall_odds_actual['mae']:.2f} RMSE={overall_odds_actual['rmse']:.2f} bias={overall_odds_actual['bias']:+.2f}"
        )

    overall_a = _summarize_actual(df)
    if overall_a["games"] > 0:
        print(
            f"SmartSim totals vs ACTUAL ({start.isoformat()}..{end.isoformat()}) "
            f"games={overall_a['games']} MAE={overall_a['mae']:.2f} RMSE={overall_a['rmse']:.2f} "
            f"bias={overall_a['bias']:+.2f} cov[p10,p90]={overall_a['cov_p10_p90']:.1f}% "
            f"tails(low/high)={overall_a['tail_low']:.1f}%/{overall_a['tail_high']:.1f}% "
            f"avg_width={overall_a['avg_width_p10_p90']:.2f} brier(over)={overall_a['brier_over']:.4f} logloss(over)={overall_a['logloss_over']:.4f}"
        )

        # Suggested global points multiplier to remove average bias.
        try:
            dfa = df[df["actual_total"].notna()].copy()
            sim_mu = float(dfa["sim_total_mean"].astype(float).mean())
            act_mu = float(dfa["actual_total"].astype(float).mean())
            if sim_mu > 0 and pd.notna(sim_mu) and pd.notna(act_mu):
                pm = act_mu / sim_mu
                # Keep this conservative; treat as a gentle calibration, not a refit.
                pm = float(max(0.97, min(1.03, pm)))
                print(f"Means (vs ACTUAL): sim_total_mean={sim_mu:.2f} actual_total_mean={act_mu:.2f} suggested_points_mult={pm:.5f}")
        except Exception:
            pass

    if missing_dates:
        print(f"Missing/empty dates (no usable market totals): {', '.join(missing_dates)}")

    if df.empty:
        return

    # Per-day summary
    per_day = (
        df.groupby("sim_date", as_index=False)
        .apply(lambda g: pd.Series(_summarize(g)))
        .reset_index(drop=True)
        .sort_values("sim_date")
    )
    per_day["mae"] = per_day["mae"].round(2)
    per_day["rmse"] = per_day["rmse"].round(2)
    per_day["bias"] = per_day["bias"].round(2)
    per_day["within3"] = per_day["within3"].round(1)
    per_day["within5"] = per_day["within5"].round(1)
    per_day["within7"] = per_day["within7"].round(1)

    print("\nPer-day:")
    print(
        per_day[["sim_date", "games", "mae", "rmse", "bias", "within3", "within5", "within7"]]
        .to_string(index=False)
    )

    # Per-day actual summary
    per_day_a = (
        df.groupby("sim_date", as_index=False)
        .apply(lambda g: pd.Series(_summarize_actual(g)))
        .reset_index(drop=True)
        .sort_values("sim_date")
    )
    per_day_a = per_day_a[per_day_a["games"] > 0]
    if not per_day_a.empty:
        per_day_a["mae"] = per_day_a["mae"].round(2)
        per_day_a["rmse"] = per_day_a["rmse"].round(2)
        per_day_a["bias"] = per_day_a["bias"].round(2)
        per_day_a["cov_p10_p90"] = per_day_a["cov_p10_p90"].round(1)
        per_day_a["tail_low"] = per_day_a["tail_low"].round(1)
        per_day_a["tail_high"] = per_day_a["tail_high"].round(1)
        per_day_a["avg_width_p10_p90"] = per_day_a["avg_width_p10_p90"].round(2)
        per_day_a["brier_over"] = per_day_a["brier_over"].round(4)
        per_day_a["logloss_over"] = per_day_a["logloss_over"].round(4)
        print("\nPer-day (vs ACTUAL):")
        print(
            per_day_a[[
                "sim_date",
                "games",
                "mae",
                "rmse",
                "bias",
                "cov_p10_p90",
                "tail_low",
                "tail_high",
                "avg_width_p10_p90",
                "brier_over",
                "logloss_over",
            ]].to_string(index=False)
        )


if __name__ == "__main__":
    main()
