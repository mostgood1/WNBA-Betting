from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def _parse_date(s: str) -> _date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _date_range(start: _date, end: _date) -> list[_date]:
    if end < start:
        return []
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def _find_latest_date_from_glob(prefix: str) -> _date | None:
    # prefix like "recon_quarters_" and files named prefixYYYY-MM-DD.csv
    best: _date | None = None
    for p in PROCESSED.glob(f"{prefix}*.csv"):
        stem = p.stem
        if not stem.startswith(prefix):
            continue
        ds = stem[len(prefix) :]
        try:
            d = _parse_date(ds)
        except Exception:
            continue
        if best is None or d > best:
            best = d
    return best


def _pick_latest_smart_sim_eval() -> Path | None:
    # prefer the most recently modified file matching smart_sim_quarter_eval_*.csv
    candidates = list(PROCESSED.glob("smart_sim_quarter_eval_*.csv"))
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _json_sanitize(x: object) -> object:
    # Convert NaN/Inf (and numpy scalars) to JSON-safe values.
    try:
        if isinstance(x, float):
            if not np.isfinite(x):
                return None
            return x
    except Exception:
        pass
    try:
        if isinstance(x, (np.floating, np.integer)):
            return _json_sanitize(x.item())
        if isinstance(x, np.ndarray):
            return [_json_sanitize(v) for v in x.tolist()]
    except Exception:
        pass
    if isinstance(x, dict):
        return {str(k): _json_sanitize(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_json_sanitize(v) for v in x]
    return x


@dataclass(frozen=True)
class MetricRow:
    metric: str
    n: int
    mean_err: float
    mae: float
    rmse: float
    p50_abs: float
    p90_abs: float
    within_1: float
    within_2: float
    within_3: float
    within_5: float


@dataclass(frozen=True)
class ProbMetricRow:
    metric: str
    n: int
    acc: float
    brier: float
    logloss: float


def _summarize_errors(name: str, err: pd.Series) -> MetricRow:
    e = pd.to_numeric(err, errors="coerce").dropna().astype(float)
    if e.empty:
        return MetricRow(
            metric=name,
            n=0,
            mean_err=float("nan"),
            mae=float("nan"),
            rmse=float("nan"),
            p50_abs=float("nan"),
            p90_abs=float("nan"),
            within_1=float("nan"),
            within_2=float("nan"),
            within_3=float("nan"),
            within_5=float("nan"),
        )
    ae = e.abs()
    return MetricRow(
        metric=name,
        n=int(len(e)),
        mean_err=float(e.mean()),
        mae=float(ae.mean()),
        rmse=float(np.sqrt(np.mean(np.square(e)))),
        p50_abs=float(np.quantile(ae, 0.50)),
        p90_abs=float(np.quantile(ae, 0.90)),
        within_1=float(np.mean(ae <= 1.0)),
        within_2=float(np.mean(ae <= 2.0)),
        within_3=float(np.mean(ae <= 3.0)),
        within_5=float(np.mean(ae <= 5.0)),
    )


def _rows_to_df(rows: Iterable[MetricRow]) -> pd.DataFrame:
    return pd.DataFrame([r.__dict__ for r in rows])


def _clamp01(p: float) -> float:
    try:
        p = float(p)
        if not np.isfinite(p):
            return 0.5
        return float(max(1e-6, min(1.0 - 1e-6, p)))
    except Exception:
        return 0.5


def _phi(z: float) -> float:
    # Normal CDF
    return 0.5 * (1.0 + float(math.erf(float(z) / float(np.sqrt(2.0)))))


def _logloss_prob(p: float, y: float) -> float:
    p = _clamp01(p)
    y = float(max(0.0, min(1.0, float(y))))
    return float(-(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))


def _sigma_for_quarter_points(mu_points: float) -> float:
    # Mirrors nba_betting.sim.quarters._sigma_for_quarter (bounded 6-10)
    return float(max(6.0, min(10.0, 0.9 * np.sqrt(max(1.0, float(mu_points))))))


def _p_home_win_from_quarter_mus(home_mu: float, away_mu: float, corr: float = 0.25) -> float:
    sh = _sigma_for_quarter_points(home_mu)
    sa = _sigma_for_quarter_points(away_mu)
    # Variance of (H - A) with corr between H and A
    sigma_margin = float(np.sqrt(max(1e-6, (sh * sh) + (sa * sa) - 2.0 * float(corr) * sh * sa)))
    mu_margin = float(home_mu) - float(away_mu)
    return _phi(mu_margin / sigma_margin)


def _summarize_prob_metrics(name: str, y: np.ndarray, p: np.ndarray) -> ProbMetricRow:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    m = np.isfinite(y) & np.isfinite(p)
    y = y[m]
    p = p[m]
    if y.size == 0:
        return ProbMetricRow(metric=name, n=0, acc=float('nan'), brier=float('nan'), logloss=float('nan'))
    p = np.clip(p, 1e-6, 1.0 - 1e-6)
    acc = float(np.mean((p >= 0.5) == (y >= 0.5)))
    brier = float(np.mean((p - y) ** 2))
    ll = float(np.mean([_logloss_prob(float(pi), float(yi)) for pi, yi in zip(p.tolist(), y.tolist())]))
    return ProbMetricRow(metric=name, n=int(y.size), acc=acc, brier=brier, logloss=ll)


def eval_recon_quarters(start: _date, end: _date) -> tuple[pd.DataFrame, dict]:
    frames: list[pd.DataFrame] = []
    for d in _date_range(start, end):
        p = PROCESSED / f"recon_quarters_{d}.csv"
        if not p.exists():
            continue
        df = pd.read_csv(p)
        df["date"] = str(d)
        frames.append(df)

    if not frames:
        return pd.DataFrame(), {"source": "recon_quarters", "rows": 0}

    all_df = pd.concat(frames, ignore_index=True)

    metrics: list[MetricRow] = []
    for period in ["q1", "q2", "q3", "q4", "h1", "h2", "game"]:
        col = f"err_{period}_total"
        if col in all_df.columns:
            metrics.append(_summarize_errors(f"{period}_total", all_df[col]))

    out_df = _rows_to_df(metrics)
    meta = {
        "source": "recon_quarters",
        "start": str(start),
        "end": str(end),
        "games": int(len(all_df)),
        "rows": int(len(all_df)),
    }
    return out_df, meta


def _smart_err(df: pd.DataFrame, act_col: str, pred_col: str) -> pd.Series:
    act = pd.to_numeric(df.get(act_col), errors="coerce")
    pred = pd.to_numeric(df.get(pred_col), errors="coerce")
    return act - pred


def eval_smart_sim_quarters(
    path: Path,
    start: _date | None = None,
    end: _date | None = None,
    *,
    use_pbp_filter: bool = True,
) -> tuple[pd.DataFrame, dict]:
    try:
        df = pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame(), {
            "source": "smart_sim_quarter_eval",
            "path": str(path),
            "rows": 0,
            "error": "Empty CSV (no columns to parse)",
        }

    # If the file includes dates, filter to the requested window.
    if start is not None and end is not None and "date" in df.columns:
        dt = pd.to_datetime(df["date"], errors="coerce")
        d = dt.dt.date
        df = df[(d >= start) & (d <= end)].copy()

    # Filter to rows with actuals present for at least one quarter
    # (Many rows are pre-game / missing actuals.)
    keep_mask = pd.Series(False, index=df.index)
    for q in [1, 2, 3, 4]:
        keep_mask |= pd.to_numeric(df.get(f"q{q}_total_act"), errors="coerce").notna()
    df = df[keep_mask].copy()

    if use_pbp_filter and "use_pbp" in df.columns:
        # use_pbp is best quality; keep True only when present
        m = df["use_pbp"].astype(str).str.lower().isin(["true", "1", "yes"])
        if m.any():
            df = df[m].copy()

    if df.empty:
        return pd.DataFrame(), {"source": "smart_sim_quarter_eval", "rows": 0, "path": str(path)}

    # Add full-game derived fields (requires all 4 quarters).
    # This supports accuracy gating on team totals, not just per-quarter metrics.
    def _sum4(cols: list[str]) -> pd.Series:
        block = pd.DataFrame({c: pd.to_numeric(df.get(c), errors="coerce") for c in cols})
        return block.sum(axis=1, min_count=4)

    if all(c in df.columns for c in [
        "q1_home_act", "q2_home_act", "q3_home_act", "q4_home_act",
        "q1_home_pred", "q2_home_pred", "q3_home_pred", "q4_home_pred",
        "q1_away_act", "q2_away_act", "q3_away_act", "q4_away_act",
        "q1_away_pred", "q2_away_pred", "q3_away_pred", "q4_away_pred",
    ]):
        df["game_home_act"] = _sum4(["q1_home_act", "q2_home_act", "q3_home_act", "q4_home_act"])
        df["game_home_pred"] = _sum4(["q1_home_pred", "q2_home_pred", "q3_home_pred", "q4_home_pred"])
        df["game_away_act"] = _sum4(["q1_away_act", "q2_away_act", "q3_away_act", "q4_away_act"])
        df["game_away_pred"] = _sum4(["q1_away_pred", "q2_away_pred", "q3_away_pred", "q4_away_pred"])
        df["game_total_act"] = df["game_home_act"] + df["game_away_act"]
        df["game_total_pred"] = df["game_home_pred"] + df["game_away_pred"]
        df["game_margin_act"] = df["game_home_act"] - df["game_away_act"]
        df["game_margin_pred"] = df["game_home_pred"] - df["game_away_pred"]

    metrics: list[MetricRow] = []
    for q in [1, 2, 3, 4]:
        metrics.append(_summarize_errors(f"q{q}_home", _smart_err(df, f"q{q}_home_act", f"q{q}_home_pred")))
        metrics.append(_summarize_errors(f"q{q}_away", _smart_err(df, f"q{q}_away_act", f"q{q}_away_pred")))
        metrics.append(_summarize_errors(f"q{q}_total", _smart_err(df, f"q{q}_total_act", f"q{q}_total_pred")))
        metrics.append(_summarize_errors(f"q{q}_margin", _smart_err(df, f"q{q}_margin_act", f"q{q}_margin_pred")))

    for h in ["h1", "h2"]:
        metrics.append(_summarize_errors(f"{h}_home", _smart_err(df, f"{h}_home_act", f"{h}_home_pred")))
        metrics.append(_summarize_errors(f"{h}_away", _smart_err(df, f"{h}_away_act", f"{h}_away_pred")))
        metrics.append(_summarize_errors(f"{h}_total", _smart_err(df, f"{h}_total_act", f"{h}_total_pred")))
        metrics.append(_summarize_errors(f"{h}_margin", _smart_err(df, f"{h}_margin_act", f"{h}_margin_pred")))

    # Full-game metrics (if derived fields exist)
    if all(c in df.columns for c in ["game_home_act", "game_home_pred", "game_away_act", "game_away_pred", "game_total_act", "game_total_pred", "game_margin_act", "game_margin_pred"]):
        metrics.append(_summarize_errors("game_home", _smart_err(df, "game_home_act", "game_home_pred")))
        metrics.append(_summarize_errors("game_away", _smart_err(df, "game_away_act", "game_away_pred")))
        metrics.append(_summarize_errors("game_total", _smart_err(df, "game_total_act", "game_total_pred")))
        metrics.append(_summarize_errors("game_margin", _smart_err(df, "game_margin_act", "game_margin_pred")))

    out_df = _rows_to_df(metrics)

    meta = {
        "source": "smart_sim_quarter_eval",
        "path": str(path),
        "start": str(start) if start is not None else None,
        "end": str(end) if end is not None else None,
        "rows": int(len(df)),
        "unique_games": int(df[[c for c in ["date", "home_tri", "away_tri", "game_id"] if c in df.columns]].drop_duplicates().shape[0]),
    }

    # Winner-probability evaluation from predicted quarter points.
    # This is separate from the existing (points) accuracy metrics above.
    try:
        prob_rows: list[ProbMetricRow] = []
        all_y: list[float] = []
        all_p: list[float] = []
        for q in [1, 2, 3, 4]:
            ha = pd.to_numeric(df.get(f"q{q}_home_act"), errors="coerce")
            aa = pd.to_numeric(df.get(f"q{q}_away_act"), errors="coerce")
            hp = pd.to_numeric(df.get(f"q{q}_home_pred"), errors="coerce")
            ap = pd.to_numeric(df.get(f"q{q}_away_pred"), errors="coerce")

            m = ha.notna() & aa.notna() & hp.notna() & ap.notna() & (ha != aa)
            if not m.any():
                prob_rows.append(ProbMetricRow(metric=f"q{q}_winner", n=0, acc=float('nan'), brier=float('nan'), logloss=float('nan')))
                continue

            y = (ha[m].astype(float) > aa[m].astype(float)).astype(float).to_numpy()
            p = np.array([
                _clamp01(_p_home_win_from_quarter_mus(float(h), float(a)))
                for h, a in zip(hp[m].astype(float).tolist(), ap[m].astype(float).tolist())
            ], dtype=float)
            prob_rows.append(_summarize_prob_metrics(f"q{q}_winner", y, p))
            all_y += y.tolist()
            all_p += p.tolist()

        if all_y and all_p:
            prob_rows.append(_summarize_prob_metrics("q_all_winner", np.array(all_y, dtype=float), np.array(all_p, dtype=float)))

        meta["winner_prob_metrics"] = [r.__dict__ for r in prob_rows]
    except Exception as e:
        meta["winner_prob_metrics"] = {"error": str(e)}

    # Optional: aggregate any precomputed calibration fields if present
    calib_cols = [c for c in df.columns if c.endswith("_over_brier") or c.endswith("_over_logloss")]
    if calib_cols:
        meta["calibration_means"] = {c: float(pd.to_numeric(df[c], errors="coerce").mean()) for c in calib_cols}

    return out_df, meta


def main() -> int:
    ap = argparse.ArgumentParser(description="Sanity-check quarter predictions vs actuals")
    ap.add_argument("--start", type=str, default=None, help="YYYY-MM-DD")
    ap.add_argument("--end", type=str, default=None, help="YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=30, help="If start/end not provided, evaluate last N days ending at latest available")
    ap.add_argument(
        "--source",
        type=str,
        default="both",
        choices=["both", "recon", "smart_sim"],
        help="Which dataset to evaluate",
    )
    ap.add_argument(
        "--smart-sim-path",
        type=str,
        default=None,
        help="Optional explicit smart_sim_quarter_eval CSV path (defaults to latest)",
    )
    ap.add_argument(
        "--no-use-pbp-filter",
        action="store_true",
        help="Disable filtering to use_pbp==True rows (for apples-to-apples comparisons)",
    )
    ap.add_argument(
        "--out-suffix",
        type=str,
        default="",
        help="Optional suffix appended to output filenames (e.g. _baseline, _new, _nopbp)",
    )
    args = ap.parse_args()

    # Default window: based on latest recon_quarters date
    latest_recon = _find_latest_date_from_glob("recon_quarters_")
    if args.start and args.end:
        start = _parse_date(args.start)
        end = _parse_date(args.end)
    else:
        if latest_recon is None:
            # fallback to yesterday
            end = _date.today() - timedelta(days=1)
        else:
            end = latest_recon
        start = end - timedelta(days=int(args.days) - 1)

    out: dict[str, object] = {
        "start": str(start),
        "end": str(end),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }

    tag = f"{start}_{end}"
    suffix = str(args.out_suffix or "")

    if args.source in ("both", "recon"):
        recon_df, recon_meta = eval_recon_quarters(start, end)
        out["recon_quarters"] = recon_meta
        if not recon_df.empty:
            out_path = PROCESSED / f"quarters_eval_recon_{tag}{suffix}.csv"
            recon_df.to_csv(out_path, index=False)
            out["recon_quarters"]["output_csv"] = str(out_path)

    if args.source in ("both", "smart_sim"):
        if args.smart_sim_path:
            p = Path(args.smart_sim_path)
            if not p.is_absolute():
                p = (ROOT / p).resolve()
        else:
            p = _pick_latest_smart_sim_eval()
        if p is None or not Path(p).exists():
            out["smart_sim_quarter_eval"] = {"rows": 0, "error": "No smart_sim_quarter_eval_*.csv found"}
        else:
            sim_df, sim_meta = eval_smart_sim_quarters(
                Path(p),
                start=start,
                end=end,
                use_pbp_filter=(not args.no_use_pbp_filter),
            )
            out["smart_sim_quarter_eval"] = sim_meta
            if not sim_df.empty:
                out_path = PROCESSED / f"quarters_eval_team_{tag}{suffix}.csv"
                sim_df.to_csv(out_path, index=False)
                out["smart_sim_quarter_eval"]["output_csv"] = str(out_path)

    out_sanitized = _json_sanitize(out)

    out_json = PROCESSED / f"quarters_eval_summary_{tag}{suffix}.json"
    out_json.write_text(json.dumps(out_sanitized, indent=2, allow_nan=False), encoding="utf-8")

    # Minimal console output (keep CI/task logs readable)
    print(json.dumps(out_sanitized, indent=2, allow_nan=False))
    print(f"Wrote: {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
