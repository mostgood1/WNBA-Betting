#!/usr/bin/env python3
"""Analyze live_lens_signals_*.jsonl vs recon actuals and suggest tuning knobs.

Reads:
- data/processed/live_lens_signals_<date>.jsonl
- data/processed/recon_games_<date>.csv (for game totals)
- data/processed/recon_quarters_<date>.csv (optional; for half/quarter totals)

Outputs:
- Summary metrics (count/MAE/RMSE/bias) by market
- Best simple gating thresholds (min elapsed minutes; optional w_pace filter)
- Optional JSON override stub for data/processed/live_lens_tuning_override.json

This is intentionally conservative: it does not claim EV, only predictive accuracy.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
_DATA_ROOT = os.environ.get("NBA_BETTING_DATA_ROOT")
DATA_ROOT = Path(_DATA_ROOT).expanduser() if _DATA_ROOT else (BASE_DIR / "data")
PROCESSED = DATA_ROOT / "processed"
LIVE_LENS_DIR = Path((os.getenv("NBA_LIVE_LENS_DIR") or os.getenv("LIVE_LENS_DIR") or "").strip() or str(PROCESSED))


def _parse_date(s: str) -> _date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: _date, end: _date) -> Iterable[_date]:
    cur = start
    while cur <= end:
        yield cur
        cur = cur + timedelta(days=1)


def _n(x: Any) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


@dataclass(frozen=True)
class SignalRow:
    date: str
    game_id: str | None
    home: str | None
    away: str | None
    market: str
    horizon: str | None
    elapsed: float | None
    live_line: float | None
    edge: float | None
    predicted_total: float | None
    w_pace: float | None


def _load_signals(ds: str) -> list[SignalRow]:
    fp = LIVE_LENS_DIR / f"live_lens_signals_{ds}.jsonl"
    if not fp.exists():
        return []

    out: list[SignalRow] = []
    with fp.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue

            market = str(obj.get("market") or "").strip()
            if market not in {"total", "half_total", "quarter_total"}:
                continue

            live_line = _n(obj.get("live_line"))
            edge = _n(obj.get("edge_adj")) if market == "total" else _n(obj.get("edge"))
            predicted_total = (live_line + edge) if (live_line is not None and edge is not None) else None

            ctx = obj.get("context")
            w_pace = None
            if isinstance(ctx, dict):
                w_pace = _n(ctx.get("w_pace"))

            out.append(
                SignalRow(
                    date=str(obj.get("date") or ds),
                    game_id=str(obj.get("game_id")) if obj.get("game_id") else None,
                    home=str(obj.get("home") or "").strip().upper() or None,
                    away=str(obj.get("away") or "").strip().upper() or None,
                    market=market,
                    horizon=str(obj.get("horizon") or "").strip().lower() or None,
                    elapsed=_n(obj.get("elapsed")),
                    live_line=live_line,
                    edge=edge,
                    predicted_total=predicted_total,
                    w_pace=w_pace,
                )
            )
    return out


def _load_recon_games(ds: str) -> pd.DataFrame:
    fp = PROCESSED / f"recon_games_{ds}.csv"
    if not fp.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(fp)
    except Exception:
        return pd.DataFrame()


def _load_recon_quarters(ds: str) -> pd.DataFrame:
    fp = PROCESSED / f"recon_quarters_{ds}.csv"
    if not fp.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(fp)
    except Exception:
        return pd.DataFrame()


def _actual_total_for_signal(sig: SignalRow, rg: pd.DataFrame, rq: pd.DataFrame) -> float | None:
    if sig.home is None or sig.away is None:
        return None

    if sig.market == "total":
        if rg.empty:
            return None
        hit = rg[(rg.get("home_tri") == sig.home) & (rg.get("away_tri") == sig.away)]
        if hit.empty:
            return None
        return _n(hit.iloc[0].get("total_actual"))

    if rq.empty:
        return None

    hit = rq[(rq.get("home_tri") == sig.home) & (rq.get("away_tri") == sig.away)]
    if hit.empty:
        return None

    if sig.market == "half_total":
        if sig.horizon == "h1":
            return _n(hit.iloc[0].get("actual_h1_total"))
        if sig.horizon == "h2":
            return _n(hit.iloc[0].get("actual_h2_total"))
        return None

    if sig.market == "quarter_total":
        hz = sig.horizon or ""
        if hz in {"q1", "q2", "q3", "q4"}:
            return _n(hit.iloc[0].get(f"actual_{hz}_total"))
        return None

    return None


def _metrics(df: pd.DataFrame) -> dict[str, float]:
    if df.empty:
        return {"n": 0}
    err = df["pred"].astype(float) - df["act"].astype(float)
    mae = float(err.abs().mean())
    rmse = float(math.sqrt(float((err**2).mean())))
    bias = float(err.mean())
    return {"n": int(len(df)), "mae": mae, "rmse": rmse, "bias": bias}


def _best_elapsed_cut(df: pd.DataFrame, cuts: list[float], min_n: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if df.empty:
        return out
    for c in cuts:
        d = df[df["elapsed"].notna() & (df["elapsed"].astype(float) >= float(c))].copy()
        if len(d) < min_n:
            continue
        m = _metrics(d)
        out.append({"min_elapsed": float(c), **m})
    out.sort(key=lambda x: (x.get("rmse") if x.get("rmse") is not None else 1e9, -x.get("n", 0)))
    return out[:5]


def _best_wpace_cut(df: pd.DataFrame, cuts: list[float], min_n: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if df.empty:
        return out
    if "w_pace" not in df.columns:
        return out
    for c in cuts:
        d = df[df["w_pace"].notna() & (df["w_pace"].astype(float) >= float(c))].copy()
        if len(d) < min_n:
            continue
        m = _metrics(d)
        out.append({"min_w_pace": float(c), **m})
    out.sort(key=lambda x: (x.get("rmse") if x.get("rmse") is not None else 1e9, -x.get("n", 0)))
    return out[:5]


def _scope_full_game_knob(scope_minutes: float, min_elapsed_scope: float) -> float:
    # Our frontend scales min_elapsed_min by (scope_minutes / 48).
    # So to achieve min_elapsed_scope within the scope, knob = min_elapsed_scope / (scope_minutes/48).
    scale = float(scope_minutes) / 48.0
    if scale <= 1e-9:
        return float(min_elapsed_scope)
    return float(min_elapsed_scope / scale)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--min-n", type=int, default=250, help="Minimum rows for a threshold to be considered")
    ap.add_argument("--write-override", default="", help="If set, write a JSON override to this path")
    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)

    rows: list[dict[str, Any]] = []
    for d in _daterange(start, end):
        ds = d.isoformat()
        sigs = _load_signals(ds)
        if not sigs:
            continue
        rg = _load_recon_games(ds)
        rq = _load_recon_quarters(ds)
        for s in sigs:
            act = _actual_total_for_signal(s, rg, rq)
            if act is None:
                continue
            if s.predicted_total is None:
                continue
            rows.append(
                {
                    "date": s.date,
                    "market": s.market,
                    "horizon": s.horizon,
                    "elapsed": s.elapsed,
                    "live_line": s.live_line,
                    "edge": s.edge,
                    "pred": s.predicted_total,
                    "act": act,
                    "w_pace": s.w_pace,
                    "home": s.home,
                    "away": s.away,
                    "game_id": s.game_id,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        print("No joinable rows found (missing recon files or live_line/edge).")
        return 2

    print(f"Rows joined: {len(df)} (dates {args.start}..{args.end})")

    # Summary by market
    print("\nSummary by market:")
    for mkt in ["total", "half_total", "quarter_total"]:
        d = df[df["market"] == mkt].copy()
        if d.empty:
            continue
        met = _metrics(d)
        print(f"- {mkt}: n={met['n']} mae={met['mae']:.2f} rmse={met['rmse']:.2f} bias={met['bias']:.2f}")

    cuts_elapsed_game = list(range(0, 13))
    cuts_elapsed_half = list(range(0, 13))
    cuts_elapsed_quarter = list(range(0, 13))

    cuts_wpace = [0.0, 0.1, 0.2, 0.3, 0.4]

    print("\nBest simple gates (lower RMSE is better):")

    suggestions: dict[str, Any] = {"adjustments": {}}

    # Game total
    d_total = df[df["market"] == "total"].copy()
    if not d_total.empty:
        best = _best_elapsed_cut(d_total, [0, 1, 2, 3, 4, 6, 8, 10, 12], min_n=max(50, min(args.min_n, len(d_total) // 4)))
        if best:
            b0 = best[0]
            print(f"- game total min_elapsed>= {b0['min_elapsed']}: n={b0['n']} rmse={b0['rmse']:.2f} mae={b0['mae']:.2f} bias={b0['bias']:.2f}")
            suggestions["adjustments"]["game_total"] = {"min_elapsed_min": float(b0["min_elapsed"])}

    # Half total
    d_half = df[df["market"] == "half_total"].copy()
    if not d_half.empty:
        best_elapsed = _best_elapsed_cut(d_half, cuts_elapsed_half, min_n=max(50, min(args.min_n, len(d_half) // 4)))
        best_w = _best_wpace_cut(d_half, cuts_wpace, min_n=max(50, min(args.min_n, len(d_half) // 4)))
        if best_elapsed:
            b0 = best_elapsed[0]
            knob = _scope_full_game_knob(24.0, float(b0["min_elapsed"]))
            print(
                f"- half total min_elapsed>= {b0['min_elapsed']} (scope) => min_elapsed_min≈{knob:.1f} (full-game knob): n={b0['n']} rmse={b0['rmse']:.2f}"
            )
            suggestions["adjustments"]["half_total"] = {"min_elapsed_min": float(knob)}
        if best_w:
            b0 = best_w[0]
            print(f"- half total w_pace>= {b0['min_w_pace']}: n={b0['n']} rmse={b0['rmse']:.2f}")

    # Quarter total
    d_q = df[df["market"] == "quarter_total"].copy()
    if not d_q.empty:
        best_elapsed = _best_elapsed_cut(d_q, cuts_elapsed_quarter, min_n=max(50, min(args.min_n, len(d_q) // 4)))
        best_w = _best_wpace_cut(d_q, cuts_wpace, min_n=max(50, min(args.min_n, len(d_q) // 4)))
        if best_elapsed:
            b0 = best_elapsed[0]
            knob = _scope_full_game_knob(12.0, float(b0["min_elapsed"]))
            print(
                f"- quarter total min_elapsed>= {b0['min_elapsed']} (scope) => min_elapsed_min≈{knob:.1f} (full-game knob): n={b0['n']} rmse={b0['rmse']:.2f}"
            )
            suggestions["adjustments"]["quarter_total"] = {"min_elapsed_min": float(knob)}
        if best_w:
            b0 = best_w[0]
            print(f"- quarter total w_pace>= {b0['min_w_pace']}: n={b0['n']} rmse={b0['rmse']:.2f}")

    if args.write_override:
        out_path = Path(args.write_override)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"adjustments": suggestions.get("adjustments", {})}
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"\nWrote override stub -> {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
