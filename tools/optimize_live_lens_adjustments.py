from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
LIVE_LENS_DIR = Path((os.getenv("NBA_LIVE_LENS_DIR") or os.getenv("LIVE_LENS_DIR") or "").strip() or str(PROCESSED))


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError("end < start")
    out: list[date] = []
    d = start
    while d <= end:
        out.append(d)
        d = d + timedelta(days=1)
    return out


def _to_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        if isinstance(x, (int, float, np.integer, np.floating)):
            v = float(x)
            return v if np.isfinite(v) else None
        s = str(x).strip()
        if not s:
            return None
        v = float(s)
        return v if np.isfinite(v) else None
    except Exception:
        return None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _load_recon_actuals_map(d: date) -> dict[tuple[str, str], float]:
    p = PROCESSED / f"recon_games_{d.isoformat()}.csv"
    if not p.exists():
        return {}
    df = pd.read_csv(p)
    for c in ["home_tri", "away_tri", "total_actual"]:
        if c not in df.columns:
            return {}
    out: dict[tuple[str, str], float] = {}
    for _, r in df.iterrows():
        h = str(r.get("home_tri") or "").strip().upper()
        a = str(r.get("away_tri") or "").strip().upper()
        t = _to_float(r.get("total_actual"))
        if h and a and t is not None:
            out[(h, a)] = float(t)
    return out


def _infer_side(rec: dict[str, Any]) -> str | None:
    # Try common keys from existing logs.
    for k in ["side", "bet_side", "selection", "pick"]:
        v = rec.get(k)
        if isinstance(v, str):
            s = v.strip().lower()
            if s in {"over", "under"}:
                return s
    return None


def _infer_line_total(rec: dict[str, Any]) -> float | None:
    for k in ["live_line", "line", "total_line", "market_line", "line_total"]:
        v = _to_float(rec.get(k))
        if v is not None:
            return v
    # some logs may nest under market/lines
    m = rec.get("market")
    if isinstance(m, dict):
        for k in ["live_line", "line", "total"]:
            v = _to_float(m.get(k))
            if v is not None:
                return v
    return None


def _infer_game_tris(rec: dict[str, Any]) -> tuple[str | None, str | None]:
    for hk, ak in [("home", "away"), ("home_tri", "away_tri"), ("homeTricode", "awayTricode")]:
        h = rec.get(hk)
        a = rec.get(ak)
        if isinstance(h, str) and isinstance(a, str) and h.strip() and a.strip():
            return h.strip().upper(), a.strip().upper()
    # try nested
    g = rec.get("game")
    if isinstance(g, dict):
        h = g.get("home") or g.get("home_tri")
        a = g.get("away") or g.get("away_tri")
        if isinstance(h, str) and isinstance(a, str) and h.strip() and a.strip():
            return h.strip().upper(), a.strip().upper()
    return None, None


def _infer_edge_raw(rec: dict[str, Any]) -> float | None:
    for k in ["edge_raw", "edge", "diff_raw", "totalDiffRaw"]:
        v = _to_float(rec.get(k))
        if v is not None:
            return v
    return None


def _infer_edge_adj(rec: dict[str, Any]) -> float | None:
    for k in ["edge_adj", "edge", "diff_adj", "totalDiff"]:
        v = _to_float(rec.get(k))
        if v is not None:
            return v
    return None


def _infer_game_id(rec: dict[str, Any]) -> str | None:
    for k in ["game_id", "gid", "id"]:
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, (int, float, np.integer, np.floating)):
            vv = str(int(v))
            if vv.strip():
                return vv.strip()
    return None


def _infer_context(rec: dict[str, Any]) -> dict[str, Any]:
    c = rec.get("context")
    if isinstance(c, dict):
        return c
    return {}


def _outcome_profit(side: str, final_total: float, line_total: float, juice: float) -> tuple[str, float]:
    # Standard -110 by default: win yields +0.909..., loss -1.0
    # juice is absolute, e.g. 110 -> win_profit = 100/110
    if abs(final_total - line_total) < 1e-9:
        return "push", 0.0
    won = (final_total > line_total and side == "over") or (final_total < line_total and side == "under")
    if won:
        return "win", 100.0 / float(juice)
    return "loss", -1.0


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(np.clip(x, lo, hi))


@dataclass(frozen=True)
class GameTotalAdjParams:
    pace_weight: float
    eff_weight: float
    pace_cap_points: float
    eff_cap_points: float
    min_elapsed_min: float


def _apply_adjustment(
    raw_diff: float,
    line_total: float,
    pace_ratio: float | None,
    eff_ppp_delta: float | None,
    exp_home_pace: float | None,
    exp_away_pace: float | None,
    elapsed_min: float | None,
    p: GameTotalAdjParams,
) -> float:
    # Mirrors the frontend `adjustGameTotalDiffWithContext()` logic:
    # - only applies after min elapsed
    # - pace_points = clamp((pace_ratio - 1) * line_total, -30..30)
    # - eff_points = clamp(eff_ppp_delta * exp_pace, -30..30)
    # - paceBoost = clamp(pace_points * pace_weight, -pace_cap..pace_cap)
    # - effPenalty = clamp(eff_points * eff_weight, -eff_cap..eff_cap)
    # - for OVER edges: adj = raw + max(0,paceBoost) - max(0,effPenalty)
    # - for UNDER edges: adj = raw - max(0,-paceBoost) + max(0,-effPenalty)

    rd = float(raw_diff)
    lt = float(line_total)

    if elapsed_min is None or not np.isfinite(elapsed_min) or float(elapsed_min) < float(p.min_elapsed_min):
        return rd

    exp_pace = None
    if exp_home_pace is not None and exp_away_pace is not None:
        if np.isfinite(exp_home_pace) and np.isfinite(exp_away_pace):
            exp_pace = 0.5 * (float(exp_home_pace) + float(exp_away_pace))
    if exp_pace is None or exp_pace <= 1e-6:
        return rd

    # deadzone: no adjustment when edge is tiny
    if not (rd > 0.5 or rd < -0.5):
        return rd

    pace_points = None
    if pace_ratio is not None and np.isfinite(pace_ratio):
        pace_points = _clamp((float(pace_ratio) - 1.0) * lt, -30.0, 30.0)

    eff_points = None
    if eff_ppp_delta is not None and np.isfinite(eff_ppp_delta):
        eff_points = _clamp(float(eff_ppp_delta) * float(exp_pace), -30.0, 30.0)

    pace_boost = 0.0
    if pace_points is not None:
        pace_boost = _clamp(float(pace_points) * float(p.pace_weight), -float(p.pace_cap_points), float(p.pace_cap_points))

    eff_pen = 0.0
    if eff_points is not None:
        eff_pen = _clamp(float(eff_points) * float(p.eff_weight), -float(p.eff_cap_points), float(p.eff_cap_points))

    if rd > 0.5:
        return rd + max(0.0, pace_boost) - max(0.0, eff_pen)
    return rd - max(0.0, -pace_boost) + max(0.0, -eff_pen)


def _candidate_grid(
    pace_ws: Iterable[float],
    eff_ws: Iterable[float],
    pace_caps: Iterable[float],
    eff_caps: Iterable[float],
    min_elapsed_mins: Iterable[float],
) -> list[GameTotalAdjParams]:
    out: list[GameTotalAdjParams] = []
    for w1 in pace_ws:
        for w2 in eff_ws:
            for c1 in pace_caps:
                for c2 in eff_caps:
                    for me in min_elapsed_mins:
                        out.append(
                            GameTotalAdjParams(
                                pace_weight=float(w1),
                                eff_weight=float(w2),
                                pace_cap_points=float(c1),
                                eff_cap_points=float(c2),
                                min_elapsed_min=float(me),
                            )
                        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Optimize Live Lens baseline adjustment knobs from logged JSONL signals")
    ap.add_argument("--start", type=str, required=True, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, required=True, help="End date YYYY-MM-DD")
    ap.add_argument("--bet-threshold", type=float, default=6.0, help="Absolute adjusted edge required to bet")
    ap.add_argument("--juice", type=float, default=110.0, help="Assumed juice for ROI calc (e.g. 110 for -110)")
    ap.add_argument("--min-bets", type=int, default=25, help="Minimum bets required for a candidate to be considered")
    ap.add_argument(
        "--out",
        type=str,
        default=None,
        help="Write best params JSON (default: data/processed/live_lens_adjustments_optimized_<start>_<end>.json)",
    )
    ap.add_argument(
        "--write-override",
        action="store_true",
        help="Write <NBA_LIVE_LENS_DIR>/live_lens_tuning_override.json with best adjustments.game_total (defaults to data/processed)",
    )
    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    days = _daterange(start, end)

    # Load signals
    sig_rows: list[dict[str, Any]] = []
    missing_signal_days: list[str] = []
    present_signal_days: list[str] = []
    for d in days:
        p = LIVE_LENS_DIR / f"live_lens_signals_{d.isoformat()}.jsonl"
        if not p.exists():
            missing_signal_days.append(d.isoformat())
            continue
        present_signal_days.append(d.isoformat())
        sig_rows.extend(_read_jsonl(p))

    if not sig_rows:
        print(
            json.dumps(
                {
                    "ok": True,
                    "note": "NO_SIGNALS",
                    "window": {"start": start.isoformat(), "end": end.isoformat()},
                    "present_signal_days": present_signal_days,
                    "missing_signal_days": missing_signal_days,
                },
                indent=2,
            )
        )
        return 0

    # Join to recon actual totals
    by_day_actuals: dict[str, dict[tuple[str, str], float]] = {}
    records: list[dict[str, Any]] = []
    dropped_no_actual = 0
    dropped_missing_fields = 0
    dropped_bad_line = 0

    seq = 0

    for rec in sig_rows:
        seq += 1
        ds = str(rec.get("date") or "").strip()
        if not ds:
            continue
        try:
            d0 = _parse_date(ds)
        except Exception:
            continue
        if d0 < start or d0 > end:
            continue

        line_total = _infer_line_total(rec)
        raw_diff = _infer_edge_raw(rec)
        adj_logged = _infer_edge_adj(rec)
        home_tri, away_tri = _infer_game_tris(rec)
        game_id = _infer_game_id(rec)

        market = str(rec.get("market") or "").strip().lower()
        horizon = str(rec.get("horizon") or "").strip().lower()
        # Only optimize game total adjustments.
        if market not in {"total", "game_total", "game-total"}:
            continue
        if horizon and horizon not in {"game"}:
            continue

        if line_total is None or raw_diff is None or adj_logged is None or home_tri is None or away_tri is None:
            dropped_missing_fields += 1
            continue

        # Guard against placeholder/invalid totals (e.g., 0) that will create nonsense edges.
        # NBA game totals should almost always be within a reasonable band.
        if not (100.0 <= float(line_total) <= 350.0):
            dropped_bad_line += 1
            continue

        if ds not in by_day_actuals:
            by_day_actuals[ds] = _load_recon_actuals_map(d0)
        actuals_map = by_day_actuals[ds]
        final_total = actuals_map.get((home_tri, away_tri))
        if final_total is None:
            # totals are order-invariant; try swapped
            final_total = actuals_map.get((away_tri, home_tri))
        if final_total is None:
            dropped_no_actual += 1
            continue

        ctx = _infer_context(rec)

        records.append(
            {
                "date": ds,
                "seq": int(seq),
                "game_id": game_id or f"{home_tri}_{away_tri}",
                "home_tri": home_tri,
                "away_tri": away_tri,
                "line_total": float(line_total),
                "edge_raw": float(raw_diff),
                "edge_adj_logged": float(adj_logged),
                "final_total": float(final_total),
                "pace_ratio": _to_float(ctx.get("pace_ratio")),
                "eff_ppp_delta": _to_float(ctx.get("eff_ppp_delta")),
                "exp_home_pace": _to_float(ctx.get("exp_home_pace")),
                "exp_away_pace": _to_float(ctx.get("exp_away_pace")),
                "elapsed_min": _to_float(ctx.get("elapsed_min")) if _to_float(ctx.get("elapsed_min")) is not None else _to_float(rec.get("elapsed")),
            }
        )

    if not records:
        print(
            json.dumps(
                {
                    "ok": True,
                    "rows": 0,
                    "note": "No joinable records (missing recon actuals or required fields)",
                    "present_signal_days": present_signal_days,
                    "missing_signal_days": missing_signal_days,
                    "dropped_missing_fields": int(dropped_missing_fields),
                    "dropped_bad_line": int(dropped_bad_line),
                    "dropped_no_actual": int(dropped_no_actual),
                },
                indent=2,
            )
        )
        return 0

    df = pd.DataFrame.from_records(records)

    bet_thr = float(args.bet_threshold)
    min_bets = int(args.min_bets)

    def _eval_from_edges(edge_arr: np.ndarray) -> dict[str, Any]:
        # pick earliest qualifying bet per game (avoid overcounting repeated logs)
        tmp = df[["date", "game_id", "seq", "line_total", "final_total"]].copy()
        tmp["edge"] = edge_arr
        tmp = tmp[np.isfinite(tmp["edge"].to_numpy(dtype=float))]
        tmp = tmp[np.abs(tmp["edge"].to_numpy(dtype=float)) >= bet_thr]
        if tmp.empty:
            return {"bets": 0, "win": 0, "loss": 0, "push": 0, "profit": 0.0, "roi_per_bet": 0.0}
        tmp = tmp.sort_values(["date", "game_id", "seq"], ascending=[True, True, True])
        tmp = tmp.groupby(["date", "game_id"], as_index=False).head(1)

        outcomes = {"win": 0, "loss": 0, "push": 0}
        profits: list[float] = []
        for _, r in tmp.iterrows():
            side = "over" if float(r["edge"]) > 0 else "under"
            o, pft = _outcome_profit(side, float(r["final_total"]), float(r["line_total"]), float(args.juice))
            outcomes[o] += 1
            profits.append(pft)
        return {
            "bets": int(len(tmp)),
            "win": int(outcomes["win"]),
            "loss": int(outcomes["loss"]),
            "push": int(outcomes["push"]),
            "profit": float(np.sum(profits)),
            "roi_per_bet": float(np.mean(profits)) if profits else 0.0,
        }

    base = _eval_from_edges(df["edge_adj_logged"].to_numpy(dtype=float))

    # Candidate search space (intentionally small; expand once we have more signal volume)
    grid = _candidate_grid(
        pace_ws=[0.0, 0.1, 0.25, 0.4],
        eff_ws=[0.0, 0.1, 0.25, 0.4],
        pace_caps=[1.5, 3.0, 4.5],
        eff_caps=[2.0, 4.0, 6.0],
        min_elapsed_mins=[0.0, 3.0, 6.0, 9.0],
    )

    best: dict[str, Any] | None = None
    best_key: tuple[float, float, int] | None = None

    # Pre-extract arrays for speed
    raw = df["edge_raw"].to_numpy(dtype=float)
    line = df["line_total"].to_numpy(dtype=float)
    pace_ratio = df["pace_ratio"].to_numpy(dtype=float, copy=True)
    eff = df["eff_ppp_delta"].to_numpy(dtype=float, copy=True)
    hpace = df["exp_home_pace"].to_numpy(dtype=float, copy=True)
    apace = df["exp_away_pace"].to_numpy(dtype=float, copy=True)
    elapsed = df["elapsed_min"].to_numpy(dtype=float, copy=True)

    # Treat NaNs as missing
    pace_ratio[~np.isfinite(pace_ratio)] = np.nan
    eff[~np.isfinite(eff)] = np.nan
    hpace[~np.isfinite(hpace)] = np.nan
    apace[~np.isfinite(apace)] = np.nan
    elapsed[~np.isfinite(elapsed)] = np.nan

    for p in grid:
        # Compute candidate adjusted edge for each logged snapshot.
        adj = np.empty_like(raw, dtype=float)
        for i in range(len(raw)):
            pr = None if not np.isfinite(pace_ratio[i]) else float(pace_ratio[i])
            ef = None if not np.isfinite(eff[i]) else float(eff[i])
            hp = None if not np.isfinite(hpace[i]) else float(hpace[i])
            ap = None if not np.isfinite(apace[i]) else float(apace[i])
            el = None if not np.isfinite(elapsed[i]) else float(elapsed[i])
            adj[i] = _apply_adjustment(
                raw_diff=float(raw[i]),
                line_total=float(line[i]),
                pace_ratio=pr,
                eff_ppp_delta=ef,
                exp_home_pace=hp,
                exp_away_pace=ap,
                elapsed_min=el,
                p=p,
            )

        metrics = _eval_from_edges(adj)
        n_keep = int(metrics["bets"])
        if n_keep < min_bets:
            continue

        profit_sum = float(metrics["profit"])
        roi = float(metrics["roi_per_bet"])

        # Primary: max total profit, then ROI, then more bets.
        key = (profit_sum, roi, n_keep)
        if best_key is None or key > best_key:
            best_key = key
            best = {
                "params": {
                    "pace_weight": float(p.pace_weight),
                    "eff_weight": float(p.eff_weight),
                    "pace_cap_points": float(p.pace_cap_points),
                    "eff_cap_points": float(p.eff_cap_points),
                    "min_elapsed_min": float(p.min_elapsed_min),
                },
                **metrics,
            }

    out = {
        "ok": True,
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "present_signal_days": present_signal_days,
        "missing_signal_days": missing_signal_days,
        "base_logged": base,
        "dropped_missing_fields": int(dropped_missing_fields),
        "dropped_bad_line": int(dropped_bad_line),
        "dropped_no_actual": int(dropped_no_actual),
        "assumptions": {
            "juice": float(args.juice),
            "bet_threshold": float(args.bet_threshold),
            "min_bets": int(args.min_bets),
            "note": "This simulates which bets would have fired from logged snapshots (WATCH+BET), and counts at most one bet per game (earliest qualifying snapshot) to avoid overcounting repeated logs.",
        },
        "best": best,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    print(json.dumps(out, indent=2))

    out_path = Path(args.out) if args.out else (LIVE_LENS_DIR / f"live_lens_adjustments_optimized_{start.isoformat()}_{end.isoformat()}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote: {out_path}")

    if bool(args.write_override) and best and isinstance(best.get("params"), dict):
        op = LIVE_LENS_DIR / "live_lens_tuning_override.json"
        op.parent.mkdir(parents=True, exist_ok=True)

        existing: dict[str, Any] = {}
        if op.exists():
            try:
                ex = json.loads(op.read_text(encoding="utf-8"))
                if isinstance(ex, dict):
                    existing = ex
            except Exception:
                existing = {}

        override: dict[str, Any] = dict(existing)
        adj = override.get("adjustments")
        if not isinstance(adj, dict):
            adj = {}
        adj = dict(adj)
        adj["game_total"] = {
            "enabled": True,
            **best["params"],
        }
        override["adjustments"] = adj

        override["trained"] = {
            "window": {"start": start.isoformat(), "end": end.isoformat()},
            "source": "optimize_live_lens_adjustments.py",
            "generated_at": out["generated_at"],
        }

        op.write_text(json.dumps(override, indent=2), encoding="utf-8")
        print(f"Wrote override: {op}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
