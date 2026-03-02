#!/usr/bin/env python3
"""Generate offline Live Lens signals JSONL for a given date.

This is a pragmatic unblocker for audit/ROI when we don't have real
POST-appended live logs.

Currently supported:
- Full-game totals (market="total")

Inputs:
- data/processed/game_cards_<date>.csv (market lines + game_id)
- data/processed/_predictions_backup_<date>.csv (model totals)
- data/processed/live_lens_tuning_override.json (optional; bias_points)

Output:
- data/processed/live_lens_signals_<date>.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


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


def _team_key(x: Any) -> str:
    s = str(x or "").strip().lower()
    # Keep letters only; normalize punctuation/spacing.
    return "".join([c for c in s if "a" <= c <= "z"])


def _load_tuning_bias_defaults() -> tuple[float, float]:
    """Returns (bias_points, bias_cap_points)."""
    path = PROCESSED / "live_lens_tuning_override.json"
    if not path.exists():
        return (0.0, 0.0)
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return (0.0, 0.0)
    try:
        gt = (((obj or {}).get("adjustments") or {}).get("game_total") or {})
        b = _n(gt.get("bias_points")) or 0.0
        cap = _n(gt.get("bias_cap_points")) or 0.0
        return (float(b), float(cap))
    except Exception:
        return (0.0, 0.0)


def _classify(abs_edge: float, watch: float, bet: float) -> str:
    if abs_edge >= bet:
        return "BET"
    if abs_edge >= watch:
        return "WATCH"
    return "NONE"


def main() -> int:
    ap = argparse.ArgumentParser(description="Offline generator for live_lens_signals_<date>.jsonl")
    ap.add_argument("--date", required=True, help="Target date YYYY-MM-DD")
    ap.add_argument(
        "--out",
        default=None,
        help="Output JSONL path (default: data/processed/live_lens_signals_<date>.jsonl)",
    )
    ap.add_argument(
        "--min-left",
        type=float,
        default=24.0,
        help="Assumed minutes remaining when the signal was generated (default: 24)",
    )
    ap.add_argument(
        "--watch",
        type=float,
        default=3.0,
        help="WATCH threshold for totals abs(edge_adj) (default: 3.0)",
    )
    ap.add_argument(
        "--bet",
        type=float,
        default=6.0,
        help="BET threshold for totals abs(edge_adj) (default: 6.0)",
    )
    ap.add_argument(
        "--bias-points",
        type=float,
        default=None,
        help="Override bias_points (default: read from live_lens_tuning_override.json; else 0)",
    )
    ap.add_argument(
        "--bias-cap-points",
        type=float,
        default=None,
        help="Override bias cap points (default: read from live_lens_tuning_override.json; else 0)",
    )
    args = ap.parse_args()

    ds = str(args.date).strip()
    out_path = Path(args.out) if args.out else (PROCESSED / f"live_lens_signals_{ds}.jsonl")

    cards_path = PROCESSED / f"game_cards_{ds}.csv"
    preds_path = PROCESSED / f"_predictions_backup_{ds}.csv"

    if not cards_path.exists():
        raise SystemExit(f"Missing {cards_path}")
    if not preds_path.exists():
        raise SystemExit(f"Missing {preds_path}")

    cards = pd.read_csv(cards_path)
    preds = pd.read_csv(preds_path)

    # Build a lookup from predictions by normalized team names.
    preds = preds.copy()
    preds["_home_key"] = preds.get("home_team", "").map(_team_key)
    preds["_away_key"] = preds.get("visitor_team", "").map(_team_key)
    pred_map: dict[tuple[str, str], dict[str, Any]] = {}
    for _, r in preds.iterrows():
        hk = str(r.get("_home_key") or "")
        ak = str(r.get("_away_key") or "")
        if not hk or not ak:
            continue
        k = (hk, ak)
        if k not in pred_map:
            pred_map[k] = dict(r)

    bias_default, cap_default = _load_tuning_bias_defaults()
    bias_points = float(args.bias_points) if args.bias_points is not None else float(bias_default)
    bias_cap = float(args.bias_cap_points) if args.bias_cap_points is not None else float(cap_default)

    min_left = float(args.min_left)
    min_left = max(0.0, min(48.0, min_left))
    elapsed_min = 48.0 - min_left
    frac = max(0.0, min(1.0, elapsed_min / 48.0))
    bias_eff = bias_points * frac
    if bias_cap and bias_cap > 0:
        bias_eff = max(-bias_cap, min(bias_cap, bias_eff))

    out_path.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    missing_pred = 0
    by_klass: dict[str, int] = {"BET": 0, "WATCH": 0, "NONE": 0}
    by_side: dict[str, int] = {"over": 0, "under": 0}

    now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    with out_path.open("w", encoding="utf-8") as f:
        for _, c in cards.iterrows():
            hk = _team_key(c.get("home_team"))
            ak = _team_key(c.get("visitor_team"))
            pr = pred_map.get((hk, ak))
            if not pr:
                missing_pred += 1
                continue

            game_id = str(c.get("game_id") or "").strip() or None
            home_tri = str(c.get("home_tri") or "").strip().upper() or None
            away_tri = str(c.get("away_tri") or "").strip().upper() or None
            commence_time = str(c.get("commence_time") or "").strip() or None

            live_line = _n(c.get("total"))
            pred_total_raw = _n(pr.get("totals"))
            if live_line is None or pred_total_raw is None:
                continue

            edge = float(pred_total_raw) - float(live_line)
            edge_adj = edge + float(bias_eff)

            # Mirror the frontend: only choose a side if the edge is meaningfully directional.
            side = None
            if edge_adj > 1.0:
                side = "over"
            elif edge_adj < -1.0:
                side = "under"

            klass = _classify(abs(edge_adj), float(args.watch), float(args.bet))
            if side is None:
                klass = "NONE"

            pred_adj = float(live_line) + float(edge_adj)

            obj = {
                "date": ds,
                "market": "total",
                "game_id": game_id,
                "home": home_tri,
                "away": away_tri,
                "live_line": float(live_line),
                "pred": float(pred_adj),
                "edge": float(edge),
                "edge_adj": float(edge_adj),
                "side": side,
                "klass": klass,
                "elapsed": float(elapsed_min),
                "remaining": float(min_left),
                "strength": float(abs(edge_adj)),
                "received_at": commence_time or now_iso,
                "signal_key": f"total::{game_id or (home_tri or '?') + '_' + (away_tri or '?')}::{side or 'none'}",
                "context": {
                    "source": "offline_generate_offline_live_lens_signals",
                    "pred_source": str(preds_path.name),
                    "cards_source": str(cards_path.name),
                    "pred_total_raw": float(pred_total_raw),
                    "bias_points": float(bias_points),
                    "bias_cap_points": float(bias_cap),
                    "bias_eff": float(bias_eff),
                    "bias_frac": float(frac),
                    "min_left_assumed": float(min_left),
                },
            }

            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            written += 1
            by_klass[klass] = by_klass.get(klass, 0) + 1
            if side in {"over", "under"}:
                by_side[side] = by_side.get(side, 0) + 1

    print(
        json.dumps(
            {
                "date": ds,
                "out": str(out_path),
                "cards": int(len(cards)),
                "pred_rows": int(len(preds)),
                "written": int(written),
                "missing_pred": int(missing_pred),
                "assumed": {
                    "min_left": float(min_left),
                    "elapsed": float(elapsed_min),
                    "bias_points": float(bias_points),
                    "bias_eff": float(bias_eff),
                    "bias_frac": float(frac),
                    "watch": float(args.watch),
                    "bet": float(args.bet),
                },
                "counts": {"klass": by_klass, "side": by_side},
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
