#!/usr/bin/env python3
"""Quick sanity check: do our Live Lens JSONL logs contain new fields?

This is intentionally lightweight and does NOT require recon outputs.

Reads:
- data/processed/live_lens_signals_<date>.jsonl

Prints counts by market for:
- pred present
- context.edge_shrink_lambda present

Usage:
  python tools/check_live_lens_log_fields.py --date 2026-02-13
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
LIVE_LENS_DIR = Path((os.getenv("NBA_LIVE_LENS_DIR") or os.getenv("LIVE_LENS_DIR") or "").strip() or str(PROCESSED))


def _parse_date(s: str) -> _date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _safe_get_ctx(obj: dict[str, Any]) -> dict[str, Any] | None:
    ctx = obj.get("context")
    return ctx if isinstance(ctx, dict) else None


def main() -> int:
    ap = argparse.ArgumentParser(description="Check Live Lens JSONL log fields")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    args = ap.parse_args()

    if args.date:
        ds = _parse_date(args.date).isoformat()
    else:
        ds = _date.today().isoformat()

    fp = LIVE_LENS_DIR / f"live_lens_signals_{ds}.jsonl"
    if not fp.exists():
        print(f"Missing: {fp}")
        return 2

    total = 0
    by_market: dict[str, dict[str, int]] = {}

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

            total += 1
            market = str(obj.get("market") or "").strip() or "(missing)"
            ent = by_market.setdefault(
                market,
                {
                    "n": 0,
                    "pred_present": 0,
                    "edge_shrink_lambda_present": 0,
                },
            )
            ent["n"] += 1

            if obj.get("pred") is not None:
                ent["pred_present"] += 1

            ctx = _safe_get_ctx(obj)
            if ctx and ctx.get("edge_shrink_lambda") is not None:
                ent["edge_shrink_lambda_present"] += 1

    print(f"Date: {ds}")
    print(f"Rows: {total}")
    for mkt in sorted(by_market.keys()):
        ent = by_market[mkt]
        n = ent["n"]
        pp = ent["pred_present"]
        sl = ent["edge_shrink_lambda_present"]
        print(f"- {mkt}: n={n} pred={pp}/{n} shrink_lambda={sl}/{n}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
