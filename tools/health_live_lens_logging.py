#!/usr/bin/env python3
"""Live Lens logging health check.

This is a lightweight go-forward self-test to ensure the closed-loop logging
artifacts are being created correctly.

What it checks:
- Server is reachable (/api/version)
- Live Lens tuning endpoint is reachable (/api/live_lens_tuning)
- Signal logging endpoint works and appends JSONL (/api/live_lens_signal)
- Projection logging endpoint works and appends JSONL (/api/live_lens_projection)
- Download endpoints can fetch created artifacts

It uses a dedicated far-future test date (default 2099-01-01) to avoid
polluting real processed dates.

By default it cleans up the test artifacts after verifying them.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
LIVE_LENS_DIR = Path((os.getenv("NBA_LIVE_LENS_DIR") or os.getenv("LIVE_LENS_DIR") or "").strip() or str(PROCESSED))


def _post_json(url: str, payload: dict[str, Any], timeout: float = 8.0) -> tuple[int, str]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "ignore")
        return int(resp.status), body


def _get(url: str, timeout: float = 8.0) -> tuple[int, str]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "ignore")
        return int(resp.status), body


def _try_get(url: str, timeout: float = 8.0) -> tuple[int, str]:
    try:
        return _get(url, timeout=timeout)
    except urllib.error.HTTPError as e:
        return int(e.code), e.read().decode("utf-8", "ignore")


def _try_post_json(url: str, payload: dict[str, Any], timeout: float = 8.0) -> tuple[int, str]:
    try:
        return _post_json(url, payload, timeout=timeout)
    except urllib.error.HTTPError as e:
        return int(e.code), e.read().decode("utf-8", "ignore")


def _file_len(path: Path) -> int:
    try:
        return path.stat().st_size
    except Exception:
        return -1


def main() -> int:
    ap = argparse.ArgumentParser(description="Live Lens logging health check")
    ap.add_argument("--base-url", default="http://127.0.0.1:5051", help="Server base URL")
    ap.add_argument("--date", default="2099-01-01", help="Test date used for artifacts")
    ap.add_argument("--no-cleanup", action="store_true", help="Keep test JSONL artifacts")
    ap.add_argument("--timeout", type=float, default=8.0, help="HTTP timeout seconds")
    args = ap.parse_args()

    base = str(args.base_url).rstrip("/")
    ds = str(args.date).strip()
    timeout = float(args.timeout)

    LIVE_LENS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Base URL: {base}")
    print(f"Test date: {ds}")

    # 1) Basic reachability
    st, body = _try_get(f"{base}/api/version", timeout=timeout)
    if st != 200:
        print("FAIL: /api/version", st, body[:200])
        return 2
    print("OK: /api/version")

    st, body = _try_get(f"{base}/api/live_lens_tuning", timeout=timeout)
    if st != 200:
        print("FAIL: /api/live_lens_tuning", st, body[:200])
        return 2
    print("OK: /api/live_lens_tuning")

    sig_path = LIVE_LENS_DIR / f"live_lens_signals_{ds}.jsonl"
    proj_path = LIVE_LENS_DIR / f"live_lens_projections_{ds}.jsonl"

    sig_before = _file_len(sig_path)
    proj_before = _file_len(proj_path)

    # 2) Signal logging
    sig_payload = {
        "date": ds,
        "schema_version": 2,
        "healthcheck": True,
        "market": "player_prop",
        "klass": "WATCH",
        "game_id": "0026000001",
        "home": "BOS",
        "away": "NYK",
        "player": "Health Check",
        "name_key": "HEALTH CHECK",
        "stat": "pts",
        "side": "OVER",
        "line": 20.5,
        "strength": 2.5,
        "remaining": 18,
        "context": {"price_over": -110, "price_under": -110},
    }
    st, body = _try_post_json(f"{base}/api/live_lens_signal", sig_payload, timeout=timeout)
    if st != 200:
        print("FAIL: /api/live_lens_signal", st, body[:200])
        return 2
    print("OK: /api/live_lens_signal")

    # 2b) Game market signal logging (ATS) - validates that ATS context fields persist.
    ats_payload = {
        "date": ds,
        "schema_version": 2,
        "healthcheck": True,
        "market": "ats",
        "klass": "WATCH",
        "horizon": "game",
        "game_id": "0026000001",
        "home": "BOS",
        "away": "NYK",
        "elapsed": 24,
        "remaining": 24,
        "total_points": 120,
        # Use picked-side spread, consistent with the client logger.
        "live_line": -3.5,
        # Use a stable side key (team tricode), consistent with the client logger.
        "side": "BOS",
        "edge": 1.2,
        "edge_raw": 1.2,
        "edge_adj": 1.2,
        "strength": 1.2,
        # Mirrors the ATS context shape derived in web/cards.js.
        "context": {
            "thr_watch": 1.5,
            "thr_bet": 3.0,
            "spr_home": -3.5,
            "spr_home_raw": -3.5,
            "pregame_margin_mean": 0.5,
            "cur_margin_home": 2.0,
            "elapsed_min": 24.0,
            "blend_w": 0.5,
            "adj_margin_home": 1.25,
            "edge_home": 1.25 + (-3.5),
            "edge_away": -1.25 - (-3.5),
            "pick_home": 1,
        },
    }
    st, body = _try_post_json(f"{base}/api/live_lens_signal", ats_payload, timeout=timeout)
    if st != 200:
        print("FAIL: /api/live_lens_signal (ats)", st, body[:200])
        return 2
    print("OK: /api/live_lens_signal (ats)")

    # 3) Projection logging
    proj_payload = {
        "date": ds,
        "schema_version": 2,
        "healthcheck": True,
        "market": "player_prop",
        "klass": "WATCH",
        "game_id": "0026000001",
        "home": "BOS",
        "away": "NYK",
        "proj_key": "player_prop:Health Check:pts:OVER:20.5",
        "player": "Health Check",
        "name_key": "HEALTH CHECK",
        "stat": "pts",
        "side": "OVER",
        "line": 20.5,
        "proj": 22.1,
        "win_prob_over": 0.55,
        "win_prob_under": 0.45,
        "price_over": -110,
        "price_under": -110,
        "strength": 2.5,
        "remaining": 18,
    }
    st, body = _try_post_json(f"{base}/api/live_lens_projection", proj_payload, timeout=timeout)
    if st != 200:
        print("FAIL: /api/live_lens_projection", st, body[:200])
        return 2
    print("OK: /api/live_lens_projection")

    # 4) File checks (give filesystem a beat)
    time.sleep(0.15)

    sig_after = _file_len(sig_path)
    proj_after = _file_len(proj_path)

    if sig_after <= max(-1, sig_before):
        print("FAIL: signals jsonl did not grow", "before=", sig_before, "after=", sig_after, str(sig_path))
        return 2
    if proj_after <= max(-1, proj_before):
        print("FAIL: projections jsonl did not grow", "before=", proj_before, "after=", proj_after, str(proj_path))
        return 2

    if not sig_path.exists():
        print("FAIL: missing signals file", str(sig_path))
        return 2
    if not proj_path.exists():
        print("FAIL: missing projections file", str(proj_path))
        return 2

    print("OK: artifacts written")
    print(" -", sig_path)
    print(" -", proj_path)

    # 5) Download endpoints (should return 200 now that files exist)
    st, body = _try_get(f"{base}/api/download_live_lens_signals?date={ds}", timeout=timeout)
    if st != 200:
        print("FAIL: download signals", st, body[:200])
        return 2
    print("OK: download signals")

    st, body = _try_get(f"{base}/api/download_live_lens_projections?date={ds}", timeout=timeout)
    if st != 200:
        print("FAIL: download projections", st, body[:200])
        return 2
    print("OK: download projections")

    # 6) Cleanup
    if not bool(args.no_cleanup):
        try:
            if sig_path.exists():
                sig_path.unlink()
            if proj_path.exists():
                proj_path.unlink()
            print("OK: cleaned up test artifacts")
        except Exception as e:
            print("WARN: cleanup failed:", str(e))

    print("PASS: Live Lens logging is functional")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
