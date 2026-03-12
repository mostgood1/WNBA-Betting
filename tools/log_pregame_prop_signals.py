"""Emit pregame prop recommendations as Live Lens signals.

This script pulls pregame prop recommendations from the existing `/api/cards` payload
and logs the actionable ones into the Live Lens JSONL stream via `/api/live_lens_signal`.

Why:
  - Reuse the existing Live Lens settlement + ROI + audit tooling for pregame props.
  - Keep the signal schema compatible with the existing `player_prop` contract.

Notes:
  - Signals are logged with `market="player_prop"` and `horizon="pregame"`.
  - By default, only guidance action "Bet now" is emitted (klass=BET).

Example:
  python tools/log_pregame_prop_signals.py --base-url http://127.0.0.1:5051 --date 2026-03-08
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen


def _canon_nba_game_id(game_id: Any) -> str:
    try:
        raw = str(game_id or "").strip()
    except Exception:
        return ""
    digits = "".join([c for c in raw if c.isdigit()])
    if len(digits) == 8:
        return "00" + digits
    if len(digits) == 9:
        return "0" + digits
    return digits


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v) or (not math.isfinite(v)):
            return None
        return v
    except Exception:
        return None


def _norm_player_name(s: Any) -> str:
    if s is None:
        return ""
    t = str(s)
    if "(" in t:
        t = t.split("(", 1)[0]
    t = t.replace("-", " ")
    t = t.replace(".", "").replace("'", "").replace(",", " ").strip()
    for suf in [" JR", " SR", " II", " III", " IV"]:
        if t.upper().endswith(suf):
            t = t[: -len(suf)]
    try:
        import unicodedata as _ud

        t = _ud.normalize("NFKD", t)
        t = t.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    return t.upper().strip()


def _http_json(method: str, url: str, *, payload: dict[str, Any] | None, timeout_sec: float) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"

    req = Request(url, data=data, method=method.upper(), headers=headers)
    try:
        max_attempts = int(float(os.environ.get("NBA_HTTP_RETRY_ATTEMPTS") or 3))
    except Exception:
        max_attempts = 3
    try:
        retry_backoff_sec = float(os.environ.get("NBA_HTTP_RETRY_BACKOFF_SEC") or 1.5)
    except Exception:
        retry_backoff_sec = 1.5
    max_attempts = max(1, min(5, max_attempts))
    retry_backoff_sec = max(0.0, min(10.0, retry_backoff_sec))

    for attempt in range(1, max_attempts + 1):
        try:
            with urlopen(req, timeout=float(timeout_sec)) as resp:
                raw = resp.read() or b""
            obj = json.loads(raw.decode("utf-8")) if raw else {}
            return obj if isinstance(obj, dict) else {"_raw": obj}
        except HTTPError as e:
            body = ""
            try:
                body = (e.read() or b"").decode("utf-8", errors="replace")
            except Exception:
                body = ""
            if e.code in {502, 503, 504} and attempt < max_attempts:
                time.sleep(retry_backoff_sec * attempt)
                continue
            raise RuntimeError(f"HTTP {e.code} for {url}: {body[:500]}")
        except URLError as e:
            if attempt < max_attempts:
                time.sleep(retry_backoff_sec * attempt)
                continue
            raise RuntimeError(f"URL error for {url}: {e}")

    raise RuntimeError(f"HTTP request failed after {max_attempts} attempts for {url}")


def _build_signal(*, ds: str, game: dict[str, Any], rec: dict[str, Any]) -> dict[str, Any] | None:
    best = rec.get("best")
    if not isinstance(best, dict):
        return None

    guidance = best.get("guidance")
    guidance = guidance if isinstance(guidance, dict) else {}

    player = str(rec.get("player") or "").strip() or None
    if not player:
        return None

    stat = str(best.get("market") or "").strip().lower() or None
    side = str(best.get("side") or "").strip().upper() or None
    line = _safe_float(best.get("line"))
    if not stat or not side or line is None:
        return None

    sim_mu = _safe_float(best.get("sim_mu"))
    edge = (float(sim_mu) - float(line)) if (sim_mu is not None) else None
    strength = abs(float(edge)) if edge is not None else None

    price = _safe_float(best.get("price"))

    sim = game.get("sim") if isinstance(game.get("sim"), dict) else {}
    gid = _canon_nba_game_id(sim.get("game_id") or sim.get("gameId") or game.get("game_id") or "")

    home_tri = str(game.get("home_tri") or "").strip().upper() or None
    away_tri = str(game.get("away_tri") or "").strip().upper() or None

    name_key = _norm_player_name(rec.get("name_key") or player)
    if not name_key:
        return None

    ctx: dict[str, Any] = {
        "source": "pregame_cards",
        "book": (str(best.get("book") or "").strip().lower() or None),
        "price": price,
        "price_over": (price if side == "OVER" else None),
        "price_under": (price if side == "UNDER" else None),
        "sim_mu": sim_mu,
        "sim_sd": _safe_float(best.get("sim_sd")),
        "p_win": _safe_float(best.get("p_win")),
        "ev": _safe_float(best.get("ev")),
        "ev_pct": _safe_float(best.get("ev_pct")),
        "guidance": guidance,
        "within_play_to": (bool(guidance.get("within_play_to")) if ("within_play_to" in guidance) else None),
        "books_count": (int(guidance.get("books_count")) if str(guidance.get("books_count") or "").strip().isdigit() else None),
        "play_to_line": _safe_float(guidance.get("play_to_line")),
        "edge_cushion": _safe_float(guidance.get("edge_cushion")),
    }

    action = str(guidance.get("action") or "").strip()
    signal_key = f"pregame|player_prop|{gid or (home_tri or '')+'@'+(away_tri or '')}|{name_key}|{stat}|{side}|{line}"

    return {
        "date": ds,
        "horizon": "pregame",
        "market": "player_prop",
        "klass": "BET",
        "signal_key": signal_key,
        "action": action or None,
        "game_id": gid or None,
        "game_id_canon": gid or None,
        "home": home_tri,
        "away": away_tri,
        "team_tri": (str(rec.get("team") or "").strip().upper() or None),
        "player": player,
        "name_key": name_key,
        "stat": stat,
        "side": side,
        "line": float(line),
        "line_source": "oddsapi",
        "edge": edge,
        "strength": strength,
        "elapsed": 0.0,
        "remaining": 48.0,
        "context": ctx,
    }


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Log pregame prop recommendations as Live Lens signals")
    p.add_argument(
        "--base-url",
        default=(os.environ.get("NBA_API_BASE_URL") or os.environ.get("NBA_LIVE_LENS_BASE_URL") or "http://127.0.0.1:5051"),
        help="Base URL for the Flask app (default: http://127.0.0.1:5051)",
    )
    p.add_argument(
        "--date",
        default="",
        help="Slate date YYYY-MM-DD (default: app 'today' via /api/cards)",
    )
    p.add_argument(
        "--actions",
        default="Bet now",
        help='Comma-separated guidance actions to emit (default: "Bet now")',
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not POST signals; only print counts",
    )
    p.add_argument(
        "--timeout-sec",
        type=float,
        default=float(os.environ.get("NBA_HTTP_TIMEOUT_SEC") or 20.0),
        help="HTTP timeout seconds (default: 20)",
    )
    args = p.parse_args(argv)

    base_url = str(args.base_url or "").strip().rstrip("/") + "/"
    actions = {a.strip() for a in str(args.actions or "").split(",") if a.strip()}
    if not actions:
        actions = {"Bet now"}

    cards_url = urljoin(base_url, "api/cards")
    params = {}
    if str(args.date or "").strip():
        params["date"] = str(args.date).strip()
    if params:
        cards_url = cards_url + "?" + urlencode(params)

    cards = _http_json("GET", cards_url, payload=None, timeout_sec=float(args.timeout_sec))
    if not cards or not isinstance(cards, dict):
        print("ERROR: /api/cards returned non-object JSON")
        return 2
    if cards.get("error"):
        print(f"ERROR: /api/cards error: {cards.get('error')}")
        return 2

    # Derive ds used for logging. Prefer the requested date, else use API's first game date, else today.
    ds = str(args.date or "").strip()
    if not ds:
        try:
            games0 = cards.get("games") if isinstance(cards.get("games"), list) else []
            if games0 and isinstance(games0[0], dict) and games0[0].get("date"):
                ds = str(games0[0].get("date")).strip()
        except Exception:
            ds = ""
    if not ds:
        ds = datetime.utcnow().date().isoformat()

    games = cards.get("games") if isinstance(cards.get("games"), list) else []
    posted = 0
    candidates = 0

    post_url = urljoin(base_url, "api/live_lens_signal")

    for game in games:
        if not isinstance(game, dict):
            continue
        prop_recs = game.get("prop_recommendations")
        if not isinstance(prop_recs, dict):
            continue

        for side_key in ("home", "away"):
            rows = prop_recs.get(side_key)
            if not isinstance(rows, list):
                continue
            for rec in rows:
                if not isinstance(rec, dict):
                    continue

                best = rec.get("best")
                guidance = (best.get("guidance") if isinstance(best, dict) else None)
                action = str((guidance or {}).get("action") or "").strip()
                if action not in actions:
                    continue

                sig = _build_signal(ds=ds, game=game, rec=rec)
                if sig is None:
                    continue
                candidates += 1

                if args.dry_run:
                    continue

                resp = _http_json("POST", post_url, payload=sig, timeout_sec=float(args.timeout_sec))
                if not resp.get("ok"):
                    print(f"WARN: failed POST: {resp}")
                    continue
                posted += 1

    mode = "DRY_RUN" if args.dry_run else "POST"
    print(f"[{mode}] date={ds} actions={sorted(actions)} candidates={candidates} posted={posted}")
    if not args.dry_run and posted == 0 and candidates > 0:
        print("WARN: had candidates but posted=0; check server logs")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
