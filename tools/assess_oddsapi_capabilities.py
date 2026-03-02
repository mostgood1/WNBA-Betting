from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import Any

import pandas as pd
import requests

from nba_betting.config import paths
from nba_betting.odds_api import (
    NBA_SPORT_KEY,
    ODDS_HOST,
    OddsApiConfig,
    discover_event_market_keys,
    fetch_event_odds_current,
    list_events_current,
)


def _headers() -> dict[str, str]:
    return {"Accept": "application/json", "User-Agent": "nba-betting/1.0"}


def _usage_headers(resp: requests.Response) -> dict[str, Any]:
    h = {str(k).lower(): v for k, v in (resp.headers or {}).items()}
    out: dict[str, Any] = {}
    for k in [
        "x-requests-remaining",
        "x-requests-used",
        "x-requests-last",
        "x-requests-per-minute-remaining",
        "x-requests-per-minute-used",
    ]:
        if k in h:
            out[k] = h.get(k)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Assess OddsAPI key capabilities for NBA.")
    ap.add_argument("--date", type=str, default="", help="Target date YYYY-MM-DD (US/Eastern day). Defaults to today UTC.")
    ap.add_argument("--api-key", type=str, default=os.environ.get("ODDS_API_KEY", ""), help="OddsAPI key (or set ODDS_API_KEY)")
    ap.add_argument("--regions", type=str, default="us", help="OddsAPI regions (e.g. us, us2)")
    ap.add_argument("--event-id", type=str, default="", help="Optional: assess a specific event id")
    ap.add_argument(
        "--sample-markets",
        type=str,
        default="h2h,spreads,totals,player_points,player_rebounds,player_assists,player_threes",
        help="Comma-separated list of markets to sample via /odds for the chosen event",
    )
    args = ap.parse_args()

    if not args.api_key:
        raise SystemExit("Missing OddsAPI key. Provide --api-key or set ODDS_API_KEY.")

    try:
        target = pd.to_datetime(args.date).date() if args.date else pd.Timestamp.utcnow().date()
    except Exception:
        raise SystemExit("Invalid --date; expected YYYY-MM-DD")

    cfg = OddsApiConfig(api_key=args.api_key, regions=(args.regions or "us").strip() or "us")

    # Hit events endpoint directly once to capture quota headers.
    events_url = f"{ODDS_HOST}/v4/sports/{NBA_SPORT_KEY}/events"
    usage: dict[str, Any] = {}
    try:
        r = requests.get(events_url, params={"apiKey": args.api_key}, headers=_headers(), timeout=45)
        r.raise_for_status()
        usage = _usage_headers(r)
    except Exception:
        usage = {}

    events = list_events_current(cfg, pd.to_datetime(str(target)))

    chosen_event: dict[str, Any] | None = None
    if args.event_id:
        for ev in events:
            if str(ev.get("id")) == str(args.event_id):
                chosen_event = ev
                break
    if chosen_event is None and events:
        chosen_event = events[0]

    event_id = str(chosen_event.get("id")) if chosen_event else ""

    discovered: list[str] = []
    if event_id:
        discovered = sorted(discover_event_market_keys(cfg, event_id))

    sample_markets = [m.strip() for m in str(args.sample_markets or "").split(",") if m.strip()]
    sampled_df = pd.DataFrame()
    if event_id and sample_markets:
        sampled_df = fetch_event_odds_current(cfg, event_id, sample_markets)

    books: list[dict[str, Any]] = []
    try:
        if sampled_df is not None and not sampled_df.empty:
            cols = [c for c in ["bookmaker", "bookmaker_title"] if c in sampled_df.columns]
            if cols:
                books = (
                    sampled_df[cols]
                    .dropna()
                    .drop_duplicates()
                    .sort_values(cols)
                    .to_dict(orient="records")
                )
    except Exception:
        books = []

    summary: dict[str, Any] = {
        "asof_utc": datetime.utcnow().isoformat() + "Z",
        "target_date": str(target),
        "regions": cfg.regions,
        "usage_headers": usage,
        "events_count": int(len(events)),
        "chosen_event": {
            "event_id": event_id,
            "commence_time": (chosen_event.get("commence_time") if chosen_event else None),
            "home_team": (chosen_event.get("home_team") if chosen_event else None),
            "away_team": (chosen_event.get("away_team") if chosen_event else None),
        },
        "discovered_markets_count": int(len(discovered)),
        "discovered_markets": discovered,
        "sample_markets": sample_markets,
        "sample_rows": int(0 if sampled_df is None else len(sampled_df)),
        "sample_markets_returned": (
            sorted([str(x) for x in sampled_df["market"].dropna().astype(str).unique()])
            if (sampled_df is not None and ("market" in sampled_df.columns) and (not sampled_df.empty))
            else []
        ),
        "sample_bookmakers": books,
    }

    # Persist
    try:
        paths.data_processed.mkdir(parents=True, exist_ok=True)
        out = paths.data_processed / f"oddsapi_capabilities_{target}.json"
        out.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        summary["output"] = str(out)
    except Exception:
        pass

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
