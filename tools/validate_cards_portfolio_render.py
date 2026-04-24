import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def derive_season(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.year + 1 if dt.month >= 7 else dt.year


def load_remote_json(base_url: str, path: str) -> dict:
    url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    with urlopen(Request(url, headers={"Cache-Control": "no-cache"}), timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def load_remote_text(base_url: str, path: str) -> str:
    url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    with urlopen(Request(url, headers={"Cache-Control": "no-cache"}), timeout=60) as response:
        return response.read().decode("utf-8", errors="replace")


def build_local_client():
    import app

    client = app.app.test_client()

    def get_json(path: str) -> dict:
        response = client.get(path)
        payload = response.get_json(silent=True)
        if response.status_code >= 400:
            raise RuntimeError(f"{path} returned HTTP {response.status_code}: {payload}")
        if not isinstance(payload, dict):
            raise RuntimeError(f"{path} returned non-object JSON")
        return payload

    def get_text(path: str) -> str:
        response = client.get(path)
        if response.status_code >= 400:
            raise RuntimeError(f"{path} returned HTTP {response.status_code}")
        return response.get_data(as_text=True)

    return get_json, get_text


def count_portfolio_rows_from_cards(cards_payload: dict) -> int:
    total = 0
    for game in cards_payload.get("games") or []:
        if not isinstance(game, dict):
            continue
        for row in game.get("game_market_recommendations") or []:
            if isinstance(row, dict) and (row.get("stake_amount") is not None or row.get("portfolio_rank") is not None):
                total += 1
        prop_map = game.get("prop_recommendations") if isinstance(game.get("prop_recommendations"), dict) else {}
        for side_key in ("away", "home"):
            for row in prop_map.get(side_key) or []:
                if isinstance(row, dict) and (row.get("stake_amount") is not None or row.get("portfolio_rank") is not None):
                    total += 1
    return total


def count_portfolio_rows_from_season_day(day_payload: dict) -> int:
    total = 0
    for game in day_payload.get("games") or []:
        if not isinstance(game, dict):
            continue
        betting = game.get("betting") if isinstance(game.get("betting"), dict) else {}
        for row in betting.get("officialRows") or []:
            if isinstance(row, dict) and (row.get("stake_amount") is not None or row.get("portfolio_rank") is not None):
                total += 1
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate cards and betting-card portfolio rendering inputs.")
    parser.add_argument("--date", required=True, help="Slate date YYYY-MM-DD")
    parser.add_argument("--season", type=int, default=None, help="Season year for /season/<season>/betting-card")
    parser.add_argument("--profile", default="retuned", help="Season betting-card profile")
    parser.add_argument("--props-source", default="source", help="props_source for /api/cards")
    parser.add_argument("--base-url", default="", help="Optional remote base URL; omit to use Flask test_client")
    args = parser.parse_args()

    season = int(args.season or derive_season(args.date))
    cards_query = urlencode({"date": args.date, "props_source": args.props_source})
    season_query = urlencode({"date": args.date, "profile": args.profile})
    cards_api_path = f"/api/cards?{cards_query}"
    season_day_api_path = f"/api/season/{season}/betting-card/day/{args.date}?profile={args.profile}"
    cards_page_path = f"/?date={args.date}"
    season_page_path = f"/season/{season}/betting-card?{season_query}"

    try:
        if args.base_url:
            get_json = lambda path: load_remote_json(args.base_url, path)
            get_text = lambda path: load_remote_text(args.base_url, path)
        else:
            get_json, get_text = build_local_client()

        cards_page_html = get_text(cards_page_path)
        season_page_html = get_text(season_page_path)
        cards_payload = get_json(cards_api_path)
        season_day_payload = get_json(season_day_api_path)
    except (RuntimeError, HTTPError, URLError, json.JSONDecodeError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2))
        return 1

    cards_games = [game for game in cards_payload.get("games") or [] if isinstance(game, dict)]
    season_games = [game for game in season_day_payload.get("games") or [] if isinstance(game, dict)]
    cards_portfolio = cards_payload.get("pregame_portfolio") if isinstance(cards_payload.get("pregame_portfolio"), dict) else {}
    season_portfolio = season_day_payload.get("pregame_portfolio") if isinstance(season_day_payload.get("pregame_portfolio"), dict) else {}
    cards_portfolio_rows = count_portfolio_rows_from_cards(cards_payload)
    season_portfolio_rows = count_portfolio_rows_from_season_day(season_day_payload)

    checks = {
        "cards_page_marker": "cardsSourceMeta" in cards_page_html and "cards-parity.js" in cards_page_html,
        "season_page_marker": "bettingCardDayTitle" in season_page_html and "betting-card-v2.js" in season_page_html,
        "cards_payload_object": isinstance(cards_payload, dict),
        "season_day_payload_object": isinstance(season_day_payload, dict),
    }

    if cards_games:
        checks["cards_portfolio_enabled"] = bool(cards_portfolio.get("enabled"))
        checks["season_portfolio_enabled"] = bool(season_portfolio.get("enabled"))
        checks["selected_counts_match"] = int(cards_portfolio.get("selected") or 0) == int(season_portfolio.get("selected") or 0)
        checks["candidate_counts_match"] = int(cards_portfolio.get("candidates") or 0) == int(season_portfolio.get("candidates") or 0)
        checks["cards_ranked_rows_match_selected"] = int(cards_portfolio.get("selected") or 0) == cards_portfolio_rows
        checks["season_ranked_rows_match_selected"] = int(season_portfolio.get("selected") or 0) == season_portfolio_rows
    else:
        checks["no_slate"] = True

    ok = all(bool(value) for value in checks.values())
    summary = {
        "ok": ok,
        "mode": "remote" if args.base_url else "local",
        "date": args.date,
        "season": season,
        "cards_games": len(cards_games),
        "season_games": len(season_games),
        "cards_selected": int(cards_portfolio.get("selected") or 0),
        "cards_candidates": int(cards_portfolio.get("candidates") or 0),
        "cards_portfolio_rows": cards_portfolio_rows,
        "season_selected": int(season_portfolio.get("selected") or 0),
        "season_candidates": int(season_portfolio.get("candidates") or 0),
        "season_portfolio_rows": season_portfolio_rows,
        "checks": checks,
    }
    print(json.dumps(summary, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())