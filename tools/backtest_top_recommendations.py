"""Backtest accuracy of the *top N* exported recommendations.

Supports backtest kinds:

Props (`--kind props`):
    - Inputs: data/processed/props_recommendations_YYYY-MM-DD.csv + recon_props_YYYY-MM-DD.csv
    - Selection: one best play per player (prefers `top_play`, falls back to best from `plays`)

Games (`--kind games`):
    - Inputs: data/processed/recommendations_YYYY-MM-DD.csv + recon_games_YYYY-MM-DD.csv
    - Optional: data/processed/game_odds_YYYY-MM-DD.csv (fallback for missing price/line)
    - Selection: top N rows by EV (or edge)

Outputs:
    - a JSON summary (overall + by_date + by_market)
    - an optional CSV ledger with one row per bet

Notes:
    - "Accuracy" = wins / (wins + losses), pushes excluded.
    - "ROI" = profit_sum / graded_bets, where profit is per 1u stake.
        - `--kind slate_prob` calls the current Flask `/recommendations?view=slate` logic
            (SmartSim normal-approx win probability) and grades the returned props against
            `recon_props_YYYY-MM-DD.csv`.
"""

from __future__ import annotations

import argparse
import ast
import json
from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


def _dedupe_key(row: dict[str, Any]) -> tuple:
    return (
        str(row.get("date") or ""),
        _norm_name(row.get("player")),
        _tri_team(row.get("team")),
        str(row.get("market") or "").lower().strip(),
        str(row.get("side") or "").upper().strip(),
        float(row.get("line") or 0.0),
    )

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def _parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(str(s).strip())


def _iter_dates(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    cur = start
    while cur <= end:
        yield cur
        cur = cur + dt.timedelta(days=1)


def _american_to_decimal(a: Any) -> float | None:
    try:
        aa = float(a)
    except Exception:
        return None
    if aa == 0:
        return None
    if aa > 0:
        return 1.0 + (aa / 100.0)
    return 1.0 + (100.0 / abs(aa))


def _profit_per_unit(result: str, price: Any) -> float:
    if result == "P":
        return 0.0
    if result == "L":
        return -1.0
    dec = _american_to_decimal(price) or 1.909090909
    return float(dec - 1.0)


def _norm_name(s: Any) -> str:
    return str(s or "").strip().lower()


def _tri_team(s: Any) -> str:
    return str(s or "").strip().upper()


def _price_allowed(price: Any, max_plus_odds: float) -> bool:
    if max_plus_odds is None:
        return True
    try:
        mx = float(max_plus_odds)
    except Exception:
        return True
    if mx <= 0:
        return True
    p = _safe_float(price)
    if p is None:
        return True
    return float(p) <= float(mx)


@dataclass(frozen=True)
class Play:
    date: str
    player: str
    team: str
    market: str
    side: str
    line: float
    price: float | None
    ev: float | None
    ev_pct: float | None
    book: str | None


@dataclass(frozen=True)
class GamePick:
    date: str
    market: str
    side: str
    home: str
    away: str
    line: float | None
    price: float | None
    ev: float | None
    edge: float | None
    tier: str | None


def _safe_float(x: Any) -> float | None:
    try:
        v = float(pd.to_numeric(x, errors="coerce"))
    except Exception:
        return None
    return None if pd.isna(v) else float(v)


def _parse_top_play(v: Any) -> dict[str, Any] | None:
    if v is None:
        return None
    if isinstance(v, dict):
        return v
    s = str(v).strip()
    if not s or s == "nan":
        return None
    # Stored like "{'market': 'pts', ...}" (python literal)
    try:
        obj = ast.literal_eval(s)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _parse_plays_list(v: Any) -> list[dict[str, Any]] | None:
    if v is None:
        return None
    if isinstance(v, list):
        return [x for x in v if isinstance(x, dict)]
    s = str(v).strip()
    if not s or s == "nan":
        return None
    try:
        obj = ast.literal_eval(s)
    except Exception:
        return None
    if not isinstance(obj, list):
        return None
    return [x for x in obj if isinstance(x, dict)]


def _resolve_prop_play(market: str, side: str, line: float, stats: dict[str, float]) -> str | None:
    mkt = str(market or "").lower().strip()
    sd = str(side or "").upper().strip()

    actual: float | None = None
    if mkt == "pts":
        actual = stats.get("pts")
    elif mkt == "reb":
        actual = stats.get("reb")
    elif mkt == "ast":
        actual = stats.get("ast")
    elif mkt in {"threes", "3pt", "3pm"}:
        actual = stats.get("threes")
    elif mkt == "pra":
        actual = stats.get("pra")
    elif mkt == "pr":
        actual = (stats.get("pts", 0.0) + stats.get("reb", 0.0))
    elif mkt == "ra":
        actual = (stats.get("reb", 0.0) + stats.get("ast", 0.0))
    elif mkt == "pa":
        actual = (stats.get("pts", 0.0) + stats.get("ast", 0.0))

    if actual is None:
        return None

    if abs(float(actual) - float(line)) < 1e-9:
        return "P"

    if sd == "OVER":
        return "W" if float(actual) > float(line) else "L"
    if sd == "UNDER":
        return "W" if float(actual) < float(line) else "L"

    return None


def _load_recon_props(date_str: str) -> dict[tuple[str, str], dict[str, float]]:
    p = PROCESSED / f"recon_props_{date_str}.csv"
    if not p.exists():
        return {}
    df = pd.read_csv(p)
    if df is None or df.empty:
        return {}

    out: dict[tuple[str, str], dict[str, float]] = {}

    for _, r in df.iterrows():
        name = _norm_name(r.get("player_name") or r.get("player"))
        team = _tri_team(r.get("team_abbr") or r.get("team"))
        if not name or not team:
            continue

        stats: dict[str, float] = {}
        for k in ["pts", "reb", "ast", "threes", "pra"]:
            v = _safe_float(r.get(k))
            if v is not None:
                stats[k] = v
        out[(name, team)] = stats

    return out


def _load_recon_games(date_str: str) -> dict[tuple[str, str], tuple[float, float]]:
    p = PROCESSED / f"recon_games_{date_str}.csv"
    if not p.exists():
        return {}
    df = pd.read_csv(p)
    if df is None or df.empty:
        return {}

    out: dict[tuple[str, str], tuple[float, float]] = {}
    for _, r in df.iterrows():
        home = str(r.get("home_team") or "").strip()
        away = str(r.get("visitor_team") or "").strip()
        hp = _safe_float(r.get("home_pts"))
        ap = _safe_float(r.get("visitor_pts"))
        if not home or not away or hp is None or ap is None:
            continue
        out[(_norm_name(away), _norm_name(home))] = (float(ap), float(hp))
    return out


def _load_game_odds_row(date_str: str, home: str, away: str) -> dict[str, Any] | None:
    p = PROCESSED / f"game_odds_{date_str}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p)
    if df is None or df.empty:
        return None
    h = str(home or "").strip()
    a = str(away or "").strip()
    if not h or not a:
        return None

    # Exact match first, then a normalized fallback.
    m = df[(df.get("home_team").astype(str).str.strip() == h) & (df.get("visitor_team").astype(str).str.strip() == a)]
    if m is None or m.empty:
        m = df[
            (df.get("home_team").astype(str).str.strip().str.lower() == h.lower())
            & (df.get("visitor_team").astype(str).str.strip().str.lower() == a.lower())
        ]
    if m is None or m.empty:
        return None
    return dict(m.iloc[0].to_dict())


def _load_game_odds_map(date_str: str) -> dict[tuple[str, str], dict[str, Any]]:
    p = PROCESSED / f"game_odds_{date_str}.csv"
    if not p.exists():
        return {}
    df = pd.read_csv(p)
    if df is None or df.empty:
        return {}

    out: dict[tuple[str, str], dict[str, Any]] = {}
    for _, r in df.iterrows():
        home = str(r.get("home_team") or "").strip()
        away = str(r.get("visitor_team") or "").strip()
        if not home or not away:
            continue
        out[(_norm_name(away), _norm_name(home))] = dict(r.to_dict())
    return out


def _fill_game_price_line_from_odds(
    market: str,
    side: str,
    home: str,
    away: str,
    line: float | None,
    price: float | None,
    odds_row: dict[str, Any] | None,
) -> tuple[float | None, float | None]:
    if odds_row is None:
        return line, price

    mkt = str(market or "").upper().strip()
    sd = str(side or "").strip()
    used_line = line
    used_price = price

    if mkt == "ML":
        if used_price is None:
            used_price = odds_row.get("home_ml") if sd == home else odds_row.get("away_ml")
        return used_line, _safe_float(used_price)

    if mkt == "ATS":
        if used_line is None:
            used_line = odds_row.get("home_spread") if sd == home else odds_row.get("away_spread")
        if used_price is None:
            used_price = odds_row.get("home_spread_price") if sd == home else odds_row.get("away_spread_price")
        return _safe_float(used_line), _safe_float(used_price)

    if mkt == "TOTAL":
        if used_line is None:
            used_line = odds_row.get("total")
        if used_price is None:
            sd_l = sd.lower()
            used_price = odds_row.get("total_over_price") if sd_l.startswith("o") else odds_row.get("total_under_price")
        return _safe_float(used_line), _safe_float(used_price)

    return used_line, _safe_float(used_price)


def _settle_game_pick(
    market: str,
    side: str,
    line: float | None,
    price: float | None,
    home: str,
    away: str,
    finals: tuple[float, float],
    odds_row: dict[str, Any] | None,
) -> tuple[str, float, float | None]:
    ap, hp = finals
    mkt = str(market or "").upper().strip()
    sd = str(side or "").strip()

    used_line = line
    used_price = price

    if mkt == "ML":
        if used_price is None and odds_row is not None:
            used_price = odds_row.get("home_ml") if sd == home else odds_row.get("away_ml")
        home_won = hp > ap
        pick_home = sd == home
        res = "W" if (pick_home == home_won) else "L"
        prof = _profit_per_unit(res, used_price if used_price is not None else -110)
        return res, prof, None

    if mkt == "ATS":
        if used_line is None and odds_row is not None:
            used_line = odds_row.get("home_spread") if sd == home else odds_row.get("away_spread")
        if used_price is None and odds_row is not None:
            used_price = odds_row.get("home_spread_price") if sd == home else odds_row.get("away_spread_price")
        if used_line is None:
            raise ValueError("Missing ATS line")

        spread = float(used_line)
        if sd == home:
            diff = (hp + spread) - ap
        else:
            diff = (ap + spread) - hp

        if abs(diff) < 1e-9:
            res = "P"
        elif diff > 0:
            res = "W"
        else:
            res = "L"
        prof = _profit_per_unit(res, used_price if used_price is not None else -110)
        return res, prof, float(spread)

    if mkt == "TOTAL":
        if used_line is None and odds_row is not None:
            used_line = odds_row.get("total")
        if used_price is None and odds_row is not None:
            sd_l = sd.lower()
            used_price = odds_row.get("total_over_price") if sd_l.startswith("o") else odds_row.get("total_under_price")
        if used_line is None:
            raise ValueError("Missing TOTAL line")

        tot = float(used_line)
        pts = float(hp + ap)
        sd_l = sd.lower()
        if sd_l.startswith("o"):
            diff = pts - tot
        else:
            diff = tot - pts

        if abs(diff) < 1e-9:
            res = "P"
        elif diff > 0:
            res = "W"
        else:
            res = "L"
        prof = _profit_per_unit(res, used_price if used_price is not None else -110)
        return res, prof, float(tot)

    raise ValueError(f"Unknown market: {mkt}")


def _load_daily_top_games(date_str: str, top_n: int, sort_by: str, max_plus_odds: float) -> list[GamePick]:
    p = PROCESSED / f"recommendations_{date_str}.csv"
    if not p.exists():
        return []
    df = pd.read_csv(p)
    if df is None or df.empty:
        return []

    df = df.copy()
    df["market"] = df.get("market").astype(str).str.upper().str.strip()
    df["side"] = df.get("side").astype(str).str.strip()
    df["home"] = df.get("home").astype(str).str.strip()
    df["away"] = df.get("away").astype(str).str.strip()

    # keep only the markets we know how to settle
    df = df[df["market"].isin(["ML", "ATS", "TOTAL"])].copy()
    if df.empty:
        return []

    df["ev"] = pd.to_numeric(df.get("ev"), errors="coerce")
    df["edge"] = pd.to_numeric(df.get("edge"), errors="coerce")
    df["price"] = pd.to_numeric(df.get("price"), errors="coerce")
    df["line"] = pd.to_numeric(df.get("line"), errors="coerce")

    if float(max_plus_odds or 0.0) > 0.0:
        odds_map = _load_game_odds_map(date_str)
        used_prices: list[float | None] = []
        for _, r in df.iterrows():
            market = str(r.get("market") or "").upper().strip()
            side = str(r.get("side") or "").strip()
            home = str(r.get("home") or "").strip()
            away = str(r.get("away") or "").strip()
            odds_row = odds_map.get((_norm_name(away), _norm_name(home)))
            _, used_price = _fill_game_price_line_from_odds(
                market=market,
                side=side,
                home=home,
                away=away,
                line=_safe_float(r.get("line")),
                price=_safe_float(r.get("price")),
                odds_row=odds_row,
            )
            used_prices.append(used_price)
        df["_used_price_for_guard"] = used_prices
        df = df[df["_used_price_for_guard"].apply(lambda x: _price_allowed(x, max_plus_odds))].copy()
        if df.empty:
            return []

    if sort_by == "ev":
        df["rank"] = df["ev"].fillna(float("-inf"))
    elif sort_by == "edge":
        df["rank"] = df["edge"].fillna(float("-inf"))
    else:
        raise ValueError(f"Unknown --sort-by for games: {sort_by}")

    df = df.sort_values("rank", ascending=False).head(int(top_n))

    out: list[GamePick] = []
    for _, r in df.iterrows():
        market = str(r.get("market") or "").upper().strip()
        side = str(r.get("side") or "").strip()
        home = str(r.get("home") or "").strip()
        away = str(r.get("away") or "").strip()
        if not market or not side or not home or not away:
            continue

        line = _safe_float(r.get("line"))
        price = _safe_float(r.get("price"))
        ev = _safe_float(r.get("ev"))
        edge = _safe_float(r.get("edge"))
        tier = str(r.get("tier") or "").strip() or None

        # basic validity
        if market in {"ML", "ATS"} and side not in {home, away}:
            continue
        if market == "TOTAL" and not str(side).lower().startswith(("o", "u")):
            continue
        if market in {"ATS", "TOTAL"} and line is None:
            # allow odds fallback later
            line = None

        out.append(
            GamePick(
                date=date_str,
                market=market,
                side=side,
                home=home,
                away=away,
                line=float(line) if line is not None else None,
                price=float(price) if price is not None else None,
                ev=float(ev) if ev is not None else None,
                edge=float(edge) if edge is not None else None,
                tier=tier,
            )
        )

    return out


def _load_daily_top_props(date_str: str, top_n: int, sort_by: str, max_plus_odds: float) -> list[Play]:
    p = PROCESSED / f"props_recommendations_{date_str}.csv"
    if not p.exists():
        return []

    df = pd.read_csv(p)
    if df is None or df.empty:
        return []

    def _rank_value(d: dict[str, Any]) -> float:
        if sort_by == "ev_pct":
            v = _safe_float(d.get("ev_pct"))
        elif sort_by == "ev":
            v = _safe_float(d.get("ev"))
        else:
            raise ValueError(f"Unknown --sort-by: {sort_by}")
        return float(v) if v is not None else float("-inf")

    plays: list[Play] = []

    for _, r in df.iterrows():
        player = str(r.get("player") or "").strip()
        team = str(r.get("team") or "").strip().upper()
        if not player or not team:
            continue

        top_play = _parse_top_play(r.get("top_play"))
        if float(max_plus_odds or 0.0) > 0.0:
            if top_play is not None and _price_allowed(top_play.get("price"), max_plus_odds):
                pass
            else:
                candidates = _parse_plays_list(r.get("plays")) or []
                candidates = [c for c in candidates if _price_allowed(c.get("price"), max_plus_odds)]
                if not candidates:
                    continue
                top_play = max(candidates, key=_rank_value)
        else:
            if top_play is None:
                candidates = _parse_plays_list(r.get("plays"))
                if not candidates:
                    continue
                top_play = max(candidates, key=_rank_value)

        market = str(top_play.get("market") or "").strip().lower()
        side = str(top_play.get("side") or "").strip().upper()
        line = _safe_float(top_play.get("line"))
        price = _safe_float(top_play.get("price"))
        ev = _safe_float(top_play.get("ev"))
        ev_pct = _safe_float(top_play.get("ev_pct"))
        book = str(top_play.get("book") or "").strip() or None

        if not market or side not in {"OVER", "UNDER"} or line is None:
            continue

        plays.append(
            Play(
                date=date_str,
                player=player,
                team=team,
                market=market,
                side=side,
                line=float(line),
                price=float(price) if price is not None else None,
                ev=float(ev) if ev is not None else None,
                ev_pct=float(ev_pct) if ev_pct is not None else None,
                book=book,
            )
        )

    if not plays:
        return []

    if sort_by == "ev_pct":
        key = lambda pl: (pl.ev_pct if pl.ev_pct is not None else float("-inf"))
    elif sort_by == "ev":
        key = lambda pl: (pl.ev if pl.ev is not None else float("-inf"))
    else:
        raise ValueError(f"Unknown --sort-by: {sort_by}")

    plays = sorted(plays, key=key, reverse=True)
    return plays[: int(top_n)]


def _empty_summary() -> dict[str, Any]:
    return {
        "available": False,
        "dates": 0,
        "bets": 0,
        "wins": 0,
        "losses": 0,
        "pushes": 0,
        "accuracy": None,
        "roi": None,
        "profit_sum": None,
    }


def _summarize_ledger(df: pd.DataFrame) -> dict[str, Any]:
    if df is None or df.empty:
        return _empty_summary()

    w = int((df["result"] == "W").sum())
    l = int((df["result"] == "L").sum())
    p = int((df["result"] == "P").sum())
    graded = int(df["result"].isin(["W", "L", "P"]).sum())

    profit_sum = float(pd.to_numeric(df.get("profit"), errors="coerce").sum())
    roi = profit_sum / max(1, graded)
    acc = w / max(1, (w + l))

    return {
        "available": True,
        "dates": int(df["date"].nunique()) if "date" in df.columns else None,
        "bets": int(len(df)),
        "wins": w,
        "losses": l,
        "pushes": p,
        "accuracy": acc,
        "roi": roi,
        "profit_sum": profit_sum,
    }


def backtest_props_recommendations(
    start: dt.date,
    end: dt.date,
    top_n: int,
    sort_by: str,
    max_plus_odds: float,
) -> tuple[dict[str, Any], pd.DataFrame]:
    rows: list[dict[str, Any]] = []

    for d in _iter_dates(start, end):
        dstr = d.isoformat()
        picks = _load_daily_top_props(dstr, top_n=top_n, sort_by=sort_by, max_plus_odds=max_plus_odds)
        if not picks:
            continue

        recon = _load_recon_props(dstr)
        if not recon:
            continue

        for pl in picks:
            stats = recon.get((_norm_name(pl.player), _tri_team(pl.team)))
            if not stats:
                continue

            res = _resolve_prop_play(pl.market, pl.side, pl.line, stats)
            if res is None:
                continue

            price = pl.price if pl.price is not None else -110.0
            profit = _profit_per_unit(res, price)

            rows.append(
                {
                    "date": pl.date,
                    "player": pl.player,
                    "team": pl.team,
                    "market": pl.market,
                    "side": pl.side,
                    "line": pl.line,
                    "price": price,
                    "ev": pl.ev,
                    "ev_pct": pl.ev_pct,
                    "book": pl.book,
                    "result": res,
                    "profit": profit,
                }
            )

    df = pd.DataFrame(rows)
    summary = _summarize_ledger(df)

    # breakouts
    if df is not None and not df.empty:
        try:
            summary["by_market"] = {
                str(m): _summarize_ledger(gdf)
                for m, gdf in df.groupby("market", dropna=False)
            }
        except Exception:
            summary["by_market"] = {}

        try:
            summary["by_date"] = {
                str(dd): {
                    "bets": int(len(gdf)),
                    "wins": int((gdf["result"] == "W").sum()),
                    "losses": int((gdf["result"] == "L").sum()),
                    "pushes": int((gdf["result"] == "P").sum()),
                    "profit_sum": float(pd.to_numeric(gdf.get("profit"), errors="coerce").sum()),
                }
                for dd, gdf in df.groupby("date", dropna=False)
            }
        except Exception:
            summary["by_date"] = {}

    return summary, df


def backtest_games_recommendations(
    start: dt.date,
    end: dt.date,
    top_n: int,
    sort_by: str,
    max_plus_odds: float,
) -> tuple[dict[str, Any], pd.DataFrame]:
    rows: list[dict[str, Any]] = []

    for d in _iter_dates(start, end):
        dstr = d.isoformat()
        picks = _load_daily_top_games(dstr, top_n=top_n, sort_by=sort_by, max_plus_odds=max_plus_odds)
        if not picks:
            continue

        recon = _load_recon_games(dstr)
        if not recon:
            continue

        for pk in picks:
            finals = recon.get((_norm_name(pk.away), _norm_name(pk.home)))
            if not finals:
                continue

            odds_row = _load_game_odds_row(dstr, home=pk.home, away=pk.away)
            try:
                res, profit, used_line = _settle_game_pick(
                    market=pk.market,
                    side=pk.side,
                    line=pk.line,
                    price=pk.price,
                    home=pk.home,
                    away=pk.away,
                    finals=finals,
                    odds_row=odds_row,
                )
            except Exception:
                continue

            used_price = pk.price
            if used_price is None and odds_row is not None:
                if pk.market == "ML":
                    used_price = _safe_float(odds_row.get("home_ml") if pk.side == pk.home else odds_row.get("away_ml"))
                elif pk.market == "ATS":
                    used_price = _safe_float(
                        odds_row.get("home_spread_price") if pk.side == pk.home else odds_row.get("away_spread_price")
                    )
                elif pk.market == "TOTAL":
                    sd_l = pk.side.lower()
                    used_price = _safe_float(odds_row.get("total_over_price") if sd_l.startswith("o") else odds_row.get("total_under_price"))

            rows.append(
                {
                    "date": pk.date,
                    "market": pk.market,
                    "side": pk.side,
                    "home": pk.home,
                    "away": pk.away,
                    "line": used_line,
                    "price": float(used_price) if used_price is not None else -110.0,
                    "ev": pk.ev,
                    "edge": pk.edge,
                    "tier": pk.tier,
                    "result": res,
                    "profit": profit,
                }
            )

    df = pd.DataFrame(rows)
    summary = _summarize_ledger(df)

    if df is not None and not df.empty:
        try:
            summary["by_market"] = {str(m): _summarize_ledger(gdf) for m, gdf in df.groupby("market", dropna=False)}
        except Exception:
            summary["by_market"] = {}

        try:
            summary["by_date"] = {
                str(dd): {
                    "bets": int(len(gdf)),
                    "wins": int((gdf["result"] == "W").sum()),
                    "losses": int((gdf["result"] == "L").sum()),
                    "pushes": int((gdf["result"] == "P").sum()),
                    "profit_sum": float(pd.to_numeric(gdf.get("profit"), errors="coerce").sum()),
                }
                for dd, gdf in df.groupby("date", dropna=False)
            }
        except Exception:
            summary["by_date"] = {}

    return summary, df


def _load_slate_prob_picks(
    date_str: str,
    n_game: int,
    n_market: int,
    scope: str,
    max_plus_odds: float,
    smart_sim_prefix: str,
    rank: str,
    p_shrink: float,
    max_per_player: int,
    min_prob: float | None,
    min_ev: float | None,
    w_ev: float | None,
    w_prob: float | None,
    w_z: float | None,
    w_unc: float | None,
    w_ctx: float | None,
    w_pace: float | None,
    w_inj: float | None,
    w_blowout: float | None,
) -> list[dict[str, Any]]:
    """Load picks from the current `/recommendations?view=slate` implementation.

    We call the Flask test client to ensure the backtest is always aligned to the
    exact codepath the app serves.
    """
    try:
        import sys
        root_s = str(ROOT)
        if root_s not in sys.path:
            sys.path.insert(0, root_s)
        import app as _app
    except Exception:
        return []

    try:
        _app.app.testing = True
        client = _app.app.test_client()
        sim_pref = str(smart_sim_prefix or "smart_sim").strip() or "smart_sim"
        rk = str(rank or "ev").strip().lower() or "ev"
        try:
            ps = float(p_shrink)
        except Exception:
            ps = 1.0
        ps = max(0.0, min(1.0, float(ps)))
        try:
            mpp = int(max_per_player)
        except Exception:
            mpp = 1
        if mpp <= 0:
            mpp = 1
        qs = (
            f"/recommendations?format=json&view=slate&date={date_str}"
            f"&n_game={int(n_game)}&n_market={int(n_market)}&refresh=1"
            f"&smart_sim_prefix={sim_pref}"
            f"&rank={rk}&p_shrink={ps}&max_per_player={mpp}"
            f"&max_plus_odds={float(max_plus_odds or 0.0)}"
            + (f"&min_prob={float(min_prob)}" if (min_prob is not None) else "")
            + (f"&min_ev={float(min_ev)}" if (min_ev is not None) else "")
            + (f"&w_ev={float(w_ev)}" if (w_ev is not None) else "")
            + (f"&w_prob={float(w_prob)}" if (w_prob is not None) else "")
            + (f"&w_z={float(w_z)}" if (w_z is not None) else "")
            + (f"&w_unc={float(w_unc)}" if (w_unc is not None) else "")
            + (f"&w_ctx={float(w_ctx)}" if (w_ctx is not None) else "")
            + (f"&w_pace={float(w_pace)}" if (w_pace is not None) else "")
            + (f"&w_inj={float(w_inj)}" if (w_inj is not None) else "")
            + (f"&w_blowout={float(w_blowout)}" if (w_blowout is not None) else "")
        )
        payload = client.get(qs).get_json(silent=True)
    except Exception:
        payload = None

    if not isinstance(payload, dict):
        return []

    out: list[dict[str, Any]] = []
    sc = str(scope or "union").strip().lower()

    def _add_pick(p: dict[str, Any], source: str) -> None:
        try:
            if not isinstance(p, dict):
                return
            price = _safe_float(p.get("price"))
            if (price is not None) and (not _price_allowed(price, max_plus_odds=max_plus_odds)):
                return
            out.append(
                {
                    "date": date_str,
                    "source": source,
                    "player": p.get("player"),
                    "team": p.get("team"),
                    "market": p.get("market"),
                    "side": p.get("side"),
                    "line": _safe_float(p.get("line")),
                    "price": price,
                    "book": p.get("book"),
                    "win_prob": _safe_float(p.get("win_prob")),
                    "sim_mean": _safe_float(p.get("sim_mean")),
                    "sim_sd": _safe_float(p.get("sim_sd")),
                }
            )
        except Exception:
            return

    if sc in {"per_game", "both", "union"}:
        for g in payload.get("per_game") or []:
            if not isinstance(g, dict):
                continue
            for p in g.get("picks") or []:
                _add_pick(p, source="per_game")

    if sc in {"per_market", "both", "union"}:
        pm = payload.get("per_market") or {}
        if isinstance(pm, dict):
            for _, arr in pm.items():
                if not isinstance(arr, list):
                    continue
                for p in arr:
                    _add_pick(p, source="per_market")

    # Deduplicate across sources.
    seen: set[tuple] = set()
    deduped: list[dict[str, Any]] = []
    for r in out:
        if r.get("line") is None:
            continue
        k = _dedupe_key(r)
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)
    return deduped


def backtest_slate_prob(
    start: dt.date,
    end: dt.date,
    n_game: int,
    n_market: int,
    scope: str,
    max_plus_odds: float,
    smart_sim_prefix: str,
    rank: str,
    p_shrink: float,
    max_per_player: int,
    min_prob: float | None,
    min_ev: float | None,
    w_ev: float | None = None,
    w_prob: float | None = None,
    w_z: float | None = None,
    w_unc: float | None = None,
    w_ctx: float | None = None,
    w_pace: float | None = None,
    w_inj: float | None = None,
    w_blowout: float | None = None,
) -> tuple[dict[str, Any], pd.DataFrame]:
    rows: list[dict[str, Any]] = []

    for d in _iter_dates(start, end):
        dstr = d.isoformat()
        picks = _load_slate_prob_picks(
            dstr,
            n_game=int(n_game),
            n_market=int(n_market),
            scope=str(scope or "union"),
            max_plus_odds=float(max_plus_odds or 0.0),
            smart_sim_prefix=str(smart_sim_prefix or "smart_sim_pregame"),
            rank=str(rank or "ev"),
            p_shrink=float(p_shrink),
            max_per_player=int(max_per_player),
            min_prob=min_prob,
            min_ev=min_ev,
            w_ev=w_ev,
            w_prob=w_prob,
            w_z=w_z,
            w_unc=w_unc,
            w_ctx=w_ctx,
            w_pace=w_pace,
            w_inj=w_inj,
            w_blowout=w_blowout,
        )
        if not picks:
            continue

        recon = _load_recon_props(dstr)
        if not recon:
            continue

        for pl in picks:
            stats = recon.get((_norm_name(pl.get("player")), _tri_team(pl.get("team"))))
            if not stats:
                continue
            res = _resolve_prop_play(
                str(pl.get("market") or ""),
                str(pl.get("side") or ""),
                float(pl.get("line") or 0.0),
                stats,
            )
            if res is None:
                continue

            used_price = pl.get("price") if pl.get("price") is not None else -110.0
            profit = _profit_per_unit(res, used_price)

            rows.append(
                {
                    **pl,
                    "result": res,
                    "profit": profit,
                    "price": float(used_price),
                }
            )

    df = pd.DataFrame(rows)
    summary = _summarize_ledger(df)

    if df is not None and not df.empty:
        try:
            summary["by_market"] = {str(m): _summarize_ledger(gdf) for m, gdf in df.groupby("market", dropna=False)}
        except Exception:
            summary["by_market"] = {}
        try:
            summary["by_date"] = {
                str(dd): {
                    "bets": int(len(gdf)),
                    "wins": int((gdf["result"] == "W").sum()),
                    "losses": int((gdf["result"] == "L").sum()),
                    "pushes": int((gdf["result"] == "P").sum()),
                    "profit_sum": float(pd.to_numeric(gdf.get("profit"), errors="coerce").sum()),
                }
                for dd, gdf in df.groupby("date", dropna=False)
            }
        except Exception:
            summary["by_date"] = {}

    return summary, df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", choices=["props", "games", "slate_prob"], default="props")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--top-n", type=int, default=25, help="Top N per date")
    ap.add_argument("--sort-by", choices=["ev", "ev_pct", "edge"], default="ev")
    ap.add_argument("--n-game", type=int, default=3, help="For --kind slate_prob: top N per game")
    ap.add_argument("--n-market", type=int, default=4, help="For --kind slate_prob: top N per market")
    ap.add_argument(
        "--smart-sim-prefix",
        type=str,
        default="smart_sim_pregame",
        help=(
            "For --kind slate_prob: SmartSim JSON prefix to use (default: smart_sim_pregame). "
            "Use smart_sim for historical/in-game simulations (often inflates backtests vs true pregame)."
        ),
    )
    ap.add_argument(
        "--scope",
        choices=["union", "per_game", "per_market", "both"],
        default="union",
        help="For --kind slate_prob: which parts of the slate payload to grade.",
    )
    ap.add_argument(
        "--slate-rank",
        default="ev",
        choices=["ev", "prob", "z", "combo"],
        help=(
            "For --kind slate_prob: rank picks by sim EV (profit), sim win_prob (accuracy), sim z-score, "
            "or a sim-rooted combo score (EV + context)."
        ),
    )
    ap.add_argument(
        "--p-shrink",
        type=float,
        default=1.0,
        help="For --kind slate_prob: shrink probability extremes toward 0.5 (0..1).",
    )
    ap.add_argument(
        "--max-per-player",
        type=int,
        default=1,
        help="For --kind slate_prob: max props per player across the slate selection.",
    )
    ap.add_argument(
        "--min-prob",
        type=float,
        default=None,
        help="For --kind slate_prob: minimum (possibly shrunk) sim win probability to include.",
    )
    ap.add_argument(
        "--min-ev",
        type=float,
        default=None,
        help="For --kind slate_prob: minimum sim EV per 1u to include (uses sim win prob + price).",
    )

    # Combo rank weights (passed to /recommendations?view=slate as w_* query params)
    ap.add_argument("--w-ev", type=float, default=None, help="For --kind slate_prob + --slate-rank combo: weight on EV")
    ap.add_argument("--w-prob", type=float, default=None, help="For --kind slate_prob + --slate-rank combo: weight on (win_prob-0.5)")
    ap.add_argument("--w-z", type=float, default=None, help="For --kind slate_prob + --slate-rank combo: weight on z-score")
    ap.add_argument("--w-unc", type=float, default=None, help="For --kind slate_prob + --slate-rank combo: weight on uncertainty penalty")
    ap.add_argument("--w-ctx", type=float, default=None, help="For --kind slate_prob + --slate-rank combo: weight on team/opponent context")
    ap.add_argument("--w-pace", type=float, default=None, help="For --kind slate_prob + --slate-rank combo: weight on game pace context")
    ap.add_argument("--w-inj", type=float, default=None, help="For --kind slate_prob + --slate-rank combo: weight on injuries context")
    ap.add_argument("--w-blowout", type=float, default=None, help="For --kind slate_prob + --slate-rank combo: weight on blowout risk penalty")
    ap.add_argument(
        "--max-plus-odds",
        type=float,
        default=0.0,
        help="If >0, exclude picks with price above this +odds threshold (e.g. 125 => no picks above +125).",
    )
    ap.add_argument("--out-json", default=str(PROCESSED / "top_props_recommendations_backtest.json"))
    ap.add_argument("--out-ledger-csv", default="")
    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    if end < start:
        start, end = end, start

    kind = str(args.kind)
    sort_by = str(args.sort_by)

    if kind == "props":
        if sort_by not in {"ev", "ev_pct"}:
            raise SystemExit("For --kind props, --sort-by must be ev or ev_pct")
        summary, ledger = backtest_props_recommendations(
            start=start,
            end=end,
            top_n=int(args.top_n),
            sort_by=sort_by,
            max_plus_odds=float(args.max_plus_odds or 0.0),
        )
        payload_key = "props_recommendations"
    elif kind == "games":
        if sort_by not in {"ev", "edge"}:
            raise SystemExit("For --kind games, --sort-by must be ev or edge")
        summary, ledger = backtest_games_recommendations(
            start=start,
            end=end,
            top_n=int(args.top_n),
            sort_by=sort_by,
            max_plus_odds=float(args.max_plus_odds or 0.0),
        )
        payload_key = "games_recommendations"
    else:
        summary, ledger = backtest_slate_prob(
            start=start,
            end=end,
            n_game=int(args.n_game),
            n_market=int(args.n_market),
            scope=str(args.scope or "union"),
            max_plus_odds=float(args.max_plus_odds or 0.0),
            smart_sim_prefix=str(args.smart_sim_prefix or "smart_sim"),
            rank=str(args.slate_rank or "ev"),
            p_shrink=float(args.p_shrink),
            max_per_player=int(args.max_per_player),
            min_prob=args.min_prob,
            min_ev=args.min_ev,
            w_ev=args.w_ev,
            w_prob=args.w_prob,
            w_z=args.w_z,
            w_unc=args.w_unc,
            w_ctx=args.w_ctx,
            w_pace=args.w_pace,
            w_inj=args.w_inj,
            w_blowout=args.w_blowout,
        )
        payload_key = "slate_prob"

    payload = {
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "selection": {
            "kind": kind,
            "top_n_per_day": int(args.top_n) if kind in {"props", "games"} else None,
            "sort_by": sort_by if kind in {"props", "games"} else "sim_win_prob",
            "n_game": int(args.n_game) if kind == "slate_prob" else None,
            "n_market": int(args.n_market) if kind == "slate_prob" else None,
            "smart_sim_prefix": str(args.smart_sim_prefix) if kind == "slate_prob" else None,
            "scope": str(args.scope) if kind == "slate_prob" else None,
            "max_plus_odds": float(args.max_plus_odds or 0.0),
            "slate_rank": str(args.slate_rank) if kind == "slate_prob" else None,
            "p_shrink": float(args.p_shrink) if kind == "slate_prob" else None,
            "max_per_player": int(args.max_per_player) if kind == "slate_prob" else None,
            "min_prob": float(args.min_prob) if (kind == "slate_prob" and args.min_prob is not None) else None,
            "min_ev": float(args.min_ev) if (kind == "slate_prob" and args.min_ev is not None) else None,
            "combo_weights": (
                {
                    "w_ev": (None if args.w_ev is None else float(args.w_ev)),
                    "w_prob": (None if args.w_prob is None else float(args.w_prob)),
                    "w_z": (None if args.w_z is None else float(args.w_z)),
                    "w_unc": (None if args.w_unc is None else float(args.w_unc)),
                    "w_ctx": (None if args.w_ctx is None else float(args.w_ctx)),
                    "w_pace": (None if args.w_pace is None else float(args.w_pace)),
                    "w_inj": (None if args.w_inj is None else float(args.w_inj)),
                    "w_blowout": (None if args.w_blowout is None else float(args.w_blowout)),
                }
                if kind == "slate_prob"
                else None
            ),
        },
        payload_key: summary,
    }

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if args.out_ledger_csv:
        out_csv = Path(args.out_ledger_csv)
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        ledger.to_csv(out_csv, index=False)

    def _fmt_pct(x: Any) -> float | None:
        return None if x is None else round(100.0 * float(x), 2)

    s = summary
    label = "Props" if kind == "props" else ("Games" if kind == "games" else "SlateProb")
    if s.get("available"):
        print(
            f"{label} top-N recommendations:",
            {
                "dates": s.get("dates"),
                "bets": s.get("bets"),
                "acc_pct": _fmt_pct(s.get("accuracy")),
                "roi_pct": _fmt_pct(s.get("roi")),
                "profit": round(float(s.get("profit_sum") or 0.0), 3),
            },
        )
    else:
        print(f"{label} top-N recommendations: unavailable (missing files or no recon coverage)")

    print(f"Wrote: {out_json}")
    if args.out_ledger_csv:
        print(f"Wrote: {Path(args.out_ledger_csv)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
