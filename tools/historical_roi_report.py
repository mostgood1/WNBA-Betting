"""Historical ROI report for games + props.

Computes ROI/accuracy over a date window.

Default mode (candidate re-scoring):
Games:
- Inputs: recommendations_YYYY-MM-DD.csv + recon_games_YYYY-MM-DD.csv + game_odds_YYYY-MM-DD.csv
- Selection: score each row, de-dupe to 1 per game, take top N per date.

Props:
- Inputs: props_edges_YYYY-MM-DD.csv + recon_props_YYYY-MM-DD.csv
- Selection: same regular-market filters used elsewhere, score, de-dupe to 1 per player, take top N per date.

Snapshot mode (authoritative surfaced picks):
- Games: best_edges_games_YYYY-MM-DD.csv
- Props: best_edges_props_YYYY-MM-DD.csv

Settlement:
- Games: uses recon_games for final scores; uses snapshot line/price when present; ML falls back to game_odds for price if needed.
- Props: uses recon_props for actual stats; uses snapshot line/price.

Writes a JSON report and prints a concise summary.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"

from nba_betting.scoring import GameScoreConfig, PropScoreConfig, score_game_pick_0_100, score_prop_pick_0_100


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def _american_to_decimal(a) -> float | None:
    try:
        aa = float(a)
    except Exception:
        return None
    if aa == 0:
        return None
    if aa > 0:
        return 1.0 + (aa / 100.0)
    return 1.0 + (100.0 / abs(aa))


def _profit_per_unit(result: str, price) -> float:
    if result == "P":
        return 0.0
    if result == "L":
        return -1.0
    dec = _american_to_decimal(price) or 1.909090909
    return float(dec - 1.0)


def _iter_dates_with(prefix: str, start: datetime, end: datetime) -> list[str]:
    out: list[str] = []
    for p in sorted(PROCESSED.glob(f"{prefix}_*.csv")):
        d = p.stem.replace(f"{prefix}_", "")
        try:
            dt = _parse_date(d)
        except Exception:
            continue
        if start.date() <= dt.date() <= end.date():
            out.append(d)
    return sorted(set(out))


def _load_csv(path: Path) -> pd.DataFrame | None:
    try:
        if path.exists():
            df = pd.read_csv(path)
            return df if df is not None and not df.empty else pd.DataFrame()
    except Exception:
        return None
    return None


def _empty_summary() -> dict:
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


def _num_or_none(x) -> float | None:
    try:
        v = float(pd.to_numeric(x, errors="coerce"))
    except Exception:
        return None
    return None if pd.isna(v) else float(v)


def backtest_games(start: datetime, end: datetime, top_n: int, cfg: GameScoreConfig, use_snapshots: bool) -> dict:
    recon_dates = set(_iter_dates_with("recon_games", start, end))
    odds_dates = set(_iter_dates_with("game_odds", start, end))
    if use_snapshots:
        pick_dates = set(_iter_dates_with("best_edges_games", start, end))
        dates = sorted(pick_dates & recon_dates)
    else:
        rec_dates = set(_iter_dates_with("recommendations", start, end))
        dates = sorted(rec_dates & recon_dates & odds_dates)

    ledger: list[dict] = []

    for d in dates:
        if use_snapshots:
            rec = _load_csv(PROCESSED / f"best_edges_games_{d}.csv")
        else:
            rec = _load_csv(PROCESSED / f"recommendations_{d}.csv")
        fin = _load_csv(PROCESSED / f"recon_games_{d}.csv")
        odds = _load_csv(PROCESSED / f"game_odds_{d}.csv")
        if rec is None or fin is None or rec.empty or fin.empty:
            continue

        rec = rec.copy()
        rec["market"] = rec.get("market").astype(str).str.upper()
        rec["home"] = rec.get("home").astype(str)
        rec["away"] = rec.get("away").astype(str)

        if use_snapshots:
            if "score" in rec.columns:
                rec["score_100"] = pd.to_numeric(rec.get("score"), errors="coerce")
            elif "best_edge_value" in rec.columns:
                rec["score_100"] = pd.to_numeric(rec.get("best_edge_value"), errors="coerce")
            else:
                rec["score_100"] = 0.0
        else:
            scores = []
            for _, r in rec.iterrows():
                s, _, _ = score_game_pick_0_100(
                    market=r.get("market"), ev=r.get("ev"), edge=r.get("edge"), price=r.get("price"), cfg=cfg
                )
                scores.append(s)
            rec["score_100"] = scores

        rec["game_key"] = (
            rec.get("away").astype(str).str.strip().str.lower()
            + "@@"
            + rec.get("home").astype(str).str.strip().str.lower()
        )
        rec = (
            rec.sort_values(["game_key", "score_100"], ascending=[True, False])
            .groupby("game_key", as_index=False, sort=False)
            .head(1)
        )
        rec = rec.sort_values("score_100", ascending=False).head(int(top_n))

        fin = fin.copy()
        fin["home_team"] = fin.get("home_team").astype(str)
        fin["visitor_team"] = fin.get("visitor_team").astype(str)
        f_map: dict[tuple[str, str], tuple[float, float]] = {}
        for _, rr in fin.iterrows():
            key = (str(rr.get("visitor_team")).strip(), str(rr.get("home_team")).strip())
            hp = pd.to_numeric(rr.get("home_pts"), errors="coerce")
            ap = pd.to_numeric(rr.get("visitor_pts"), errors="coerce")
            if pd.notna(hp) and pd.notna(ap):
                f_map[key] = (float(ap), float(hp))

        if odds is not None and not odds.empty:
            odds = odds.copy()
            odds["home_team"] = odds.get("home_team").astype(str)
            odds["visitor_team"] = odds.get("visitor_team").astype(str)

        for _, r in rec.iterrows():
            home = str(r.get("home") or "").strip()
            away = str(r.get("away") or "").strip()
            market = str(r.get("market") or "").upper().strip()
            side = str(r.get("side") or "").strip()
            score = float(r.get("score_100") or 0.0)

            finals = f_map.get((away, home))
            if not finals:
                continue
            ap, hp = finals

            o0 = None
            if odds is not None and not odds.empty:
                o = odds[(odds.get("home_team") == home) & (odds.get("visitor_team") == away)]
                if not o.empty:
                    o0 = o.iloc[0]

            result = None
            profit = None

            if market == "ML":
                home_won = hp > ap
                pick_home = (side == home)
                price = r.get("price")
                if (price is None or pd.isna(price)) and o0 is not None:
                    price = o0.get("home_ml") if pick_home else o0.get("away_ml")
                result = "W" if (pick_home == home_won) else "L"
                profit = _profit_per_unit(result, price)

            elif market == "ATS":
                margin = hp - ap
                line = _num_or_none(r.get("line"))
                if line is None and o0 is not None:
                    hsp = _num_or_none(o0.get("home_spread"))
                    if hsp is not None:
                        line = float(hsp) if side == home else float(-hsp)
                if line is None:
                    continue

                if side == home:
                    diff = margin + float(line)
                    fallback_price = o0.get("home_spread_price") if o0 is not None else None
                else:
                    diff = (-margin) + float(line)
                    fallback_price = o0.get("away_spread_price") if o0 is not None else None

                price = r.get("price")
                if price is None or pd.isna(price):
                    price = fallback_price
                if abs(diff) < 1e-9:
                    result = "P"
                elif diff > 0:
                    result = "W"
                else:
                    result = "L"
                profit = _profit_per_unit(result, price if price is not None else -110)

            elif market == "TOTAL":
                pts = hp + ap
                tot = _num_or_none(r.get("line"))
                if tot is None and o0 is not None:
                    tot = _num_or_none(o0.get("total"))
                if tot is None:
                    continue
                side_l = str(side).lower()
                if side_l.startswith("o"):
                    diff = pts - float(tot)
                    fallback_price = o0.get("total_over_price") if o0 is not None else None
                else:
                    diff = float(tot) - pts
                    fallback_price = o0.get("total_under_price") if o0 is not None else None

                price = r.get("price")
                if price is None or pd.isna(price):
                    price = fallback_price
                if abs(diff) < 1e-9:
                    result = "P"
                elif diff > 0:
                    result = "W"
                else:
                    result = "L"
                profit = _profit_per_unit(result, price if price is not None else -110)

            else:
                continue

            ledger.append({"date": d, "market": market, "score": score, "result": result, "profit": profit})

    df = pd.DataFrame(ledger)
    if df.empty:
        return _empty_summary()

    def _group_summary(gdf: pd.DataFrame) -> dict:
        if gdf.empty:
            return _empty_summary()
        n = int(len(gdf))
        w = int((gdf["result"] == "W").sum())
        l = int((gdf["result"] == "L").sum())
        p = int((gdf["result"] == "P").sum())
        graded = int(gdf["result"].isin(["W", "L", "P"]).sum())
        profit_sum = float(pd.to_numeric(gdf["profit"], errors="coerce").sum())
        roi = profit_sum / max(1, graded)
        acc = w / max(1, (w + l))
        return {
            "available": True,
            "dates": int(gdf["date"].nunique()) if "date" in gdf.columns else None,
            "bets": n,
            "wins": w,
            "losses": l,
            "pushes": p,
            "accuracy": acc,
            "roi": roi,
            "profit_sum": profit_sum,
        }

    out = _group_summary(df)
    try:
        by_market = {}
        for m, gdf in df.groupby("market"):
            by_market[str(m)] = _group_summary(gdf)
        out["by_market"] = by_market
    except Exception:
        out["by_market"] = {}

    try:
        by_date = {}
        for d, gdf in df.groupby("date"):
            by_date[str(d)] = {
                "bets": int(len(gdf)),
                "profit_sum": float(pd.to_numeric(gdf["profit"], errors="coerce").sum()),
                "wins": int((gdf["result"] == "W").sum()),
                "losses": int((gdf["result"] == "L").sum()),
                "pushes": int((gdf["result"] == "P").sum()),
            }
        out["by_date"] = by_date
    except Exception:
        out["by_date"] = {}

    return out


def backtest_props(start: datetime, end: datetime, top_n: int, cfg: PropScoreConfig, use_snapshots: bool) -> dict:
    recon_dates = set(_iter_dates_with("recon_props", start, end))
    if use_snapshots:
        pick_dates = set(_iter_dates_with("best_edges_props", start, end))
        dates = sorted(pick_dates & recon_dates)
    else:
        edge_dates = set(_iter_dates_with("props_edges", start, end))
        dates = sorted(edge_dates & recon_dates)

    ledger: list[dict] = []

    for d in dates:
        if use_snapshots:
            e = _load_csv(PROCESSED / f"best_edges_props_{d}.csv")
        else:
            e = _load_csv(PROCESSED / f"props_edges_{d}.csv")
        r = _load_csv(PROCESSED / f"recon_props_{d}.csv")
        if e is None or r is None or e.empty or r.empty:
            continue

        e = e.copy()
        if use_snapshots:
            e["stat"] = e.get("market").astype(str).str.lower()
            e["player_name"] = e.get("player").astype(str)
            e["team_abbr"] = e.get("team").astype(str)
        else:
            e["stat"] = e.get("stat").astype(str).str.lower()
            e["player_name"] = e.get("player_name").astype(str)
            e["team_abbr"] = e.get("team").astype(str)

        e["side"] = e.get("side").astype(str).str.upper()
        e["price_num"] = pd.to_numeric(e.get("price"), errors="coerce")
        e["line_num"] = pd.to_numeric(e.get("line"), errors="coerce")
        e = e[(~e["stat"].isin(["dd", "td"])) & (e["side"].isin(["OVER", "UNDER"]))]
        e = e[(e["price_num"].notna()) & (e["line_num"].notna())]

        if not use_snapshots:
            e = e[(e["price_num"] >= -150) & (e["price_num"] <= 150)]

            if "edge" in e.columns:
                edge_num = pd.to_numeric(e.get("edge"), errors="coerce")
                mask_pts = e["stat"].isin(["pts", "pra"])
                e = e[(~mask_pts) | (edge_num.abs() >= 0.15)]

        if e.empty:
            continue

        if use_snapshots:
            if "score" in e.columns:
                e["score_100"] = pd.to_numeric(e.get("score"), errors="coerce")
            elif "best_edge_value" in e.columns:
                e["score_100"] = pd.to_numeric(e.get("best_edge_value"), errors="coerce")
            else:
                e["score_100"] = 0.0
        else:
            scores = []
            for _, rr in e.iterrows():
                s, _, _ = score_prop_pick_0_100(
                    ev=rr.get("ev"),
                    edge=rr.get("edge"),
                    model_prob=rr.get("model_prob"),
                    implied_prob=rr.get("implied_prob"),
                    price=rr.get("price"),
                    cfg=cfg,
                )
                scores.append(s)
            e["score_100"] = scores

        e["player_key"] = (
            e.get("player_name").astype(str).str.strip().str.lower()
            + "@@"
            + e.get("team_abbr").astype(str).str.strip().str.upper()
        )
        e = (
            e.sort_values(["player_key", "score_100"], ascending=[True, False])
            .groupby("player_key", as_index=False, sort=False)
            .head(1)
        )
        e = e.sort_values("score_100", ascending=False).head(int(top_n))

        r = r.copy()
        r["pkey"] = r.get("player_name").astype(str).str.strip().str.lower() + "@@" + r.get("team_abbr").astype(str).str.strip().str.upper()
        r_idx = r.set_index("pkey")

        for _, rr in e.iterrows():
            pkey = str(rr.get("player_key"))
            if pkey not in r_idx.index:
                continue
            stat = str(rr.get("stat") or "").lower()
            side = str(rr.get("side") or "").upper()
            line = float(rr.get("line_num"))
            price = rr.get("price")
            score = float(rr.get("score_100") or 0.0)

            row = r_idx.loc[pkey]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            actual = None
            if stat == "pts":
                actual = float(pd.to_numeric(row.get("pts"), errors="coerce"))
            elif stat == "reb":
                actual = float(pd.to_numeric(row.get("reb"), errors="coerce"))
            elif stat == "ast":
                actual = float(pd.to_numeric(row.get("ast"), errors="coerce"))
            elif stat in {"threes", "3pt"}:
                actual = float(pd.to_numeric(row.get("threes"), errors="coerce"))
            elif stat == "pr":
                actual = float(pd.to_numeric(row.get("pts", 0), errors="coerce") + pd.to_numeric(row.get("reb", 0), errors="coerce"))
            elif stat == "ra":
                actual = float(pd.to_numeric(row.get("reb", 0), errors="coerce") + pd.to_numeric(row.get("ast", 0), errors="coerce"))
            elif stat == "pra":
                actual = float(pd.to_numeric(row.get("pra"), errors="coerce"))
            elif stat == "pa":
                actual = float(pd.to_numeric(row.get("pts", 0), errors="coerce") + pd.to_numeric(row.get("ast", 0), errors="coerce"))
            else:
                continue

            if actual is None or pd.isna(actual):
                continue

            if abs(actual - line) < 1e-9:
                res = "P"
            else:
                if side == "OVER":
                    res = "W" if actual > line else "L"
                else:
                    res = "W" if actual < line else "L"

            profit = _profit_per_unit(res, price)
            ledger.append({"date": d, "stat": stat, "score": score, "result": res, "profit": profit})

    df = pd.DataFrame(ledger)
    if df.empty:
        return _empty_summary()

    def _group_summary(gdf: pd.DataFrame) -> dict:
        if gdf.empty:
            return _empty_summary()
        n = int(len(gdf))
        w = int((gdf["result"] == "W").sum())
        l = int((gdf["result"] == "L").sum())
        p = int((gdf["result"] == "P").sum())
        graded = int(gdf["result"].isin(["W", "L", "P"]).sum())
        profit_sum = float(pd.to_numeric(gdf["profit"], errors="coerce").sum())
        roi = profit_sum / max(1, graded)
        acc = w / max(1, (w + l))
        return {
            "available": True,
            "dates": int(gdf["date"].nunique()) if "date" in gdf.columns else None,
            "bets": n,
            "wins": w,
            "losses": l,
            "pushes": p,
            "accuracy": acc,
            "roi": roi,
            "profit_sum": profit_sum,
        }

    out = _group_summary(df)
    try:
        by_stat = {}
        for s, gdf in df.groupby("stat"):
            by_stat[str(s)] = _group_summary(gdf)
        out["by_stat"] = by_stat
    except Exception:
        out["by_stat"] = {}

    try:
        by_date = {}
        for d, gdf in df.groupby("date"):
            by_date[str(d)] = {
                "bets": int(len(gdf)),
                "profit_sum": float(pd.to_numeric(gdf["profit"], errors="coerce").sum()),
                "wins": int((gdf["result"] == "W").sum()),
                "losses": int((gdf["result"] == "L").sum()),
                "pushes": int((gdf["result"] == "P").sum()),
            }
        out["by_date"] = by_date
    except Exception:
        out["by_date"] = {}

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--top-games", type=int, default=10)
    ap.add_argument("--top-props", type=int, default=25)
    ap.add_argument(
        "--use-snapshots",
        action="store_true",
        help="Evaluate from best_edges_games_*/best_edges_props_* snapshots (authoritative surfaced picks) instead of rescoring candidate files.",
    )
    ap.add_argument("--out", default=str(PROCESSED / "historical_roi_report.json"))
    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)

    game_cfg = GameScoreConfig()
    prop_cfg = PropScoreConfig()

    games = backtest_games(start, end, int(args.top_games), game_cfg, bool(args.use_snapshots))
    props = backtest_props(start, end, int(args.top_props), prop_cfg, bool(args.use_snapshots))

    payload = {
        "window": {"start": args.start, "end": args.end},
        "selection": {"top_games": int(args.top_games), "top_props": int(args.top_props)},
        "selection_mode": "snapshots" if bool(args.use_snapshots) else "candidates",
        "game_cfg": asdict(game_cfg),
        "prop_cfg": asdict(prop_cfg),
        "games": games,
        "props": props,
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _fmt_pct(x):
        return None if x is None else round(100.0 * float(x), 2)

    print(f"Window {args.start} to {args.end}")
    if games.get("available"):
        print(
            "Games:",
            {
                "dates": games.get("dates"),
                "bets": games.get("bets"),
                "acc_pct": _fmt_pct(games.get("accuracy")),
                "roi_pct": _fmt_pct(games.get("roi")),
                "profit": round(float(games.get("profit_sum") or 0.0), 3),
            },
        )
    else:
        print("Games: unavailable")

    if props.get("available"):
        print(
            "Props:",
            {
                "dates": props.get("dates"),
                "bets": props.get("bets"),
                "acc_pct": _fmt_pct(props.get("accuracy")),
                "roi_pct": _fmt_pct(props.get("roi")),
                "profit": round(float(props.get("profit_sum") or 0.0), 3),
            },
        )
    else:
        print("Props: unavailable")

    print(f"Wrote: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
