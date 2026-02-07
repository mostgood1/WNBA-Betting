import argparse
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
import json
import random

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"

from nba_betting.scoring import (
    GameScoreConfig,
    PropScoreConfig,
    score_game_pick_0_100,
    score_prop_pick_0_100,
)


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


def _implied_prob(american: object) -> float | None:
    try:
        a = float(american)
    except Exception:
        return None
    if a == 0:
        return None
    if a > 0:
        return float(100.0 / (a + 100.0))
    return float((-a) / ((-a) + 100.0))


def _num_or_none(x) -> float | None:
    try:
        v = float(pd.to_numeric(x, errors="coerce"))
    except Exception:
        return None
    return None if pd.isna(v) else float(v)


def _iter_dates_with(prefix: str, start: datetime, end: datetime) -> list[str]:
    out = []
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


def backtest_games(
    start: datetime,
    end: datetime,
    top_n: int,
    cfg: GameScoreConfig,
    use_snapshots: bool = False,
    simulate_best_edges: bool = False,
) -> dict:
    recon_dates = set(_iter_dates_with("recon_games", start, end))
    odds_dates = set(_iter_dates_with("game_odds", start, end))
    if bool(use_snapshots):
        pick_dates = set(_iter_dates_with("best_edges_games", start, end))
        dates = sorted(pick_dates & recon_dates)
    else:
        # Need recs + recon + odds
        rec_dates = set(_iter_dates_with("recommendations", start, end))
        dates = sorted(rec_dates & recon_dates & odds_dates)

    ledger = []
    for d in dates:
        if bool(use_snapshots):
            rec = _load_csv(PROCESSED / f"best_edges_games_{d}.csv")
        else:
            rec = _load_csv(PROCESSED / f"recommendations_{d}.csv")
        fin = _load_csv(PROCESSED / f"recon_games_{d}.csv")
        odds = _load_csv(PROCESSED / f"game_odds_{d}.csv")
        if rec is None or fin is None:
            continue
        if rec.empty:
            continue

        # compute score
        rec = rec.copy()
        rec["market"] = rec.get("market").astype(str).str.upper()
        rec["home"] = rec.get("home").astype(str)
        rec["away"] = rec.get("away").astype(str)

        # Snapshot-style preprocessing: backfill ATS/TOTAL price/line from odds before scoring
        if (not bool(use_snapshots)) and bool(simulate_best_edges) and (odds is not None) and (not odds.empty):
            try:
                odds2 = odds.copy()
                odds2["home_team"] = odds2.get("home_team").astype(str)
                odds2["visitor_team"] = odds2.get("visitor_team").astype(str)
                odds_map: dict[str, dict] = {}
                for _, rr in odds2.iterrows():
                    h = str(rr.get("home_team") or "").strip()
                    a = str(rr.get("visitor_team") or rr.get("away_team") or "").strip()
                    if not h or not a:
                        continue
                    odds_map[f"{a}@@{h}".lower()] = rr.to_dict()
                    odds_map[f"{h}@@{a}".lower()] = rr.to_dict()

                prices = []
                lines = []
                implieds = []
                for _, r0 in rec.iterrows():
                    mkt = str(r0.get("market") or "").upper()
                    side = str(r0.get("side") or "")
                    home = str(r0.get("home") or "").strip()
                    away = str(r0.get("away") or "").strip()
                    price_v = _num_or_none(r0.get("price"))
                    line_v = _num_or_none(r0.get("line"))
                    imp_v = _num_or_none(r0.get("implied_prob"))

                    if mkt in {"ATS", "TOTAL"} and price_v is None:
                        orec = odds_map.get(f"{away}@@{home}".lower())
                        if isinstance(orec, dict):
                            if mkt == "ATS":
                                hp = _num_or_none(orec.get("home_spread_price"))
                                ap = _num_or_none(orec.get("away_spread_price"))
                                hp = hp if hp is not None else -110.0
                                ap = ap if ap is not None else -110.0
                                price_v = hp if str(side) == str(home) else ap
                                if line_v is None:
                                    hs = _num_or_none(orec.get("home_spread"))
                                    if hs is not None:
                                        line_v = hs if str(side) == str(home) else -hs
                            else:
                                op = _num_or_none(orec.get("total_over_price"))
                                up = _num_or_none(orec.get("total_under_price"))
                                op = op if op is not None else -110.0
                                up = up if up is not None else -110.0
                                is_over = str(side or "").strip().lower().startswith("o")
                                price_v = op if is_over else up
                                if line_v is None:
                                    tt = _num_or_none(orec.get("total"))
                                    if tt is not None:
                                        line_v = tt

                    if imp_v is None and price_v is not None:
                        imp_v = _implied_prob(price_v)

                    prices.append(price_v if price_v is not None else r0.get("price"))
                    lines.append(line_v if line_v is not None else r0.get("line"))
                    implieds.append(imp_v if imp_v is not None else r0.get("implied_prob"))

                rec["price"] = prices
                rec["line"] = lines
                if "implied_prob" in rec.columns:
                    rec["implied_prob"] = implieds
                else:
                    rec["implied_prob"] = implieds
            except Exception:
                pass

        if bool(use_snapshots):
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

        # de-dupe one per game
        rec["game_key"] = rec.get("away").astype(str).str.strip().str.lower() + "@@" + rec.get("home").astype(str).str.strip().str.lower()
        rec = (
            rec.sort_values(["game_key", "score_100"], ascending=[True, False])
            .groupby("game_key", as_index=False, sort=False)
            .head(1)
        )
        rec = rec.sort_values("score_100", ascending=False).head(int(top_n))

        # build finals map
        fin = fin.copy()
        fin["home_team"] = fin.get("home_team").astype(str)
        fin["visitor_team"] = fin.get("visitor_team").astype(str)
        f_map = {}
        for _, rr in fin.iterrows():
            key = (str(rr.get("visitor_team")).strip(), str(rr.get("home_team")).strip())
            hp = pd.to_numeric(rr.get("home_pts"), errors="coerce")
            ap = pd.to_numeric(rr.get("visitor_pts"), errors="coerce")
            if pd.notna(hp) and pd.notna(ap):
                f_map[key] = (float(ap), float(hp))

        o0 = None
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
                if pick_home == home_won:
                    result = "W"
                else:
                    result = "L"
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

            ledger.append(
                {
                    "date": d,
                    "market": market,
                    "score": score,
                    "result": result,
                    "profit": profit,
                }
            )

    df = pd.DataFrame(ledger)
    if df.empty:
        return {"available": False, "dates": 0, "bets": 0}

    n = int(len(df))
    w = int((df["result"] == "W").sum())
    l = int((df["result"] == "L").sum())
    p = int((df["result"] == "P").sum())
    graded = int((df["result"].isin(["W", "L", "P"])).sum())
    profit_sum = float(pd.to_numeric(df["profit"], errors="coerce").sum())
    roi = profit_sum / max(1, graded)
    acc = w / max(1, (w + l))

    return {
        "available": True,
        "dates": int(df["date"].nunique()),
        "bets": n,
        "wins": w,
        "losses": l,
        "pushes": p,
        "accuracy": acc,
        "roi": roi,
    }


def backtest_props(
    start: datetime,
    end: datetime,
    top_n: int,
    cfg: PropScoreConfig,
    use_snapshots: bool = False,
    simulate_best_edges: bool = False,
) -> dict:
    recon_dates = set(_iter_dates_with("recon_props", start, end))
    if bool(use_snapshots):
        pick_dates = set(_iter_dates_with("best_edges_props", start, end))
        dates = sorted(pick_dates & recon_dates)
    else:
        edge_dates = set(_iter_dates_with("props_edges", start, end))
        dates = sorted(edge_dates & recon_dates)

    ledger = []

    for d in dates:
        if bool(use_snapshots):
            e = _load_csv(PROCESSED / f"best_edges_props_{d}.csv")
        else:
            e = _load_csv(PROCESSED / f"props_edges_{d}.csv")
        r = _load_csv(PROCESSED / f"recon_props_{d}.csv")
        if e is None or r is None or e.empty or r.empty:
            continue

        e = e.copy()
        if bool(use_snapshots):
            e["stat"] = e.get("market").astype(str).str.lower()
            e["player_name"] = e.get("player").astype(str)
            e["team_abbr"] = e.get("team").astype(str)
        else:
            # regular play filter (match snapshot constraints)
            e["stat"] = e.get("stat").astype(str).str.lower()
            e["player_name"] = e.get("player_name").astype(str)
            e["team_abbr"] = e.get("team").astype(str)

        e["side"] = e.get("side").astype(str).str.upper()
        e["price_num"] = pd.to_numeric(e.get("price"), errors="coerce")
        e["line_num"] = pd.to_numeric(e.get("line"), errors="coerce")
        e = e[(~e["stat"].isin(["dd", "td"])) & (e["side"].isin(["OVER", "UNDER"]))]
        e = e[(e["price_num"].notna()) & (e["line_num"].notna())]

        # Keep simulated best-edges selection consistent with production snapshots
        if (not bool(use_snapshots)) and bool(simulate_best_edges):
            e = e[e["stat"].isin(["pa", "pr", "ra"])].copy()

        # Snapshot-style preprocessing: normalize EV if encoded as percent units
        if (not bool(use_snapshots)) and bool(simulate_best_edges) and ("ev" in e.columns):
            try:
                evn = pd.to_numeric(e.get("ev"), errors="coerce")
                mask = evn.notna() & (evn.abs() > 1.5)
                if mask.any():
                    e.loc[mask, "ev"] = (evn.loc[mask] / 100.0)
            except Exception:
                pass

        if not bool(use_snapshots):
            e = e[(e["price_num"] >= -150) & (e["price_num"] <= 150)]
            # PTS/PRA guardrail
            if "edge" in e.columns:
                edge_num = pd.to_numeric(e.get("edge"), errors="coerce")
                mask_pts = e["stat"].isin(["pts", "pra"])
                e = e[(~mask_pts) | (edge_num.abs() >= 0.15)]

        if e.empty:
            continue

        if bool(use_snapshots):
            if "score" in e.columns:
                e["score_100"] = pd.to_numeric(e.get("score"), errors="coerce")
            elif "best_edge_value" in e.columns:
                e["score_100"] = pd.to_numeric(e.get("best_edge_value"), errors="coerce")
            else:
                e["score_100"] = 0.0
        else:
            # score
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

        # de-dupe: one per player
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

        # recon index
        r = r.copy()
        r["pkey"] = (
            r.get("player_name").astype(str).str.strip().str.lower()
            + "@@"
            + r.get("team_abbr").astype(str).str.strip().str.upper()
        )
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
                pra = pd.to_numeric(row.get("pra"), errors="coerce")
                if pd.isna(pra):
                    pra = pd.to_numeric(row.get("pts", 0), errors="coerce") + pd.to_numeric(row.get("reb", 0), errors="coerce") + pd.to_numeric(row.get("ast", 0), errors="coerce")
                actual = float(pra)
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
            ledger.append({"date": d, "score": score, "result": res, "profit": profit})

    df = pd.DataFrame(ledger)
    if df.empty:
        return {"available": False, "dates": 0, "bets": 0}

    n = int(len(df))
    w = int((df["result"] == "W").sum())
    l = int((df["result"] == "L").sum())
    p = int((df["result"] == "P").sum())
    graded = int((df["result"].isin(["W", "L", "P"])).sum())
    profit_sum = float(pd.to_numeric(df["profit"], errors="coerce").sum())
    roi = profit_sum / max(1, graded)
    acc = w / max(1, (w + l))

    return {
        "available": True,
        "dates": int(df["date"].nunique()),
        "bets": n,
        "wins": w,
        "losses": l,
        "pushes": p,
        "accuracy": acc,
        "roi": roi,
    }


def random_simplex(k: int) -> list[float]:
    xs = np.random.rand(k)
    xs = xs / xs.sum()
    return [float(x) for x in xs]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--iters", type=int, default=100)
    ap.add_argument("--top-games", type=int, default=10)
    ap.add_argument("--top-props", type=int, default=25)
    ap.add_argument(
        "--objective",
        choices=["combined_roi", "roi", "accuracy", "games_roi", "props_roi"],
        default="combined_roi",
    )
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--min-prob-weight", type=float, default=0.15, help="Minimum prop w_prob_edge to avoid degenerate configs")
    ap.add_argument("--max-price-weight", type=float, default=0.35, help="Maximum prop w_price to avoid overfitting to price")
    ap.add_argument("--min-bets", type=int, default=50, help="Minimum bets required (props preferred) to consider a config")
    ap.add_argument("--min-games-roi", type=float, default=None, help="Require games ROI >= this (decimal, e.g. 0.00)")
    ap.add_argument("--min-props-roi", type=float, default=None, help="Require props ROI >= this (decimal, e.g. 0.00)")
    ap.add_argument("--freeze-games", action="store_true", help="Do not vary game scoring weights (optimize props only)")
    ap.add_argument("--freeze-props", action="store_true", help="Do not vary prop scoring weights (optimize games only)")
    ap.add_argument(
        "--use-snapshots",
        action="store_true",
        help="Evaluate from best_edges_games_*/best_edges_props_* snapshots (authoritative surfaced picks) instead of rescoring candidate files. Note: this is evaluation-only; optimization iterations are disabled.",
    )
    ap.add_argument(
        "--simulate-best-edges",
        action="store_true",
        help="When optimizing from candidate files, simulate best-edges snapshot preprocessing (e.g., ATS/TOTAL price backfill from odds before scoring; EV normalization for props).",
    )
    ap.add_argument("--out", default=str(PROCESSED / "scoring_optimization_report.json"))
    args = ap.parse_args()

    if bool(args.use_snapshots) and int(args.iters) > 0:
        raise SystemExit("--use-snapshots fixes the pick set (snapshots), so iterative optimization cannot learn anything. Re-run with --iters 0 for snapshot-aligned evaluation.")

    np.random.seed(args.seed)
    random.seed(args.seed)

    start = _parse_date(args.start)
    end = _parse_date(args.end)

    def _roi(d: dict) -> float | None:
        try:
            if not bool(d.get("available")):
                return None
            v = d.get("roi")
            return float(v) if v is not None else None
        except Exception:
            return None

    def _objective(games: dict, props: dict) -> float:
        g = _roi(games) if isinstance(games, dict) else None
        p = _roi(props) if isinstance(props, dict) else None
        obj = str(args.objective)
        if obj == "roi":
            obj = "combined_roi"
        if obj == "games_roi":
            return -999.0 if g is None else float(g)
        if obj == "props_roi":
            return -999.0 if p is None else float(p)
        if obj == "accuracy":
            # Keep existing behavior: prefer props if available else games
            try:
                if bool(props.get("available")):
                    return float(props.get("accuracy") or 0.0)
                if bool(games.get("available")):
                    return float(games.get("accuracy") or 0.0)
            except Exception:
                return -999.0
            return -999.0
        # combined_roi
        if g is None and p is None:
            return -999.0
        if g is None:
            return float(p)
        if p is None:
            return float(g)
        return 0.5 * (float(g) + float(p))

    def _passes_roi_floors(games: dict, props: dict) -> bool:
        try:
            if args.min_games_roi is not None:
                g = _roi(games)
                if g is None or float(g) < float(args.min_games_roi):
                    return False
            if args.min_props_roi is not None:
                p = _roi(props)
                if p is None or float(p) < float(args.min_props_roi):
                    return False
            return True
        except Exception:
            return False

    results = []

    # include baseline
    base_game = GameScoreConfig()
    base_prop = PropScoreConfig()
    g = backtest_games(start, end, args.top_games, base_game, bool(args.use_snapshots), bool(args.simulate_best_edges))
    p = backtest_props(start, end, args.top_props, base_prop, bool(args.use_snapshots), bool(args.simulate_best_edges))
    results.append({"tag": "baseline", "game_cfg": asdict(base_game), "prop_cfg": asdict(base_prop), "games": g, "props": p})

    for i in range(int(args.iters)):
        # Randomize only weights (keep scales/centers stable for now)
        # Games (ATS/TOTAL): (edge_pts, ev_non_ml, price) + randomize ML w_ev in a safe range
        wg = random_simplex(3)
        w_ev_ml = float(np.random.uniform(0.65, 0.95))
        wp = random_simplex(3)  # (ev, prob, price)

        # Simple constraints to avoid pathological configs (props)
        if not bool(args.freeze_props):
            if wp[1] < float(args.min_prob_weight):
                continue
            if wp[2] > float(args.max_price_weight):
                continue

        # Inherit all non-weight parameters from current defaults so we can tune
        # centers/scales in nba_betting.scoring and have optimization reflect it.
        game_cfg = base_game
        if not bool(args.freeze_games):
            game_cfg = GameScoreConfig(
                w_ev=w_ev_ml,
                w_edge_pts=wg[0],
                w_ev_non_ml=wg[1],
                w_price=wg[2],
            )

        prop_cfg = base_prop
        if not bool(args.freeze_props):
            prop_cfg = PropScoreConfig(
                w_ev=wp[0],
                w_prob_edge=wp[1],
                w_price=wp[2],
            )

        games = backtest_games(start, end, args.top_games, game_cfg, bool(args.use_snapshots), bool(args.simulate_best_edges))
        props = backtest_props(start, end, args.top_props, prop_cfg, bool(args.use_snapshots), bool(args.simulate_best_edges))

        if not _passes_roi_floors(games, props):
            continue

        obj = _objective(games, props)

        # Enforce minimum sample size (prefer props, fallback to games)
        bets = int((props.get("bets") or 0) if props.get("available") else (games.get("bets") or 0))
        if bets < int(args.min_bets):
            continue

        results.append(
            {
                "tag": f"rand_{i}",
                "objective": obj,
                "game_cfg": asdict(game_cfg),
                "prop_cfg": asdict(prop_cfg),
                "games": games,
                "props": props,
            }
        )

    # rank
    def keyfn(x):
        v = x.get("objective")
        if v is not None:
            return float(v)

        # Baseline does not have objective field; compute it consistently.
        games = x.get("games") or {}
        props = x.get("props") or {}
        g_av = bool(games.get("available"))
        p_av = bool(props.get("available"))

        return _objective(games, props)

    ranked = sorted(results, key=keyfn, reverse=True)

    top = ranked[:25]
    out = {
        "window": {"start": args.start, "end": args.end},
        "objective": args.objective,
        "selection_mode": "snapshots" if bool(args.use_snapshots) else "candidates",
        "iters": int(args.iters),
        "top_games": int(args.top_games),
        "top_props": int(args.top_props),
        "notes": {
            "constraints": {
                "min_prob_weight": float(args.min_prob_weight),
                "max_price_weight": float(args.max_price_weight),
                "min_bets": int(args.min_bets),
                "min_games_roi": args.min_games_roi,
                "min_props_roi": args.min_props_roi,
            },
            "search": "Optimization only varies weights (not scales/centers) in this first pass.",
        },
        "top": top,
    }

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, indent=2), encoding="utf-8")

    # concise console output
    best = ranked[0]
    print("Best config:")
    print(json.dumps({"tag": best.get("tag"), "games": best.get("games"), "props": best.get("props"), "game_cfg": best.get("game_cfg"), "prop_cfg": best.get("prop_cfg")}, indent=2))
    print(f"Wrote: {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
