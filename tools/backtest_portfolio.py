import os
import sys
import json
import math
import argparse
import datetime as dt
from typing import Any, Dict, List

import pandas as pd

try:
    import requests
except Exception as e:
    print(f"error: requests not installed: {e}")
    sys.exit(1)

BASE_URL = os.environ.get("NBA_API_BASE_URL", "http://127.0.0.1:5051").rstrip("/")
# processed directory lives at repo_root/data/processed
PROCESSED_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed")


def _slugify(s: str) -> str:
    out = []
    for ch in str(s or ""):
        if ch.isalnum():
            out.append(ch)
        elif ch in ("-", "_", "."):
            out.append(ch)
    return "".join(out)[:80] or "run"


def _config_tag(
    limit: int,
    optimize: bool,
    penalize_correlation: bool,
    opt_alpha: float,
    corr_scale: float,
    cap_team: int,
    cap_market: int,
    regular_only: bool,
    markets: str | None,
    min_ev: float | None,
    sort_by: str | None,
    use_snapshot: bool,
) -> str:
    parts: list[str] = []
    parts.append(f"lim{int(limit)}")
    parts.append("snap" if use_snapshot else "live")
    parts.append("opt" if optimize else "rank")
    if optimize:
        parts.append(f"ct{int(cap_team)}")
        parts.append(f"cm{int(cap_market)}")
    if penalize_correlation or (corr_scale is not None and float(corr_scale) > 0):
        parts.append(f"corr{float(corr_scale):g}")
        parts.append(f"a{float(opt_alpha):g}")
    if regular_only:
        parts.append("reg")
    if markets:
        parts.append(f"mkt{_slugify(markets).replace('_','').replace('-','')}")
    if min_ev is not None:
        parts.append(f"minEV{float(min_ev):g}")
    if sort_by:
        parts.append(f"sort{_slugify(sort_by)}")
    return "_".join(parts)


def american_to_decimal(a: Any) -> float | None:
    try:
        aa = float(a)
    except Exception:
        return None
    if aa == 0:
        return None
    return (1.0 + (aa/100.0)) if aa > 0 else (1.0 + (100.0/abs(aa)))


def norm_name(s: Any) -> str:
    return str(s or "").strip().lower()


def tri_team(v: Any) -> str:
    return str(v or "").strip().upper()


def fetch_portfolio(
    date_str: str,
    limit: int,
    optimize: bool,
    penalize_correlation: bool,
    opt_alpha: float,
    corr_scale: float,
    cap_team: int,
    cap_market: int,
    regular_only: bool,
    markets: str | None,
    min_ev: float | None,
    sort_by: str | None,
    use_snapshot: bool,
) -> List[Dict[str, Any]]:
    params = {
        "date": date_str,
        "compact": "1",
        "portfolio_only": "1",
        "limit": str(limit),
        "use_snapshot": ("1" if use_snapshot else "0"),
    }
    # Penalize-correlation and caps are meaningful even without full optimizer.
    # (The API uses correlation penalties during portfolio selection for ranking-only mode too.)
    if penalize_correlation or (corr_scale is not None and float(corr_scale) > 0):
        params["penalize_correlation"] = "1"
        params["corr_penalty_scale"] = str(corr_scale)
        params["opt_alpha"] = str(opt_alpha)
    if optimize:
        params["optimize"] = "1"
        if cap_team:
            params["cap_team"] = str(cap_team)
        if cap_market:
            params["cap_market"] = str(cap_market)
    if regular_only:
        params["regular_only"] = "1"
    if markets:
        params["markets"] = str(markets)
    if min_ev is not None:
        params["minEV"] = str(min_ev)
    if sort_by:
        params["sortBy"] = str(sort_by)
    u = f"{BASE_URL}/api/props/recommendations"
    r = requests.get(u, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    port = data.get("portfolio") or data.get("data") or []
    return port if isinstance(port, list) else []


def load_recon(date_str: str) -> pd.DataFrame:
    fp = os.path.join(PROCESSED_DIR, f"recon_props_{date_str}.csv")
    if not os.path.exists(fp):
        return pd.DataFrame()
    try:
        df = pd.read_csv(fp)
    except Exception:
        return pd.DataFrame()
    return df


def resolve_play(tp: Dict[str, Any], stats: Dict[str, float]) -> Dict[str, Any]:
    mkt = str((tp.get("market") or "")).lower()
    side = str((tp.get("side") or "")).upper()
    line = tp.get("line")
    try:
        ln = float(line) if line is not None and not pd.isna(line) else None
    except Exception:
        ln = None
    if ln is None:
        return {"resolved": False}
    actual = None
    if mkt == "pts":
        actual = stats.get("pts")
    elif mkt == "reb":
        actual = stats.get("reb")
    elif mkt == "ast":
        actual = stats.get("ast")
    elif mkt == "threes":
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
        return {"resolved": False}
    is_push = abs(float(actual) - float(ln)) < 1e-9
    is_win = False
    if not is_push:
        if side == "OVER":
            is_win = (float(actual) > float(ln))
        elif side == "UNDER":
            is_win = (float(actual) < float(ln))
    price_val = tp.get("price")
    if price_val is None:
        price_val = tp.get("american") or tp.get("odds")
    dec = american_to_decimal(price_val) or 1.909090909
    stake = 1.0
    if is_push:
        profit = 0.0
    elif is_win:
        profit = (float(dec) - 1.0) * stake
    else:
        profit = -stake
    # predicted win probability if available
    p_pred = None
    try:
        # prefer explicit probability fields
        for key in [
            "prob",
            "probability",
            "win_prob",
            "p",
            "model_prob",
            "prob_calib",
            "prob_cal",
        ]:
            if key in tp and tp.get(key) is not None and not pd.isna(tp.get(key)):
                val = float(tp.get(key))
                if val <= 1.0:
                    p_pred = val
                else:
                    p_pred = val / 100.0
                break
    except Exception:
        p_pred = None
    return {
        "resolved": True,
        "push": is_push,
        "win": is_win,
        "profit": profit,
        "prob_pred": p_pred,
    }


def date_range(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        yield cur
        cur = cur + dt.timedelta(days=1)


def backtest(
    start: dt.date,
    end: dt.date,
    limit: int,
    optimize: bool,
    penalize_correlation: bool,
    opt_alpha: float,
    corr_scale: float,
    cap_team: int,
    cap_market: int,
    regular_only: bool,
    markets: str | None,
    min_ev: float | None,
    sort_by: str | None,
    use_snapshot: bool,
) -> Dict[str, Any]:
    daily_rows: List[Dict[str, Any]] = []
    summary = {"bets": 0, "resolved": 0, "wins": 0, "losses": 0, "pushes": 0, "stake_total": 0.0, "profit_total": 0.0}
    brier_sum = 0.0
    brier_n = 0
    logloss_sum = 0.0
    logloss_n = 0
    for d in date_range(start, end):
        ds = d.isoformat()
        try:
            port = fetch_portfolio(
                ds,
                limit,
                optimize,
                penalize_correlation,
                opt_alpha,
                corr_scale,
                cap_team,
                cap_market,
                regular_only,
                markets,
                min_ev,
                sort_by,
                use_snapshot,
            )
        except Exception as e:
            # skip day if API failed
            continue
        recon = load_recon(ds)
        idx: Dict[tuple, Dict[str, float]] = {}
        if not recon.empty:
            r = recon.copy()
            # flexible columns: player and team
            player_col = None
            for c in ["player_name", "player", "PLAYER", "name"]:
                if c in r.columns:
                    player_col = c
                    break
            team_col = None
            for c in ["team_abbr", "TEAM", "team", "team_tricode", "tricode"]:
                if c in r.columns:
                    team_col = c
                    break
            if player_col is None:
                r["_pname"] = ""
            else:
                r["_pname"] = r[player_col].astype(str).str.lower()
            if team_col is None:
                r["_team"] = ""
            else:
                r["_team"] = r[team_col].astype(str).str.upper()
            for _, rr in r.iterrows():
                idx[(rr["_pname"], rr["_team"])] = {
                    "pts": float(pd.to_numeric(rr.get("pts"), errors="coerce") or 0.0),
                    "reb": float(pd.to_numeric(rr.get("reb"), errors="coerce") or 0.0),
                    "ast": float(pd.to_numeric(rr.get("ast"), errors="coerce") or 0.0),
                    "threes": float(pd.to_numeric(rr.get("threes"), errors="coerce") or 0.0),
                    "pra": float(pd.to_numeric(rr.get("pra"), errors="coerce") or 0.0),
                }
        day_profit = 0.0
        day_bets = 0
        for p in port:
            if not isinstance(p, dict):
                # skip non-dict entries safely
                continue
            tp = p.get("top_play") if isinstance(p.get("top_play"), dict) else None
            # some APIs may return the play object directly
            if tp is None and ("market" in p and "side" in p):
                tp = p
            player_disp = (p.get("player") if isinstance(p, dict) else None) or (tp.get("player") if isinstance(tp, dict) else None)
            player = norm_name(player_disp)
            # Prefer tricode when available; recon files usually key by tricode.
            team = tri_team(
                (p.get("team_tricode") if isinstance(p, dict) else None)
                or (p.get("team") if isinstance(p, dict) else None)
                or (p.get("TEAM") if isinstance(p, dict) else None)
                or (tp.get("team_tricode") if isinstance(tp, dict) else None)
                or (tp.get("team") if isinstance(tp, dict) else None)
            )
            stats = idx.get((player, team))
            if not stats:
                # unresolved (DNP or missing)
                daily_rows.append({"date": ds, "player": player_disp, "team": team, "resolved": False})
                continue
            res = resolve_play(tp or {}, stats)
            day_bets += 1
            summary["bets"] += 1
            summary["stake_total"] += 1.0
            if res["resolved"]:
                summary["resolved"] += 1
                if res["push"]:
                    summary["pushes"] += 1
                elif res["win"]:
                    summary["wins"] += 1
                else:
                    summary["losses"] += 1
                summary["profit_total"] += float(res["profit"])
                day_profit += float(res["profit"])
                # calibration metrics
                p_pred = res.get("prob_pred")
                if p_pred is not None:
                    y = 1.0 if res["win"] else 0.0
                    brier_sum += (p_pred - y) ** 2
                    brier_n += 1
                    # clamp to avoid log(0)
                    eps = 1e-12
                    p = min(1.0 - eps, max(eps, p_pred))
                    logloss_sum += -(y * math.log(p) + (1.0 - y) * math.log(1.0 - p))
                    logloss_n += 1
            daily_rows.append({
                "date": ds,
                "player": player_disp,
                "team": team,
                "market": (tp.get("market") if tp else None),
                "side": (tp.get("side") if tp else None),
                "line": (tp.get("line") if tp else None),
                "price": (tp.get("price") if tp else None),
                "resolved": bool(res.get("resolved")),
                "win": bool(res.get("win")),
                "push": bool(res.get("push")),
                "profit": float(res.get("profit") or 0.0),
                "prob_pred": res.get("prob_pred"),
            })
        # optional: print per-day profit
        # print(ds, day_bets, day_profit)
    roi = (summary["profit_total"] / summary["stake_total"]) if summary["stake_total"] > 0 else 0.0
    hit_rate = (summary["wins"] / summary["resolved"]) if summary["resolved"] > 0 else 0.0
    out = {
        "params": {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": limit,
            "optimize": optimize,
            "corr_penalty_scale": corr_scale,
            "cap_team": cap_team,
            "cap_market": cap_market,
            "regular_only": regular_only,
            "base_url": BASE_URL,
        },
        "summary": {
            **summary,
            "roi": roi,
            "hit_rate": hit_rate,
            "brier_mean": (brier_sum / brier_n) if brier_n > 0 else None,
            "logloss_mean": (logloss_sum / logloss_n) if logloss_n > 0 else None,
        },
        "rows": daily_rows,
    }
    return out


def main():
    ap = argparse.ArgumentParser(description="Backtest props portfolio using API + recon outcomes")
    ap.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, help="End date YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=60, help="If start/end not set, backtest last N days")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--optimize", action="store_true", default=True, help="Enable portfolio optimizer (default on for backwards compatibility)")
    ap.add_argument("--no-optimize", action="store_true", help="Disable portfolio optimizer (use API ranking only)")
    ap.add_argument("--penalize-correlation", action="store_true", default=False, help="Enable correlation penalties when optimizing")
    ap.add_argument("--opt-alpha", type=float, default=0.15, help="Optimizer alpha (only used if optimizing)")
    ap.add_argument("--corr-penalty-scale", type=float, default=0.0, help="Correlation penalty scale (only used if optimizing)")
    ap.add_argument("--cap-team", type=int, default=2, help="Max picks per team (only used if optimizing)")
    ap.add_argument("--cap-market", type=int, default=4, help="Max picks per market (only used if optimizing)")
    ap.add_argument("--markets", type=str, default=None, help="Comma-separated markets, e.g. pts,threes")
    ap.add_argument("--min-ev", type=float, default=None, help="Minimum EV%% filter (API minEV)")
    ap.add_argument("--sort-by", type=str, default=None, help="Sort metric (API sortBy), e.g. ev or edge")
    ap.add_argument("--use-snapshot", action="store_true", default=True, help="Use best_edges_props snapshot when portfolio_only is requested (default on)")
    ap.add_argument("--no-snapshot", action="store_true", help="Bypass best_edges_props snapshot and recompute portfolio using current query params")
    ap.add_argument("--regular-only", action="store_true")
    ap.add_argument("--outdir", type=str, default=os.path.join(PROCESSED_DIR, "backtests"))
    ap.add_argument("--tag", type=str, default=None, help="Optional tag to include in output filenames")
    args = ap.parse_args()

    optimize = bool(args.optimize) and (not bool(args.no_optimize))
    use_snapshot = bool(args.use_snapshot) and (not bool(args.no_snapshot))

    if args.start and args.end:
        start = dt.date.fromisoformat(args.start)
        end = dt.date.fromisoformat(args.end)
    else:
        end = dt.date.today()
        start = end - dt.timedelta(days=args.days)

    res = backtest(
        start,
        end,
        args.limit,
        optimize,
        bool(args.penalize_correlation),
        float(args.opt_alpha),
        float(args.corr_penalty_scale),
        int(args.cap_team),
        int(args.cap_market),
        bool(args.regular_only),
        (str(args.markets) if args.markets else None),
        (float(args.min_ev) if args.min_ev is not None else None),
        (str(args.sort_by) if args.sort_by else None),
        use_snapshot,
    )
    os.makedirs(args.outdir, exist_ok=True)
    stamp = f"{start.isoformat()}_{end.isoformat()}"

    tag = _slugify(args.tag) if args.tag else _config_tag(
        args.limit,
        optimize,
        bool(args.penalize_correlation),
        float(args.opt_alpha),
        float(args.corr_penalty_scale),
        int(args.cap_team),
        int(args.cap_market),
        bool(args.regular_only),
        (str(args.markets) if args.markets else None),
        (float(args.min_ev) if args.min_ev is not None else None),
        (str(args.sort_by) if args.sort_by else None),
        use_snapshot,
    )

    out_json = os.path.join(args.outdir, f"portfolio_{stamp}_{tag}.json")
    with open(out_json, "w", encoding="utf-8") as fh:
        json.dump(res, fh, indent=2)
    # Write rows CSV
    try:
        df = pd.DataFrame(res.get("rows", []))
        if not df.empty:
            df.to_csv(os.path.join(args.outdir, f"portfolio_rows_{stamp}_{tag}.csv"), index=False)
    except Exception:
        pass
    # Print summary
    print(json.dumps(res.get("summary", {}), indent=2))


if __name__ == "__main__":
    main()
