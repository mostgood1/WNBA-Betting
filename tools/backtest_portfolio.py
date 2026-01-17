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


def fetch_portfolio(date_str: str, limit: int, optimize: bool, corr_scale: float, cap_team: int, cap_market: int, regular_only: bool) -> List[Dict[str, Any]]:
    params = {
        "date": date_str,
        "compact": "1",
        "portfolio_only": "1",
        "limit": str(limit),
    }
    # Always pass optimizer-related defaults for consistency with API
    if optimize:
        params["optimize"] = "1"
    else:
        params["optimize"] = "1"  # default optimize on
    params["penalize_correlation"] = "1"
    params["opt_alpha"] = "0.15"
    params["corr_penalty_scale"] = str(corr_scale)
    if cap_team:
        params["cap_team"] = str(cap_team)
    if cap_market:
        params["cap_market"] = str(cap_market)
    if regular_only:
        params["regular_only"] = "1"
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


def backtest(start: dt.date, end: dt.date, limit: int, optimize: bool, corr_scale: float, cap_team: int, cap_market: int, regular_only: bool) -> Dict[str, Any]:
    daily_rows: List[Dict[str, Any]] = []
    summary = {"bets": 0, "resolved": 0, "wins": 0, "losses": 0, "pushes": 0, "stake_total": 0.0, "profit_total": 0.0}
    brier_sum = 0.0
    brier_n = 0
    logloss_sum = 0.0
    logloss_n = 0
    for d in date_range(start, end):
        ds = d.isoformat()
        try:
            port = fetch_portfolio(ds, limit, optimize, corr_scale, cap_team, cap_market, regular_only)
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
            team = tri_team((p.get("team") if isinstance(p, dict) else None) or (p.get("team_tricode") if isinstance(p, dict) else None) or (p.get("TEAM") if isinstance(p, dict) else None) or (tp.get("team") if isinstance(tp, dict) else None))
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
    ap.add_argument("--optimize", action="store_true", default=True)
    ap.add_argument("--corr-penalty-scale", type=float, default=0.0)
    ap.add_argument("--cap-team", type=int, default=2)
    ap.add_argument("--cap-market", type=int, default=4)
    ap.add_argument("--regular-only", action="store_true")
    ap.add_argument("--outdir", type=str, default=os.path.join(PROCESSED_DIR, "backtests"))
    args = ap.parse_args()

    if args.start and args.end:
        start = dt.date.fromisoformat(args.start)
        end = dt.date.fromisoformat(args.end)
    else:
        end = dt.date.today()
        start = end - dt.timedelta(days=args.days)

    res = backtest(start, end, args.limit, args.optimize, args.corr_penalty_scale, args.cap_team, args.cap_market, args.regular_only)
    os.makedirs(args.outdir, exist_ok=True)
    stamp = f"{start.isoformat()}_{end.isoformat()}"
    with open(os.path.join(args.outdir, f"portfolio_{stamp}.json"), "w", encoding="utf-8") as fh:
        json.dump(res, fh, indent=2)
    # Write rows CSV
    try:
        df = pd.DataFrame(res.get("rows", []))
        if not df.empty:
            df.to_csv(os.path.join(args.outdir, f"portfolio_rows_{stamp}.csv"), index=False)
    except Exception:
        pass
    # Print summary
    print(json.dumps(res.get("summary", {}), indent=2))


if __name__ == "__main__":
    main()
