import os
import json
import argparse
from datetime import datetime, timedelta
import pandas as pd
import requests

# Evaluate correlation method (phi/proxy/off) by matching API selections
# to recon_props outcomes over a given date window. Computes total picks,
# wins, win-rate, and ROI using American odds.

def american_odds_profit(price: float) -> float:
    try:
        p = float(price)
        if p > 0:
            return p / 100.0
        else:
            return 100.0 / abs(p)
    except Exception:
        return 0.0


def eval_window(base_url: str, start: str, end: str, alpha: float, cap_team: int, cap_market: int, limit_n: int, corr_scale: float):
    methods = ["phi", "proxy", "off"]
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    results = {}
    for m in methods:
        total = 0
        wins = 0
        roi = 0.0
        per_day = []
        dt = start_dt
        while dt <= end_dt:
            d = dt.strftime("%Y-%m-%d")
            recon_path = os.path.join("data", "processed", f"recon_props_{d}.csv")
            if not os.path.exists(recon_path):
                dt += timedelta(days=1)
                continue
            try:
                recon = pd.read_csv(recon_path)
            except Exception:
                dt += timedelta(days=1)
                continue
            # Normalize for matching
            recon["player_key"] = recon["player_name"].astype(str).str.strip().str.lower()
            recon["team_key"] = recon["team_abbr"].astype(str).str.strip().str.upper()
            recon = recon.set_index(["player_key", "team_key"])  # fast lookups
            # Fetch API selections
            params = {
                "date": d,
                "compact": 0,
                "regular_only": 1,
                "limit": limit_n,
                "penalize_correlation": 1,
                "optimize": 1,
                "portfolio_only": 1,
                "cap_team": cap_team,
                "cap_market": cap_market,
                "corr_method": m,
                "opt_alpha": alpha,
                "corr_penalty_scale": corr_scale,
            }
            try:
                r = requests.get(f"{base_url}/api/props/recommendations", params=params, timeout=45)
                r.raise_for_status()
                payload = r.json()
            except Exception:
                dt += timedelta(days=1)
                continue
            data = payload.get("data", [])
            day_total = 0
            day_wins = 0
            day_roi = 0.0
            for item in data:
                try:
                    player = str(item.get("player") or "").strip().lower()
                    team = str(item.get("team") or "").strip().upper()
                    tp = item.get("top_play") or {}
                    market = str(tp.get("market") or "").strip().lower()
                    side = str(tp.get("side") or "").strip().upper()
                    line = tp.get("line")
                    price = tp.get("price")
                    if not market or line is None or not side:
                        continue
                    idx = (player, team)
                    if idx not in recon.index:
                        continue
                    recon_row = recon.loc[idx]
                    if market not in recon_row.index:
                        continue
                    actual = recon_row[market]
                    # win/loss
                    win = False
                    try:
                        if side == "OVER":
                            win = float(actual) >= float(line)
                        elif side == "UNDER":
                            win = float(actual) <= float(line)
                    except Exception:
                        win = False
                    prof = american_odds_profit(price) if win else -1.0
                    total += 1
                    day_total += 1
                    if win:
                        wins += 1
                        day_wins += 1
                    roi += prof
                    day_roi += prof
                except Exception:
                    continue
            if day_total > 0:
                per_day.append({"date": d, "total": day_total, "wins": day_wins, "roi": round(day_roi, 4)})
            dt += timedelta(days=1)
        results[m] = {
            "total": total,
            "wins": wins,
            "win_rate": round((wins / total) * 100.0, 2) if total else 0.0,
            "roi": round(roi, 4),
            "per_day": per_day,
            "window": {"start": start, "end": end},
            "caps": {"team": cap_team, "market": cap_market},
            "alpha": alpha,
        }
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--alpha", type=float, default=0.15)
    ap.add_argument("--cap-team", type=int, default=2)
    ap.add_argument("--cap-market", type=int, default=3)
    ap.add_argument("--base", default="http://127.0.0.1:5051")
    ap.add_argument("--limit", type=int, default=25)
    ap.add_argument("--corr-scale", type=float, default=1.0)
    ap.add_argument("--outfile", default=None)
    args = ap.parse_args()
    res = eval_window(args.base, args.start, args.end, args.alpha, args.cap_team, args.cap_market, args.limit, args.corr_scale)
    print(json.dumps(res, indent=2))
    if args.outfile:
        try:
            os.makedirs(os.path.dirname(args.outfile), exist_ok=True)
            with open(args.outfile, "w", encoding="utf-8") as f:
                json.dump(res, f, indent=2)
        except Exception:
            pass

if __name__ == "__main__":
    main()
