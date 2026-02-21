"""Audit the reality of the probability-first Top-N slate backtest.

This script is designed to answer: "is this performance likely real, or an artifact?"

It computes:
- Calibration table: predicted win_prob bins vs realized win rate
- Brier score (lower is better)
- Distribution of win_prob (how often we're claiming 90%+ etc)
- Random baseline: for each date, sample K plays uniformly from the *same candidate pool*
  (built from props_recommendations + smart_sim) and grade them vs recon_props.

Inputs:
- A ledger CSV produced by tools/backtest_top_recommendations.py --kind slate_prob
- Processed artifacts in data/processed:
  - props_recommendations_YYYY-MM-DD.csv
  - smart_sim_YYYY-MM-DD_*.json
  - recon_props_YYYY-MM-DD.csv

Notes:
- The candidate pool is collapsed to the best play per (player, team, market) by win_prob
  to match the app selection granularity.
- This does *not* prove there is no lookahead, but it provides strong sanity checks.
"""

from __future__ import annotations

import argparse
import ast
import datetime as dt
import json
import math
import random
import re
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def _safe_float(x: Any) -> float | None:
    try:
        v = float(pd.to_numeric(x, errors="coerce"))
    except Exception:
        return None
    return None if pd.isna(v) else float(v)


def _norm_name(s: Any) -> str:
    return str(s or "").strip().lower()


def _tri_team(s: Any) -> str:
    return str(s or "").strip().upper()


def _profit_per_unit(result: str, price: Any) -> float:
    if result == "P":
        return 0.0
    if result == "L":
        return -1.0
    try:
        aa = float(price)
    except Exception:
        aa = -110.0
    if aa == 0:
        aa = -110.0
    if aa > 0:
        dec = 1.0 + (aa / 100.0)
    else:
        dec = 1.0 + (100.0 / abs(aa))
    return float(dec - 1.0)


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


def _parse_obj(val: object) -> object:
    if isinstance(val, (dict, list)):
        return val
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        pass
    try:
        return ast.literal_eval(s)
    except Exception:
        return None


def _norm_player(name: str | None) -> str:
    s = str(name or "").strip().upper()
    if not s:
        return ""
    s = re.sub(r"[^A-Z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    parts = s.split(" ")
    while parts and parts[-1] in {"JR", "SR", "II", "III", "IV", "V"}:
        parts = parts[:-1]
    return " ".join(parts).strip()


def _norm_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _prob_over(mean: float, sd: float, line: float) -> float:
    if not math.isfinite(mean) or not math.isfinite(sd) or not math.isfinite(line):
        return 0.0
    if sd <= 1e-9:
        return 1.0 if mean > line else 0.0
    z = (line - mean) / sd
    return max(0.0, min(1.0, 1.0 - _norm_cdf(z)))


def _play_win_prob(mean: float, sd: float, line: float, side: str) -> float | None:
    s = str(side or "").strip().upper()
    p_over = _prob_over(mean, sd, line)
    if s == "OVER":
        return p_over
    if s == "UNDER":
        return max(0.0, min(1.0, 1.0 - p_over))
    return None


def _build_candidate_pool(date_str: str, smart_sim_prefix: str) -> list[dict[str, Any]]:
    props_fp = PROCESSED / f"props_recommendations_{date_str}.csv"
    pref = str(smart_sim_prefix or "smart_sim").strip() or "smart_sim"
    sim_files = sorted(list(PROCESSED.glob(f"{pref}_{date_str}_*.json")))
    if not props_fp.exists() or not sim_files:
        return []

    # team->game and (team, player)->stats
    team_to_game: dict[str, dict[str, Any]] = {}
    player_stats: dict[tuple[str, str], dict[str, float]] = {}

    for sf in sim_files:
        try:
            sj = json.loads(sf.read_text(encoding="utf-8"))
        except Exception:
            continue
        home = str(sj.get("home") or "").strip().upper()
        away = str(sj.get("away") or "").strip().upper()
        gid = str(sj.get("game_id") or "").strip()
        if home and away:
            meta = {"game_id": gid or None, "home": home, "away": away}
            team_to_game[home] = meta
            team_to_game[away] = meta

        players_obj = sj.get("players") or {}
        if not isinstance(players_obj, dict):
            continue
        for side_key, team_tri in (("home", home), ("away", away)):
            plist = players_obj.get(side_key)
            if not team_tri or not isinstance(plist, list):
                continue
            for p in plist:
                if not isinstance(p, dict):
                    continue
                nm = _norm_player(p.get("player_name") or p.get("player") or "")
                if not nm:
                    continue
                stats: dict[str, float] = {}
                for k, v in p.items():
                    if not isinstance(k, str):
                        continue
                    if not (k.endswith("_mean") or k.endswith("_sd")):
                        continue
                    fv = _safe_float(v)
                    if fv is None:
                        continue
                    stats[k] = float(fv)
                if stats:
                    player_stats[(team_tri, nm)] = stats

    try:
        pdf = pd.read_csv(props_fp)
    except Exception:
        return []
    if pdf is None or pdf.empty:
        return []

    def get_mean_sd(stats: dict[str, float], market: str) -> tuple[float | None, float | None]:
        km = f"{market}_mean"; ks = f"{market}_sd"
        if km in stats and ks in stats:
            return float(stats[km]), float(stats[ks])
        comps: dict[str, list[str]] = {"pr": ["pts", "reb"], "pa": ["pts", "ast"], "ra": ["reb", "ast"]}
        if market in comps:
            means = []
            vars_ = []
            for part in comps[market]:
                km2 = f"{part}_mean"; ks2 = f"{part}_sd"
                if km2 not in stats or ks2 not in stats:
                    return None, None
                means.append(float(stats[km2]))
                vars_.append(float(stats[ks2]) ** 2)
            return float(sum(means)), float(math.sqrt(sum(vars_)))
        return None, None

    candidates: list[dict[str, Any]] = []
    for _, r in pdf.fillna("").iterrows():
        player = str(r.get("player") or "").strip()
        team = _tri_team(r.get("team"))
        if not player or not team:
            continue
        nm = _norm_player(player)
        stats = player_stats.get((team, nm))
        if not isinstance(stats, dict):
            continue

        plays = _parse_obj(r.get("plays"))
        plays_list = plays if isinstance(plays, list) else []
        for pl in plays_list:
            if not isinstance(pl, dict):
                continue
            mkt = str(pl.get("market") or "").strip().lower()
            if not mkt:
                continue
            side = str(pl.get("side") or "").strip().upper()
            line = _safe_float(pl.get("line"))
            if line is None:
                continue
            mean, sd = get_mean_sd(stats, mkt)
            if mean is None or sd is None:
                continue
            wp = _play_win_prob(mean, sd, float(line), side)
            if wp is None:
                continue

            gm = team_to_game.get(team) or {}
            candidates.append(
                {
                    "date": date_str,
                    "player": player,
                    "team": team,
                    "market": mkt,
                    "side": side,
                    "line": float(line),
                    "price": _safe_float(pl.get("price")),
                    "book": pl.get("book") or None,
                    "win_prob": float(wp),
                    "sim_mean": float(mean),
                    "sim_sd": float(sd),
                    "game_id": gm.get("game_id"),
                    "home": gm.get("home"),
                    "away": gm.get("away"),
                }
            )

    # Collapse to best per (player, team, market)
    best: dict[tuple[str, str, str], dict[str, Any]] = {}
    for c in candidates:
        key = (str(c.get("player") or ""), str(c.get("team") or ""), str(c.get("market") or ""))
        prev = best.get(key)
        if prev is None or float(c.get("win_prob") or 0.0) > float(prev.get("win_prob") or 0.0):
            best[key] = c

    return list(best.values())


def audit_ledger_df(
    df: pd.DataFrame,
    *,
    smart_sim_prefix: str,
    seed: int = 7,
    trials: int = 200,
    bins: int = 10,
) -> dict[str, Any]:
    if df is None or df.empty:
        raise ValueError("Empty ledger")

    # Basic headline
    graded = int(df["result"].isin(["W", "L", "P"]).sum())
    wins = int((df["result"] == "W").sum())
    losses = int((df["result"] == "L").sum())
    pushes = int((df["result"] == "P").sum())
    acc = wins / max(1, wins + losses)
    profit = float(pd.to_numeric(df.get("profit"), errors="coerce").sum())
    roi = profit / max(1, int(graded))

    # Win prob distribution
    wp = pd.to_numeric(df.get("win_prob"), errors="coerce")
    wp_ok = wp.dropna()
    wp_summary = {
        "count": int(wp_ok.shape[0]),
        "mean": float(wp_ok.mean()) if len(wp_ok) else None,
        "p50": float(wp_ok.quantile(0.50)) if len(wp_ok) else None,
        "p90": float(wp_ok.quantile(0.90)) if len(wp_ok) else None,
        "p95": float(wp_ok.quantile(0.95)) if len(wp_ok) else None,
        "p99": float(wp_ok.quantile(0.99)) if len(wp_ok) else None,
        "gt_0p9": int((wp_ok > 0.90).sum()),
        "gt_0p95": int((wp_ok > 0.95).sum()),
        "gt_0p99": int((wp_ok > 0.99).sum()),
    }

    # Brier score (ignore pushes)
    df_b = df[df["result"].isin(["W", "L"])].copy()
    df_b["y"] = (df_b["result"] == "W").astype(int)
    df_b["p"] = pd.to_numeric(df_b.get("win_prob"), errors="coerce")
    df_b = df_b[df_b["p"].notna()]
    brier = float(((df_b["p"] - df_b["y"]) ** 2).mean()) if not df_b.empty else None

    # Calibration
    calib = _calibration_table(df, n_bins=int(bins))

    # Random baseline: for each date, sample K from candidate pool (same day)
    rnd = random.Random(int(seed))
    k_by_date = df.groupby("date").size().to_dict()

    baseline_trials: list[dict[str, Any]] = []
    for t in range(int(trials)):
        rows: list[dict[str, Any]] = []
        for dstr, k in k_by_date.items():
            recon = _load_recon_props(str(dstr))
            if not recon:
                continue
            pool = _build_candidate_pool(str(dstr), smart_sim_prefix=str(smart_sim_prefix))
            if not pool:
                continue
            k2 = int(min(int(k), len(pool)))
            sample = rnd.sample(pool, k=k2)
            for pl in sample:
                stats = recon.get((_norm_name(pl.get("player")), _tri_team(pl.get("team"))))
                if not stats:
                    continue
                res = _resolve_prop_play(str(pl.get("market")), str(pl.get("side")), float(pl.get("line") or 0.0), stats)
                if res is None:
                    continue
                used_price = pl.get("price") if pl.get("price") is not None else -110.0
                rows.append(
                    {
                        "date": dstr,
                        "result": res,
                        "profit": _profit_per_unit(res, used_price),
                    }
                )

        bdf = pd.DataFrame(rows)
        if bdf is None or bdf.empty:
            baseline_trials.append({"trial": t, "bets": 0, "acc": None, "roi": None, "profit": 0.0})
            continue
        w = int((bdf["result"] == "W").sum())
        l = int((bdf["result"] == "L").sum())
        graded2 = int(bdf["result"].isin(["W", "L", "P"]).sum())
        prof2 = float(pd.to_numeric(bdf.get("profit"), errors="coerce").sum())
        baseline_trials.append(
            {
                "trial": t,
                "bets": int(len(bdf)),
                "acc": float(w / max(1, w + l)),
                "roi": float(prof2 / max(1, graded2)),
                "profit": float(prof2),
            }
        )

    bt = pd.DataFrame(baseline_trials)
    bt_ok = bt[bt["acc"].notna() & bt["roi"].notna()]
    baseline_summary = {
        "trials": int(len(baseline_trials)),
        "acc_mean": float(bt_ok["acc"].mean()) if not bt_ok.empty else None,
        "acc_p95": float(bt_ok["acc"].quantile(0.95)) if not bt_ok.empty else None,
        "roi_mean": float(bt_ok["roi"].mean()) if not bt_ok.empty else None,
        "roi_p95": float(bt_ok["roi"].quantile(0.95)) if not bt_ok.empty else None,
        "profit_mean": float(bt_ok["profit"].mean()) if not bt_ok.empty else None,
        "profit_p95": float(bt_ok["profit"].quantile(0.95)) if not bt_ok.empty else None,
    }

    payload = {
        "headline": {
            "dates": int(df["date"].nunique()),
            "bets": int(len(df)),
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "accuracy": float(acc),
            "roi": float(roi),
            "profit": float(profit),
        },
        "smart_sim_prefix": str(smart_sim_prefix),
        "win_prob": wp_summary,
        "brier": brier,
        "calibration": calib,
        "random_baseline": baseline_summary,
    }

    return payload


def audit_ledger_file(
    ledger_fp: Path,
    *,
    smart_sim_prefix: str,
    seed: int = 7,
    trials: int = 200,
    bins: int = 10,
) -> dict[str, Any]:
    df = pd.read_csv(ledger_fp) if ledger_fp.exists() else pd.DataFrame()
    if df is None or df.empty:
        raise ValueError(f"Empty ledger: {ledger_fp}")
    payload = audit_ledger_df(
        df,
        smart_sim_prefix=str(smart_sim_prefix),
        seed=int(seed),
        trials=int(trials),
        bins=int(bins),
    )
    payload["ledger"] = str(ledger_fp)
    return payload


def _calibration_table(df: pd.DataFrame, n_bins: int = 10) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    x = pd.to_numeric(df.get("win_prob"), errors="coerce")
    ok = x.notna()
    if ok.sum() <= 0:
        return []
    dd = df.loc[ok].copy()
    dd["win_prob"] = pd.to_numeric(dd["win_prob"], errors="coerce")
    dd["is_win"] = (dd.get("result") == "W").astype(int)

    # equal-width bins in [0,1]
    bins = [i / n_bins for i in range(n_bins + 1)]
    dd["bin"] = pd.cut(dd["win_prob"], bins=bins, include_lowest=True, right=True)

    out: list[dict[str, Any]] = []
    for b, g in dd.groupby("bin", dropna=False, observed=False):
        if g is None or len(g) == 0:
            continue
        out.append(
            {
                "bin": str(b),
                "bets": int(len(g)),
                "avg_pred": float(g["win_prob"].mean()),
                "win_rate": float(g["is_win"].mean()),
            }
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", required=True, help="Ledger CSV from backtest_top_recommendations.py --kind slate_prob")
    ap.add_argument(
        "--smart-sim-prefix",
        type=str,
        default="smart_sim_pregame",
        help="SmartSim JSON prefix to use when building the candidate pool for the random baseline.",
    )
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--trials", type=int, default=200)
    ap.add_argument("--bins", type=int, default=10)
    ap.add_argument("--out-json", default=str(PROCESSED / "audit_slate_prob_backtest.json"))
    args = ap.parse_args()

    ledger_fp = Path(args.ledger)
    try:
        payload = audit_ledger_file(
            ledger_fp,
            smart_sim_prefix=str(args.smart_sim_prefix),
            seed=int(args.seed),
            trials=int(args.trials),
            bins=int(args.bins),
        )
    except Exception as e:
        raise SystemExit(str(e))

    out = Path(args.out_json)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    brier = payload.get("brier")
    baseline_summary = payload.get("random_baseline") or {}
    print(
        "Audit:",
        {
            "dates": payload["headline"]["dates"],
            "bets": payload["headline"]["bets"],
            "acc_pct": round(100 * payload["headline"]["accuracy"], 2),
            "roi_pct": round(100 * payload["headline"]["roi"], 2),
            "profit": round(payload["headline"]["profit"], 3),
            "brier": None if brier is None else round(float(brier), 4),
            "baseline_acc_mean_pct": None
            if baseline_summary.get("acc_mean") is None
            else round(100 * float(baseline_summary["acc_mean"]), 2),
            "baseline_roi_mean_pct": None
            if baseline_summary.get("roi_mean") is None
            else round(100 * float(baseline_summary["roi_mean"]), 2),
        },
    )
    print(f"Wrote: {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
