#!/usr/bin/env python3
"""Optimize Live Lens player-prop watch/bet thresholds from logged signals + actuals.

Goal: choose (watch, bet) cutoffs on `strength` for market=player_prop that maximize
profit per bet (units), subject to minimum sample size.

Reads:
- data/processed/live_lens_signals_<date>.jsonl
- data/processed/recon_props_<date>.csv

Writes:
- data/processed/live_lens_player_prop_thresholds_<start>_<end>.csv
- (optional) data/processed/live_lens_tuning_override.json (merge update)

This mirrors NCAAB's “retune from logs” concept, but tuned to NBA’s player props.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
LIVE_LENS_DIR = Path((os.getenv("NBA_LIVE_LENS_DIR") or os.getenv("LIVE_LENS_DIR") or "").strip() or str(PROCESSED))


def _parse_date(s: str) -> _date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: _date, end: _date) -> Iterable[_date]:
    cur = start
    while cur <= end:
        yield cur
        cur = cur + timedelta(days=1)


def _n(x: Any) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _norm_player_name(s: str) -> str:
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


def _live_stat_key(x: Any) -> str:
    s = str(x or "").strip().lower()
    m = {
        "points": "pts",
        "point": "pts",
        "pts": "pts",
        "rebounds": "reb",
        "rebound": "reb",
        "reb": "reb",
        "assists": "ast",
        "assist": "ast",
        "ast": "ast",
        "3pt": "threes",
        "3pm": "threes",
        "threes": "threes",
        "threes_made": "threes",
        "pra": "pra",
        "points+rebounds+assists": "pra",
        "pr": "pr",
        "points+rebounds": "pr",
        "pa": "pa",
        "points+assists": "pa",
        "ra": "ra",
        "rebounds+assists": "ra",
    }
    return m.get(s, s)


def _american_profit(price: float, win: bool) -> float:
    if not win:
        return -1.0
    try:
        p = float(price)
    except Exception:
        return float("nan")
    if p == 0:
        return float("nan")
    if p > 0:
        return p / 100.0
    return 100.0 / abs(p)


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                out.append(obj)
    return out


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _prep_recon_props(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "player_name" in out.columns:
        out["_name_key"] = out["player_name"].astype(str).map(_norm_player_name)
    else:
        out["_name_key"] = ""

    for c in ("pts", "reb", "ast", "threes"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    if all(c in out.columns for c in ("pts", "reb")) and "pr" not in out.columns:
        out["pr"] = out["pts"] + out["reb"]
    if all(c in out.columns for c in ("pts", "ast")) and "pa" not in out.columns:
        out["pa"] = out["pts"] + out["ast"]
    if all(c in out.columns for c in ("reb", "ast")) and "ra" not in out.columns:
        out["ra"] = out["reb"] + out["ast"]
    if all(c in out.columns for c in ("pts", "reb", "ast")) and "pra" not in out.columns:
        out["pra"] = out["pts"] + out["reb"] + out["ast"]

    return out


def _actual_prop(name_key: str, stat_key: str, rp: pd.DataFrame) -> float | None:
    if rp.empty:
        return None
    if not name_key:
        return None
    hit = rp[rp.get("_name_key") == name_key]
    if hit.empty:
        return None
    if stat_key not in hit.columns:
        return None
    return _n(hit.iloc[0].get(stat_key))


def _settle_over_under(actual: float, line: float, side: str) -> tuple[str, bool | None]:
    s = (side or "").strip().upper()
    if s not in {"OVER", "UNDER"}:
        return "", None
    if actual == line:
        return "PUSH", None
    if s == "OVER":
        return ("WIN", True) if actual > line else ("LOSS", False)
    return ("WIN", True) if actual < line else ("LOSS", False)


def _iter_prop_rows(ds: str, assumed_juice: float) -> list[dict[str, Any]]:
    sigs = _load_jsonl(LIVE_LENS_DIR / f"live_lens_signals_{ds}.jsonl")
    if not sigs:
        return []
    rp = _prep_recon_props(_load_csv(PROCESSED / f"recon_props_{ds}.csv"))
    if rp.empty:
        return []

    rows: list[dict[str, Any]] = []
    for obj in sigs:
        if str(obj.get("market") or "").strip().lower() != "player_prop":
            continue

        strength = _n(obj.get("strength"))
        if strength is None:
            continue

        side = str(obj.get("side") or "").strip().upper()
        line = _n(obj.get("line"))
        if not side or line is None:
            continue

        player = str(obj.get("player") or "").strip()
        stat = str(obj.get("stat") or "").strip()
        stat_key = _live_stat_key(stat)
        name_key = str(obj.get("name_key") or "").strip().upper() or _norm_player_name(player)

        actual = _actual_prop(name_key, stat_key, rp)
        if actual is None:
            continue

        outcome, win = _settle_over_under(float(actual), float(line), side)
        if outcome not in {"WIN", "LOSS", "PUSH"}:
            continue

        ctx = obj.get("context")
        ctx_price = None
        if isinstance(ctx, dict):
            if side == "OVER":
                ctx_price = _n(ctx.get("price_over"))
            elif side == "UNDER":
                ctx_price = _n(ctx.get("price_under"))
            if ctx_price is None:
                ctx_price = _n(ctx.get("price"))

        price = float(ctx_price) if ctx_price is not None else -float(abs(assumed_juice))
        profit = 0.0 if outcome == "PUSH" else (_american_profit(price, bool(win)) if win is not None else float("nan"))

        rows.append(
            {
                "date": ds,
                "name_key": name_key,
                "player": player,
                "stat": stat_key,
                "side": side,
                "line": float(line),
                "actual": float(actual),
                "strength": float(strength),
                "price": float(price),
                "outcome": outcome,
                "profit_u": float(profit),
            }
        )

    return rows


def _score_thresholds(df: pd.DataFrame, watch_thr: float, bet_thr: float, min_bets: int) -> dict[str, Any] | None:
    if df is None or df.empty:
        return None
    if bet_thr < watch_thr:
        return None

    d = df.copy()
    s = d["strength"].astype(float)
    d["klass"] = "NONE"
    d.loc[s >= float(watch_thr), "klass"] = "WATCH"
    d.loc[s >= float(bet_thr), "klass"] = "BET"

    bets = d[d["klass"] == "BET"].copy()
    if len(bets) < int(min_bets):
        return None

    profit = float(pd.to_numeric(bets["profit_u"], errors="coerce").fillna(0.0).sum())
    wins = int((bets["outcome"] == "WIN").sum())
    losses = int((bets["outcome"] == "LOSS").sum())
    pushes = int((bets["outcome"] == "PUSH").sum())
    denom = max(1, (wins + losses + pushes))
    roi = profit / float(denom)
    wr_denom = max(1, (wins + losses))
    wr = wins / float(wr_denom)

    return {
        "watch": float(watch_thr),
        "bet": float(bet_thr),
        "bets": int(len(bets)),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "profit_u": round(profit, 4),
        "roi_u_per_bet": round(roi, 4),
        "win_rate": round(wr, 4),
    }


def _merge_override(payload: dict[str, Any], override_path: Path) -> None:
    base: dict[str, Any] = {}
    if override_path.exists():
        try:
            base_obj = json.loads(override_path.read_text(encoding="utf-8"))
            if isinstance(base_obj, dict):
                base = base_obj
        except Exception:
            base = {}

    markets = base.get("markets")
    if not isinstance(markets, dict):
        markets = {}

    pp = markets.get("player_prop")
    if not isinstance(pp, dict):
        pp = {}

    pp["watch"] = float(payload["watch"])
    pp["bet"] = float(payload["bet"])
    markets["player_prop"] = pp
    base["markets"] = markets

    base["generated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    override_path.write_text(json.dumps(base, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Optimize Live Lens player prop thresholds")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--assumed-juice", type=float, default=110.0)
    ap.add_argument("--min-bets", type=int, default=40)
    ap.add_argument(
        "--out",
        default="",
        help="Output CSV path (default: <NBA_LIVE_LENS_DIR>/live_lens_player_prop_thresholds_<start>_<end>.csv; defaults to data/processed)",
    )
    ap.add_argument(
        "--write-override",
        action="store_true",
        help="Merge best thresholds into <NBA_LIVE_LENS_DIR>/live_lens_tuning_override.json (defaults to data/processed)",
    )
    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)

    frames: list[pd.DataFrame] = []
    for d in _daterange(start, end):
        ds = d.isoformat()
        rows = _iter_prop_rows(ds, assumed_juice=float(args.assumed_juice))
        if rows:
            frames.append(pd.DataFrame(rows))

    if not frames:
        print("No settled player_prop rows found in window")
        return 2

    df = pd.concat(frames, ignore_index=True)

    watch_grid = [1.5, 2.0, 2.5, 3.0, 3.5]
    bet_grid = [2.5, 3.0, 3.5, 4.0, 4.5, 5.0]

    scored: list[dict[str, Any]] = []
    for w in watch_grid:
        for b in bet_grid:
            ent = _score_thresholds(df, w, b, min_bets=int(args.min_bets))
            if ent:
                scored.append(ent)

    if not scored:
        print("No thresholds met min_bets")
        return 2

    res = pd.DataFrame(scored).sort_values(["roi_u_per_bet", "profit_u", "bets"], ascending=[False, False, False])

    out_path = Path(args.out) if args.out else (LIVE_LENS_DIR / f"live_lens_player_prop_thresholds_{start.isoformat()}_{end.isoformat()}.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    res.to_csv(out_path, index=False)

    best = res.iloc[0].to_dict()
    print("Best:", best)
    print("Wrote:", out_path)

    if args.write_override:
        override_path = LIVE_LENS_DIR / "live_lens_tuning_override.json"
        override_path.parent.mkdir(parents=True, exist_ok=True)
        _merge_override(best, override_path)
        print("Wrote override:", override_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
