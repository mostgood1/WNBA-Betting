#!/usr/bin/env python3
"""Optimize Live Lens player-prop bettability gating threshold.

Goal
- Learn a minimum bettability score threshold that improves ROI for player-prop bets,
  using the existing live_lens_signals_*.jsonl logs and recon_props_*.csv actuals.

Notes
- This tunes *gating* only. It does not change how `strength` is computed.
- The bettability score is computed from fields already logged in player_prop signals.
  (It intentionally does not depend on live-line dispersion metadata since that is
  not currently logged in signals.)

Reads
- <LIVE_LENS_DIR>/live_lens_signals_<date>.jsonl
- data/processed/recon_props_<date>.csv

Writes
- data/processed/live_lens_player_prop_bettability_<start>_<end>.csv
- (optional) merges into <LIVE_LENS_DIR>/live_lens_tuning_override.json:
    markets.player_prop.bettability = { gating: true, min_score: <float>, ... }

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


def _safe_int(x: Any) -> int | None:
    try:
        if x is None:
            return None
        v = int(round(float(x)))
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
        "steals": "stl",
        "steal": "stl",
        "stl": "stl",
        "blocks": "blk",
        "block": "blk",
        "blk": "blk",
        "turnovers": "tov",
        "turnover": "tov",
        "tov": "tov",
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

    for c in ("pts", "reb", "ast", "threes", "stl", "blk", "tov", "pra"):
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


@dataclass(frozen=True)
class PropBetRow:
    date: str
    name_key: str
    player: str
    stat: str
    side: str
    line: float
    strength: float
    sim_sd: float | None
    line_source: str | None
    elapsed: float | None
    margin_home: float | None
    pf: float | None
    starter: bool | None
    injury_flag: bool | None
    implied_over: float | None
    implied_under: float | None
    price: float
    outcome: str
    profit_u: float


def _bettability_score(row: PropBetRow) -> tuple[float | None, list[str], float | None, float | None]:
    """Return (score, reasons, price_hold, edge_sigma)."""
    try:
        reasons: list[str] = []
        score = 1.0

        # Logs may not include line provenance. Only treat explicit model fallback as unbettable.
        if row.line_source == "model":
            return 0.0, ["no_market_line"], None, None
        if row.line_source in {None, "", "unknown"}:
            score -= 0.05
            reasons.append("unknown_line_source")

        em = float(row.elapsed) if row.elapsed is not None else 0.0
        if row.line_source == "pregame" and em >= 12.0:
            score -= 0.20
            reasons.append("pregame_line_in_live")

        # Price fields are often missing in older logs; don't strongly penalize.
        if not math.isfinite(float(row.price)):
            score -= 0.05
            reasons.append("missing_prices")

        price_hold = None
        if row.implied_over is not None and row.implied_under is not None:
            try:
                price_hold = float(row.implied_over) + float(row.implied_under) - 1.0
                if math.isfinite(price_hold) and price_hold > 0.08:
                    score -= 0.10
                    reasons.append("high_hold")
            except Exception:
                price_hold = None

        if bool(row.injury_flag):
            score -= 0.45
            reasons.append("injury_flag")

        pf_i = _safe_int(row.pf)
        if pf_i is not None and pf_i >= 5:
            score -= 0.20
            reasons.append("foul_trouble")

        try:
            mabs = abs(float(row.margin_home)) if row.margin_home is not None else None
        except Exception:
            mabs = None
        if row.starter is True and mabs is not None and mabs >= 20.0 and em >= 30.0:
            score -= 0.20
            reasons.append("blowout_risk")

        edge_sigma = None
        if row.sim_sd is not None and row.sim_sd > 1e-6:
            try:
                edge_sigma = float(row.strength) / float(row.sim_sd)
                if math.isfinite(edge_sigma) and edge_sigma < 0.50:
                    score -= 0.10
                    reasons.append("weak_vs_vol")
            except Exception:
                edge_sigma = None

        score = float(max(0.0, min(1.0, score)))
        return score, reasons, price_hold, edge_sigma
    except Exception:
        return None, [], None, None


def _iter_bet_rows(ds: str, assumed_juice: float, *, require_logged_bets: bool) -> list[PropBetRow]:
    sigs = _load_jsonl(LIVE_LENS_DIR / f"live_lens_signals_{ds}.jsonl")
    if not sigs:
        return []
    rp = _prep_recon_props(_load_csv(PROCESSED / f"recon_props_{ds}.csv"))
    if rp.empty:
        return []

    # De-dup: logs can include the same prop signal many times (polling).
    # Keep the earliest instance per unique decision key.
    by_key: dict[tuple[Any, ...], tuple[float, PropBetRow]] = {}
    for obj in sigs:
        if str(obj.get("market") or "").strip().lower() != "player_prop":
            continue

        # Default: tune only live player-prop signals.
        # Treat missing horizon as live for backward compatibility.
        hz = str(obj.get("horizon") or "").strip().lower()
        if hz and hz != "live":
            continue

        klass = str(obj.get("klass") or "").strip().upper()
        if require_logged_bets:
            if klass != "BET":
                continue
        else:
            if klass not in {"BET", "WATCH"}:
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
        sim_sd = None
        injury_flag = None
        implied_over = None
        implied_under = None
        ctx_line_source = None
        if isinstance(ctx, dict):
            if side == "OVER":
                ctx_price = _n(ctx.get("price_over"))
            elif side == "UNDER":
                ctx_price = _n(ctx.get("price_under"))
            if ctx_price is None:
                ctx_price = _n(ctx.get("price"))

            # Older logs sometimes store non-American values (e.g., -2.5, 0.55).
            # Treat those as missing and fall back to assumed juice.
            try:
                if ctx_price is not None:
                    apx = float(ctx_price)
                    if (abs(apx) < 50.0) or (abs(apx) > 10000.0):
                        ctx_price = None
            except Exception:
                ctx_price = None
            sim_sd = _n(ctx.get("sim_sd"))
            injury_flag = bool(ctx.get("injury_flag")) if ("injury_flag" in ctx) else None
            implied_over = _n(ctx.get("implied_prob_over"))
            implied_under = _n(ctx.get("implied_prob_under"))
            ctx_line_source = str(ctx.get("line_source") or "").strip() or None

        line_source = str(obj.get("line_source") or "").strip() or ctx_line_source or "unknown"

        price = float(ctx_price) if ctx_price is not None else -float(abs(assumed_juice))
        profit = 0.0 if outcome == "PUSH" else (_american_profit(price, bool(win)) if win is not None else float("nan"))

        # Derive a stable key
        gid0 = str(obj.get("event_id") or obj.get("game_id") or "").strip()
        sk0 = str(obj.get("signal_key") or "").strip() or f"player_prop:{player}:{stat_key}"
        try:
            line_key = float(line)
        except Exception:
            line_key = line
        key = (ds, gid0, sk0.lower(), side, line_key)

        # Choose earliest received_at when present
        ra = str(obj.get("received_at") or "").strip()
        ts = float("inf")
        if ra:
            try:
                ts = datetime.fromisoformat(ra.replace("Z", "+00:00")).timestamp()
            except Exception:
                ts = float("inf")

        row = (
            PropBetRow(
                date=ds,
                name_key=name_key,
                player=player,
                stat=stat_key,
                side=side,
                line=float(line),
                strength=float(strength),
                sim_sd=float(sim_sd) if sim_sd is not None else None,
                line_source=line_source,
                elapsed=_n(obj.get("elapsed")),
                margin_home=_n(obj.get("margin_home")),
                pf=_n(obj.get("pf")),
                starter=(bool(obj.get("starter")) if ("starter" in obj and obj.get("starter") is not None) else None),
                injury_flag=injury_flag,
                implied_over=implied_over,
                implied_under=implied_under,
                price=float(price),
                outcome=outcome,
                profit_u=float(profit),
            )
        )

        prev = by_key.get(key)
        if prev is None or ts < prev[0]:
            by_key[key] = (ts, row)

    return [v[1] for v in by_key.values()]


def _score_threshold(df: pd.DataFrame, thr: float, min_bets: int) -> dict[str, Any] | None:
    if df is None or df.empty:
        return None

    d = df[df["bettable_score"].notna() & (df["bettable_score"].astype(float) >= float(thr))].copy()
    if len(d) < int(min_bets):
        return None

    profit = float(pd.to_numeric(d["profit_u"], errors="coerce").fillna(0.0).sum())
    wins = int((d["outcome"] == "WIN").sum())
    losses = int((d["outcome"] == "LOSS").sum())
    pushes = int((d["outcome"] == "PUSH").sum())
    denom = max(1, (wins + losses + pushes))
    roi = profit / float(denom)
    wr_denom = max(1, (wins + losses))
    wr = wins / float(wr_denom)

    return {
        "min_score": float(thr),
        "bets": int(len(d)),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "profit_u": round(profit, 4),
        "roi_u_per_bet": round(roi, 4),
        "win_rate": round(wr, 4),
    }


def _merge_override(best: dict[str, Any], override_path: Path, start: str, end: str) -> None:
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

    bett = pp.get("bettability")
    if not isinstance(bett, dict):
        bett = {}

    bett.update(
        {
            "gating": True,
            "min_score": float(best.get("min_score")),
            "window_start": start,
            "window_end": end,
            "bets": int(best.get("bets", 0)),
            "roi_u_per_bet": float(best.get("roi_u_per_bet", 0.0)),
        }
    )

    pp["bettability"] = bett
    markets["player_prop"] = pp
    base["markets"] = markets

    override_path.write_text(json.dumps(base, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--min-bets", type=int, default=40)
    ap.add_argument("--assumed-juice", type=float, default=110.0)
    ap.add_argument(
        "--require-logged-bets",
        action="store_true",
        help="Only tune gating on rows that were logged as klass=BET (recommended for stability).",
    )
    ap.add_argument(
        "--write-override",
        action="store_true",
        help="Merge best bettability threshold into live_lens_tuning_override.json",
    )
    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)

    all_rows: list[dict[str, Any]] = []
    for d in _daterange(start, end):
        ds = d.isoformat()
        rows = _iter_bet_rows(ds, assumed_juice=float(args.assumed_juice), require_logged_bets=bool(args.require_logged_bets))
        for r in rows:
            score, reasons, price_hold, edge_sigma = _bettability_score(r)
            if score is None:
                continue
            all_rows.append(
                {
                    "date": r.date,
                    "name_key": r.name_key,
                    "player": r.player,
                    "stat": r.stat,
                    "side": r.side,
                    "line": r.line,
                    "strength": r.strength,
                    "sim_sd": r.sim_sd,
                    "line_source": r.line_source,
                    "elapsed": r.elapsed,
                    "margin_home": r.margin_home,
                    "pf": r.pf,
                    "starter": r.starter,
                    "injury_flag": r.injury_flag,
                    "implied_prob_over": r.implied_over,
                    "implied_prob_under": r.implied_under,
                    "price_hold": price_hold,
                    "edge_sigma": edge_sigma,
                    "bettable_score": score,
                    "bettable_reasons": "|".join(reasons),
                    "price": r.price,
                    "outcome": r.outcome,
                    "profit_u": r.profit_u,
                }
            )

    df = pd.DataFrame(all_rows)
    out_csv = PROCESSED / f"live_lens_player_prop_bettability_{start.isoformat()}_{end.isoformat()}.csv"
    try:
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv, index=False)
        print(f"Wrote: {out_csv} rows={len(df)}")
    except Exception as e:
        print(f"WARN: failed to write csv: {e}")

    if df.empty:
        print("No rows; nothing to tune")
        return 0

    # Candidate thresholds
    # Prefer a sensible operating range; fall back to lower values only if needed.
    cands_primary = [round(x, 2) for x in [0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90]]
    cands_fallback = [round(x, 2) for x in [0.35, 0.40, 0.45, 0.50]]

    def _score_cands(cands: list[float]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for thr in cands:
            rec = _score_threshold(df, float(thr), min_bets=int(args.min_bets))
            if rec:
                out.append(rec)
        return out

    scored = _score_cands(cands_primary)
    if not scored:
        scored = _score_cands(cands_fallback)

    if not scored:
        print("No candidate thresholds met min-bets; leaving override unchanged")
        return 0

    scored.sort(key=lambda r: (float(r.get("roi_u_per_bet", -1e9)), int(r.get("bets", 0))), reverse=True)
    best = scored[0]

    print("Best bettability threshold:")
    print(json.dumps(best, indent=2, sort_keys=True))

    if args.write_override:
        override_path = LIVE_LENS_DIR / "live_lens_tuning_override.json"
        _merge_override(best, override_path, start.isoformat(), end.isoformat())
        print(f"Merged into override: {override_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
