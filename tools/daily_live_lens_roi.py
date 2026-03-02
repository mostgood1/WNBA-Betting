#!/usr/bin/env python3
"""Daily Live Lens ROI report: JSONL signals -> settled rows + markdown summary.

Reads (for a given date):
- data/processed/live_lens_signals_<date>.jsonl
- data/processed/recon_games_<date>.csv (game totals + spreads)
- data/processed/recon_quarters_<date>.csv (half/quarter totals)
- data/processed/recon_props_<date>.csv (player props actuals)

Writes:
- data/processed/reports/live_lens_roi_<date>.md
- data/processed/reports/live_lens_roi_scored_<date>.csv

This report settles logged *signals* (BET/WATCH/NONE) into realized outcomes.
It is intentionally conservative:
- Uses 1u risk units.
- Uses logged prices when available (player props), otherwise assumes -110.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
LIVE_LENS_DIR = Path((os.getenv("NBA_LIVE_LENS_DIR") or os.getenv("LIVE_LENS_DIR") or "").strip() or str(PROCESSED))
REPORTS = PROCESSED / "reports"


def _parse_date(s: str) -> _date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _iso(d: _date) -> str:
    return d.isoformat()


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


def _canon_nba_game_id(game_id: Any) -> str:
    try:
        raw = str(game_id or "").strip()
    except Exception:
        return ""
    digits = "".join([c for c in raw if c.isdigit()])
    if len(digits) == 8:
        return "00" + digits
    if len(digits) == 9:
        return "0" + digits
    return digits


def _is_canon_gid(gid: str | None) -> bool:
    g = str(gid or "").strip()
    return len(g) == 10 and g.isdigit()


def _safe_upper(x: Any) -> str | None:
    try:
        if x is None:
            return None
        if isinstance(x, float) and math.isnan(x):
            return None
        s = str(x).strip().upper()
        if not s or s in {"NAN", "NONE", "NULL"}:
            return None
        return s
    except Exception:
        return None


def _load_gid_map(ds: str) -> dict[tuple[str, str], str]:
    """Build (home_tri, away_tri) -> canonical NBA game_id map for a date.

    Live Lens logs often carry matchup ids like "WAS@ATL" while recon files
    are keyed by the canonical 10-digit NBA game_id.
    """
    out: dict[tuple[str, str], str] = {}
    sched_path = PROCESSED / "schedule_2025_26.json"
    if not sched_path.exists():
        return out
    try:
        data = json.loads(sched_path.read_text(encoding="utf-8"))
    except Exception:
        return out
    if not isinstance(data, list):
        return out
    for g in data:
        if not isinstance(g, dict):
            continue
        date_est = str(g.get("date_est") or "")
        if date_est[:10] != ds:
            continue
        home = _safe_upper(g.get("home_tricode"))
        away = _safe_upper(g.get("away_tricode"))
        gid = _canon_nba_game_id(g.get("game_id"))
        if home and away and _is_canon_gid(gid):
            out[(home, away)] = gid
    return out


def _resolve_gid(obj: dict[str, Any], gid_map: dict[tuple[str, str], str]) -> str | None:
    gid0 = _canon_nba_game_id(obj.get("game_id_canon") or obj.get("game_id"))
    if _is_canon_gid(gid0):
        return gid0

    home = _safe_upper(obj.get("home"))
    away = _safe_upper(obj.get("away"))
    if home and away:
        gid = gid_map.get((home, away))
        if gid:
            return gid

    raw = str(obj.get("game_id") or "").strip().upper()
    m = re.match(r"^([A-Z]{3})\s*@\s*([A-Z]{3})$", raw)
    if m:
        away2, home2 = m.group(1), m.group(2)
        gid = gid_map.get((home2, away2))
        if gid:
            return gid

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


def _parse_iso_ts(s: Any) -> Optional[datetime]:
    try:
        t = str(s or "").strip()
        if not t:
            return None
        # pandas handles many variants; keep this local/simple
        dt = pd.to_datetime(t, errors="coerce", utc=True)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


def _american_profit(price: float, win: bool) -> float:
    """Return profit for 1u risk at American odds, excluding stake (loss is -1)."""
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


def _clean_american_price(price: float | None) -> float | None:
    """Validate that an American odds price looks plausible for betting markets."""
    try:
        if price is None:
            return None
        p = float(price)
        if not math.isfinite(p) or p == 0:
            return None
        ap = abs(p)
        # Props odds are rarely shorter than -2000 or longer than +2000.
        # Also guard against clearly-wrong values like -4 or 0.95.
        if ap < 50:
            return None
        if ap > 10000:
            return None
        return float(p)
    except Exception:
        return None

def _settle_over_under(actual: float, line: float, side: str) -> tuple[str, bool | None]:
    s = (side or "").strip().upper()
    if s not in {"OVER", "UNDER"}:
        return "", None
    if actual == line:
        return "PUSH", None
    if s == "OVER":
        return ("WIN", True) if actual > line else ("LOSS", False)
    return ("WIN", True) if actual < line else ("LOSS", False)


def _settle_ats(actual_margin_home: float, side: str) -> tuple[str, bool | None, float | None, str | None]:
    """Settle ATS given final home margin and a side string like 'BOS -3.5'."""
    s = str(side or "").strip().upper()
    if not s:
        return "", None, None, None

    # Extract first token as team code, and first signed float as spread
    m_team = re.match(r"^([A-Z]{2,4})\b", s)
    m_spread = re.search(r"([+-]?\d+(?:\.\d+)?)", s)
    if not m_team or not m_spread:
        return "", None, None, None

    team = m_team.group(1)
    try:
        spread = float(m_spread.group(1))
    except Exception:
        return "", None, None, team

    # If the side includes something like 'BOS -3.5', spread is relative to that team.
    # To settle, we need that team's final margin (team pts - opp pts).
    # Caller provides only home margin, so we return team and spread; caller decides sign.
    return "", None, spread, team


def _rem_bucket(market: str, horizon: str | None, remaining_min: float | None) -> str | None:
    if remaining_min is None:
        return None
    rm = float(remaining_min)

    # Bucket shapes: simple + stable (good for dashboarding)
    if market == "quarter_total":
        # 12 minute quarter
        if rm >= 6:
            return "6+"
        if rm >= 4:
            return "4-6"
        if rm >= 2:
            return "2-4"
        return "<2"

    if market == "half_total":
        # 24 minute half
        if rm >= 12:
            return "12+"
        if rm >= 8:
            return "8-12"
        if rm >= 4:
            return "4-8"
        return "<4"

    # Full game totals, ATS, and player props bucket on game minutes remaining
    if rm >= 24:
        return "24+"
    if rm >= 18:
        return "18-24"
    if rm >= 12:
        return "12-18"
    if rm >= 6:
        return "6-12"
    return "<6"


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


def _dedup_key_for_signal(obj: dict[str, Any]) -> tuple[Any, ...] | None:
    try:
        market = str(obj.get("market") or "").strip().lower()
        if market not in {"total", "half_total", "quarter_total", "ats", "player_prop"}:
            return None

        gid = obj.get("_gid") or (_canon_nba_game_id(obj.get("game_id")) or None)
        horizon = str(obj.get("horizon") or "").strip().lower() or None
        side = str(obj.get("side") or "").strip().upper() or None

        if market == "player_prop":
            stat_key = _live_stat_key(obj.get("stat"))
            name_key = _norm_player_name(str(obj.get("name_key") or obj.get("player") or ""))
            if not name_key or not stat_key:
                return None
            # "Bet idea" key: (game, player, stat, side)
            return (market, gid, name_key, stat_key, side)

        if market in {"total", "half_total", "quarter_total"}:
            return (market, gid, horizon, side)

        # ATS side strings include team+spread, so side is already descriptive.
        return (market, gid, side)
    except Exception:
        return None


def _signal_sort_key(obj: dict[str, Any], idx: int) -> tuple[float, int]:
    # Earlier first. Fall back to original order for stability.
    ts = None
    for k in ("received_at", "ts", "created_at"):
        if obj.get(k):
            ts = _parse_iso_ts(obj.get(k))
            if ts is not None:
                break
    if ts is not None:
        try:
            return (float(ts.timestamp()), int(idx))
        except Exception:
            return (float(idx), int(idx))
    return (float(idx), int(idx))


def _dedup_signals(signals: list[dict[str, Any]], policy: str) -> list[dict[str, Any]]:
    p = str(policy or "none").strip().lower()
    if p in {"none", "off", "0", "false"}:
        return signals

    groups: dict[tuple[Any, ...], list[tuple[int, dict[str, Any]]]] = {}
    for i, obj in enumerate(signals):
        if not isinstance(obj, dict):
            continue
        k = _dedup_key_for_signal(obj)
        if k is None:
            # Keep unkeyable rows (rare) by giving them unique keys.
            k = ("__unkeyable__", i)
        groups.setdefault(k, []).append((i, obj))

    picked: list[tuple[int, dict[str, Any]]] = []
    for _, items in groups.items():
        if not items:
            continue

        if p == "latest":
            best = max(items, key=lambda it: _signal_sort_key(it[1], it[0]))
            picked.append(best)
            continue

        if p == "max_strength":
            def _strength_key(it: tuple[int, dict[str, Any]]) -> tuple[float, float, int]:
                i0, o0 = it
                s = _n(o0.get("strength"))
                v = abs(float(s)) if (s is not None and math.isfinite(float(s))) else float("-inf")
                ts0, _ = _signal_sort_key(o0, i0)
                return (v, -float(ts0), -int(i0))

            best = max(items, key=_strength_key)
            picked.append(best)
            continue

        if p == "first_bet":
            bet_items = [it for it in items if str(it[1].get("klass") or "").strip().upper() == "BET"]
            cand = bet_items or items
            best = min(cand, key=lambda it: _signal_sort_key(it[1], it[0]))
            picked.append(best)
            continue

        # default: first
        best = min(items, key=lambda it: _signal_sort_key(it[1], it[0]))
        picked.append(best)

    picked.sort(key=lambda it: it[0])
    return [obj for _, obj in picked]


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _prep_recon_games(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "game_id" in out.columns:
        out["_gid"] = out["game_id"].map(_canon_nba_game_id)
    else:
        out["_gid"] = ""
    for c in ("home_tri", "away_tri"):
        if c in out.columns:
            out[c] = out[c].astype(str).str.strip().str.upper()
    return out


def _prep_recon_quarters(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "game_id" in out.columns:
        out["_gid"] = out["game_id"].map(_canon_nba_game_id)
    else:
        out["_gid"] = ""
    for c in ("home_tri", "away_tri"):
        if c in out.columns:
            out[c] = out[c].astype(str).str.strip().str.upper()
    return out


def _prep_recon_props(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "game_id" in out.columns:
        out["_gid"] = out["game_id"].map(_canon_nba_game_id)
    else:
        out["_gid"] = ""

    if "player_name" in out.columns:
        out["_name_key"] = out["player_name"].astype(str).map(_norm_player_name)
    else:
        out["_name_key"] = ""

    # Ensure numeric stat cols
    for c in ("pts", "reb", "ast", "threes"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    # Derive combos if absent
    if all(c in out.columns for c in ("pts", "reb")) and "pr" not in out.columns:
        out["pr"] = out["pts"] + out["reb"]
    if all(c in out.columns for c in ("pts", "ast")) and "pa" not in out.columns:
        out["pa"] = out["pts"] + out["ast"]
    if all(c in out.columns for c in ("reb", "ast")) and "ra" not in out.columns:
        out["ra"] = out["reb"] + out["ast"]
    if all(c in out.columns for c in ("pts", "reb", "ast")) and "pra" not in out.columns:
        out["pra"] = out["pts"] + out["reb"] + out["ast"]

    return out


def _actual_total(market: str, horizon: str | None, gid: str | None, home: str | None, away: str | None, rg: pd.DataFrame, rq: pd.DataFrame) -> float | None:
    if market == "total":
        if rg.empty:
            return None
        if gid:
            hit = rg[rg.get("_gid") == gid]
            if not hit.empty:
                return _n(hit.iloc[0].get("total_actual"))
        if home and away:
            hit = rg[(rg.get("home_tri") == home) & (rg.get("away_tri") == away)]
            if not hit.empty:
                return _n(hit.iloc[0].get("total_actual"))
        return None

    if market in {"half_total", "quarter_total"}:
        if rq.empty:
            return None
        hit = pd.DataFrame()
        if gid and "_gid" in rq.columns:
            hit = rq[rq.get("_gid") == gid]
        if hit.empty and home and away:
            hit = rq[(rq.get("home_tri") == home) & (rq.get("away_tri") == away)]
        if hit.empty:
            return None

        row = hit.iloc[0]
        if market == "half_total":
            if horizon == "h1":
                return _n(row.get("actual_h1_total"))
            if horizon == "h2":
                return _n(row.get("actual_h2_total"))
            return None

        if market == "quarter_total":
            hz = horizon or ""
            if hz in {"q1", "q2", "q3", "q4"}:
                return _n(row.get(f"actual_{hz}_total"))
            return None

    return None


def _actual_margin_home(gid: str | None, home: str | None, away: str | None, rg: pd.DataFrame) -> float | None:
    if rg.empty:
        return None
    hit = pd.DataFrame()
    if gid:
        hit = rg[rg.get("_gid") == gid]
    if hit.empty and home and away:
        hit = rg[(rg.get("home_tri") == home) & (rg.get("away_tri") == away)]
    if hit.empty:
        return None
    # recon_games uses actual_margin (home - away)
    return _n(hit.iloc[0].get("actual_margin"))


def _actual_prop(name_key: str | None, stat_key: str, rp: pd.DataFrame) -> float | None:
    if rp.empty:
        return None
    nk = (name_key or "").strip().upper()
    if not nk:
        return None
    hit = rp[rp.get("_name_key") == nk]
    if hit.empty:
        return None
    col = stat_key
    if col not in hit.columns:
        return None
    return _n(hit.iloc[0].get(col))


@dataclass(frozen=True)
class Scored:
    date: str
    market: str
    horizon: str | None
    klass: str | None
    game_id: str | None
    home: str | None
    away: str | None
    player: str | None
    stat: str | None
    side: str | None
    line: float | None
    actual: float | None
    price: float | None
    outcome: str | None
    profit_u: float | None
    remaining: float | None
    rem_bucket: str | None
    received_at: str | None
    strength: float | None


def _score_rows(
    ds: str,
    assumed_juice: float,
    include_watch: bool,
    dedup_policy: str,
    include_model_lines: bool,
) -> tuple[list[Scored], dict[str, Any]]:
    sig_path = LIVE_LENS_DIR / f"live_lens_signals_{ds}.jsonl"
    sigs = _load_jsonl(sig_path)
    if not sigs:
        return [], {"dedup_policy": str(dedup_policy), "signals_raw": 0, "signals_filtered": 0, "signals_dedup": 0}

    gid_map = _load_gid_map(ds)

    # Pre-filter to the rows we're going to consider in this run, then de-dup.
    filtered: list[dict[str, Any]] = []
    for obj in sigs:
        if not isinstance(obj, dict):
            continue
        market = str(obj.get("market") or "").strip().lower()
        if market not in {"total", "half_total", "quarter_total", "ats", "player_prop"}:
            continue

        # Player props: avoid evaluating model fallback lines as "bets".
        # These are useful for UI diagnostics but are not market-settled bet ideas.
        if market == "player_prop" and not include_model_lines:
            try:
                ls = str(obj.get("line_source") or "").strip().lower()
            except Exception:
                ls = ""
            if ls == "model":
                continue

        klass = str(obj.get("klass") or "").strip().upper() or None
        if not include_watch and klass != "BET":
            continue
        obj["_gid"] = _resolve_gid(obj, gid_map)
        filtered.append(obj)

    sigs = _dedup_signals(filtered, policy=str(dedup_policy))

    rg = _prep_recon_games(_load_csv(PROCESSED / f"recon_games_{ds}.csv"))
    rq = _prep_recon_quarters(_load_csv(PROCESSED / f"recon_quarters_{ds}.csv"))
    rp = _prep_recon_props(_load_csv(PROCESSED / f"recon_props_{ds}.csv"))

    out: list[Scored] = []

    for obj in sigs:
        market = str(obj.get("market") or "").strip().lower()
        klass = str(obj.get("klass") or "").strip().upper() or None

        gid = obj.get("_gid") or (_canon_nba_game_id(obj.get("game_id")) or None)
        home = str(obj.get("home") or "").strip().upper() or None
        away = str(obj.get("away") or "").strip().upper() or None
        horizon = str(obj.get("horizon") or "").strip().lower() or None
        side = str(obj.get("side") or "").strip().upper() or None
        remaining = _n(obj.get("remaining"))
        rem_bucket = _rem_bucket(market, horizon, remaining)
        strength = _n(obj.get("strength"))

        received_at = None
        for k in ("received_at", "ts", "created_at"):
            v = obj.get(k)
            if v:
                received_at = str(v)
                break

        line = None
        if market == "player_prop":
            line = _n(obj.get("line"))
        else:
            line = _n(obj.get("live_line"))

        actual = None
        outcome = None
        win = None
        price = None
        profit_u = None

        if market in {"total", "half_total", "quarter_total"}:
            if line is not None and side is not None:
                actual = _actual_total(market, horizon, gid, home, away, rg, rq)
                if actual is not None:
                    outcome, win = _settle_over_under(float(actual), float(line), str(side))

            if outcome == "PUSH":
                profit_u = 0.0
            elif win is not None:
                price = -float(abs(assumed_juice))
                profit_u = _american_profit(price, bool(win))

        elif market == "ats":
            margin_home = _actual_margin_home(gid, home, away, rg)
            if margin_home is not None and side is not None:
                _, _, spread, team = _settle_ats(float(margin_home), str(side))
                # Determine which team was picked relative to home/away
                team_margin = None
                if spread is not None and team is not None:
                    if home and team == home:
                        team_margin = float(margin_home)
                    elif away and team == away:
                        team_margin = -float(margin_home)
                    # else: cannot determine
                if team_margin is not None and spread is not None:
                    v = float(team_margin) + float(spread)
                    if v == 0:
                        outcome, win = "PUSH", None
                    elif v > 0:
                        outcome, win = "WIN", True
                    else:
                        outcome, win = "LOSS", False

            if outcome == "PUSH":
                profit_u = 0.0
            elif win is not None:
                price = -float(abs(assumed_juice))
                profit_u = _american_profit(price, bool(win))

        elif market == "player_prop":
            player = str(obj.get("player") or "").strip() or None
            stat = str(obj.get("stat") or "").strip() or None
            stat_key = _live_stat_key(stat)
            name_key = str(obj.get("name_key") or "").strip().upper() or _norm_player_name(player or "")

            if line is not None and side is not None:
                actual = _actual_prop(name_key, stat_key, rp)
                if actual is not None:
                    outcome, win = _settle_over_under(float(actual), float(line), str(side))

            # Price from context (if present)
            ctx = obj.get("context")
            ctx_price = None
            if isinstance(ctx, dict):
                if side == "OVER":
                    ctx_price = _n(ctx.get("price_over"))
                elif side == "UNDER":
                    ctx_price = _n(ctx.get("price_under"))
                if ctx_price is None:
                    ctx_price = _n(ctx.get("price"))

            ctx_price = _clean_american_price(ctx_price)

            if ctx_price is not None:
                price = float(ctx_price)
            else:
                price = -float(abs(assumed_juice))

            if outcome == "PUSH":
                profit_u = 0.0
            elif win is not None and price is not None:
                profit_u = _american_profit(float(price), bool(win))

            out.append(
                Scored(
                    date=str(obj.get("date") or ds),
                    market=market,
                    horizon=horizon,
                    klass=klass,
                    game_id=gid,
                    home=home,
                    away=away,
                    player=player,
                    stat=stat_key,
                    side=side,
                    line=line,
                    actual=actual,
                    price=price,
                    outcome=outcome,
                    profit_u=profit_u,
                    remaining=remaining,
                    rem_bucket=rem_bucket,
                    received_at=received_at,
                    strength=strength,
                )
            )
            continue

        out.append(
            Scored(
                date=str(obj.get("date") or ds),
                market=market,
                horizon=horizon,
                klass=klass,
                game_id=gid,
                home=home,
                away=away,
                player=None,
                stat=None,
                side=side,
                line=line,
                actual=actual,
                price=price,
                outcome=outcome,
                profit_u=profit_u,
                remaining=remaining,
                rem_bucket=rem_bucket,
                received_at=received_at,
                strength=strength,
            )
        )

    meta = {
        "dedup_policy": str(dedup_policy),
        "signals_raw": int(len(_load_jsonl(sig_path))),
        "signals_filtered": int(len(filtered)),
        "signals_dedup": int(len(sigs)),
    }
    return out, meta


def _summary_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["market", "horizon", "klass", "n", "settled", "wins", "losses", "pushes", "profit_u", "roi_u_per_bet", "win_rate"])

    def _agg(g: pd.DataFrame) -> pd.Series:
        n = int(len(g))
        settled = int(g["outcome"].isin(["WIN", "LOSS", "PUSH"]).sum())
        wins = int((g["outcome"] == "WIN").sum())
        losses = int((g["outcome"] == "LOSS").sum())
        pushes = int((g["outcome"] == "PUSH").sum())
        profit = float(pd.to_numeric(g["profit_u"], errors="coerce").fillna(0.0).sum())
        denom = max(1, (wins + losses + pushes))
        roi = profit / float(denom)
        wr_denom = max(1, (wins + losses))
        wr = wins / float(wr_denom)
        return pd.Series(
            {
                "n": n,
                "settled": settled,
                "wins": wins,
                "losses": losses,
                "pushes": pushes,
                "profit_u": round(profit, 4),
                "roi_u_per_bet": round(roi, 4),
                "win_rate": round(wr, 4),
            }
        )

    d = df.copy()
    d["profit_u"] = pd.to_numeric(d["profit_u"], errors="coerce")
    d["_is_settled"] = d["outcome"].isin(["WIN", "LOSS", "PUSH"]).astype(int)
    d["_is_win"] = (d["outcome"] == "WIN").astype(int)
    d["_is_loss"] = (d["outcome"] == "LOSS").astype(int)
    d["_is_push"] = (d["outcome"] == "PUSH").astype(int)

    group_cols = ["market", "horizon", "klass"]
    out = (
        d.groupby(group_cols, dropna=False, as_index=False)
        .agg(
            n=("outcome", "size"),
            settled=("_is_settled", "sum"),
            wins=("_is_win", "sum"),
            losses=("_is_loss", "sum"),
            pushes=("_is_push", "sum"),
            profit_u=("profit_u", "sum"),
        )
        .sort_values(["market", "horizon", "klass"], ascending=[True, True, True])
    )

    out["profit_u"] = out["profit_u"].fillna(0.0).astype(float).round(4)

    denom = (out["wins"] + out["losses"] + out["pushes"]).clip(lower=1).astype(float)
    out["roi_u_per_bet"] = (out["profit_u"] / denom).round(4)

    wr_denom = (out["wins"] + out["losses"]).clip(lower=1).astype(float)
    out["win_rate"] = (out["wins"].astype(float) / wr_denom).round(4)
    return out


def _df_to_md_table(df: pd.DataFrame, max_rows: int = 60) -> str:
    if df is None or df.empty:
        return "(no rows)"
    d = df.copy().head(max_rows)

    def _is_nan(x: Any) -> bool:
        try:
            return x is None or (isinstance(x, float) and math.isnan(x))
        except Exception:
            return x is None

    def _fmt(x: Any) -> str:
        if _is_nan(x):
            return ""
        if isinstance(x, float):
            # Keep reports compact and stable
            return str(round(x, 6))
        return str(x)

    def _esc(x: Any) -> str:
        return _fmt(x).replace("|", "\\|")

    cols = [str(c) for c in d.columns]
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = ["| " + " | ".join(_esc(v) for v in row) + " |" for row in d.itertuples(index=False, name=None)]
    return "\n".join([header, sep] + body)


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily Live Lens ROI report")
    ap.add_argument("--date", default=None, help="Target date YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--assumed-juice", type=float, default=110.0, help="Assume -juice when a price is not logged")
    ap.add_argument(
        "--include-watch",
        action="store_true",
        help="Include WATCH rows (default: BET only)",
    )
    ap.add_argument(
        "--dedup-policy",
        default="first_bet",
        choices=["first_bet", "first", "latest", "max_strength", "none"],
        help="De-duplicate signals per bet-idea before settling (default: first_bet)",
    )
    ap.add_argument(
        "--include-model-lines",
        action="store_true",
        help="Include player_prop signals with line_source=model (default: excluded)",
    )
    args = ap.parse_args()

    if args.date:
        ds = _parse_date(args.date).isoformat()
    else:
        ds = (datetime.now().date() - timedelta(days=1)).isoformat()

    REPORTS.mkdir(parents=True, exist_ok=True)

    scored, meta = _score_rows(
        ds,
        assumed_juice=float(args.assumed_juice),
        include_watch=bool(args.include_watch),
        dedup_policy=str(args.dedup_policy),
        include_model_lines=bool(args.include_model_lines),
    )
    if not scored:
        print(f"No scored rows for {ds} (missing logs or no settled markets)")
        return 2

    df = pd.DataFrame([r.__dict__ for r in scored])

    # Keep only settled rows for summary
    settled = df[df["outcome"].isin(["WIN", "LOSS", "PUSH"])].copy()

    out_csv = REPORTS / f"live_lens_roi_scored_{ds}.csv"
    try:
        df.to_csv(out_csv, index=False)
    except Exception:
        pass

    sum_all = _summary_table(settled)
    sum_bucket = _summary_table(settled[settled["rem_bucket"].notna()].rename(columns={"rem_bucket": "horizon"})) if False else None

    # Bucket summary: group by market + rem_bucket + klass
    bucket_df = pd.DataFrame()
    if not settled.empty:
        tmp = settled.copy()
        tmp["rem_bucket"] = tmp["rem_bucket"].fillna("(missing)")

        def _agg_b(g: pd.DataFrame) -> pd.Series:
            wins = int((g["outcome"] == "WIN").sum())
            losses = int((g["outcome"] == "LOSS").sum())
            pushes = int((g["outcome"] == "PUSH").sum())
            profit = float(pd.to_numeric(g["profit_u"], errors="coerce").fillna(0.0).sum())
            denom = max(1, (wins + losses + pushes))
            roi = profit / float(denom)
            wr_denom = max(1, (wins + losses))
            wr = wins / float(wr_denom)
            return pd.Series(
                {
                    "n": int(len(g)),
                    "wins": wins,
                    "losses": losses,
                    "pushes": pushes,
                    "profit_u": round(profit, 4),
                    "roi_u_per_bet": round(roi, 4),
                    "win_rate": round(wr, 4),
                }
            )

        tmp["profit_u"] = pd.to_numeric(tmp["profit_u"], errors="coerce")
        tmp["_is_win"] = (tmp["outcome"] == "WIN").astype(int)
        tmp["_is_loss"] = (tmp["outcome"] == "LOSS").astype(int)
        tmp["_is_push"] = (tmp["outcome"] == "PUSH").astype(int)

        bucket_df = (
            tmp.groupby(["market", "rem_bucket", "klass"], dropna=False, as_index=False)
            .agg(
                n=("outcome", "size"),
                wins=("_is_win", "sum"),
                losses=("_is_loss", "sum"),
                pushes=("_is_push", "sum"),
                profit_u=("profit_u", "sum"),
            )
            .sort_values(["market", "klass", "rem_bucket"], ascending=[True, True, False])
        )

        bucket_df["profit_u"] = bucket_df["profit_u"].fillna(0.0).astype(float).round(4)
        denom = (bucket_df["wins"] + bucket_df["losses"] + bucket_df["pushes"]).clip(lower=1).astype(float)
        bucket_df["roi_u_per_bet"] = (bucket_df["profit_u"] / denom).round(4)
        wr_denom = (bucket_df["wins"] + bucket_df["losses"]).clip(lower=1).astype(float)
        bucket_df["win_rate"] = (bucket_df["wins"].astype(float) / wr_denom).round(4)

    out_md = REPORTS / f"live_lens_roi_{ds}.md"

    md = []
    md.append(f"# Live Lens ROI report ({ds})")
    md.append("")
    md.append(f"- include_watch: {bool(args.include_watch)}")
    md.append(f"- include_model_lines: {bool(args.include_model_lines)}")
    md.append(f"- assumed_juice: -{abs(float(args.assumed_juice))}")
    md.append(f"- dedup_policy: {str(args.dedup_policy)}")
    try:
        md.append(f"- signals: {int(meta.get('signals_filtered') or 0)} -> {int(meta.get('signals_dedup') or 0)} (deduped)")
    except Exception:
        pass
    md.append("")
    md.append("## Summary (by market / horizon / klass)")
    md.append("")
    md.append(_df_to_md_table(sum_all))
    md.append("")
    md.append("## Buckets (by market / minutes remaining / klass)")
    md.append("")
    md.append(_df_to_md_table(bucket_df, max_rows=120))
    md.append("")
    md.append("## Notes")
    md.append("")
    md.append("- Totals/period totals and ATS assume -110 unless a price is logged.")
    md.append("- Player props use logged prices when available (context.price_over/price_under), else assume -110.")

    try:
        out_md.write_text("\n".join(md) + "\n", encoding="utf-8")
    except Exception:
        pass

    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
