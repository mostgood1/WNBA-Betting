#!/usr/bin/env python3
"""Daily Live Lens audit report: JSONL signals -> scored rows + markdown summary.

Reads (for a given date):
- data/processed/live_lens_signals_<date>.jsonl
- data/processed/recon_games_<date>.csv (game totals)
- data/processed/recon_quarters_<date>.csv (half/quarter totals)
- data/processed/recon_props_<date>.csv (player props)

Writes:
- data/processed/reports/live_lens_audit_<date>.md
- data/processed/reports/live_lens_scored_<date>.csv

This is an *audit* loop (did our live signals line up with outcomes?).
It does not claim EV; it reports predictive errors and directional hits.
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
from typing import Any, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
LIVE_LENS_DIR = Path((os.getenv("NBA_LIVE_LENS_DIR") or os.getenv("LIVE_LENS_DIR") or "").strip() or str(PROCESSED))


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


def _load_gid_map(ds: str) -> dict[tuple[str, str], str]:
    """Build (home_tri, away_tri) -> canonical gid map.

    Prefer game_cards_<date>.csv because recon_games_<date>.csv typically does not
    contain NBA game_id.
    """
    path = PROCESSED / f"game_cards_{ds}.csv"
    df = _load_csv(path)
    if df is None or df.empty:
        return {}
    if "game_id" not in df.columns:
        return {}
    if "home_tri" not in df.columns or "away_tri" not in df.columns:
        return {}

    out: dict[tuple[str, str], str] = {}
    for _, r in df.iterrows():
        home = _safe_upper(r.get("home_tri"))
        away = _safe_upper(r.get("away_tri"))
        gid = _canon_nba_game_id(r.get("game_id"))
        if home and away and _is_canon_gid(gid):
            out[(home, away)] = gid
    return out


def _resolve_gid_for_props(obj: dict[str, Any], gid_map: dict[tuple[str, str], str]) -> str | None:
    # Prefer numeric NBA game id if present.
    gid0 = _canon_nba_game_id(obj.get("game_id"))
    if _is_canon_gid(gid0):
        return gid0

    home = _safe_upper(obj.get("home"))
    away = _safe_upper(obj.get("away"))
    if home and away:
        gid = gid_map.get((home, away))
        if gid:
            return gid

    # Support simple team-based ids like "BKN@ATL".
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
    }
    return m.get(s, s)


def _safe_upper(x: Any) -> str | None:
    try:
        s = str(x or "").strip().upper()
        return s or None
    except Exception:
        return None


@dataclass(frozen=True)
class ScoredRow:
    date: str
    market: str
    horizon: str | None
    signal_key: str | None
    game_id: str | None
    home: str | None
    away: str | None
    team_tri: str | None
    player: str | None
    name_key: str | None
    stat: str | None
    stat_key: str | None
    side: str | None
    klass: str | None
    elapsed: float | None
    live_line: float | None
    edge: float | None
    pred: float | None
    act: float | None
    result: str | None
    err: float | None
    interval_drift_on: int | None
    recent_window_on: int | None
    endgame_foul_on: int | None


def _load_signals(ds: str) -> list[dict[str, Any]]:
    fp = LIVE_LENS_DIR / f"live_lens_signals_{ds}.jsonl"
    if not fp.exists():
        return []
    out: list[dict[str, Any]] = []
    with fp.open("r", encoding="utf-8") as f:
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


def _prep_recon_games(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "game_id" in out.columns:
        out["_gid"] = out["game_id"].map(_canon_nba_game_id)
    else:
        out["_gid"] = ""
    if "home_tri" in out.columns:
        out["home_tri"] = out["home_tri"].astype(str).str.strip().str.upper()
    if "away_tri" in out.columns:
        out["away_tri"] = out["away_tri"].astype(str).str.strip().str.upper()
    return out


def _prep_recon_quarters(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "game_id" in out.columns:
        out["_gid"] = out["game_id"].map(_canon_nba_game_id)
    else:
        out["_gid"] = ""
    if "home_tri" in out.columns:
        out["home_tri"] = out["home_tri"].astype(str).str.strip().str.upper()
    if "away_tri" in out.columns:
        out["away_tri"] = out["away_tri"].astype(str).str.strip().str.upper()
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


def _result_for_side(side: str | None, act: float | None, line: float | None) -> str | None:
    if act is None or line is None:
        return None
    s = (side or "").strip().lower()
    if act == line:
        return "push"
    if s == "over":
        return "win" if act > line else "loss"
    if s == "under":
        return "win" if act < line else "loss"
    return None


def _metrics(df: pd.DataFrame) -> dict[str, float]:
    if df is None or df.empty:
        return {"n": 0}
    err = df["pred"].astype(float) - df["act"].astype(float)
    mae = float(err.abs().mean())
    rmse = float(math.sqrt(float((err**2).mean())))
    bias = float(err.mean())
    return {"n": int(len(df)), "mae": mae, "rmse": rmse, "bias": bias}


def _hit_rate(df: pd.DataFrame) -> dict[str, float]:
    if df is None or df.empty:
        return {"n": 0}
    d = df[df["result"].notna()].copy()
    if d.empty:
        return {"n": int(len(df))}
    wins = int((d["result"].astype(str) == "win").sum())
    losses = int((d["result"].astype(str) == "loss").sum())
    pushes = int((d["result"].astype(str) == "push").sum())
    denom = wins + losses
    hr = float(wins) / float(denom) if denom > 0 else float("nan")
    return {"n": int(len(df)), "wins": wins, "losses": losses, "pushes": pushes, "hit_rate": hr}


def _flag_adjustments(ctx: Any) -> tuple[int | None, int | None, int | None]:
    if not isinstance(ctx, dict):
        return (None, None, None)
    # Some markets nest these under context.scope_adjustments; others place them directly in context.
    sa = ctx.get("scope_adjustments")
    src = sa if isinstance(sa, dict) else ctx

    interval_on = 1 if abs(float(_n(src.get("interval_drift_adj")) or 0.0)) > 1e-9 else 0

    recent_on = 0
    if abs(float(_n(src.get("recent_window_pace_adj")) or 0.0)) > 1e-9:
        recent_on = 1
    if abs(float(_n(src.get("recent_window_eff_adj")) or 0.0)) > 1e-9:
        recent_on = 1
    if float(_n(src.get("recent_window_w")) or 0.0) > 1e-9:
        recent_on = 1

    foul_on = 1 if abs(float(_n(src.get("endgame_foul_adj")) or 0.0)) > 1e-9 else 0
    if float(_n(src.get("endgame_foul_w")) or 0.0) > 1e-9:
        foul_on = 1
    return (interval_on, recent_on, foul_on)


def _score_day(ds: str) -> pd.DataFrame:
    sigs = _load_signals(ds)
    if not sigs:
        return pd.DataFrame()

    rg_path = PROCESSED / f"recon_games_{ds}.csv"
    rq_path = PROCESSED / f"recon_quarters_{ds}.csv"
    rp_path = PROCESSED / f"recon_props_{ds}.csv"

    rg = _prep_recon_games(_load_csv(rg_path))
    rq = _prep_recon_quarters(_load_csv(rq_path))
    rp = _prep_recon_props(_load_csv(rp_path))

    rg_ok = int(not rg.empty)
    rq_ok = int(not rq.empty)
    rp_ok = int(not rp.empty)

    gid_map = _load_gid_map(ds)

    prop_index: dict[tuple[str, str], dict[str, Any]] = {}
    if not rp.empty:
        for _, r in rp.iterrows():
            gid = str(r.get("_gid") or "")
            nk = str(r.get("_name_key") or "")
            if gid and nk and (gid, nk) not in prop_index:
                prop_index[(gid, nk)] = dict(r)

    scored: list[dict[str, Any]] = []

    for obj in sigs:
        market = str(obj.get("market") or "").strip()

        if market in {"total", "half_total", "quarter_total"}:
            horizon = str(obj.get("horizon") or "").strip().lower() or None
            gid = _canon_nba_game_id(obj.get("game_id")) or None
            home = _safe_upper(obj.get("home"))
            away = _safe_upper(obj.get("away"))
            live_line = _n(obj.get("live_line"))
            edge = _n(obj.get("edge_adj")) if market == "total" else _n(obj.get("edge"))
            pred = _n(obj.get("pred"))
            if pred is None:
                pred = (live_line + edge) if (live_line is not None and edge is not None) else None
            act = _actual_total(market, horizon, gid, home, away, rg, rq)
            if act is None:
                if market == "half_total" and horizon not in {"h1", "h2"}:
                    missing_reason = "unsupported_horizon"
                elif market == "quarter_total" and horizon not in {"q1", "q2", "q3", "q4"}:
                    missing_reason = "unsupported_horizon"
                if market == "total" and rg_ok == 0:
                    missing_reason = "missing_recon_games"
                elif market in {"half_total", "quarter_total"} and rq_ok == 0:
                    missing_reason = "missing_recon_quarters"
                else:
                    missing_reason = missing_reason or "join_failed"
            else:
                missing_reason = ""
            side = str(obj.get("side") or "").strip().lower() or None
            result = _result_for_side(side, act, live_line)
            err = (pred - act) if (pred is not None and act is not None) else None
            ctx = obj.get("context")
            interval_on, recent_on, foul_on = _flag_adjustments(ctx)
            w_pace = None
            edge_shrink_lambda = None
            edge_shrink_lambda_poss = None
            edge_shrink_lambda_time = None
            if isinstance(ctx, dict):
                w_pace = _n(ctx.get("w_pace"))
                edge_shrink_lambda = _n(ctx.get("edge_shrink_lambda"))
                edge_shrink_lambda_poss = _n(ctx.get("edge_shrink_lambda_poss"))
                edge_shrink_lambda_time = _n(ctx.get("edge_shrink_lambda_time"))

            scored.append(
                {
                    "date": str(obj.get("date") or ds),
                    "market": market,
                    "horizon": horizon,
                    "signal_key": str(obj.get("signal_key") or "") or None,
                    "game_id": gid,
                    "home": home,
                    "away": away,
                    "team_tri": None,
                    "player": None,
                    "name_key": None,
                    "stat": None,
                    "stat_key": None,
                    "side": side,
                    "klass": str(obj.get("klass") or "") or None,
                    "elapsed": _n(obj.get("elapsed")),
                    "live_line": live_line,
                    "edge": edge,
                    "strength": (abs(float(edge)) if edge is not None else None),
                    "pred": pred,
                    "act": act,
                    "result": result,
                    "err": err,
                    "w_pace": w_pace,
                    "edge_shrink_lambda": edge_shrink_lambda,
                    "edge_shrink_lambda_poss": edge_shrink_lambda_poss,
                    "edge_shrink_lambda_time": edge_shrink_lambda_time,
                    "missing_reason": missing_reason,
                    "has_recon_games": rg_ok,
                    "has_recon_quarters": rq_ok,
                    "has_recon_props": rp_ok,
                    "interval_drift_on": interval_on,
                    "recent_window_on": recent_on,
                    "endgame_foul_on": foul_on,
                }
            )
            continue

        if market == "player_prop":
            gid = _resolve_gid_for_props(obj, gid_map)
            stat = str(obj.get("stat") or "").strip()
            stat_key = _live_stat_key(stat)
            player = str(obj.get("player") or "").strip() or None
            name_key_raw = str(obj.get("name_key") or "").strip() or None
            name_key = _norm_player_name(name_key_raw or player or "") or None
            side = str(obj.get("side") or "").strip().lower() or None

            line = _n(obj.get("line"))
            if line is None:
                line = _n(obj.get("live_line"))

            edge = _n(obj.get("edge"))
            if edge is None:
                edge = _n(obj.get("pace_vs_line"))

            pred = (line + edge) if (line is not None and edge is not None) else None

            act = None
            r = None
            if gid and name_key:
                r = prop_index.get((gid, name_key))
                if r is not None:
                    act = _n(r.get(stat_key))
            if act is None:
                if rp_ok == 0:
                    missing_reason = "missing_recon_props"
                elif not gid:
                    missing_reason = "missing_gid"
                elif not name_key:
                    missing_reason = "missing_name_key"
                elif r is None:
                    missing_reason = "player_join_failed"
                else:
                    missing_reason = "stat_missing"
            else:
                missing_reason = ""
            result = _result_for_side(side, act, line)
            err = (pred - act) if (pred is not None and act is not None) else None

            # Carry forward useful diagnostics for clustering.
            ctx = obj.get("context")
            exp_min = exp_min_eff = proj_min_final = None
            usage_window_sec = None
            pace_mult = role_mult = foul_mult = None
            usg_recent = usg_game = team_usg_recent = team_usg_game = None
            fg3a_recent = fg3a_game = team_3a_recent = team_3a_game = None
            if isinstance(ctx, dict):
                exp_min = _n(ctx.get("exp_min"))
                exp_min_eff = _n(ctx.get("exp_min_eff"))
                proj_min_final = _n(ctx.get("proj_min_final"))
                usage_window_sec = _n(ctx.get("usage_window_sec"))
                pace_mult = _n(ctx.get("pace_mult"))
                role_mult = _n(ctx.get("role_mult"))
                foul_mult = _n(ctx.get("foul_mult"))
                usg_recent = _n(ctx.get("usg_recent"))
                usg_game = _n(ctx.get("usg_game"))
                team_usg_recent = _n(ctx.get("team_usg_recent"))
                team_usg_game = _n(ctx.get("team_usg_game"))
                fg3a_recent = _n(ctx.get("fg3a_recent"))
                fg3a_game = _n(ctx.get("fg3a_game"))
                team_3a_recent = _n(ctx.get("team_3a_recent"))
                team_3a_game = _n(ctx.get("team_3a_game"))

            scored.append(
                {
                    "date": str(obj.get("date") or ds),
                    "market": market,
                    "horizon": None,
                    "signal_key": str(obj.get("signal_key") or "") or None,
                    "game_id": gid,
                    "home": _safe_upper(obj.get("home")),
                    "away": _safe_upper(obj.get("away")),
                    "team_tri": _safe_upper(obj.get("team_tri")),
                    "player": player,
                    "name_key": name_key,
                    "stat": stat,
                    "stat_key": stat_key,
                    "side": side,
                    "klass": str(obj.get("klass") or "") or None,
                    "elapsed": _n(obj.get("elapsed")),
                    "live_line": line,
                    "edge": edge,
                    "strength": (abs(float(edge)) if edge is not None else None),
                    "pred": pred,
                    "act": act,
                    "result": result,
                    "err": err,
                    "exp_min": exp_min,
                    "exp_min_eff": exp_min_eff,
                    "proj_min_final": proj_min_final,
                    "usage_window_sec": usage_window_sec,
                    "pace_mult": pace_mult,
                    "role_mult": role_mult,
                    "foul_mult": foul_mult,
                    "usg_recent": usg_recent,
                    "usg_game": usg_game,
                    "team_usg_recent": team_usg_recent,
                    "team_usg_game": team_usg_game,
                    "fg3a_recent": fg3a_recent,
                    "fg3a_game": fg3a_game,
                    "team_3a_recent": team_3a_recent,
                    "team_3a_game": team_3a_game,
                    "missing_reason": missing_reason,
                    "has_recon_games": rg_ok,
                    "has_recon_quarters": rq_ok,
                    "has_recon_props": rp_ok,
                    "interval_drift_on": None,
                    "recent_window_on": None,
                    "endgame_foul_on": None,
                }
            )
            continue

    if not scored:
        return pd.DataFrame()
    return pd.DataFrame(scored)


def _write_markdown(ds: str, df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    def _fmt_metrics(m: dict[str, float]) -> str:
        if not m or int(m.get("n", 0)) <= 0:
            return "n=0"
        return f"n={int(m['n'])}  mae={m.get('mae', float('nan')):.3f}  rmse={m.get('rmse', float('nan')):.3f}  bias={m.get('bias', float('nan')):.3f}"

    def _fmt_hits(h: dict[str, float]) -> str:
        if not h or int(h.get("n", 0)) <= 0:
            return "n=0"
        hr = h.get("hit_rate")
        hr_s = f"{float(hr):.3f}" if (hr is not None and not math.isnan(float(hr))) else "nan"
        return f"n={int(h['n'])}  W={int(h.get('wins', 0))}  L={int(h.get('losses', 0))}  P={int(h.get('pushes', 0))}  hit={hr_s}"

    lines: list[str] = []
    lines.append(f"# Live Lens Audit — {ds}")
    lines.append("")

    if df is None or df.empty:
        lines.append("No scored rows (missing logs or missing recon outputs).")
        out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    lines.append("## Coverage")
    counts = df.groupby("market").size().to_dict()
    lines.append(
        f"- recon files loaded: games={int(df.get('has_recon_games', pd.Series([0])).max() if not df.empty else 0)} quarters={int(df.get('has_recon_quarters', pd.Series([0])).max() if not df.empty else 0)} props={int(df.get('has_recon_props', pd.Series([0])).max() if not df.empty else 0)}"
    )
    for k in sorted(counts.keys()):
        lines.append(f"- {k}: {int(counts[k])}")
    if "missing_reason" in df.columns:
        miss = df[df["missing_reason"].astype(str).str.len() > 0]
        if not miss.empty:
            mr = miss.groupby("missing_reason").size().sort_values(ascending=False).to_dict()
            parts = [f"{k}={int(v)}" for k, v in mr.items()]
            lines.append(f"- unscored (no actual): {', '.join(parts)}")
    lines.append("")

    totals = df[df["market"].isin(["total", "half_total", "quarter_total"])].copy()
    totals = totals.dropna(subset=["pred", "act"]).copy()

    lines.append("## Totals/halves/quarters")
    totals_all = df[df["market"].isin(["total", "half_total", "quarter_total"])].copy()
    for mkt in ["total", "half_total", "quarter_total"]:
        d_all = totals_all[totals_all["market"] == mkt].copy()
        n_all = int(len(d_all))
        n_act = int(d_all["act"].notna().sum()) if "act" in d_all.columns else 0
        n_pred = int(d_all["pred"].notna().sum()) if "pred" in d_all.columns else 0
        d_scored = d_all.dropna(subset=["pred", "act"]).copy()
        m = _metrics(d_scored)
        lines.append(f"- {mkt}: scored={_fmt_metrics(m)}  have_pred={n_pred}/{n_all}  have_act={n_act}/{n_all}")

    # Quick visibility into whether the new shrink diagnostics are making it into logs.
    try:
        if "edge_shrink_lambda" in totals_all.columns:
            n_lam = int(pd.to_numeric(totals_all.get("edge_shrink_lambda"), errors="coerce").notna().sum())
            lines.append(f"- edge_shrink_lambda present: {n_lam}/{int(len(totals_all))}")
    except Exception:
        pass
    lines.append("")

    if not totals.empty and "interval_drift_on" in totals.columns:
        lines.append("### Totals breakdown (adjustments on/off)")
        for flag, title in [
            ("interval_drift_on", "interval_drift"),
            ("recent_window_on", "recent_window"),
            ("endgame_foul_on", "endgame_foul"),
        ]:
            d_on = totals[totals[flag] == 1]
            d_off = totals[totals[flag] == 0]
            if len(d_on) + len(d_off) == 0:
                continue
            lines.append(f"- {title}: on({_fmt_metrics(_metrics(d_on))})  off({_fmt_metrics(_metrics(d_off))})")
        lines.append("")

    # New: breakdown by shrink confidence lambda.
    try:
        if not totals.empty and "edge_shrink_lambda" in totals.columns and totals["edge_shrink_lambda"].notna().any():
            t0 = totals.copy()
            lam = pd.to_numeric(t0.get("edge_shrink_lambda"), errors="coerce")
            # Bins in [0,1], plus a catchall for out-of-range.
            t0["shrink_bin"] = pd.cut(
                lam,
                bins=[-1e9, 0.25, 0.5, 0.75, 1.0, 1e9],
                labels=["0-0.25", "0.25-0.5", "0.5-0.75", "0.75-1.0", ">1"],
                include_lowest=True,
            )
            t0["shrink_bin"] = t0["shrink_bin"].astype(str).where(lam.notna(), other="missing")

            lines.append("### Totals breakdown (edge shrink lambda bins)")

            # Overall
            lines.append("- overall:")
            for k, g in t0.groupby("shrink_bin"):
                gg = g.dropna(subset=["pred", "act"]).copy()
                lines.append(f"  - lambda={k}: {_fmt_metrics(_metrics(gg))}")

            # Per market
            for mkt in ["total", "half_total", "quarter_total"]:
                mm = t0[t0["market"] == mkt]
                if mm.empty:
                    continue
                lines.append(f"- {mkt}:")
                for k, g in mm.groupby("shrink_bin"):
                    gg = g.dropna(subset=["pred", "act"]).copy()
                    lines.append(f"  - lambda={k}: {_fmt_metrics(_metrics(gg))}")

            lines.append("")
    except Exception:
        pass

    props = df[df["market"] == "player_prop"].copy()
    props_scored = props.dropna(subset=["act", "live_line"]).copy()
    lines.append("## Player props")
    n_props = int(len(props))
    n_props_act = int(props["act"].notna().sum()) if "act" in props.columns else 0
    n_props_pred = int(props["pred"].notna().sum()) if "pred" in props.columns else 0
    lines.append(f"- scored: {_fmt_hits(_hit_rate(props_scored))}  have_pred={n_props_pred}/{n_props}  have_act={n_props_act}/{n_props}")

    if not props_scored.empty:
        by_klass = props_scored.groupby(props_scored["klass"].fillna(""))
        lines.append("### Hit rate by klass")
        for k, g in by_klass:
            kk = k if k else "(blank)"
            lines.append(f"- {kk}: {_fmt_hits(_hit_rate(g))}")
        lines.append("")

        by_stat = props_scored.groupby(props_scored["stat_key"].fillna(""))
        lines.append("### Hit rate by stat")
        for k, g in by_stat:
            kk = k if k else "(blank)"
            lines.append(f"- {kk}: {_fmt_hits(_hit_rate(g))}")
        lines.append("")

        # Cluster diagnostics
        try:
            props_scored["min_bin"] = pd.cut(
                pd.to_numeric(props_scored.get("proj_min_final"), errors="coerce"),
                bins=[-1e9, 16, 24, 32, 1e9],
                labels=["<16", "16-24", "24-32", "32+"],
            )
        except Exception:
            props_scored["min_bin"] = None
        try:
            props_scored["edge_bin"] = pd.cut(
                pd.to_numeric(props_scored.get("strength"), errors="coerce"),
                bins=[-1e9, 2, 4, 6, 8, 1e9],
                labels=["<2", "2-4", "4-6", "6-8", "8+"],
            )
        except Exception:
            props_scored["edge_bin"] = None

        if props_scored["min_bin"].notna().any():
            lines.append("### Hit rate by projected minutes")
            for k, g in props_scored.groupby(props_scored["min_bin"].astype(str)):
                lines.append(f"- {k}: {_fmt_hits(_hit_rate(g))}")
            lines.append("")

        if props_scored["edge_bin"].notna().any():
            lines.append("### Hit rate by |edge|")
            for k, g in props_scored.groupby(props_scored["edge_bin"].astype(str)):
                lines.append(f"- {k}: {_fmt_hits(_hit_rate(g))}")
            lines.append("")

        # Loss clusters (largest volume, low hit rate)
        try:
            grp_cols = ["stat_key", "klass", "min_bin", "edge_bin"]
            gdf = props_scored.copy()
            for c in grp_cols:
                if c not in gdf.columns:
                    gdf[c] = ""
            rows = []
            for keys, g in gdf.groupby(grp_cols, dropna=False):
                h = _hit_rate(g)
                n = int(h.get("wins", 0) + h.get("losses", 0) + h.get("pushes", 0))
                denom = int(h.get("wins", 0) + h.get("losses", 0))
                hr = float(h.get("hit_rate", float("nan")))
                if denom < 15:
                    continue
                rows.append({
                    "stat_key": keys[0] if keys[0] else "(blank)",
                    "klass": keys[1] if keys[1] else "(blank)",
                    "min_bin": str(keys[2]),
                    "edge_bin": str(keys[3]),
                    "wins": int(h.get("wins", 0)),
                    "losses": int(h.get("losses", 0)),
                    "pushes": int(h.get("pushes", 0)),
                    "hit": hr,
                    "denom": denom,
                })
            if rows:
                cdf = pd.DataFrame(rows)
                cdf = cdf.sort_values(["hit", "denom"], ascending=[True, False]).head(12)
                lines.append("### Loss clusters (low hit rate; denom>=15)")
                for _, r in cdf.iterrows():
                    lines.append(
                        f"- stat={r['stat_key']} klass={r['klass']} min={r['min_bin']} edge={r['edge_bin']}: W={int(r['wins'])} L={int(r['losses'])} P={int(r['pushes'])} hit={float(r['hit']):.3f}"
                    )
                lines.append("")
        except Exception:
            pass

        # Simple tuning suggestions (heuristics)
        suggestions: list[str] = []
        try:
            # If low-minute bin is clearly worse, suggest gating.
            if props_scored["min_bin"].notna().any():
                mins = []
                for k, g in props_scored.groupby(props_scored["min_bin"].astype(str)):
                    h = _hit_rate(g)
                    denom = int(h.get("wins", 0) + h.get("losses", 0))
                    if denom >= 30:
                        mins.append((k, float(h.get("hit_rate", float("nan"))), denom))
                if mins:
                    mins.sort(key=lambda x: x[1])
                    worst = mins[0]
                    if worst[0] in {"<16", "16-24"} and worst[1] < 0.48:
                        suggestions.append(f"Consider gating player-prop signals with proj_min_final {worst[0]} (hit={worst[1]:.3f} over {worst[2]} picks).")

            # If low-|edge| bin underperforms, suggest raising watch threshold.
            if props_scored["edge_bin"].notna().any():
                edges = []
                for k, g in props_scored.groupby(props_scored["edge_bin"].astype(str)):
                    h = _hit_rate(g)
                    denom = int(h.get("wins", 0) + h.get("losses", 0))
                    if denom >= 40:
                        edges.append((k, float(h.get("hit_rate", float("nan"))), denom))
                if edges:
                    edges.sort(key=lambda x: x[1])
                    worst = edges[0]
                    if worst[0] in {"<2", "2-4"} and worst[1] < 0.48:
                        suggestions.append(f"Consider raising player-prop WATCH threshold above {worst[0]} edges (hit={worst[1]:.3f} over {worst[2]} picks).")
        except Exception:
            suggestions = []

        if suggestions:
            lines.append("### Suggestions")
            for s in suggestions[:8]:
                lines.append(f"- {s}")
            lines.append("")

        # Biggest misses (high |edge| but wrong side)
        props_scored["abs_edge"] = props_scored["edge"].abs().fillna(0.0)
        wrong = props_scored[props_scored["result"] == "loss"].sort_values("abs_edge", ascending=False).head(15)
        if not wrong.empty:
            lines.append("### Biggest misses (by |edge|)")
            for _, r in wrong.iterrows():
                lines.append(
                    f"- {r.get('game_id','')} {r.get('player','') or r.get('name_key','')} {r.get('stat_key','')} {r.get('side','')}: line={r.get('live_line')} act={r.get('act')} edge={r.get('edge')} klass={r.get('klass')}"
                )
            lines.append("")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily Live Lens audit report")
    ap.add_argument(
        "--date",
        default=None,
        help="YYYY-MM-DD (default: yesterday, local time)",
    )
    ap.add_argument(
        "--out-dir",
        default=str(PROCESSED / "reports"),
        help="Output directory (default: data/processed/reports)",
    )
    args = ap.parse_args()

    if args.date:
        ds = _parse_date(args.date).isoformat()
    else:
        ds = ( _date.today() - timedelta(days=1) ).isoformat()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _score_day(ds)

    scored_csv = out_dir / f"live_lens_scored_{ds}.csv"
    audit_md = out_dir / f"live_lens_audit_{ds}.md"

    if df is None or df.empty:
        # Still write a markdown stub so the task has a tangible artifact.
        _write_markdown(ds, pd.DataFrame(), audit_md)
        return 0

    df.to_csv(scored_csv, index=False)
    _write_markdown(ds, df, audit_md)
    print(f"Wrote: {audit_md}")
    print(f"Wrote: {scored_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
