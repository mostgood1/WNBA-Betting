from __future__ import annotations

import argparse
import json
import unicodedata
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import numpy as np
import pandas as pd

from nba_betting.config import paths
from nba_betting.player_priors import PlayerPriorsConfig, compute_player_priors
from nba_betting.sim.connected_game import simulate_connected_game
from nba_betting.sim.quarters import GameInputs, TeamContext, simulate_quarters
from nba_betting.teams import normalize_team, to_tricode


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: date, end: date) -> list[date]:
    out: list[date] = []
    d = start
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


def _to_float(x: Any) -> float | None:
    try:
        v = float(x)
        return v if np.isfinite(v) else None
    except Exception:
        return None


def _norm_player_key(x: Any) -> str:
    try:
        t = str(x or "").strip()
        if not t:
            return ""
        if "(" in t:
            t = t.split("(", 1)[0]
        t = t.replace("-", " ")
        t = t.replace(".", "").replace("'", "").replace(",", " ")
        t = " ".join(t.split())
        u = t.upper()
        for suf in (" JR", " SR", " II", " III", " IV"):
            if u.endswith(suf):
                u = u[: -len(suf)].strip()
                break
        try:
            # Match simulator behavior: convert diacritics (e.g., Vučević -> Vucevic)
            # rather than dropping letters.
            u = unicodedata.normalize("NFKD", u)
            u = "".join(ch for ch in u if not unicodedata.combining(ch))
            u = u.encode("ascii", "ignore").decode("ascii")
        except Exception:
            pass
        return " ".join(u.split())
    except Exception:
        return ""


def _load_predictions(processed_dir: Path, d: date) -> pd.DataFrame | None:
    fp = processed_dir / f"predictions_{d.isoformat()}.csv"
    if not fp.exists():
        return None
    try:
        df = pd.read_csv(fp)
        return df if isinstance(df, pd.DataFrame) and not df.empty else None
    except Exception:
        return None


def _load_game_odds(processed_dir: Path, d: date) -> pd.DataFrame | None:
    fp = processed_dir / f"game_odds_{d.isoformat()}.csv"
    if not fp.exists():
        return None
    try:
        df = pd.read_csv(fp)
        return df if isinstance(df, pd.DataFrame) and not df.empty else None
    except Exception:
        return None


def _load_props_predictions(processed_dir: Path, d: date) -> pd.DataFrame | None:
    fp = processed_dir / f"props_predictions_{d.isoformat()}.csv"
    if not fp.exists():
        return None
    try:
        df = pd.read_csv(fp)
        return df if isinstance(df, pd.DataFrame) and not df.empty else None
    except Exception:
        return None


def _load_player_logs(processed_dir: Path) -> pd.DataFrame:
    fp = processed_dir / "player_logs.csv"
    if not fp.exists():
        raise FileNotFoundError("Missing data/processed/player_logs.csv")
    df = pd.read_csv(fp)
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise ValueError("player_logs.csv is empty")
    return df


def _fill_market_lines(preds: pd.DataFrame, odds: pd.DataFrame | None) -> pd.DataFrame:
    if odds is None or odds.empty:
        return preds
    try:
        odds = odds.copy()
        odds["home_team"] = odds.get("home_team").astype(str).str.strip()
        odds["visitor_team"] = odds.get("visitor_team").astype(str).str.strip()
        keep = [c for c in ["date", "home_team", "visitor_team", "home_spread", "total"] if c in odds.columns]
        odds = odds[keep].copy() if keep else odds.iloc[0:0]
        if odds.empty:
            return preds

        preds = preds.copy()
        preds["home_team"] = preds.get("home_team").astype(str).str.strip()
        preds["visitor_team"] = preds.get("visitor_team").astype(str).str.strip()
        m = preds.merge(odds, on=["date", "home_team", "visitor_team"], how="left", suffixes=("", "_odds"))
        for col in ("home_spread", "total"):
            if col in m.columns and f"{col}_odds" in m.columns:
                a = pd.to_numeric(m[col], errors="coerce")
                b = pd.to_numeric(m[f"{col}_odds"], errors="coerce")
                m[col] = a.where(a.notna(), b)
        m = m.drop(columns=[c for c in m.columns if c.endswith("_odds")], errors="ignore")
        return m
    except Exception:
        return preds


def _build_context_from_row(row: pd.Series) -> tuple[TeamContext, TeamContext, float | None, float | None]:
    home = str(row.get("home_team") or "").strip()
    away = str(row.get("visitor_team") or "").strip()

    pred_total = _to_float(row.get("pred_total")) or _to_float(row.get("totals")) or _to_float(row.get("total_pred"))
    pred_margin = _to_float(row.get("pred_margin")) or _to_float(row.get("spread_margin")) or _to_float(row.get("margin_pred"))

    home_pace = _to_float(row.get("home_pace")) or 98.0
    away_pace = _to_float(row.get("away_pace")) or 98.0

    home_mu_implied = None
    away_mu_implied = None
    if pred_total is not None and pred_margin is not None:
        home_mu_implied = 0.5 * (pred_total + pred_margin)
        away_mu_implied = 0.5 * (pred_total - pred_margin)

    def _rating_from_mu(mu: float | None, pace: float) -> float:
        if mu is None:
            return 112.0
        try:
            return float((mu / max(1e-6, pace)) * 100.0)
        except Exception:
            return 112.0

    home_off = _to_float(row.get("home_off_rating")) or _rating_from_mu(home_mu_implied, home_pace)
    away_off = _to_float(row.get("away_off_rating")) or _rating_from_mu(away_mu_implied, away_pace)
    home_def = _to_float(row.get("home_def_rating")) or 112.0
    away_def = _to_float(row.get("away_def_rating")) or 112.0

    home_ctx = TeamContext(team=home, pace=float(home_pace), off_rating=float(home_off), def_rating=float(home_def), injuries_out=0, back_to_back=False)
    away_ctx = TeamContext(team=away, pace=float(away_pace), off_rating=float(away_off), def_rating=float(away_def), injuries_out=0, back_to_back=False)

    market_total = _to_float(row.get("total"))
    market_home_spread = _to_float(row.get("home_spread"))

    return home_ctx, away_ctx, market_total, market_home_spread


def _games_from_player_logs_for_date(logs: pd.DataFrame, d: date) -> pd.DataFrame:
    x = logs.copy()
    x["GAME_DATE"] = pd.to_datetime(x["GAME_DATE"], errors="coerce")
    day = pd.Timestamp(d)
    x = x[x["GAME_DATE"] == day]
    if x.empty:
        return x.iloc[0:0]

    def _home_away_for_gid(g: pd.DataFrame) -> tuple[str | None, str | None]:
        try:
            # Home rows typically show "TEAM vs. OPP"; away rows show "TEAM @ OPP".
            mu = g.get("MATCHUP").astype(str)
            home_rows = g[mu.str.contains(r"\\bvs\\.", regex=True, na=False)]
            away_rows = g[mu.str.contains(r"\\b@\\b", regex=True, na=False)]
            home_tri = None
            away_tri = None
            if not home_rows.empty:
                home_tri = str(home_rows.iloc[0].get("TEAM_ABBREVIATION") or "").strip().upper() or None
            if not away_rows.empty:
                away_tri = str(away_rows.iloc[0].get("TEAM_ABBREVIATION") or "").strip().upper() or None
            # Fallback: parse matchup string
            if home_tri is None and not mu.empty:
                s = str(mu.iloc[0])
                if " vs. " in s:
                    home_tri = s.split(" vs. ", 1)[0].strip().upper() or None
            if away_tri is None and not mu.empty:
                s = str(mu.iloc[0])
                if " @ " in s:
                    away_tri = s.split(" @ ", 1)[0].strip().upper() or None
            # Another fallback: if we got one side only, infer other from any row with different team.
            if (home_tri is None or away_tri is None) and "TEAM_ABBREVIATION" in g.columns:
                teams = [str(t).strip().upper() for t in g["TEAM_ABBREVIATION"].dropna().unique().tolist()]
                teams = [t for t in teams if t]
                if len(teams) == 2:
                    if home_tri is None:
                        home_tri = teams[0]
                    if away_tri is None:
                        away_tri = teams[1] if teams[1] != home_tri else teams[0]
            return home_tri, away_tri
        except Exception:
            return None, None

    rows: list[dict[str, Any]] = []
    for gid, g in x.groupby("GAME_ID"):
        home_tri, away_tri = _home_away_for_gid(g)
        if not home_tri or not away_tri:
            continue
        rows.append({"date": d.isoformat(), "game_id": str(gid), "home_tri": home_tri, "away_tri": away_tri})
    return pd.DataFrame(rows)


def _build_minutes_priors(logs: pd.DataFrame, end_date: date, lookback_days: int) -> Dict[Tuple[str, str], float]:
    if lookback_days <= 0:
        return {}
    x = logs.copy()
    x["GAME_DATE"] = pd.to_datetime(x["GAME_DATE"], errors="coerce")
    start = pd.Timestamp(end_date - timedelta(days=lookback_days))
    end = pd.Timestamp(end_date)  # exclude current day
    x = x[(x["GAME_DATE"] >= start) & (x["GAME_DATE"] < end)]
    if x.empty:
        return {}

    def _to_min(v: Any) -> float:
        # player_logs uses integer minutes, but keep robust.
        try:
            if isinstance(v, str) and ":" in v:
                mm, ss = v.split(":", 1)
                return float(mm) + float(ss) / 60.0
            return float(pd.to_numeric(v, errors="coerce") or 0.0)
        except Exception:
            return 0.0

    x = x.copy()
    x["TEAM_ABBREVIATION"] = x.get("TEAM_ABBREVIATION").astype(str).str.upper().str.strip()
    x["PLAYER_NAME"] = x.get("PLAYER_NAME").astype(str)
    x["MIN_F"] = x.get("MIN").map(_to_min)

    grp = x.groupby(["TEAM_ABBREVIATION", "PLAYER_NAME"], dropna=False)["MIN_F"].mean().reset_index()
    pri: Dict[Tuple[str, str], float] = {}
    for _, r in grp.iterrows():
        tri = str(r.get("TEAM_ABBREVIATION") or "").strip().upper()
        name = str(r.get("PLAYER_NAME") or "").strip()
        m = _to_float(r.get("MIN_F"))
        key = _norm_player_key(name)
        if tri and key and m is not None and m > 0:
            pri[(tri, key)] = float(m)
    return pri


def _rosters_from_actual_logs(logs: pd.DataFrame, game_id: str, home_tri: str, away_tri: str) -> tuple[list[str], list[str]]:
    g = logs[logs["GAME_ID"].astype(str) == str(game_id)].copy()
    if g.empty:
        return [], []

    def _to_min(v: Any) -> float:
        try:
            if isinstance(v, str) and ":" in v:
                mm, ss = v.split(":", 1)
                return float(mm) + float(ss) / 60.0
            return float(pd.to_numeric(v, errors="coerce") or 0.0)
        except Exception:
            return 0.0

    g["TEAM_ABBREVIATION"] = g.get("TEAM_ABBREVIATION").astype(str).str.upper().str.strip()
    g["MIN_F"] = g.get("MIN").map(_to_min)
    g = g[g["MIN_F"] > 0]

    def _top_names(tri: str) -> list[str]:
        gg = g[g["TEAM_ABBREVIATION"] == tri]
        if gg.empty:
            return []
        gg = gg.sort_values(["MIN_F", "PTS"], ascending=[False, False])
        names = [str(x).strip() for x in gg["PLAYER_NAME"].tolist() if str(x).strip()]
        # keep unique order
        seen: set[str] = set()
        out: list[str] = []
        for nm in names:
            k = _norm_player_key(nm)
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(nm)
        return out

    return _top_names(home_tri), _top_names(away_tri)


def _actual_team_box(logs: pd.DataFrame, game_id: str, team_tri: str) -> pd.DataFrame:
    g = logs[logs["GAME_ID"].astype(str) == str(game_id)].copy()
    g["TEAM_ABBREVIATION"] = g.get("TEAM_ABBREVIATION").astype(str).str.upper().str.strip()
    g = g[g["TEAM_ABBREVIATION"] == str(team_tri).strip().upper()].copy()
    if g.empty:
        return g

    def _to_min(v: Any) -> float:
        try:
            if isinstance(v, str) and ":" in v:
                mm, ss = v.split(":", 1)
                return float(mm) + float(ss) / 60.0
            return float(pd.to_numeric(v, errors="coerce") or 0.0)
        except Exception:
            return 0.0

    out = pd.DataFrame(
        {
            "player_name": g.get("PLAYER_NAME").astype(str),
            "min": g.get("MIN").map(_to_min),
            "pts": pd.to_numeric(g.get("PTS"), errors="coerce").fillna(0.0),
            "reb": pd.to_numeric(g.get("REB"), errors="coerce").fillna(0.0),
            "ast": pd.to_numeric(g.get("AST"), errors="coerce").fillna(0.0),
            "threes": pd.to_numeric(g.get("FG3M"), errors="coerce").fillna(0.0),
            "fg3a": pd.to_numeric(g.get("FG3A"), errors="coerce").fillna(0.0),
            "fga": pd.to_numeric(g.get("FGA"), errors="coerce").fillna(0.0),
            "fgm": pd.to_numeric(g.get("FGM"), errors="coerce").fillna(0.0),
            "fta": pd.to_numeric(g.get("FTA"), errors="coerce").fillna(0.0),
            "ftm": pd.to_numeric(g.get("FTM"), errors="coerce").fillna(0.0),
            "pf": pd.to_numeric(g.get("PF"), errors="coerce").fillna(0.0),
            "tov": pd.to_numeric(g.get("TOV"), errors="coerce").fillna(0.0),
        }
    )
    out["player_key"] = out["player_name"].map(_norm_player_key)
    out = out[out["player_key"].ne("")].copy()
    # merge duplicates (rare)
    agg = out.groupby("player_key", as_index=False).agg(
        {
            "player_name": "first",
            "min": "sum",
            "pts": "sum",
            "reb": "sum",
            "ast": "sum",
            "threes": "sum",
            "fg3a": "sum",
            "fga": "sum",
            "fgm": "sum",
            "fta": "sum",
            "ftm": "sum",
            "pf": "sum",
            "tov": "sum",
        }
    )
    return agg


def _sim_team_box(sim_rep: dict[str, Any], key: str) -> pd.DataFrame:
    box = (sim_rep or {}).get(key) or {}
    players = box.get("players") or []
    if not isinstance(players, list) or not players:
        return pd.DataFrame(columns=["player_name", "player_key", "min", "pts", "reb", "ast", "threes", "tov"])
    df = pd.DataFrame(players)
    if df.empty:
        return df
    df = df.copy()
    df["player_name"] = df.get("player_name").astype(str)
    df["player_key"] = df["player_name"].map(_norm_player_key)
    for c in ("min", "pts", "reb", "ast", "threes", "tov", "fg3a", "fga", "fgm", "fta", "ftm", "pf"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        else:
            df[c] = 0.0
    df = df[df["player_key"].ne("")].copy()
    # ensure unique
    df = df.groupby("player_key", as_index=False).agg(
        {
            "player_name": "first",
            "min": "sum",
            "pts": "sum",
            "reb": "sum",
            "ast": "sum",
            "threes": "sum",
            "fg3a": "sum",
            "fga": "sum",
            "fgm": "sum",
            "fta": "sum",
            "ftm": "sum",
            "pf": "sum",
            "tov": "sum",
        }
    )
    return df


def _match_and_score(actual: pd.DataFrame, sim: pd.DataFrame, top_k: int = 8) -> dict[str, float]:
    if actual.empty or sim.empty:
        return {
            "min_mae_topk": float("nan"),
            "pts_mae_topk": float("nan"),
            "reb_mae_topk": float("nan"),
            "ast_mae_topk": float("nan"),
            "threes_mae_topk": float("nan"),
            "tov_mae_topk": float("nan"),
            "min_corr": float("nan"),
        }

    m = actual.merge(sim, on="player_key", how="inner", suffixes=("_act", "_sim"))
    if m.empty:
        return {
            "min_mae_topk": float("nan"),
            "pts_mae_topk": float("nan"),
            "reb_mae_topk": float("nan"),
            "ast_mae_topk": float("nan"),
            "threes_mae_topk": float("nan"),
            "tov_mae_topk": float("nan"),
            "min_corr": float("nan"),
        }

    m = m.sort_values("min_act", ascending=False).head(int(top_k)).copy()

    def _mae(col: str) -> float:
        try:
            return float(np.mean(np.abs(m[f"{col}_act"].to_numpy(dtype=float) - m[f"{col}_sim"].to_numpy(dtype=float))))
        except Exception:
            return float("nan")

    try:
        corr = float(np.corrcoef(m["min_act"].to_numpy(dtype=float), m["min_sim"].to_numpy(dtype=float))[0, 1])
    except Exception:
        corr = float("nan")

    return {
        "min_mae_topk": _mae("min"),
        "pts_mae_topk": _mae("pts"),
        "reb_mae_topk": _mae("reb"),
        "ast_mae_topk": _mae("ast"),
        "threes_mae_topk": _mae("threes"),
        "tov_mae_topk": _mae("tov"),
        "min_corr": corr,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Backtest connected (player) sim realism vs actual player logs")
    ap.add_argument("--start", type=str, required=False, default=None, help="Start date (YYYY-MM-DD)")
    ap.add_argument("--end", type=str, required=False, default=None, help="End date (YYYY-MM-DD)")
    ap.add_argument("--days", type=int, default=14, help="If start not provided, evaluate last N days ending at --end")
    ap.add_argument("--n-quarter-samples", type=int, default=3500, help="Quarter sim samples per game")
    ap.add_argument("--n-connected-samples", type=int, default=1200, help="Connected boxscore samples per game")
    ap.add_argument("--minutes-lookback-days", type=int, default=21, help="Lookback days for minutes priors")
    ap.add_argument("--top-k", type=int, default=8, help="Evaluate top-K players by actual minutes")
    ap.add_argument(
        "--hist-exp-blend-alpha",
        type=float,
        default=0.0,
        help="Optional: blend rotations-history expected minutes into bench minutes (0 disables)",
    )
    ap.add_argument(
        "--hist-exp-blend-max-cov",
        type=float,
        default=0.67,
        help="Only apply hist-exp blending when minutes_expected_coverage <= this threshold",
    )
    ap.add_argument(
        "--coach-rotation-alpha",
        type=float,
        default=0.0,
        help="Optional: scale coach/rotation shaping from rotation priors (0 disables)",
    )
    ap.add_argument(
        "--rotation-shock-alpha",
        type=float,
        default=0.0,
        help="Optional: detect rotation shock and blend minutes toward priors (0 disables)",
    )
    ap.add_argument(
        "--garbage-time-alpha",
        type=float,
        default=0.0,
        help="Optional: shift minutes from starters to bench when blowout likelihood is high (0 disables)",
    )
    ap.add_argument(
        "--correlated-scoring-alpha",
        type=float,
        default=0.0,
        help="Optional: add correlated scoring variance via latent game factors (0 disables)",
    )
    ap.add_argument(
        "--foul-trouble-alpha",
        type=float,
        default=0.0,
        help="Optional: disrupt rotation minutes from foul trouble risk (0 disables)",
    )
    ap.add_argument(
        "--guardrail-alpha",
        type=float,
        default=0.0,
        help="Optional: softly anchor quarter samples to model priors (0 disables)",
    )
    ap.add_argument(
        "--guardrail-max-scale",
        type=float,
        default=0.10,
        help="Max |scale-1| per team/quarter when applying guardrails",
    )
    ap.add_argument(
        "--event-level",
        action="store_true",
        help="Optional: use event-level (possession) stat-mix for the representative box score (keeps points allocation; default off)",
    )
    ap.add_argument("--skip-ot", action="store_true", help="Skip games where either team played >245 minutes (likely OT)")
    ap.add_argument("--seed", type=int, default=1, help="RNG seed base")
    ap.add_argument("--out-games-csv", type=str, default=None)
    ap.add_argument("--out-players-csv", type=str, default=None)
    ap.add_argument("--out-json", type=str, default=None)
    args = ap.parse_args()

    processed_dir = paths.data_processed
    logs = _load_player_logs(processed_dir)

    # Determine end date default based on latest predictions file present.
    if args.end:
        end_d = _parse_date(args.end)
    else:
        # Fall back to max date in player logs.
        logs_dt = pd.to_datetime(logs["GAME_DATE"], errors="coerce")
        end_ts = logs_dt.max()
        if pd.isna(end_ts):
            raise SystemExit("Could not infer end date from player_logs.csv")
        end_d = end_ts.date()

    start_d = _parse_date(args.start) if args.start else (end_d - timedelta(days=int(args.days) - 1))
    if start_d > end_d:
        start_d, end_d = end_d, start_d

    out_games = Path(args.out_games_csv) if args.out_games_csv else (processed_dir / f"connected_realism_games_{start_d.isoformat()}_{end_d.isoformat()}.csv")
    out_players = Path(args.out_players_csv) if args.out_players_csv else (processed_dir / f"connected_realism_players_{start_d.isoformat()}_{end_d.isoformat()}.csv")
    out_json = Path(args.out_json) if args.out_json else (processed_dir / f"connected_realism_summary_{start_d.isoformat()}_{end_d.isoformat()}.json")

    game_rows: list[dict[str, Any]] = []
    player_rows: list[dict[str, Any]] = []

    for d in _daterange(start_d, end_d):
        preds = _load_predictions(processed_dir, d)
        props = _load_props_predictions(processed_dir, d)
        if preds is None or props is None:
            continue

        odds = _load_game_odds(processed_dir, d)
        preds = _fill_market_lines(preds, odds)

        # Add tricodes for matching to player_logs games.
        preds = preds.copy()
        preds["home_team"] = preds.get("home_team").astype(str).apply(normalize_team)
        preds["visitor_team"] = preds.get("visitor_team").astype(str).apply(normalize_team)
        preds["home_tri"] = preds["home_team"].astype(str).map(to_tricode)
        preds["away_tri"] = preds["visitor_team"].astype(str).map(to_tricode)

        # Build game list from actual logs (has game_id).
        games = _games_from_player_logs_for_date(logs, d)
        if games is None or games.empty:
            continue

        minutes_priors = _build_minutes_priors(logs, end_date=d, lookback_days=int(args.minutes_lookback_days))

        try:
            pri = compute_player_priors(
                d.isoformat(),
                cfg=PlayerPriorsConfig(days_back=int(args.minutes_lookback_days), min_games=3, min_minutes_avg=4.0),
            )
            player_priors = pri.rates
        except Exception:
            player_priors = {}

        for _, g in games.iterrows():
            gid = str(g.get("game_id"))
            htri = str(g.get("home_tri") or "").strip().upper()
            atri = str(g.get("away_tri") or "").strip().upper()
            if not gid or not htri or not atri:
                continue

            # Find matching predictions row (try direct, then swapped).
            pr = preds[(preds["home_tri"] == htri) & (preds["away_tri"] == atri)]
            flipped = False
            if pr.empty:
                pr = preds[(preds["home_tri"] == atri) & (preds["away_tri"] == htri)]
                flipped = not pr.empty
            if pr.empty:
                continue
            r = pr.iloc[0]

            # If flipped, swap home/visitor fields so quarter sim has correct home/away.
            if flipped:
                r = r.copy()
                r["home_team"], r["visitor_team"] = r.get("visitor_team"), r.get("home_team")
                r["home_tri"], r["away_tri"] = htri, atri
                # spread_margin in predictions is home - away; flip sign if swapped
                if "spread_margin" in r.index:
                    try:
                        r["spread_margin"] = -float(r.get("spread_margin"))
                    except Exception:
                        pass
                if "home_spread" in r.index:
                    try:
                        r["home_spread"] = -float(r.get("home_spread"))
                    except Exception:
                        pass

            home_roster, away_roster = _rosters_from_actual_logs(logs, gid, htri, atri)
            if not home_roster or not away_roster:
                continue

            # Skip OT games if requested.
            if args.skip_ot:
                act_h = _actual_team_box(logs, gid, htri)
                act_a = _actual_team_box(logs, gid, atri)
                if (not act_h.empty and float(act_h["min"].sum()) > 245.0) or (not act_a.empty and float(act_a["min"].sum()) > 245.0):
                    continue

            home_ctx, away_ctx, market_total, market_home_spread = _build_context_from_row(r)
            inp = GameInputs(date=d.isoformat(), home=home_ctx, away=away_ctx, market_total=market_total, market_home_spread=market_home_spread)

            qsum = simulate_quarters(inp, n_samples=int(args.n_quarter_samples))

            # Guardrail priors sourced from the same predictions row used to build the quarter context.
            gr_priors: dict[str, Any] = {}
            try:
                pt = _to_float(r.get("pred_total")) or _to_float(r.get("totals")) or _to_float(r.get("total_pred"))
                pm = _to_float(r.get("pred_margin")) or _to_float(r.get("spread_margin")) or _to_float(r.get("margin_pred"))
                if pt is not None:
                    gr_priors["pred_total"] = float(pt)
                if pm is not None:
                    gr_priors["pred_margin"] = float(pm)
                for qi in (1, 2, 3, 4):
                    qt = _to_float(r.get(f"quarters_q{qi}_total"))
                    qm = _to_float(r.get(f"quarters_q{qi}_margin"))
                    if qt is not None:
                        gr_priors[f"quarters_q{qi}_total"] = float(qt)
                    if qm is not None:
                        gr_priors[f"quarters_q{qi}_margin"] = float(qm)
            except Exception:
                gr_priors = {}

            sim = simulate_connected_game(
                qsum.quarters,
                home_tri=htri,
                away_tri=atri,
                props_df=props,
                home_roster=home_roster,
                away_roster=away_roster,
                minutes_priors=minutes_priors,
                player_priors=player_priors,
                minutes_lookback_days=int(args.minutes_lookback_days),
                n_samples=int(args.n_connected_samples),
                seed=int(args.seed) + int(gid[-4:]) if gid[-4:].isdigit() else int(args.seed),
                date_str=d.isoformat(),
                hist_exp_blend_alpha=float(args.hist_exp_blend_alpha or 0.0),
                hist_exp_blend_max_cov=float(args.hist_exp_blend_max_cov if args.hist_exp_blend_max_cov is not None else 0.67),
                coach_rotation_alpha=float(args.coach_rotation_alpha or 0.0),
                rotation_shock_alpha=float(args.rotation_shock_alpha or 0.0),
                garbage_time_alpha=float(args.garbage_time_alpha or 0.0),
                correlated_scoring_alpha=float(args.correlated_scoring_alpha or 0.0),
                foul_trouble_alpha=float(args.foul_trouble_alpha or 0.0),
                use_event_level_sim=bool(getattr(args, "event_level", False)),
                guardrail_priors=gr_priors,
                guardrail_alpha=float(args.guardrail_alpha or 0.0),
                guardrail_max_scale=float(args.guardrail_max_scale if args.guardrail_max_scale is not None else 0.10),
            )
            if not isinstance(sim, dict) or sim.get("error"):
                continue

            rep = sim.get("rep") or {}
            home_box = _sim_team_box(rep, "home_box")
            away_box = _sim_team_box(rep, "away_box")

            act_home = _actual_team_box(logs, gid, htri)
            act_away = _actual_team_box(logs, gid, atri)

            def _team_abs_err(act_df: pd.DataFrame, sim_df: pd.DataFrame, col: str) -> float:
                try:
                    a = float(pd.to_numeric(act_df.get(col), errors="coerce").fillna(0.0).sum()) if act_df is not None and not act_df.empty else 0.0
                    s = float(pd.to_numeric(sim_df.get(col), errors="coerce").fillna(0.0).sum()) if sim_df is not None and not sim_df.empty else 0.0
                    return float(abs(s - a))
                except Exception:
                    return float("nan")

            home_fga_abs_err = _team_abs_err(act_home, home_box, "fga")
            away_fga_abs_err = _team_abs_err(act_away, away_box, "fga")
            home_fg3a_abs_err = _team_abs_err(act_home, home_box, "fg3a")
            away_fg3a_abs_err = _team_abs_err(act_away, away_box, "fg3a")
            home_fta_abs_err = _team_abs_err(act_home, home_box, "fta")
            away_fta_abs_err = _team_abs_err(act_away, away_box, "fta")
            home_tov_abs_err = _team_abs_err(act_home, home_box, "tov")
            away_tov_abs_err = _team_abs_err(act_away, away_box, "tov")

            home_metrics = _match_and_score(act_home, home_box, top_k=int(args.top_k))
            away_metrics = _match_and_score(act_away, away_box, top_k=int(args.top_k))

            # Pathology: 30+ min and 0 across major counting stats
            def _pathology(df: pd.DataFrame) -> int:
                if df.empty:
                    return 0
                z = df.copy()
                z["tot"] = z[["pts", "reb", "ast", "threes", "tov"]].sum(axis=1)
                return int(((z["min"] >= 30.0) & (z["tot"] <= 0.0)).sum())

            sim_path = _pathology(pd.concat([home_box.assign(team=htri), away_box.assign(team=atri)], ignore_index=True))

            # Guardrails diagnostics (compact per-game summary for A/B tuning).
            guard = ((sim.get("diagnostics") or {}).get("guardrails") or {}) if isinstance(sim, dict) else {}
            if not isinstance(guard, dict):
                guard = {}
            gr_pre = guard.get("pre") or {}
            gr_post = guard.get("post") or {}
            if not isinstance(gr_pre, dict):
                gr_pre = {}
            if not isinstance(gr_post, dict):
                gr_post = {}

            gr_enabled = bool(guard.get("enabled"))
            gr_mode = str(guard.get("mode") or "")
            gr_alpha = _to_float(guard.get("alpha"))
            gr_max_scale = _to_float(guard.get("max_scale"))
            gr_pre_total_mu = _to_float(gr_pre.get("total_mu"))
            gr_post_total_mu = _to_float(gr_post.get("total_mu"))
            gr_pre_margin_mu = _to_float(gr_pre.get("margin_mu"))
            gr_post_margin_mu = _to_float(gr_post.get("margin_mu"))

            gr_total_mu_shift = None
            gr_margin_mu_shift = None
            try:
                if gr_pre_total_mu is not None and gr_post_total_mu is not None:
                    gr_total_mu_shift = float(gr_post_total_mu - gr_pre_total_mu)
                if gr_pre_margin_mu is not None and gr_post_margin_mu is not None:
                    gr_margin_mu_shift = float(gr_post_margin_mu - gr_pre_margin_mu)
            except Exception:
                gr_total_mu_shift = None
                gr_margin_mu_shift = None

            gr_scales = guard.get("scales") or {}
            if not isinstance(gr_scales, dict):
                gr_scales = {}

            def _scale_stats(v: Any) -> tuple[float | None, float | None]:
                vals: list[float] = []
                try:
                    if isinstance(v, list):
                        for x in v:
                            fx = _to_float(x)
                            if fx is not None and np.isfinite(float(fx)):
                                vals.append(float(fx))
                    else:
                        fx = _to_float(v)
                        if fx is not None and np.isfinite(float(fx)):
                            vals.append(float(fx))
                except Exception:
                    vals = []
                if not vals:
                    return None, None
                mean = float(np.mean(vals))
                max_abs_dev = float(np.max(np.abs(np.asarray(vals, dtype=float) - 1.0)))
                return mean, max_abs_dev

            home_scales = gr_scales.get("home_q") if "home_q" in gr_scales else gr_scales.get("home")
            away_scales = gr_scales.get("away_q") if "away_q" in gr_scales else gr_scales.get("away")
            gr_home_scale_mean, gr_home_scale_max_abs_dev = _scale_stats(home_scales)
            gr_away_scale_mean, gr_away_scale_max_abs_dev = _scale_stats(away_scales)

            try:
                gr_warnings = guard.get("warnings")
                if isinstance(gr_warnings, list):
                    gr_warnings = ";".join([str(x) for x in gr_warnings if str(x or "").strip()])
                else:
                    gr_warnings = str(gr_warnings or "")
            except Exception:
                gr_warnings = ""

            # Per-player rows for the merged set (both teams)
            for team_tri, act_df, sim_df in [(htri, act_home, home_box), (atri, act_away, away_box)]:
                if act_df.empty or sim_df.empty:
                    continue
                mm = act_df.merge(sim_df, on="player_key", how="outer", suffixes=("_act", "_sim"))
                for _, rr in mm.iterrows():
                    def _clean_name(x: Any) -> str:
                        try:
                            if x is None:
                                return ""
                            if isinstance(x, float) and np.isnan(x):
                                return ""
                            s = str(x).strip()
                            return "" if s.lower() == "nan" else s
                        except Exception:
                            return ""

                    name = _clean_name(rr.get("player_name_act")) or _clean_name(rr.get("player_name_sim"))
                    player_rows.append(
                        {
                            "date": d.isoformat(),
                            "game_id": gid,
                            "team": team_tri,
                            "player_name": name,
                            "min_act": float(rr.get("min_act") or 0.0),
                            "min_sim": float(rr.get("min_sim") or 0.0),
                            "pts_act": float(rr.get("pts_act") or 0.0),
                            "pts_sim": float(rr.get("pts_sim") or 0.0),
                            "reb_act": float(rr.get("reb_act") or 0.0),
                            "reb_sim": float(rr.get("reb_sim") or 0.0),
                            "ast_act": float(rr.get("ast_act") or 0.0),
                            "ast_sim": float(rr.get("ast_sim") or 0.0),
                            "threes_act": float(rr.get("threes_act") or 0.0),
                            "threes_sim": float(rr.get("threes_sim") or 0.0),
                            "fg3a_act": float(rr.get("fg3a_act") or 0.0),
                            "fg3a_sim": float(rr.get("fg3a_sim") or 0.0),
                            "fga_act": float(rr.get("fga_act") or 0.0),
                            "fga_sim": float(rr.get("fga_sim") or 0.0),
                            "fgm_act": float(rr.get("fgm_act") or 0.0),
                            "fgm_sim": float(rr.get("fgm_sim") or 0.0),
                            "fta_act": float(rr.get("fta_act") or 0.0),
                            "fta_sim": float(rr.get("fta_sim") or 0.0),
                            "ftm_act": float(rr.get("ftm_act") or 0.0),
                            "ftm_sim": float(rr.get("ftm_sim") or 0.0),
                            "pf_act": float(rr.get("pf_act") or 0.0),
                            "pf_sim": float(rr.get("pf_sim") or 0.0),
                            "tov_act": float(rr.get("tov_act") or 0.0),
                            "tov_sim": float(rr.get("tov_sim") or 0.0),
                        }
                    )

            game_rows.append(
                {
                    "date": d.isoformat(),
                    "game_id": gid,
                    "home_tri": htri,
                    "away_tri": atri,
                    "market_total": market_total,
                    "market_home_spread": market_home_spread,
                    "sim_rep_home_pts": int(rep.get("home_score") or 0),
                    "sim_rep_away_pts": int(rep.get("away_score") or 0),
                    "home_min_mae_topk": home_metrics["min_mae_topk"],
                    "away_min_mae_topk": away_metrics["min_mae_topk"],
                    "home_pts_mae_topk": home_metrics["pts_mae_topk"],
                    "away_pts_mae_topk": away_metrics["pts_mae_topk"],
                    "home_reb_mae_topk": home_metrics.get("reb_mae_topk"),
                    "away_reb_mae_topk": away_metrics.get("reb_mae_topk"),
                    "home_ast_mae_topk": home_metrics.get("ast_mae_topk"),
                    "away_ast_mae_topk": away_metrics.get("ast_mae_topk"),
                    "home_threes_mae_topk": home_metrics.get("threes_mae_topk"),
                    "away_threes_mae_topk": away_metrics.get("threes_mae_topk"),
                    "home_tov_mae_topk": home_metrics.get("tov_mae_topk"),
                    "away_tov_mae_topk": away_metrics.get("tov_mae_topk"),
                    "home_min_corr_topk": home_metrics["min_corr"],
                    "away_min_corr_topk": away_metrics["min_corr"],
                    "sim_pathology_30min_zerostat": sim_path,
                    "event_level_enabled": int(bool(getattr(args, "event_level", False))),
                    "event_level_used": int(bool(((sim.get("diagnostics") or {}).get("event_level") or {}).get("used"))),
                    "event_level_error": str(((sim.get("diagnostics") or {}).get("event_level") or {}).get("error") or ""),
                    "guard_enabled": int(bool(gr_enabled)),
                    "guard_mode": gr_mode,
                    "guard_alpha": gr_alpha,
                    "guard_max_scale": gr_max_scale,
                    "guard_pre_total_mu": gr_pre_total_mu,
                    "guard_post_total_mu": gr_post_total_mu,
                    "guard_pre_margin_mu": gr_pre_margin_mu,
                    "guard_post_margin_mu": gr_post_margin_mu,
                    "guard_total_mu_shift": gr_total_mu_shift,
                    "guard_margin_mu_shift": gr_margin_mu_shift,
                    "guard_home_scale_mean": gr_home_scale_mean,
                    "guard_away_scale_mean": gr_away_scale_mean,
                    "guard_home_scale_max_abs_dev": gr_home_scale_max_abs_dev,
                    "guard_away_scale_max_abs_dev": gr_away_scale_max_abs_dev,
                    "guard_warnings": gr_warnings,
                    "home_team_fga_abs_err": home_fga_abs_err,
                    "away_team_fga_abs_err": away_fga_abs_err,
                    "home_team_fg3a_abs_err": home_fg3a_abs_err,
                    "away_team_fg3a_abs_err": away_fg3a_abs_err,
                    "home_team_fta_abs_err": home_fta_abs_err,
                    "away_team_fta_abs_err": away_fta_abs_err,
                    "home_team_tov_abs_err": home_tov_abs_err,
                    "away_team_tov_abs_err": away_tov_abs_err,
                    # Diagnostics to help slice failures.
                    "home_minutes_source": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("minutes_source"),
                    "away_minutes_source": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("minutes_source"),
                    "home_minutes_total_raw": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("minutes_total_raw"),
                    "away_minutes_total_raw": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("minutes_total_raw"),
                    "home_minutes_total_sim": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("minutes_total_sim"),
                    "away_minutes_total_sim": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("minutes_total_sim"),
                    "home_minutes_prior_coverage": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("minutes_prior_coverage"),
                    "away_minutes_prior_coverage": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("minutes_prior_coverage"),
                    "home_minutes_expected_coverage": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("minutes_expected_coverage"),
                    "away_minutes_expected_coverage": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("minutes_expected_coverage"),
                    "home_minutes_expected_asof_max": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("minutes_expected_asof_max"),
                    "away_minutes_expected_asof_max": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("minutes_expected_asof_max"),
                    "home_minutes_signal_n": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("minutes_signal_n"),
                    "away_minutes_signal_n": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("minutes_signal_n"),
                    "home_minutes_prior_divergence": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("minutes_prior_divergence"),
                    "away_minutes_prior_divergence": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("minutes_prior_divergence"),
                    "home_minutes_prior_divergence0": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("minutes_prior_divergence0"),
                    "away_minutes_prior_divergence0": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("minutes_prior_divergence0"),
                    "home_dtd_n": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("dtd_n"),
                    "away_dtd_n": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("dtd_n"),
                    "home_minutes_top3_share": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("minutes_top3_share"),
                    "away_minutes_top3_share": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("minutes_top3_share"),
                    "home_minutes_top5_share": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("minutes_top5_share"),
                    "away_minutes_top5_share": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("minutes_top5_share"),
                    "home_minutes_flatten_p": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("minutes_flatten_p"),
                    "away_minutes_flatten_p": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("minutes_flatten_p"),
                    "home_rotation_shock_shortfall": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("rotation_shock_shortfall"),
                    "away_rotation_shock_shortfall": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("rotation_shock_shortfall"),
                    "home_garbage_time_alpha": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("garbage_time_alpha"),
                    "away_garbage_time_alpha": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("garbage_time_alpha"),
                    "home_garbage_time_p_abs": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("garbage_time_p_abs"),
                    "away_garbage_time_p_abs": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("garbage_time_p_abs"),
                    "home_garbage_time_p_home_win_big": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("garbage_time_p_home_win_big"),
                    "away_garbage_time_p_home_win_big": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("garbage_time_p_home_win_big"),
                    "home_garbage_time_p_away_win_big": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("garbage_time_p_away_win_big"),
                    "away_garbage_time_p_away_win_big": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("garbage_time_p_away_win_big"),
                    "home_garbage_time_top3_share_sim": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("garbage_time_top3_share_sim"),
                    "away_garbage_time_top3_share_sim": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("garbage_time_top3_share_sim"),
                    "home_garbage_time_shift_minutes": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("garbage_time_shift_minutes"),
                    "away_garbage_time_shift_minutes": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("garbage_time_shift_minutes"),
                    # Foul trouble minutes diagnostics.
                    "home_foul_trouble_alpha": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("foul_trouble_alpha"),
                    "away_foul_trouble_alpha": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("foul_trouble_alpha"),
                    "home_foul_trouble_pf_coverage": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("foul_trouble_pf_coverage"),
                    "away_foul_trouble_pf_coverage": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("foul_trouble_pf_coverage"),
                    "home_foul_trouble_p_ge5_max": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("foul_trouble_p_ge5_max"),
                    "away_foul_trouble_p_ge5_max": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("foul_trouble_p_ge5_max"),
                    "home_foul_trouble_shift_minutes": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("foul_trouble_shift_minutes"),
                    "away_foul_trouble_shift_minutes": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("foul_trouble_shift_minutes"),
                    "home_foul_trouble_whistle": ((sim.get("diagnostics") or {}).get("home_minutes") or {}).get("foul_trouble_whistle"),
                    "away_foul_trouble_whistle": ((sim.get("diagnostics") or {}).get("away_minutes") or {}).get("foul_trouble_whistle"),
                    "home_points_entropy": (sim.get("diagnostics") or {}).get("home_points_entropy"),
                    "away_points_entropy": (sim.get("diagnostics") or {}).get("away_points_entropy"),
                    # Correlated scoring variance diagnostics.
                    "scoring_corr_enabled": ((sim.get("diagnostics") or {}).get("scoring_correlation") or {}).get("enabled"),
                    "scoring_corr_alpha": ((sim.get("diagnostics") or {}).get("scoring_correlation") or {}).get("alpha"),
                    "scoring_corr_env_sigma": ((sim.get("diagnostics") or {}).get("scoring_correlation") or {}).get("env_sigma"),
                    "scoring_corr_rel_sigma": ((sim.get("diagnostics") or {}).get("scoring_correlation") or {}).get("rel_sigma"),
                    "scoring_corr_home_mult_std": ((sim.get("diagnostics") or {}).get("scoring_correlation") or {}).get("home_mult_std"),
                    "scoring_corr_away_mult_std": ((sim.get("diagnostics") or {}).get("scoring_correlation") or {}).get("away_mult_std"),
                    "scoring_corr_clipped_frac": ((sim.get("diagnostics") or {}).get("scoring_correlation") or {}).get("clipped_frac"),
                    "used_target_rep": (sim.get("diagnostics") or {}).get("used_target_rep"),
                    "warnings": ";".join((sim.get("diagnostics") or {}).get("warnings") or []),
                }
            )

    games_df = pd.DataFrame(game_rows)
    players_df = pd.DataFrame(player_rows)

    games_df.to_csv(out_games, index=False)
    players_df.to_csv(out_players, index=False)

    summary: dict[str, Any] = {
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "games": int(len(games_df)),
        "players_rows": int(len(players_df)),
        "means": {},
    }

    if not games_df.empty:
        def _m(col: str) -> float:
            return float(pd.to_numeric(games_df.get(col), errors="coerce").mean())

        def _frac_nonempty(col: str) -> float:
            try:
                s = games_df.get(col)
                if s is None:
                    return 0.0
                ss = s.astype(str).fillna("").str.strip()
                return float((ss != "").mean())
            except Exception:
                return 0.0

        summary["means"] = {
            "home_min_mae_topk": _m("home_min_mae_topk"),
            "away_min_mae_topk": _m("away_min_mae_topk"),
            "home_pts_mae_topk": _m("home_pts_mae_topk"),
            "away_pts_mae_topk": _m("away_pts_mae_topk"),
            "home_reb_mae_topk": _m("home_reb_mae_topk"),
            "away_reb_mae_topk": _m("away_reb_mae_topk"),
            "home_ast_mae_topk": _m("home_ast_mae_topk"),
            "away_ast_mae_topk": _m("away_ast_mae_topk"),
            "home_threes_mae_topk": _m("home_threes_mae_topk"),
            "away_threes_mae_topk": _m("away_threes_mae_topk"),
            "home_tov_mae_topk": _m("home_tov_mae_topk"),
            "away_tov_mae_topk": _m("away_tov_mae_topk"),
            "home_min_corr_topk": _m("home_min_corr_topk"),
            "away_min_corr_topk": _m("away_min_corr_topk"),
            "sim_pathology_30min_zerostat": float(pd.to_numeric(games_df.get("sim_pathology_30min_zerostat"), errors="coerce").fillna(0).sum()),
            "event_level_used_frac": _m("event_level_used"),
            "event_level_error_frac": _frac_nonempty("event_level_error"),
            "home_team_fga_abs_err": _m("home_team_fga_abs_err"),
            "away_team_fga_abs_err": _m("away_team_fga_abs_err"),
            "home_team_fg3a_abs_err": _m("home_team_fg3a_abs_err"),
            "away_team_fg3a_abs_err": _m("away_team_fg3a_abs_err"),
            "home_team_fta_abs_err": _m("home_team_fta_abs_err"),
            "away_team_fta_abs_err": _m("away_team_fta_abs_err"),
            "home_team_tov_abs_err": _m("home_team_tov_abs_err"),
            "away_team_tov_abs_err": _m("away_team_tov_abs_err"),
        }

    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote: {out_games}")
    print(f"Wrote: {out_players}")
    print(f"Wrote: {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
