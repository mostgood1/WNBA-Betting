from __future__ import annotations

from dataclasses import dataclass
from datetime import date as _date
from typing import Iterable, Literal

import numpy as np
import pandas as pd

from .config import paths


SortKey = Literal["ev", "edge"]


@dataclass(frozen=True)
class BacktestConfig:
    sort_by: SortKey = "ev"
    top_n_per_day: int = 12
    top_n_per_game: int | None = None
    min_ev: float | None = None
    min_edge: float | None = None
    min_price: float | None = None
    max_price: float | None = None
    bookmaker: str | None = None
    exclude_bookmakers: tuple[str, ...] | None = None
    dedupe_best_book: bool = True
    include_dd_td: bool = False
    include_stats: tuple[str, ...] | None = None
    exclude_stats: tuple[str, ...] | None = None


def _date_range(start: str, end: str) -> list[str]:
    ds = pd.date_range(start=start, end=end, freq="D")
    return [d.date().isoformat() for d in ds]


def _to_int_str(x) -> str | None:
    if x is None:
        return None
    try:
        if isinstance(x, str):
            s = x.strip()
            if not s or s.lower() in {"nan", "none"}:
                return None
            # Keep digits only (player_id sometimes read as 201950.0)
            digits = "".join(ch for ch in s if ch.isdigit())
            return digits or None
        v = float(x)
        if not np.isfinite(v):
            return None
        return str(int(v))
    except Exception:
        return None


def _norm_name(s: str | None) -> str:
    return " ".join(str(s or "").strip().lower().split())


def _american_to_b(odds: float | None) -> float | None:
    if odds is None:
        return None
    try:
        o = float(odds)
    except Exception:
        return None
    if not np.isfinite(o) or o == 0:
        return None
    return (o / 100.0) if o > 0 else (100.0 / abs(o))


def _profit_per_unit(result: str | None, odds: float | None) -> float | None:
    """Profit for 1 unit staked (risk 1). Win returns +b, loss -1, push 0."""
    if result is None:
        return None
    r = str(result).upper().strip()
    if r == "P":
        return 0.0
    if r not in {"W", "L"}:
        return None
    b = _american_to_b(odds)
    if b is None:
        return None
    return b if r == "W" else -1.0


def _compute_actual(rec: pd.Series, stat: str) -> float | None:
    s = str(stat or "").strip().lower().replace("_", "").replace("-", "")
    pts = pd.to_numeric(rec.get("pts"), errors="coerce")
    reb = pd.to_numeric(rec.get("reb"), errors="coerce")
    ast = pd.to_numeric(rec.get("ast"), errors="coerce")
    threes = pd.to_numeric(rec.get("threes"), errors="coerce")
    pra = pd.to_numeric(rec.get("pra"), errors="coerce")

    if s in {"pts", "points"}:
        return float(pts) if np.isfinite(pts) else None
    if s in {"reb", "rebs", "rebounds", "trb"}:
        return float(reb) if np.isfinite(reb) else None
    if s in {"ast", "asts", "assists"}:
        return float(ast) if np.isfinite(ast) else None
    if s in {"threes", "3pm", "3ptm", "3pt", "3p"}:
        return float(threes) if np.isfinite(threes) else None

    if s == "pra":
        if np.isfinite(pra):
            return float(pra)
        if np.isfinite(pts) and np.isfinite(reb) and np.isfinite(ast):
            return float(pts + reb + ast)
        return None

    if s in {"ra", "rebast", "reboundsassists"}:
        if np.isfinite(reb) and np.isfinite(ast):
            return float(reb + ast)
        return None

    if s in {"pr", "ptsreb", "pointsrebounds"}:
        if np.isfinite(pts) and np.isfinite(reb):
            return float(pts + reb)
        return None

    if s in {"pa", "ptsast", "pointsassists"}:
        if np.isfinite(pts) and np.isfinite(ast):
            return float(pts + ast)
        return None

    # Double-double / triple-double (based on PTS/REB/AST only)
    if s in {"dd", "doubledouble"}:
        if not (np.isfinite(pts) and np.isfinite(reb) and np.isfinite(ast)):
            return None
        cats = int((pts >= 10) + (reb >= 10) + (ast >= 10))
        return 1.0 if cats >= 2 else 0.0

    if s in {"td", "tripledouble"}:
        if not (np.isfinite(pts) and np.isfinite(reb) and np.isfinite(ast)):
            return None
        cats = int((pts >= 10) + (reb >= 10) + (ast >= 10))
        return 1.0 if cats >= 3 else 0.0

    return None


def _grade(side: str, line: float, actual: float) -> str:
    s = str(side or "").upper().strip()
    # YES/NO markets (no numeric line)
    if s in {"YES", "NO"}:
        is_yes = bool(actual >= 0.5)
        want_yes = (s == "YES")
        return "W" if (is_yes == want_yes) else "L"

    # Push is possible for integer lines; for .5 lines it won't happen.
    if actual == line:
        return "P"
    if s == "OVER":
        return "W" if actual > line else "L"
    if s == "UNDER":
        return "W" if actual < line else "L"
    return ""


def _load_edges_for_date(date_str: str) -> pd.DataFrame | None:
    p = paths.data_processed / f"props_edges_{date_str}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p)
    if df is None or df.empty:
        return None
    return df


def _load_actuals_for_date(date_str: str) -> pd.DataFrame | None:
    p = paths.data_processed / f"recon_props_{date_str}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p)
    if df is None or df.empty:
        return None
    return df


def backtest_props_edges(start: str, end: str, cfg: BacktestConfig) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (ledger_df, overall_summary_df, daily_summary_df)."""

    ledgers: list[pd.DataFrame] = []

    for d in _date_range(start, end):
        edges = _load_edges_for_date(d)
        actuals = _load_actuals_for_date(d)
        if edges is None or actuals is None:
            continue

        e = edges.copy()
        a = actuals.copy()

        # Normalize identifiers
        e["player_id_norm"] = e.get("player_id").apply(_to_int_str)
        e["team_norm"] = e.get("team").astype(str).str.upper().str.strip()
        e["player_name_norm"] = e.get("player_name").astype(str).map(_norm_name)

        a["player_id_norm"] = a.get("player_id").apply(_to_int_str)
        a["team_norm"] = a.get("team_abbr").astype(str).str.upper().str.strip()
        a["player_name_norm"] = a.get("player_name").astype(str).map(_norm_name)

        # Numerics
        for col in ["edge", "ev", "line", "price"]:
            if col in e.columns:
                e[col] = pd.to_numeric(e[col], errors="coerce")

        for col in ["pts", "reb", "ast", "threes", "pra"]:
            if col in a.columns:
                a[col] = pd.to_numeric(a[col], errors="coerce")

        # Filters
        if not cfg.include_dd_td and "stat" in e.columns:
            e = e[~e["stat"].astype(str).str.lower().isin(["dd", "td"])]

        if "stat" in e.columns and (cfg.include_stats or cfg.exclude_stats):
            st = e["stat"].astype(str).str.lower().str.strip()
            if cfg.include_stats:
                allow = {str(x).lower().strip() for x in cfg.include_stats if str(x).strip()}
                if allow:
                    e = e[st.isin(allow)]
            if cfg.exclude_stats:
                block = {str(x).lower().strip() for x in cfg.exclude_stats if str(x).strip()}
                if block:
                    e = e[~st.isin(block)]
        if cfg.bookmaker:
            e = e[e.get("bookmaker").astype(str).str.lower() == str(cfg.bookmaker).lower()]
        if cfg.exclude_bookmakers:
            blocked = {str(b).strip().lower() for b in cfg.exclude_bookmakers if str(b).strip()}
            if blocked:
                e = e[~e.get("bookmaker").astype(str).str.lower().isin(blocked)]
        if cfg.min_ev is not None and "ev" in e.columns:
            e = e[pd.to_numeric(e["ev"], errors="coerce") >= float(cfg.min_ev)]
        if cfg.min_edge is not None and "edge" in e.columns:
            e = e[pd.to_numeric(e["edge"], errors="coerce") >= float(cfg.min_edge)]
        if (cfg.min_price is not None or cfg.max_price is not None) and "price" in e.columns:
            p = pd.to_numeric(e["price"], errors="coerce")
            if cfg.min_price is not None:
                pmin = float(cfg.min_price)
                e = e[p >= pmin]
                p = p.loc[e.index]
            if cfg.max_price is not None:
                pmax = float(cfg.max_price)
                e = e[p <= pmax]

        if e.empty:
            continue

        # Dedupe across books: keep best row per unique bet definition
        if cfg.dedupe_best_book:
            # Prefer player_id_norm; fallback to name/team.
            e["bet_key"] = (
                e["team_norm"].fillna("")
                + "|"
                + e["player_id_norm"].fillna("")
                + "|"
                + e["player_name_norm"].fillna("")
                + "|"
                + e.get("stat").astype(str).str.lower().fillna("")
                + "|"
                + e.get("side").astype(str).str.upper().fillna("")
                + "|"
                + e.get("line").astype(str).fillna("")
            )
            score_col = "ev" if cfg.sort_by == "ev" else "edge"
            e = e.sort_values(by=[score_col], ascending=False, na_position="last")
            e = e.drop_duplicates(subset=["bet_key"], keep="first")

        # Select top picks
        score_col = "ev" if cfg.sort_by == "ev" else "edge"
        if score_col not in e.columns:
            score_col = "ev" if "ev" in e.columns else ("edge" if "edge" in e.columns else None)
        if score_col is None:
            continue

        if cfg.top_n_per_game is not None:
            # Group by matchup if present; else fall back to per-day
            if "home_team" in e.columns and "away_team" in e.columns:
                e["game_key"] = (
                    e.get("home_team").astype(str).str.strip()
                    + "|"
                    + e.get("away_team").astype(str).str.strip()
                )
                e = (
                    e.sort_values(by=["game_key", score_col], ascending=[True, False], na_position="last")
                    .groupby("game_key", as_index=False, sort=False)
                    .head(int(cfg.top_n_per_game))
                )
            else:
                e = e.sort_values(by=[score_col], ascending=False, na_position="last").head(int(cfg.top_n_per_day))
        else:
            e = e.sort_values(by=[score_col], ascending=False, na_position="last").head(int(cfg.top_n_per_day))

        if e.empty:
            continue

        # Join actuals
        a_by_pid = a.dropna(subset=["player_id_norm"]).copy()
        a_by_pid = a_by_pid.drop_duplicates(subset=["player_id_norm"], keep="first")
        merged = e.merge(a_by_pid[["player_id_norm", "team_norm", "player_name_norm", "pts", "reb", "ast", "threes", "pra"]], on="player_id_norm", how="left", suffixes=("", "_act"))

        # Name/team fallback for missing pid matches
        miss = merged[merged["pts"].isna() & merged["player_id_norm"].isna()].copy()
        if not miss.empty:
            a_keyed = a.dropna(subset=["team_norm", "player_name_norm"]).copy()
            a_keyed["name_team_key"] = a_keyed["team_norm"] + "|" + a_keyed["player_name_norm"]
            a_keyed = a_keyed.drop_duplicates(subset=["name_team_key"], keep="first")
            merged["name_team_key"] = merged["team_norm"].fillna("") + "|" + merged["player_name_norm"].fillna("")
            merged = merged.merge(
                a_keyed[["name_team_key", "pts", "reb", "ast", "threes", "pra"]],
                on="name_team_key",
                how="left",
                suffixes=("", "_nm"),
            )
            for col in ["pts", "reb", "ast", "threes", "pra"]:
                merged[col] = merged[col].fillna(merged.get(f"{col}_nm"))

        # Compute actual stat and grade
        merged["actual"] = merged.apply(lambda r: _compute_actual(r, r.get("stat")), axis=1)
        merged["line_num"] = pd.to_numeric(merged.get("line"), errors="coerce")

        def _grade_row(r: pd.Series):
            act = r.get("actual")
            if pd.isna(act):
                return None
            side = r.get("side")
            ss = str(side or "").upper().strip()
            if ss in {"YES", "NO"}:
                return _grade(ss, 0.0, float(act))
            ln = r.get("line_num")
            if pd.isna(ln):
                return None
            return _grade(ss, float(ln), float(act))

        merged["result"] = merged.apply(_grade_row, axis=1)
        merged["profit"] = merged.apply(lambda r: _profit_per_unit(r.get("result"), r.get("price")), axis=1)
        merged["date"] = d

        # Keep compact ledger
        keep = [
            "date",
            "player_id",
            "player_name",
            "team",
            "stat",
            "side",
            "line",
            "price",
            "bookmaker",
            "edge",
            "ev",
            "actual",
            "result",
            "profit",
            "home_team",
            "away_team",
            "commence_time",
        ]
        keep = [c for c in keep if c in merged.columns]
        chunk = merged[keep].copy().dropna(axis=1, how="all")
        if not chunk.empty:
            ledgers.append(chunk)

    if not ledgers:
        empty = pd.DataFrame()
        return empty, empty, empty

    ledger = pd.concat(ledgers, ignore_index=True)

    # Summary
    n_total = int(len(ledger))
    n_graded = int(ledger["result"].notna().sum()) if "result" in ledger.columns else 0
    n_w = int((ledger["result"] == "W").sum()) if "result" in ledger.columns else 0
    n_l = int((ledger["result"] == "L").sum()) if "result" in ledger.columns else 0
    n_p = int((ledger["result"] == "P").sum()) if "result" in ledger.columns else 0

    profit_sum = float(pd.to_numeric(ledger.get("profit"), errors="coerce").sum()) if "profit" in ledger.columns else float("nan")
    roi = (profit_sum / n_graded) if n_graded else float("nan")
    hit = (n_w / (n_w + n_l)) if (n_w + n_l) else float("nan")

    summary = pd.DataFrame(
        [
            {
                "start": start,
                "end": end,
                "sort_by": cfg.sort_by,
                "top_n_per_day": cfg.top_n_per_day,
                "top_n_per_game": cfg.top_n_per_game,
                "min_ev": cfg.min_ev,
                "min_edge": cfg.min_edge,
                "min_price": cfg.min_price,
                "max_price": cfg.max_price,
                "bookmaker": cfg.bookmaker,
                "exclude_bookmakers": (",".join(cfg.exclude_bookmakers) if cfg.exclude_bookmakers else None),
                "dedupe_best_book": cfg.dedupe_best_book,
                "include_dd_td": cfg.include_dd_td,
                "include_stats": (",".join(cfg.include_stats) if cfg.include_stats else None),
                "exclude_stats": (",".join(cfg.exclude_stats) if cfg.exclude_stats else None),
                "bets_total": n_total,
                "bets_graded": n_graded,
                "wins": n_w,
                "losses": n_l,
                "pushes": n_p,
                "hit_rate": hit,
                "profit": profit_sum,
                "roi_per_bet": roi,
            }
        ]
    )

    # Daily summary (for charts)
    d = ledger.copy()
    if "result" in d.columns:
        d["graded"] = d["result"].notna()
        d["win"] = d["result"] == "W"
        d["loss"] = d["result"] == "L"
        d["push"] = d["result"] == "P"
    else:
        d["graded"] = False
        d["win"] = False
        d["loss"] = False
        d["push"] = False
    d["profit"] = pd.to_numeric(d.get("profit"), errors="coerce")
    daily = (
        d.groupby(["date"], dropna=False)
        .agg(
            bets_total=("graded", "size"),
            bets_graded=("graded", "sum"),
            wins=("win", "sum"),
            losses=("loss", "sum"),
            pushes=("push", "sum"),
            profit=("profit", "sum"),
        )
        .reset_index()
    )
    daily["hit_rate"] = daily.apply(
        lambda r: (r.wins / (r.wins + r.losses)) if (r.wins + r.losses) else np.nan,
        axis=1,
    )
    daily["roi_per_bet"] = daily.apply(
        lambda r: (r.profit / r.bets_graded) if r.bets_graded else np.nan,
        axis=1,
    )
    daily = daily.sort_values(by=["date"], ascending=True)

    return ledger, summary, daily
