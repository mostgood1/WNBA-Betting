import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def daterange(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _load_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def _load_actuals() -> pd.DataFrame:
    """Load consolidated props actuals.

    Preferred: data/processed/props_actuals.parquet (written by `fetch-prop-actuals`).
    Fallback: any `recon_props_YYYY-MM-DD.csv` / `props_actuals_*.csv` files.
    """
    act_pq = PROCESSED / "props_actuals.parquet"
    frames: list[pd.DataFrame] = []
    if act_pq.exists():
        try:
            df = pd.read_parquet(act_pq)
            if df is not None and not df.empty:
                frames.append(df)
        except Exception:
            pass
    # Fallback daily CSVs
    for pat in ("recon_props_*.csv", "props_actuals_*.csv"):
        for p in sorted(PROCESSED.glob(pat)):
            try:
                df = pd.read_csv(p)
                if df is not None and not df.empty:
                    frames.append(df)
            except Exception:
                continue
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "player_id" in out.columns:
        out["player_id"] = out["player_id"].astype("Int64").astype(str)
    if "team_abbr" in out.columns:
        out["team_abbr"] = out["team_abbr"].astype(str).str.upper().str.strip()
    if "player_name" in out.columns:
        out["player_name"] = out["player_name"].astype(str).str.strip()

    # Derive common combo stats if not provided by the source.
    # Many books list these as distinct markets (PR/PA/RA) but our reconciliation
    # can be pts/reb/ast only.
    for c in ("pts", "reb", "ast"):
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

    # Deduplicate across sources (e.g., recon_props + props_actuals) to ensure one
    # row per (date, player_id). Prefer the most complete stats row.
    if "date" in out.columns and "player_id" in out.columns:
        stat_cols = [
            c
            for c in ("pts", "reb", "ast", "threes", "pr", "pa", "ra", "pra")
            if c in out.columns
        ]
        if stat_cols:
            out["__score"] = out[stat_cols].notna().sum(axis=1)
            out = out.sort_values("__score", ascending=False).drop_duplicates(
                subset=["date", "player_id"], keep="first"
            )
            out = out.drop(columns=["__score"], errors="ignore")
        else:
            out = out.drop_duplicates(subset=["date", "player_id"], keep="first")
    return out


def _normalize_stat(s: str) -> str:
    x = (s or "").strip().lower()
    x = x.replace("3pt", "threes").replace("3pm", "threes").replace("3ptm", "threes")
    x = x.replace("3s", "threes")
    # common aliases
    if x in ("3", "3m", "fg3m"):
        return "threes"
    if x in ("points", "point", "pts"):
        return "pts"
    if x in ("rebounds", "rebs", "reb"):
        return "reb"
    if x in ("assists", "asts", "ast"):
        return "ast"
    return x


def _american_profit(price: float, win: bool) -> float:
    """Return profit for 1u stake at American odds (price), excluding stake (i.e. -1 on loss)."""
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


def evaluate_edges(
    start: datetime,
    end: datetime,
    min_edge: float = 0.0,
    dedupe_best_ev: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluate `props_edges_*.csv` vs actuals.

    Returns (summary_df, bets_df)
    - summary_df: grouped metrics (overall + by stat)
    - bets_df: per-bet graded rows
    """
    actuals = _load_actuals()
    if actuals is None or actuals.empty:
        raise RuntimeError("No props actuals available. Run `python -m nba_betting.cli fetch-prop-actuals --date YYYY-MM-DD` first.")

    need_cols = {"date", "player_id", "player_name", "stat", "side", "line", "price", "model_prob", "implied_prob", "edge"}
    bets: list[pd.DataFrame] = []
    for d in daterange(start, end):
        ds = d.strftime("%Y-%m-%d")
        edges = _load_csv(PROCESSED / f"props_edges_{ds}.csv")
        if edges is None or edges.empty:
            continue
        if not need_cols.issubset(set(edges.columns)):
            continue
        e = edges.copy()
        e["date"] = pd.to_datetime(e["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        e["player_id"] = pd.to_numeric(e["player_id"], errors="coerce").astype("Int64").astype(str)
        e["stat_norm"] = e["stat"].astype(str).map(_normalize_stat)
        e["side"] = e["side"].astype(str).str.upper().str.strip()
        # Best-ev dedupe across books for the same bet definition
        if dedupe_best_ev:
            score_col = "ev" if "ev" in e.columns else "edge"
            e["__score"] = pd.to_numeric(e[score_col], errors="coerce")
            grp_cols = ["date", "player_id", "stat_norm", "side", "line"]
            e = e.sort_values("__score", ascending=False).drop_duplicates(subset=grp_cols, keep="first")

        e["edge"] = pd.to_numeric(e["edge"], errors="coerce")
        e = e[e["edge"].fillna(-1e9) >= float(min_edge)]
        if e.empty:
            continue

        # Join to actuals by (date, player_id)
        a = actuals[actuals["date"] == ds].copy()
        if a.empty:
            continue
        m = e.merge(a, on=["date", "player_id"], how="left", suffixes=("", "_act"))
        # Map normalized stat to actual column
        stat_to_col = {
            "pts": "pts",
            "reb": "reb",
            "ast": "ast",
            "threes": "threes",
            "pr": "pr",
            "pa": "pa",
            "ra": "ra",
            "pra": "pra",
        }
        m["actual"] = np.nan
        for st, col in stat_to_col.items():
            if col in m.columns:
                mask = m["stat_norm"] == st
                if mask.any():
                    m.loc[mask, "actual"] = pd.to_numeric(m.loc[mask, col], errors="coerce")
        m["line"] = pd.to_numeric(m["line"], errors="coerce")
        m["model_prob"] = pd.to_numeric(m["model_prob"], errors="coerce").clip(0, 1)
        m["implied_prob"] = pd.to_numeric(m["implied_prob"], errors="coerce").clip(0, 1)
        m["price"] = pd.to_numeric(m["price"], errors="coerce")

        # Determine win/loss (push excluded)
        over = m["side"] == "OVER"
        under = m["side"] == "UNDER"
        win = pd.Series(np.nan, index=m.index)
        win.loc[over] = (m.loc[over, "actual"] > m.loc[over, "line"]).astype(float)
        win.loc[under] = (m.loc[under, "actual"] < m.loc[under, "line"]).astype(float)
        # Push
        push = (m["actual"] == m["line"]) & (over | under)
        win.loc[push] = np.nan
        m["win"] = win
        m = m[(~m["actual"].isna()) & (~m["line"].isna()) & (~m["win"].isna())]
        if m.empty:
            continue
        m["profit"] = [
            _american_profit(price=p, win=bool(w))
            for p, w in zip(m["price"].tolist(), m["win"].tolist())
        ]
        bets.append(m)

    if not bets:
        return pd.DataFrame(), pd.DataFrame()

    df = pd.concat(bets, ignore_index=True)
    # Brier scores
    df["brier_model"] = (df["model_prob"] - df["win"]) ** 2
    df["brier_implied"] = (df["implied_prob"] - df["win"]) ** 2

    def _opt_blend(p_model: pd.Series, p_impl: pd.Series, y: pd.Series, step: float = 0.02) -> tuple[float, float]:
        """Find w in [0,1] minimizing Brier of p = w*p_model + (1-w)*p_impl."""
        pm = pd.to_numeric(p_model, errors="coerce").clip(0, 1)
        pi = pd.to_numeric(p_impl, errors="coerce").clip(0, 1)
        yy = pd.to_numeric(y, errors="coerce")
        m = (~pm.isna()) & (~pi.isna()) & (~yy.isna())
        if m.sum() == 0:
            return float("nan"), float("nan")
        pm = pm[m]; pi = pi[m]; yy = yy[m]
        best_w = 0.0
        best_b = float("inf")
        w = 0.0
        while w <= 1.0 + 1e-9:
            p = (w * pm + (1.0 - w) * pi)
            b = float(((p - yy) ** 2).mean())
            if b < best_b:
                best_b = b
                best_w = float(w)
            w += float(step)
        return best_w, best_b

    def _agg(g: pd.DataFrame) -> dict:
        w_opt, b_blend = _opt_blend(g["model_prob"], g["implied_prob"], g["win"], step=0.02)
        return {
            "n": int(len(g)),
            "hit_rate": float(pd.to_numeric(g["win"], errors="coerce").mean()),
            "roi": float(pd.to_numeric(g["profit"], errors="coerce").mean()),
            "avg_edge": float(pd.to_numeric(g["edge"], errors="coerce").mean()),
            "avg_ev": float(pd.to_numeric(g.get("ev"), errors="coerce").mean()) if "ev" in g.columns else float("nan"),
            "brier_model": float(pd.to_numeric(g["brier_model"], errors="coerce").mean()),
            "brier_implied": float(pd.to_numeric(g["brier_implied"], errors="coerce").mean()),
            "blend_w_opt": float(w_opt),
            "brier_blend_opt": float(b_blend),
        }

    summary_rows = []
    summary_rows.append({"group": "ALL", **_agg(df)})
    for stat, grp in df.groupby("stat_norm"):
        summary_rows.append({"group": f"stat={stat}", **_agg(grp)})
    summary = pd.DataFrame(summary_rows)

    # Clean up columns for bet-level output
    keep = [
        "date",
        "player_id",
        "player_name",
        "team",
        "team_abbr",
        "stat",
        "stat_norm",
        "side",
        "line",
        "price",
        "implied_prob",
        "model_prob",
        "edge",
        "ev",
        "actual",
        "win",
        "profit",
    ]
    keep = [c for c in keep if c in df.columns]
    bets_out = df[keep].copy()
    return summary, bets_out


def main():
    ap = argparse.ArgumentParser(description="Evaluate props betting edges vs actuals over a date range")
    ap.add_argument("--start", type=str, help="YYYY-MM-DD start")
    ap.add_argument("--end", type=str, help="YYYY-MM-DD end")
    ap.add_argument("--days", type=int, default=14, help="If start/end not provided, evaluate last N days (inclusive)")
    ap.add_argument("--min-edge", type=float, default=0.0, help="Only evaluate bets with edge >= this threshold")
    ap.add_argument("--no-dedupe", action="store_true", help="Do not dedupe across bookmakers (counts each book as a separate bet)")
    args = ap.parse_args()
    if args.start and args.end:
        start = datetime.strptime(args.start, "%Y-%m-%d"); end = datetime.strptime(args.end, "%Y-%m-%d")
    else:
        end = datetime.today() - timedelta(days=1)
        start = end - timedelta(days=max(0, int(args.days) - 1))

    summary, bets = evaluate_edges(
        start=start,
        end=end,
        min_edge=float(args.min_edge),
        dedupe_best_ev=(not args.no_dedupe),
    )
    ds0 = start.strftime("%Y-%m-%d")
    ds1 = end.strftime("%Y-%m-%d")
    edge_tag = f"edge{float(args.min_edge):.2f}".replace(".", "p")
    dedupe_tag = "dedupe" if (not args.no_dedupe) else "allbooks"
    out_sum = PROCESSED / f"props_eval_edges_{ds0}_{ds1}_{edge_tag}_{dedupe_tag}.csv"
    out_bets = PROCESSED / f"props_eval_bets_{ds0}_{ds1}_{edge_tag}_{dedupe_tag}.csv"
    if summary is not None and not summary.empty:
        summary.to_csv(out_sum, index=False)
    if bets is not None and not bets.empty:
        bets.to_csv(out_bets, index=False)
    print({
        "start": ds0,
        "end": ds1,
        "min_edge": float(args.min_edge),
        "dedupe": bool(not args.no_dedupe),
        "summary_rows": 0 if summary is None else int(len(summary)),
        "bets": 0 if bets is None else int(len(bets)),
        "summary_out": str(out_sum),
        "bets_out": str(out_bets),
    })
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
