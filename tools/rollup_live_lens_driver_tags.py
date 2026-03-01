#!/usr/bin/env python3
"""Roll up Live Lens driver-tag performance across multiple daily audit CSVs.

This consumes the scored CSVs produced by tools/daily_live_lens_audit.py and
aggregates tag hit rates on a *decision-level* view:

- For each decision key, use the first BET signal if any exists,
  otherwise use the first observed signal.

Decision keys match the audit tool's intent:
- player_prop: (market, game, player, stat, side)
- totals/quarters/halves/ats: (market, horizon, game, side)

Outputs a single markdown report.
"""

from __future__ import annotations

import argparse
import ast
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


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


def _parse_tags_cell(x: Any) -> list[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(t).strip() for t in x if str(t).strip()]

    s = str(x).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return []

    # CSV stores python-ish lists like "['a', 'b']".
    if s.startswith("[") and s.endswith("]"):
        try:
            v = ast.literal_eval(s)
            if isinstance(v, list):
                out = [str(t).strip() for t in v if str(t).strip()]
                # De-dupe, preserve order.
                seen: set[str] = set()
                uniq: list[str] = []
                for t in out:
                    if t not in seen:
                        seen.add(t)
                        uniq.append(t)
                return uniq
        except Exception:
            pass

    # Fallback: accept pipe/comma/semicolon.
    parts = [p.strip() for p in re.split(r"[|,;]", s) if p.strip()]
    seen2: set[str] = set()
    uniq2: list[str] = []
    for t in parts:
        if t not in seen2:
            seen2.add(t)
            uniq2.append(t)
    return uniq2


def _gid2(df: pd.DataFrame) -> pd.Series:
    gid = df.get("game_id", "").astype(str).fillna("").str.strip()
    home = df.get("home", "").astype(str).fillna("").str.strip().str.upper()
    away = df.get("away", "").astype(str).fillna("").str.strip().str.upper()
    return gid.where(gid.str.len() > 0, other=(home + "@" + away))


def _decision_key(df: pd.DataFrame) -> pd.Series:
    market = df.get("market", "").astype(str).fillna("").str.strip().str.lower()
    horizon = df.get("horizon", "").astype(str).fillna("").str.strip().str.lower()
    side = df.get("side", "").astype(str).fillna("").str.strip().str.lower()
    name_key = df.get("name_key", "").astype(str).fillna("").str.strip().str.lower()
    stat_key = df.get("stat_key", "").astype(str).fillna("").str.strip().str.lower()
    day = df.get("date", "").astype(str).fillna("").str.strip()

    gid2 = _gid2(df)

    key_player = day + "|" + market + "|" + gid2 + "|" + name_key + "|" + stat_key + "|" + side
    key_other = day + "|" + market + "|" + horizon + "|" + gid2 + "|" + side
    return key_other.where(market != "player_prop", other=key_player)


def _decision_view_first_bet_else_first(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    d = df.copy()
    d["_rownum"] = range(len(d))
    d["_elapsed"] = pd.to_numeric(d.get("elapsed"), errors="coerce")
    d["_decision_key"] = _decision_key(d)
    d["_klass"] = d.get("klass", "").astype(str).fillna("").str.strip().str.upper()

    d = d.sort_values(["_elapsed", "_rownum"], ascending=[True, True], na_position="last")

    first_any = d.drop_duplicates(subset=["_decision_key"], keep="first")
    first_bet = d[d["_klass"] == "BET"].drop_duplicates(subset=["_decision_key"], keep="first")

    out = first_any.set_index("_decision_key")
    if not first_bet.empty:
        fb = first_bet.set_index("_decision_key")
        out.loc[fb.index, :] = fb.loc[fb.index, :].values

    out = out.reset_index(drop=True)
    return out.drop(columns=["_rownum", "_elapsed", "_decision_key", "_klass"], errors="ignore")


def _decision_view_first_actionable_else_first(df: pd.DataFrame) -> pd.DataFrame:
    """Decision-level view: first BET/WATCH else first signal.

    This is useful for analyzing WATCH drivers that may later become BET.
    """
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    d = df.copy()
    d["_rownum"] = range(len(d))
    d["_elapsed"] = pd.to_numeric(d.get("elapsed"), errors="coerce")
    d["_decision_key"] = _decision_key(d)
    d["_klass"] = d.get("klass", "").astype(str).fillna("").str.strip().str.upper()

    d = d.sort_values(["_elapsed", "_rownum"], ascending=[True, True], na_position="last")

    first_any = d.drop_duplicates(subset=["_decision_key"], keep="first")
    first_actionable = d[d["_klass"].isin(["BET", "WATCH"])].drop_duplicates(subset=["_decision_key"], keep="first")

    out = first_any.set_index("_decision_key")
    if not first_actionable.empty:
        fa = first_actionable.set_index("_decision_key")
        out.loc[fa.index, :] = fa.loc[fa.index, :].values

    out = out.reset_index(drop=True)
    return out.drop(columns=["_rownum", "_elapsed", "_decision_key", "_klass"], errors="ignore")


def _decision_view_first_signal(df: pd.DataFrame) -> pd.DataFrame:
    """Decision-level view: the earliest observed signal per decision key."""
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    d = df.copy()
    d["_rownum"] = range(len(d))
    d["_elapsed"] = pd.to_numeric(d.get("elapsed"), errors="coerce")
    d["_decision_key"] = _decision_key(d)
    d = d.sort_values(["_elapsed", "_rownum"], ascending=[True, True], na_position="last")
    out = d.drop_duplicates(subset=["_decision_key"], keep="first").reset_index(drop=True)
    return out.drop(columns=["_rownum", "_elapsed", "_decision_key"], errors="ignore")


def _hit_summary(df: pd.DataFrame) -> dict[str, float]:
    if df is None or df.empty:
        return {"n": 0}
    d = df[df.get("result").notna()].copy()
    if d.empty:
        return {"n": int(len(df))}

    r = d["result"].astype(str)
    wins = int((r == "win").sum())
    losses = int((r == "loss").sum())
    pushes = int((r == "push").sum())
    denom = wins + losses
    hit = float(wins) / float(denom) if denom else float("nan")
    return {"n": int(len(df)), "wins": wins, "losses": losses, "pushes": pushes, "hit": hit, "denom": denom}


def _rollup_tags(df: pd.DataFrame, *, min_denom: int, include_base_tags: bool) -> pd.DataFrame:
    if df is None or df.empty or "tags" not in df.columns:
        return pd.DataFrame(columns=["tag", "wins", "losses", "pushes", "denom", "hit"])

    # Tags that are mostly structural / instrumentation rather than "drivers".
    base_prefixes = ("market:", "horizon:", "klass:", "ctx:")

    d = df.copy()
    d["_tags"] = d["tags"].apply(_parse_tags_cell)
    d = d.explode("_tags")
    d = d[d["_tags"].notna() & (d["_tags"].astype(str).str.len() > 0)].copy()
    d["tag"] = d["_tags"].astype(str)
    if not include_base_tags:
        d = d[~d["tag"].str.lower().str.startswith(base_prefixes)].copy()

    r = d.get("result").astype(str)
    d["_win"] = (r == "win").astype(int)
    d["_loss"] = (r == "loss").astype(int)
    d["_push"] = (r == "push").astype(int)

    g = (
        d.groupby("tag", dropna=False)
        .agg(wins=("_win", "sum"), losses=("_loss", "sum"), pushes=("_push", "sum"))
        .reset_index()
    )
    g["denom"] = g["wins"] + g["losses"]
    g["hit"] = g.apply(lambda r0: (float(r0["wins"]) / float(r0["denom"])) if r0["denom"] else float("nan"), axis=1)

    g = g[g["denom"] >= int(min_denom)].copy()
    g = g.sort_values(["denom", "hit"], ascending=[False, False])
    return g[["tag", "wins", "losses", "pushes", "denom", "hit"]]


def _klass_norm(x: Any) -> str:
    return str(x or "").strip().upper()


def _write_tag_table(out_md: list[str], tags: pd.DataFrame, *, limit: int = 50) -> None:
    out_md.append("| tag | W | L | P | denom | hit |")
    out_md.append("|---|---:|---:|---:|---:|---:|")
    for _, r0 in tags.head(int(limit)).iterrows():
        out_md.append(
            f"| {r0['tag']} | {int(r0['wins'])} | {int(r0['losses'])} | {int(r0['pushes'])} | {int(r0['denom'])} | {float(r0['hit']):.3f} |"
        )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", default=str(ROOT / "data" / "processed" / "reports_v3_multi"))
    ap.add_argument("--glob", default="live_lens_scored_*.csv")
    ap.add_argument("--out", default=None)
    ap.add_argument("--min-denom", type=int, default=50)
    ap.add_argument("--include-base-tags", action="store_true")
    ap.add_argument("--by-klass", action="store_true", help="Also split tag tables by klass (WATCH vs BET)")
    ap.add_argument(
        "--markets",
        default="player_prop,total,half_total,quarter_total,ats",
        help="Comma-separated market filter (blank = all)",
    )
    ap.add_argument(
        "--decision-policy",
        default="first_bet_else_first",
        choices=["first_bet_else_first", "first_actionable_else_first", "first_signal"],
        help="How to choose one row per decision key before scoring tag performance",
    )

    args = ap.parse_args()
    in_dir = Path(args.in_dir)
    files = sorted(in_dir.glob(args.glob))
    if not files:
        raise SystemExit(f"No files matched: {in_dir / args.glob}")

    df_list: list[pd.DataFrame] = []
    for p in files:
        try:
            d = pd.read_csv(p)
            d["_src"] = p.name
            df_list.append(d)
        except Exception:
            continue

    if not df_list:
        raise SystemExit("No CSVs could be read")

    raw = pd.concat(df_list, ignore_index=True)

    markets_filter = [m.strip().lower() for m in str(args.markets or "").split(",") if m.strip()]
    if markets_filter:
        raw = raw[raw.get("market").astype(str).str.strip().str.lower().isin(markets_filter)].copy()

    # Decision-level view.
    policy = str(args.decision_policy or "first_bet_else_first").strip().lower()
    if policy == "first_signal":
        dec = _decision_view_first_signal(raw)
    elif policy == "first_actionable_else_first":
        dec = _decision_view_first_actionable_else_first(raw)
    else:
        dec = _decision_view_first_bet_else_first(raw)

    # Basic summary.
    dates = sorted(set(dec.get("date").astype(str).fillna("").tolist()))
    dates = [d for d in dates if d]

    out_md = []
    out_md.append("# Live Lens driver-tag rollup")
    out_md.append("")
    out_md.append(f"Input: `{in_dir.as_posix()}/{args.glob}`")
    out_md.append(f"Files: {len(files)}")
    out_md.append(f"Decision policy: `{policy}`")
    if dates:
        out_md.append(f"Dates: {dates[0]} .. {dates[-1]} ({len(dates)} unique)")
    out_md.append("")

    overall = _hit_summary(dec)
    out_md.append("## Overall (decision-level)")
    out_md.append(f"- n={overall.get('n')} W={overall.get('wins', 0)} L={overall.get('losses', 0)} P={overall.get('pushes', 0)} hit={overall.get('hit', float('nan')):.3f} denom={overall.get('denom', 0)}")
    out_md.append("")

    # By market.
    if "market" in dec.columns:
        out_md.append("## By market")
        for m, g in dec.groupby(dec["market"].astype(str).str.strip().str.lower()):
            s = _hit_summary(g)
            out_md.append(f"- {m}: n={s.get('n')} W={s.get('wins', 0)} L={s.get('losses', 0)} P={s.get('pushes', 0)} hit={s.get('hit', float('nan')):.3f} denom={s.get('denom', 0)}")
        out_md.append("")

    # Tag rollups by market.
    out_md.append(f"## Tag performance (decision-level; min_denom={int(args.min_denom)})")
    out_md.append("(Excludes base tags like `market:*`, `horizon:*`, `klass:*` unless `--include-base-tags` is set.)")
    out_md.append("")

    for m, g in dec.groupby(dec["market"].astype(str).str.strip().str.lower()):
        tags = _rollup_tags(g, min_denom=int(args.min_denom), include_base_tags=bool(args.include_base_tags))
        if tags.empty:
            continue
        out_md.append(f"### {m}")
        out_md.append("")
        _write_tag_table(out_md, tags, limit=50)
        out_md.append("")

        if bool(args.by_klass) and "klass" in g.columns:
            out_md.append(f"#### {m}: by klass")
            out_md.append("")
            g2 = g.copy()
            g2["_klass"] = g2["klass"].apply(_klass_norm)
            for k in ["BET", "WATCH", "NONE"]:
                sub = g2[g2["_klass"] == k].copy()
                if sub.empty:
                    continue
                s = _hit_summary(sub)
                out_md.append(f"##### klass={k} (n={s.get('n')}, denom={s.get('denom', 0)}, hit={s.get('hit', float('nan')):.3f})")
                out_md.append("")
                t = _rollup_tags(sub, min_denom=int(args.min_denom), include_base_tags=bool(args.include_base_tags))
                if t.empty:
                    out_md.append("(no tags meeting min_denom)")
                    out_md.append("")
                    continue
                _write_tag_table(out_md, t, limit=30)
                out_md.append("")

    out_path = Path(args.out) if args.out else (in_dir / "live_lens_driver_tag_rollup.md")
    out_path.write_text("\n".join(out_md) + "\n", encoding="utf-8")
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
