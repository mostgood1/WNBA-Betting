#!/usr/bin/env python3
"""Live Lens accuracy report: projections vs reconciled actuals.

Goal
- For a date range (typically Fri/Sat/Sun), evaluate logged Live Lens *projections*
  against reconciled prop actuals, by day and all-up.

Reads
- data/processed/live_lens_projections_<date>.jsonl
- data/processed/recon_props_<date>.csv

Writes
- data/processed/reports/live_lens_accuracy_<start>_<end>.md
- data/processed/reports/live_lens_accuracy_scored_<start>_<end>.csv

Notes
- If projection logs are missing for some or all days, the report still writes and
  calls out the missing artifacts.
- Currently the UI logs projections primarily for player props; this report is
  therefore focused on market == 'player_prop'.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
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
    if "game_id" in out.columns:
        out["_gid"] = out["game_id"].map(_canon_nba_game_id)
    else:
        out["_gid"] = ""
    if "player_name" in out.columns:
        out["_name_key"] = out["player_name"].astype(str).map(_norm_player_name)
    else:
        out["_name_key"] = ""
    return out


def _metrics_err(err: pd.Series) -> dict[str, float]:
    if err is None or len(err) == 0:
        return {"n": 0}
    e = pd.to_numeric(err, errors="coerce")
    e = e[e.notna()]
    if e.empty:
        return {"n": 0}
    mae = float(e.abs().mean())
    rmse = float(math.sqrt(float((e**2).mean())))
    bias = float(e.mean())
    return {"n": int(len(e)), "mae": mae, "rmse": rmse, "bias": bias}


def _brier(p: pd.Series, y: pd.Series) -> dict[str, float]:
    if p is None or y is None:
        return {"n": 0}
    pp = pd.to_numeric(p, errors="coerce")
    yy = pd.to_numeric(y, errors="coerce")
    d = pd.DataFrame({"p": pp, "y": yy})
    d = d.dropna()
    if d.empty:
        return {"n": 0}
    # Clamp to [0,1] to avoid garbage logs.
    d["p"] = d["p"].clip(lower=0.0, upper=1.0)
    score = float(((d["p"] - d["y"]) ** 2).mean())
    return {"n": int(len(d)), "brier": score}


def _hit_rate(pred_over: pd.Series, y: pd.Series) -> dict[str, float]:
    if pred_over is None or y is None:
        return {"n": 0}
    p = pd.to_numeric(pred_over, errors="coerce")
    yy = pd.to_numeric(y, errors="coerce")
    d = pd.DataFrame({"p": p, "y": yy}).dropna()
    if d.empty:
        return {"n": 0}
    hits = int((d["p"] == d["y"]).sum())
    n = int(len(d))
    return {"n": n, "hits": hits, "hit_rate": float(hits) / float(n) if n > 0 else float("nan")}


@dataclass(frozen=True)
class DayArtifacts:
    ds: str
    projections_path: Path
    recon_props_path: Path


def _artifacts_for_day(ds: str) -> DayArtifacts:
    return DayArtifacts(
        ds=ds,
        projections_path=PROCESSED / f"live_lens_projections_{ds}.jsonl",
        recon_props_path=PROCESSED / f"recon_props_{ds}.csv",
    )


def _score_day(ds: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    art = _artifacts_for_day(ds)
    proj_rows = _load_jsonl(art.projections_path)
    rp = _prep_recon_props(_load_csv(art.recon_props_path))

    info: dict[str, Any] = {
        "date": ds,
        "has_projections": int(art.projections_path.exists()),
        "has_recon_props": int(art.recon_props_path.exists()),
        "n_proj_rows": int(len(proj_rows)),
        "n_recon_rows": int(len(rp)) if not rp.empty else 0,
    }

    if not proj_rows:
        return pd.DataFrame(), info

    prop_index: dict[tuple[str, str], dict[str, Any]] = {}
    if not rp.empty:
        for _, r in rp.iterrows():
            gid = str(r.get("_gid") or "")
            nk = str(r.get("_name_key") or "")
            if gid and nk and (gid, nk) not in prop_index:
                prop_index[(gid, nk)] = dict(r)

    scored: list[dict[str, Any]] = []

    for obj in proj_rows:
        market = str(obj.get("market") or "").strip()
        if market != "player_prop":
            continue

        gid = _canon_nba_game_id(obj.get("game_id"))
        if not gid:
            continue

        raw_name_key = str(obj.get("name_key") or "").strip()
        player = str(obj.get("player") or "").strip() or None
        name_key = _norm_player_name(raw_name_key) if raw_name_key else (_norm_player_name(player) if player else "")
        if not name_key:
            continue

        stat = str(obj.get("stat") or "").strip()
        stat_key = _live_stat_key(stat)

        proj = _n(obj.get("proj"))
        if proj is None:
            # Some earlier schemas might use sim_mu as the main projection.
            proj = _n(obj.get("sim_mu"))

        line = _n(obj.get("line"))

        # Actual join
        r = prop_index.get((gid, name_key))
        act = None
        if r is not None:
            act = _n(r.get(stat_key))

        missing_reason = ""
        if act is None:
            if not info["has_recon_props"]:
                missing_reason = "missing_recon_props"
            elif r is None:
                missing_reason = "player_join_failed"
            else:
                missing_reason = "stat_missing"

        err = (proj - act) if (proj is not None and act is not None) else None

        # Outcome vs line (for optional probability/hit-rate diagnostics)
        outcome_over = None
        if act is not None and line is not None:
            if act > line:
                outcome_over = 1
            elif act < line:
                outcome_over = 0

        win_prob_over = _n(obj.get("win_prob_over"))
        win_prob_under = _n(obj.get("win_prob_under"))
        if win_prob_over is None and win_prob_under is not None:
            win_prob_over = 1.0 - win_prob_under

        implied_prob_over = _n(obj.get("implied_prob_over"))
        implied_prob_under = _n(obj.get("implied_prob_under"))
        if implied_prob_over is None and implied_prob_under is not None:
            implied_prob_over = 1.0 - implied_prob_under

        pred_over_from_proj = None
        if proj is not None and line is not None:
            pred_over_from_proj = 1 if proj > line else (0 if proj < line else None)

        scored.append(
            {
                "date": str(obj.get("date") or ds),
                "received_at": str(obj.get("received_at") or "") or None,
                "market": market,
                "game_id": gid,
                "player": player,
                "name_key": name_key,
                "stat": stat,
                "stat_key": stat_key,
                "line": line,
                "proj": proj,
                "act": act,
                "err": err,
                "abs_err": (abs(float(err)) if err is not None else None),
                "outcome_over": outcome_over,
                "pred_over_from_proj": pred_over_from_proj,
                "win_prob_over": win_prob_over,
                "implied_prob_over": implied_prob_over,
                "strength": _n(obj.get("strength")),
                "missing_reason": missing_reason,
            }
        )

    if not scored:
        return pd.DataFrame(), info

    df = pd.DataFrame(scored)
    info["n_prop_logged"] = int(len(df))
    info["n_scored"] = int(df.dropna(subset=["proj", "act"]).shape[0])
    return df, info


def _fmt_metrics(m: dict[str, float]) -> str:
    if not m or int(m.get("n", 0)) <= 0:
        return "n=0"
    return f"n={int(m['n'])}  mae={m.get('mae', float('nan')):.3f}  rmse={m.get('rmse', float('nan')):.3f}  bias={m.get('bias', float('nan')):.3f}"


def _fmt_brier(m: dict[str, float]) -> str:
    if not m or int(m.get("n", 0)) <= 0:
        return "n=0"
    return f"n={int(m['n'])}  brier={m.get('brier', float('nan')):.4f}"


def _fmt_hit(m: dict[str, float]) -> str:
    if not m or int(m.get("n", 0)) <= 0:
        return "n=0"
    return f"n={int(m['n'])}  hits={int(m.get('hits', 0))}  hit={m.get('hit_rate', float('nan')):.3f}"


def _write_markdown(start: str, end: str, day_infos: list[dict[str, Any]], scored_all: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append(f"# Live Lens Accuracy — {start}..{end}")
    lines.append("")

    lines.append("## Coverage")
    for info in day_infos:
        ds = str(info.get("date"))
        n_proj = int(info.get("n_proj_rows", 0))
        n_prop = int(info.get("n_prop_logged", 0))
        n_scored = int(info.get("n_scored", 0))
        hp = int(info.get("has_projections", 0))
        hrp = int(info.get("has_recon_props", 0))
        lines.append(f"- {ds}: projections={hp} recon_props={hrp}  rows={n_proj}  prop_logged={n_prop}  scored={n_scored}")
    lines.append("")

    if scored_all is None or scored_all.empty:
        lines.append("## Summary")
        lines.append("No scored rows (missing projection logs and/or missing recon actuals).")
        out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    # Per-day
    lines.append("## By day")
    for info in day_infos:
        ds = str(info.get("date"))
        d = scored_all[scored_all["date"].astype(str) == ds].copy()
        d_scored = d.dropna(subset=["proj", "act"]).copy()
        m = _metrics_err(d_scored.get("err"))

        b_model = _brier(d_scored.get("win_prob_over"), d_scored.get("outcome_over"))
        b_impl = _brier(d_scored.get("implied_prob_over"), d_scored.get("outcome_over"))
        h_dir = _hit_rate(d_scored.get("pred_over_from_proj"), d_scored.get("outcome_over"))

        lines.append(f"- {ds}: proj_vs_act=({_fmt_metrics(m)})  dir_vs_line=({_fmt_hit(h_dir)})  brier_model=({_fmt_brier(b_model)})  brier_implied=({_fmt_brier(b_impl)})")
    lines.append("")

    # All-up
    lines.append("## All up")
    scored = scored_all.dropna(subset=["proj", "act"]).copy()
    m_all = _metrics_err(scored.get("err"))
    b_model_all = _brier(scored.get("win_prob_over"), scored.get("outcome_over"))
    b_impl_all = _brier(scored.get("implied_prob_over"), scored.get("outcome_over"))
    h_dir_all = _hit_rate(scored.get("pred_over_from_proj"), scored.get("outcome_over"))
    lines.append(f"- proj_vs_act: {_fmt_metrics(m_all)}")
    lines.append(f"- dir_vs_line: {_fmt_hit(h_dir_all)}")
    lines.append(f"- brier_model: {_fmt_brier(b_model_all)}")
    lines.append(f"- brier_implied: {_fmt_brier(b_impl_all)}")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _compute_last_weekend(asof: _date) -> tuple[_date, _date]:
    # Find most recent Sunday <= asof.
    days_since_sun = (asof.weekday() + 1) % 7
    end = asof - timedelta(days=days_since_sun)
    start = end - timedelta(days=2)
    return start, end


def main() -> int:
    p = argparse.ArgumentParser(description="Live Lens accuracy report (projections vs recon actuals)")
    p.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD")
    p.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD")
    p.add_argument("--weekend", action="store_true", help="Use last Fri/Sat/Sun ending on most recent Sunday")
    p.add_argument("--asof", type=str, default=None, help="As-of date for --weekend (YYYY-MM-DD); default today")
    args = p.parse_args()

    if args.weekend or (args.start is None and args.end is None):
        asof = _parse_date(args.asof) if args.asof else datetime.now().date()
        start_d, end_d = _compute_last_weekend(asof)
    else:
        if not args.start or not args.end:
            raise SystemExit("Provide --start and --end (or use --weekend)")
        start_d, end_d = _parse_date(args.start), _parse_date(args.end)

    if end_d < start_d:
        start_d, end_d = end_d, start_d

    start_s, end_s = _iso(start_d), _iso(end_d)

    day_infos: list[dict[str, Any]] = []
    scored_days: list[pd.DataFrame] = []

    d = start_d
    while d <= end_d:
        ds = _iso(d)
        df_day, info = _score_day(ds)
        day_infos.append(info)
        if df_day is not None and not df_day.empty:
            scored_days.append(df_day)
        d += timedelta(days=1)

    scored_all = pd.concat(scored_days, ignore_index=True) if scored_days else pd.DataFrame()

    md_path = REPORTS / f"live_lens_accuracy_{start_s}_{end_s}.md"
    csv_path = REPORTS / f"live_lens_accuracy_scored_{start_s}_{end_s}.csv"

    _write_markdown(start_s, end_s, day_infos, scored_all, md_path)

    if scored_all is not None and not scored_all.empty:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        scored_all.to_csv(csv_path, index=False)

    print(f"Wrote {md_path}")
    if scored_all is not None and not scored_all.empty:
        print(f"Wrote {csv_path}")
    else:
        print("No scored rows; CSV not written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
