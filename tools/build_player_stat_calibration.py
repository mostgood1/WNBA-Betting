"""Build per-player stat calibration (bias corrections) from recon_players CSVs.

Output: data/processed/player_stat_calibration.json

This file is consumed by SmartSim (optional). Biases are applied as:
  sim_mean_corrected = sim_mean + bias

Where bias is (actual - sim) learned over a window, with shrinkage toward
a global bias to reduce noise for low-sample players.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
PROC_DIR = BASE_DIR / "data" / "processed"


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if np.isnan(v):
            return None
        return float(v)
    except Exception:
        return None


def _safe_int_str(x: Any) -> str:
    try:
        s = str(x or "").strip()
        if not s:
            return ""
        if s.endswith(".0") and s[:-2].isdigit():
            s = s[:-2]
        return s
    except Exception:
        return ""


def _load_recon_players_range(start: str, end: str) -> pd.DataFrame:
    fps = []
    for fp in sorted(PROC_DIR.glob("recon_players_*.csv")):
        name = fp.name
        if not name.startswith("recon_players_"):
            continue
        ds = name.replace("recon_players_", "").replace(".csv", "")
        if start <= ds <= end:
            fps.append(fp)

    if not fps:
        return pd.DataFrame()

    dfs = []
    for fp in fps:
        try:
            dfs.append(pd.read_csv(fp))
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, axis=0, ignore_index=True)


def build_player_stat_calibration(
    start: str,
    end: str,
    min_games: int = 1,
    min_total_minutes: float = 10.0,
    shrink_k_minutes: float = 240.0,
) -> dict[str, Any]:
    df = _load_recon_players_range(start, end)
    if df.empty:
        return {"start": start, "end": end, "global": {}, "players": {}}

    # Ensure numeric columns
    for col in [
        "player_id",
        "actual_min",
        "sim_pts",
        "sim_reb",
        "sim_ast",
        "sim_3pm",
        "sim_pra",
        "sim_stl",
        "sim_blk",
        "sim_tov",
        "actual_pts",
        "actual_reb",
        "actual_ast",
        "actual_3pm",
        "actual_pra",
        "actual_stl",
        "actual_blk",
        "actual_tov",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filter to rows with actuals
    if "missing_actual" in df.columns:
        miss = df["missing_actual"].astype(str).str.lower().str.strip().isin(["true", "1", "yes", "y"])
        df = df[~miss].copy()

    if df.empty:
        return {"start": start, "end": end, "global": {}, "players": {}}

    # Weight by actual minutes when available, else 1
    w = df["actual_min"].fillna(1.0).clip(lower=0.0)
    df["_w"] = w

    stats = [
        ("pts", "sim_pts", "actual_pts"),
        ("reb", "sim_reb", "actual_reb"),
        ("ast", "sim_ast", "actual_ast"),
        ("threes", "sim_3pm", "actual_3pm"),
        ("pra", "sim_pra", "actual_pra"),
        ("stl", "sim_stl", "actual_stl"),
        ("blk", "sim_blk", "actual_blk"),
        ("tov", "sim_tov", "actual_tov"),
    ]

    # Global bias: weighted average of (actual - sim)
    global_bias: dict[str, float] = {}
    for key, sim_col, act_col in stats:
        if sim_col not in df.columns or act_col not in df.columns:
            continue
        delta = (df[act_col] - df[sim_col]).astype(float)
        ok = delta.notna() & df["_w"].notna()
        if not ok.any():
            continue
        global_bias[key] = float(np.average(delta[ok], weights=df.loc[ok, "_w"]))

    # Per-player bias with shrinkage toward global
    df["player_id"] = df["player_id"].apply(_safe_int_str)
    df = df[df["player_id"].astype(str).str.len() > 0].copy()

    # Games proxy: unique (date, game_id) per player
    if "date" in df.columns and "game_id" in df.columns:
        df["_game_key"] = df["date"].astype(str) + "_" + df["game_id"].astype(str)
    else:
        df["_game_key"] = df.index.astype(str)

    players_out: dict[str, dict[str, float]] = {}
    meta_out: dict[str, dict[str, float]] = {}

    for pid, g in df.groupby("player_id"):
        try:
            n_games = int(g["_game_key"].nunique())
        except Exception:
            n_games = int(len(g))
        total_min = float(g["actual_min"].fillna(0.0).sum()) if "actual_min" in g.columns else float(g["_w"].sum())

        if n_games < int(min_games) or total_min < float(min_total_minutes):
            continue

        out: dict[str, float] = {}
        for key, sim_col, act_col in stats:
            if sim_col not in g.columns or act_col not in g.columns:
                continue
            delta = (g[act_col] - g[sim_col]).astype(float)
            ww = g["_w"].astype(float)
            ok = delta.notna() & ww.notna()
            if not ok.any():
                continue

            # Shrunk bias: (sum(w*delta) + k*global) / (sum(w) + k)
            sum_w = float(ww[ok].sum())
            if sum_w <= 0:
                continue
            gb = float(global_bias.get(key, 0.0))
            k = float(shrink_k_minutes)
            b = (float((delta[ok] * ww[ok]).sum()) + k * gb) / (sum_w + k)
            out[key] = float(b)

        if out:
            players_out[str(pid)] = out
            meta_out[str(pid)] = {"games": float(n_games), "minutes": float(total_min)}

    return {
        "start": start,
        "end": end,
        "global": global_bias,
        "players": players_out,
        "meta": meta_out,
        "notes": {
            "bias_definition": "actual_minus_sim",
            "application": "sim_corrected = sim + bias",
            "shrink_k_minutes": float(shrink_k_minutes),
            "min_games": int(min_games),
            "min_total_minutes": float(min_total_minutes),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--min-games", type=int, default=1)
    ap.add_argument("--min-total-minutes", type=float, default=10.0)
    ap.add_argument("--shrink-k-minutes", type=float, default=240.0)
    ap.add_argument(
        "--out",
        default=str(PROC_DIR / "player_stat_calibration.json"),
        help="Output JSON path (default: data/processed/player_stat_calibration.json)",
    )
    args = ap.parse_args()

    obj = build_player_stat_calibration(
        start=str(args.start).strip(),
        end=str(args.end).strip(),
        min_games=int(args.min_games),
        min_total_minutes=float(args.min_total_minutes),
        shrink_k_minutes=float(args.shrink_k_minutes),
    )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")
    print(f"wrote {out}")
    print(f"players={len(obj.get('players') or {})}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
