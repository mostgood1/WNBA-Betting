"""Build daily tuning dataset for Player Live Lens (props).

Output: data/processed/live_player_lens_tuning_<YYYY-MM-DD>.csv

Joins (date-scoped):
- Props lines:        data/processed/props_edges_<date>.csv (median line per player/stat)
- Props sim means:    data/processed/props_predictions_<date>.csv (mean_* and roll10_min)
- Actual boxscores:   data/processed/boxscores/boxscore_<gameId>.csv

Goal:
Provide a compact daily artifact to tune Live Lens thresholds and name/team matching.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
_DATA_ROOT = os.environ.get("NBA_BETTING_DATA_ROOT")
DATA_ROOT = Path(_DATA_ROOT).expanduser() if _DATA_ROOT else (BASE_DIR / "data")
PROC_DIR = DATA_ROOT / "processed"
BOXSCORES_DIR = PROC_DIR / "boxscores"


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


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and not x.strip():
            return None
        v = float(x)
        if pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and not x.strip():
            return None
        return int(float(x))
    except Exception:
        return None


def _parse_minutes_to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    s = str(v).strip()
    if not s:
        return None
    if ":" not in s:
        return _safe_float(s)
    try:
        mm, ss = s.split(":", 1)
        m = int(mm)
        sec = int(ss)
        return float(m) + float(sec) / 60.0
    except Exception:
        return None


def _norm_player_name(s: Any) -> str:
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


STAT_TO_PRED_COL = {
    "pts": "mean_pts",
    "reb": "mean_reb",
    "ast": "mean_ast",
    "threes": "mean_threes",
    "pra": "mean_pra",
    # optional composites if present in props_predictions
    "pr": "mean_pr",
    "ra": "mean_ra",
    "pa": "mean_pa",
}


def _load_edges_median(date_str: str) -> pd.DataFrame:
    fp = PROC_DIR / f"props_edges_{date_str}.csv"
    if not fp.exists():
        return pd.DataFrame(columns=["team_tri", "name_key", "stat", "line", "line_n_books"])

    try:
        df = pd.read_csv(fp)
    except Exception:
        return pd.DataFrame(columns=["team_tri", "name_key", "stat", "line", "line_n_books"])

    if df.empty:
        return pd.DataFrame(columns=["team_tri", "name_key", "stat", "line", "line_n_books"])

    # Required columns (best-effort)
    if "team" not in df.columns or "player_name" not in df.columns or "stat" not in df.columns:
        return pd.DataFrame(columns=["team_tri", "name_key", "stat", "line", "line_n_books"])

    df = df.copy()
    df["team_tri"] = df["team"].astype(str).str.upper().str.strip()
    df["name_key"] = df["player_name"].apply(_norm_player_name)
    df["stat"] = df["stat"].astype(str).str.lower().str.strip()
    df["line"] = pd.to_numeric(df["line"], errors="coerce") if "line" in df.columns else pd.NA

    df = df[df["team_tri"].astype(str).str.len() == 3]
    df = df[df["name_key"].astype(str).str.len() > 0]
    df = df[df["stat"].astype(str).str.len() > 0]
    df = df[df["line"].notna()].copy()

    if df.empty:
        return pd.DataFrame(columns=["team_tri", "name_key", "stat", "line", "line_n_books"])

    agg = (
        df.groupby(["team_tri", "name_key", "stat"], as_index=False)
        .agg(line=("line", "median"), line_n_books=("line", "count"))
    )
    return agg


def _expected_min_from_preds_row(r: dict[str, Any]) -> Optional[float]:
    for key in [
        "roll10_min",
        "roll5_min",
        "min_mean",
        "mean_min",
        "minutes_mean",
        "minutes",
    ]:
        if key in r:
            v = _safe_float(r.get(key))
            if v is not None:
                return v
    return None


def _load_preds_long(date_str: str) -> pd.DataFrame:
    fp = PROC_DIR / f"props_predictions_{date_str}.csv"
    if not fp.exists():
        return pd.DataFrame(
            columns=[
                "team_tri",
                "name_key",
                "player_id",
                "player_name",
                "stat",
                "sim_mu",
                "expected_min",
            ]
        )

    try:
        df = pd.read_csv(fp)
    except Exception:
        return pd.DataFrame(
            columns=[
                "team_tri",
                "name_key",
                "player_id",
                "player_name",
                "stat",
                "sim_mu",
                "expected_min",
            ]
        )

    if df.empty:
        return pd.DataFrame(
            columns=[
                "team_tri",
                "name_key",
                "player_id",
                "player_name",
                "stat",
                "sim_mu",
                "expected_min",
            ]
        )

    # Standardize identifiers
    team_col = "team" if "team" in df.columns else ("team_tri" if "team_tri" in df.columns else None)
    name_col = "player_name" if "player_name" in df.columns else ("name" if "name" in df.columns else None)
    pid_col = "player_id" if "player_id" in df.columns else None

    if team_col is None or name_col is None:
        return pd.DataFrame(
            columns=[
                "team_tri",
                "name_key",
                "player_id",
                "player_name",
                "stat",
                "sim_mu",
                "expected_min",
            ]
        )

    base = df.copy()
    base["team_tri"] = base[team_col].astype(str).str.upper().str.strip()
    base["player_name"] = base[name_col].astype(str).str.strip()
    base["name_key"] = base["player_name"].apply(_norm_player_name)
    if pid_col is not None:
        base["player_id"] = base[pid_col].apply(_safe_int)
    else:
        base["player_id"] = pd.NA

    # expected minutes (single scalar per player)
    exp = []
    for _, row in base.iterrows():
        exp.append(_expected_min_from_preds_row(row.to_dict()))
    base["expected_min"] = exp

    # Build long rows across supported stats
    out_rows: list[dict[str, Any]] = []
    for stat, col in STAT_TO_PRED_COL.items():
        if col not in base.columns:
            continue
        vals = pd.to_numeric(base[col], errors="coerce")
        for i, v in enumerate(vals):
            if pd.isna(v):
                continue
            r = base.iloc[i]
            out_rows.append(
                {
                    "team_tri": r.get("team_tri"),
                    "name_key": r.get("name_key"),
                    "player_id": r.get("player_id"),
                    "player_name": r.get("player_name"),
                    "stat": stat,
                    "sim_mu": float(v),
                    "expected_min": _safe_float(r.get("expected_min")),
                }
            )

    out = pd.DataFrame(out_rows)
    if out.empty:
        return pd.DataFrame(
            columns=[
                "team_tri",
                "name_key",
                "player_id",
                "player_name",
                "stat",
                "sim_mu",
                "expected_min",
            ]
        )

    out = out[out["team_tri"].astype(str).str.len() == 3]
    out = out[out["name_key"].astype(str).str.len() > 0]
    return out


def _load_actuals_long(date_str: str) -> pd.DataFrame:
    # Try to use the repo's schedule mapping (fast + precise)
    game_ids: list[str] = []
    try:
        from nba_betting.boxscores import _nba_gid_to_tricodes

        gid_map = _nba_gid_to_tricodes(str(date_str)) or {}
        for gid in gid_map.keys():
            g = _canon_nba_game_id(gid)
            if g:
                game_ids.append(g)
    except Exception:
        game_ids = []

    # Fallback: use smart_sim files to discover game ids
    if not game_ids:
        try:
            import json

            for fp in sorted(PROC_DIR.glob(f"smart_sim_{date_str}_*.json")):
                try:
                    obj = json.loads(fp.read_text(encoding="utf-8"))
                except Exception:
                    continue
                if isinstance(obj, dict):
                    g = _canon_nba_game_id(obj.get("game_id"))
                    if g:
                        game_ids.append(g)
        except Exception:
            game_ids = []

    game_ids = sorted(set(game_ids))

    rows: list[dict[str, Any]] = []
    for gid in game_ids:
        fp = BOXSCORES_DIR / f"boxscore_{gid}.csv"
        if not fp.exists():
            continue
        try:
            df = pd.read_csv(fp)
        except Exception:
            continue
        if df.empty:
            continue

        # Ensure key cols
        for c in ["teamTricode", "personId", "minutes", "points", "reboundsTotal", "assists", "threePointersMade"]:
            if c not in df.columns:
                df[c] = pd.NA

        for _, r in df.iterrows():
            team_tri = str(r.get("teamTricode") or "").upper().strip()
            pid = _safe_int(r.get("personId"))
            if not team_tri or len(team_tri) != 3 or pid is None:
                continue

            first = str(r.get("firstName") or "").strip()
            last = str(r.get("familyName") or "").strip()
            player_name = (first + " " + last).strip() if (first or last) else str(r.get("nameI") or "").strip()
            name_key = _norm_player_name(player_name)
            actual_min = _parse_minutes_to_float(r.get("minutes"))

            pts = _safe_float(r.get("points"))
            reb = _safe_float(r.get("reboundsTotal"))
            ast = _safe_float(r.get("assists"))
            thr = _safe_float(r.get("threePointersMade"))

            def add(stat: str, val: Optional[float]) -> None:
                if val is None:
                    return
                rows.append(
                    {
                        "game_id": gid,
                        "team_tri": team_tri,
                        "player_id": pid,
                        "player_name": player_name,
                        "name_key": name_key,
                        "stat": stat,
                        "actual": float(val),
                        "actual_min": actual_min,
                    }
                )

            add("pts", pts)
            add("reb", reb)
            add("ast", ast)
            add("threes", thr)
            if pts is not None and reb is not None and ast is not None:
                add("pra", float(pts + reb + ast))
            if pts is not None and reb is not None:
                add("pr", float(pts + reb))
            if reb is not None and ast is not None:
                add("ra", float(reb + ast))
            if pts is not None and ast is not None:
                add("pa", float(pts + ast))

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(
            columns=[
                "game_id",
                "team_tri",
                "player_id",
                "player_name",
                "name_key",
                "stat",
                "actual",
                "actual_min",
            ]
        )

    # Keep only plausible, non-empty keys
    out = out[out["team_tri"].astype(str).str.len() == 3]
    out = out[out["name_key"].astype(str).str.len() > 0]
    return out


def build_live_player_lens_tuning(date_str: str) -> pd.DataFrame:
    edges = _load_edges_median(date_str)
    preds = _load_preds_long(date_str)
    actuals = _load_actuals_long(date_str)

    if preds.empty or edges.empty:
        # Still write an empty artifact with stable columns.
        return pd.DataFrame(
            columns=[
                "date",
                "team_tri",
                "player_id",
                "player_name",
                "stat",
                "line",
                "line_n_books",
                "sim_mu",
                "expected_min",
                "game_id",
                "actual_min",
                "actual",
                "pace_proj_final",
                "sim_vs_line",
                "actual_vs_line",
                "pace_vs_line",
            ]
        )

    joined = preds.merge(edges, on=["team_tri", "name_key", "stat"], how="inner")
    if joined.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "team_tri",
                "player_id",
                "player_name",
                "stat",
                "line",
                "line_n_books",
                "sim_mu",
                "expected_min",
                "game_id",
                "actual_min",
                "actual",
                "pace_proj_final",
                "sim_vs_line",
                "actual_vs_line",
                "pace_vs_line",
            ]
        )

    # Join actuals: prefer player_id match when present; fallback to name_key.
    act_pid = actuals.copy()
    act_pid = act_pid[act_pid["player_id"].notna()].copy()

    out = joined.copy()

    out["player_id_num"] = pd.to_numeric(out["player_id"], errors="coerce")
    out_pid = out.merge(
        act_pid[["team_tri", "player_id", "stat", "game_id", "actual_min", "actual"]],
        left_on=["team_tri", "player_id_num", "stat"],
        right_on=["team_tri", "player_id", "stat"],
        how="left",
        suffixes=("", "_act"),
    )

    # Fill missing actuals via name_key
    missing_mask = out_pid["actual"].isna()
    if missing_mask.any():
        act_name = actuals.copy()
        act_name["name_key"] = act_name["name_key"].astype(str)
        out_missing = out_pid[missing_mask].drop(columns=["game_id", "actual_min", "actual"], errors="ignore")
        out_missing = out_missing.merge(
            act_name[["team_tri", "name_key", "stat", "game_id", "actual_min", "actual"]],
            on=["team_tri", "name_key", "stat"],
            how="left",
        )
        out_pid.loc[missing_mask, "game_id"] = out_missing["game_id"].values
        out_pid.loc[missing_mask, "actual_min"] = out_missing["actual_min"].values
        out_pid.loc[missing_mask, "actual"] = out_missing["actual"].values

    # Compute pace projection based on final per-minute rate
    def pace_proj(row: pd.Series) -> Optional[float]:
        exp_m = _safe_float(row.get("expected_min"))
        act_m = _safe_float(row.get("actual_min"))
        act_v = _safe_float(row.get("actual"))
        if exp_m is None or act_m is None or act_v is None:
            return None
        if act_m <= 0:
            return None
        return float((act_v / act_m) * exp_m)

    out_pid["pace_proj_final"] = out_pid.apply(pace_proj, axis=1)

    out_pid["date"] = str(date_str)
    out_pid["sim_vs_line"] = pd.to_numeric(out_pid["sim_mu"], errors="coerce") - pd.to_numeric(out_pid["line"], errors="coerce")
    out_pid["actual_vs_line"] = pd.to_numeric(out_pid["actual"], errors="coerce") - pd.to_numeric(out_pid["line"], errors="coerce")
    out_pid["pace_vs_line"] = pd.to_numeric(out_pid["pace_proj_final"], errors="coerce") - pd.to_numeric(out_pid["line"], errors="coerce")

    out_pid = out_pid.rename(columns={"player_id_num": "player_id"})
    keep = [
        "date",
        "team_tri",
        "player_id",
        "player_name",
        "stat",
        "line",
        "line_n_books",
        "sim_mu",
        "expected_min",
        "game_id",
        "actual_min",
        "actual",
        "pace_proj_final",
        "sim_vs_line",
        "actual_vs_line",
        "pace_vs_line",
    ]
    out_pid = out_pid[keep].copy()

    # Stable ordering
    try:
        out_pid = out_pid.sort_values(["team_tri", "player_name", "stat"], ascending=[True, True, True])
    except Exception:
        pass

    return out_pid


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--out",
        default=None,
        help="Output CSV path (default: data/processed/live_player_lens_tuning_<date>.csv)",
    )
    args = ap.parse_args()

    d = str(args.date).strip()
    out = Path(args.out) if args.out else (PROC_DIR / f"live_player_lens_tuning_{d}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)

    df = build_live_player_lens_tuning(d)
    df.to_csv(out, index=False)

    print(f"wrote {out}")
    print(f"rows={len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
