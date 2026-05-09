from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def _try_import_norm_key():
    try:
        sys.path.insert(0, str(ROOT / "src"))
        from nba_betting.sim.connected_game import _norm_player_key  # type: ignore

        return _norm_player_key
    except Exception:
        return None


_NORM_PLAYER_KEY = _try_import_norm_key()


def _norm_player_key_fallback(name: Any) -> str:
    try:
        s = str(name or "").strip().lower()
        # very small fallback normalization; connected_game has richer unicode handling.
        s = " ".join(s.split())
        return s
    except Exception:
        return ""


def _norm_player_key(name: Any) -> str:
    if _NORM_PLAYER_KEY is not None:
        try:
            return str(_NORM_PLAYER_KEY(name))
        except Exception:
            return _norm_player_key_fallback(name)
    return _norm_player_key_fallback(name)


def _build_espn_id_to_name(processed: Path) -> dict[str, str]:
    """Best-effort ESPN athlete_id -> name mapping from existing histories."""

    def _merge_id_name_pairs(out: dict[str, str], pairs: pd.DataFrame) -> None:
        if pairs is None or pairs.empty:
            return
        if "pid" not in pairs.columns or "name" not in pairs.columns:
            return
        x = pairs.copy()
        x["pid"] = x["pid"].astype(str).str.strip()
        x["name"] = x["name"].astype(str).str.strip()
        x = x[(x["pid"].str.len() > 0) & (x["name"].str.len() > 0)].copy()
        if x.empty:
            return
        # choose longest name per id (usually full name)
        x["_nlen"] = x["name"].str.len()
        x = x.sort_values(["pid", "_nlen"], ascending=[True, False], kind="stable")
        x = x.drop_duplicates(subset=["pid"], keep="first")
        for pid, nm in zip(x["pid"].tolist(), x["name"].tolist()):
            if pid and nm:
                out[pid] = nm

    out: dict[str, str] = {}

    # 1) Pair minutes history is relatively small and already includes names.
    pair_fp = processed / "pair_minutes_history.csv"
    if pair_fp.exists():
        try:
            use = ["player1_id", "player1_name", "player2_id", "player2_name"]
            df = pd.read_csv(pair_fp, usecols=use, dtype=str)
            p1 = df[["player1_id", "player1_name"]].rename(columns={"player1_id": "pid", "player1_name": "name"})
            p2 = df[["player2_id", "player2_name"]].rename(columns={"player2_id": "pid", "player2_name": "name"})
            _merge_id_name_pairs(out, pd.concat([p1, p2], ignore_index=True))
        except Exception:
            pass

    # 2) Play context history can be large; read in chunks, vectorized.
    pc_fp = processed / "play_context_history.csv"
    if pc_fp.exists():
        try:
            use = ["enter_player_id", "enter_player_name", "exit_player_id", "exit_player_name"]
            for chunk in pd.read_csv(pc_fp, usecols=use, dtype=str, chunksize=200_000):
                e = chunk[["enter_player_id", "enter_player_name"]].rename(columns={"enter_player_id": "pid", "enter_player_name": "name"})
                x = chunk[["exit_player_id", "exit_player_name"]].rename(columns={"exit_player_id": "pid", "exit_player_name": "name"})
                _merge_id_name_pairs(out, pd.concat([e, x], ignore_index=True))
        except Exception:
            # this source is optional; don't fail the build
            pass

    return out


def _load_rotation_minutes_history(processed: Path) -> pd.DataFrame:
    """Return per-game minutes by ESPN player_id derived from rotation_stints_history.csv."""
    fp = processed / "rotation_stints_history.csv"
    if not fp.exists():
        return pd.DataFrame()

    st = pd.read_csv(fp)
    if st is None or st.empty:
        return pd.DataFrame()

    needed = {"team", "date", "event_id", "duration_sec", "lineup_player_ids", "start_sec"}
    if not needed.issubset(set(st.columns)):
        return pd.DataFrame()

    st = st.copy()
    st["team"] = st["team"].astype(str).str.upper().str.strip()
    st["date_dt"] = pd.to_datetime(st["date"], errors="coerce")
    st = st.dropna(subset=["date_dt"]).copy()
    if st.empty:
        return pd.DataFrame()

    # starter ids from first stint (start_sec==0) per team/event
    starters = st[pd.to_numeric(st["start_sec"], errors="coerce").fillna(-1).astype(int) == 0].copy()
    starters = starters.sort_values(["team", "event_id", "date_dt"], kind="stable")
    starters = starters.drop_duplicates(subset=["team", "event_id"], keep="first")
    starters["_ids"] = starters["lineup_player_ids"].astype(str).str.split(";")
    starters = starters[["team", "event_id", "_ids"]].explode("_ids")
    starters = starters.rename(columns={"_ids": "player_espn_id"})
    starters["player_espn_id"] = starters["player_espn_id"].astype(str).str.strip()
    starters = starters[starters["player_espn_id"].astype(str).str.len() > 0].copy()
    starters["started"] = 1.0

    # minutes by player per game
    x = st[["team", "event_id", "date_dt", "duration_sec", "lineup_player_ids"]].copy()
    x["_ids"] = x["lineup_player_ids"].astype(str).str.split(";")
    x = x.explode("_ids")
    x = x.rename(columns={"_ids": "player_espn_id"})
    x["player_espn_id"] = x["player_espn_id"].astype(str).str.strip()
    x = x[x["player_espn_id"].astype(str).str.len() > 0].copy()
    x["minutes"] = pd.to_numeric(x["duration_sec"], errors="coerce").fillna(0.0).astype(float) / 60.0
    g = x.groupby(["team", "event_id", "date_dt", "player_espn_id"], as_index=False)["minutes"].sum()

    g = g.merge(starters[["team", "event_id", "player_espn_id", "started"]], on=["team", "event_id", "player_espn_id"], how="left")
    g["started"] = pd.to_numeric(g.get("started"), errors="coerce").fillna(0.0)
    return g


def _compute_rotation_expected_minutes_asof(
    rot_game_minutes: pd.DataFrame,
    id_to_name: dict[str, str],
    cutoff_date: pd.Timestamp,
    lookback_days: int,
    half_life_days: float,
) -> pd.DataFrame:
    if rot_game_minutes is None or rot_game_minutes.empty:
        return pd.DataFrame()

    cutoff = pd.to_datetime(cutoff_date, errors="coerce")
    if pd.isna(cutoff):
        return pd.DataFrame()

    start = cutoff - pd.Timedelta(days=int(lookback_days))
    df = rot_game_minutes.copy()
    df = df[(df["date_dt"] >= start) & (df["date_dt"] <= cutoff)].copy()
    if df.empty:
        return pd.DataFrame()

    df["days_ago"] = (cutoff - df["date_dt"]).dt.days.astype(int)
    hl = float(half_life_days)
    if not np.isfinite(hl) or hl <= 0:
        hl = 10.0
    # exponential decay with half-life
    df["w"] = np.exp(-np.log(2.0) * (df["days_ago"].astype(float) / hl))

    def _name_for_id(pid: Any) -> str:
        k = str(pid or "").strip()
        return id_to_name.get(k, "")

    df["player_name"] = df["player_espn_id"].map(_name_for_id)
    df["_pkey"] = df["player_name"].map(_norm_player_key)
    df = df[df["_pkey"].astype(str).str.len() > 0].copy()
    if df.empty:
        return pd.DataFrame()

    # weighted mean minutes and starter probability
    df["wm"] = df["w"] * pd.to_numeric(df["minutes"], errors="coerce").fillna(0.0).astype(float)
    df["ws"] = df["w"] * pd.to_numeric(df["started"], errors="coerce").fillna(0.0).astype(float)

    agg = df.groupby(["team", "_pkey"], as_index=False).agg(w_sum=("w", "sum"), wm_sum=("wm", "sum"), ws_sum=("ws", "sum"))
    agg["exp_min_mean_rot"] = np.where(agg["w_sum"] > 0, agg["wm_sum"] / agg["w_sum"], np.nan)
    agg["starter_prob"] = np.where(agg["w_sum"] > 0, agg["ws_sum"] / agg["w_sum"], np.nan)
    agg = agg[["team", "_pkey", "exp_min_mean_rot", "starter_prob"]]
    return agg


def _date_range(start: str, end: str) -> list[str]:
    s = pd.to_datetime(start, errors="coerce")
    e = pd.to_datetime(end, errors="coerce")
    if pd.isna(s) or pd.isna(e):
        raise ValueError(f"Invalid start/end: {start}..{end}")
    s = s.date()
    e = e.date()
    if s > e:
        s, e = e, s
    out: list[str] = []
    d = s
    while d <= e:
        out.append(str(d))
        d = (pd.Timestamp(d) + pd.Timedelta(days=1)).date()
    return out


def _norm_bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    s = df[col]
    if s.dtype == bool:
        return s
    return s.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def _first_present(cols: list[str], candidates: list[str]) -> str | None:
    s = set(cols)
    for c in candidates:
        if c in s:
            return c
    return None


def build_for_date(
    date_str: str,
    processed: Path,
    overwrite: bool,
    cap_minutes: float,
    include_starters: bool,
    include_uncertainty: bool,
    source: str,
    rot_game_minutes: pd.DataFrame | None,
    espn_id_to_name: dict[str, str] | None,
    rotations_lookback_days: int,
    rotations_half_life_days: float,
    rotations_blend_alpha: float,
) -> dict[str, Any]:
    ds = str(date_str).strip()

    props_fp = processed / f"props_predictions_{ds}.csv"
    out_fp = processed / f"pregame_expected_minutes_{ds}.csv"

    if not props_fp.exists():
        return {"date": ds, "status": "skip_missing_props", "props": str(props_fp), "out": str(out_fp)}

    if out_fp.exists() and (not overwrite):
        return {"date": ds, "status": "skip_exists", "props": str(props_fp), "out": str(out_fp)}

    try:
        props = pd.read_csv(props_fp)
    except Exception as e:
        return {"date": ds, "status": "error_read_props", "error": repr(e), "props": str(props_fp), "out": str(out_fp)}

    if props is None or props.empty:
        return {"date": ds, "status": "skip_empty_props", "props": str(props_fp), "out": str(out_fp)}

    focus = props.copy()
    if "team_on_slate" in focus.columns:
        try:
            focus = focus[_norm_bool_series(focus, "team_on_slate")].copy()
        except Exception:
            pass
    if "playing_today" in focus.columns:
        try:
            focus = focus[_norm_bool_series(focus, "playing_today")].copy()
        except Exception:
            pass

    if focus.empty:
        return {"date": ds, "status": "skip_empty_focus", "props": str(props_fp), "out": str(out_fp)}

    # Determine which minutes signal to treat as the baseline "expected minutes".
    min_candidates = ["roll5_min", "roll10_min", "roll3_min", "lag1_min", "pred_min"]
    min_col = _first_present(list(focus.columns), min_candidates)
    if not min_col:
        return {"date": ds, "status": "skip_no_minutes_col", "props": str(props_fp), "out": str(out_fp)}

    if "team" not in focus.columns:
        return {"date": ds, "status": "skip_no_team", "props": str(props_fp), "out": str(out_fp)}

    out = pd.DataFrame()
    out["date"] = ds
    out["team_tri"] = focus["team"].astype(str).str.upper().str.strip()

    if "player_id" in focus.columns:
        out["player_id"] = pd.to_numeric(focus["player_id"], errors="coerce")
    else:
        out["player_id"] = np.nan

    if "player_name" in focus.columns:
        out["player_name"] = focus["player_name"].astype(str)
    else:
        out["player_name"] = ""

    minute_weights = {
        "lag1_min": 0.20,
        "roll3_min": 0.25,
        "roll5_min": 0.35,
        "roll10_min": 0.20,
        "pred_min": 1.00,
    }
    baseline = pd.Series(np.nan, index=focus.index, dtype=float)
    baseline_source = pd.Series("", index=focus.index, dtype=object)
    weighted_sum = pd.Series(0.0, index=focus.index, dtype=float)
    weight_total = pd.Series(0.0, index=focus.index, dtype=float)
    source_labels: dict[str, pd.Series] = {}
    source_counts = pd.Series(0, index=focus.index, dtype=int)
    for candidate in min_candidates:
        if candidate not in focus.columns:
            continue
        values = pd.to_numeric(focus[candidate], errors="coerce")
        source_labels[candidate] = values
        candidate_weight = float(minute_weights.get(candidate, 0.0))
        present_mask = values.notna()
        if candidate_weight > 0.0 and bool(present_mask.any()):
            weighted_sum.loc[present_mask] = weighted_sum.loc[present_mask] + (values.loc[present_mask] * candidate_weight)
            weight_total.loc[present_mask] = weight_total.loc[present_mask] + candidate_weight
            source_counts.loc[present_mask] = source_counts.loc[present_mask] + 1

    weighted_mask = weight_total.gt(0.0)
    if bool(weighted_mask.any()):
        baseline.loc[weighted_mask] = (weighted_sum.loc[weighted_mask] / weight_total.loc[weighted_mask]).astype(float)

    if bool(baseline.isna().any()):
        fallback_values = pd.to_numeric(focus[min_col], errors="coerce")
        fallback_mask = baseline.isna() & fallback_values.notna()
        if bool(fallback_mask.any()):
            baseline.loc[fallback_mask] = fallback_values.loc[fallback_mask]
            baseline_source.loc[fallback_mask] = min_col

    single_source_mask = source_counts.eq(1)
    for candidate in min_candidates:
        values = source_labels.get(candidate)
        if values is None:
            continue
        only_source_mask = single_source_mask & values.notna() & baseline_source.eq("")
        if bool(only_source_mask.any()):
            baseline_source.loc[only_source_mask] = candidate
    blend_mask = baseline_source.eq("") & weight_total.gt(0.0)
    if bool(blend_mask.any()):
        baseline_source.loc[blend_mask] = "blend_lag1_roll3_roll5_roll10"

    baseline = baseline.where(baseline.notna(), np.nan)
    baseline = baseline.clip(lower=0.0, upper=float(cap_minutes))
    baseline_source = baseline_source.where(baseline_source.astype(str).str.len().gt(0), other=min_col)
    # Preserve baseline minutes aligned to the output rows so merges don't break shape/index alignment.
    out["_baseline_min"] = baseline.to_numpy(copy=True)
    out["_baseline_source"] = baseline_source.to_numpy(copy=True)

    src = str(source or "props").strip().lower()
    if src not in {"props", "rotations"}:
        src = "props"

    if src == "rotations":
        rgm = rot_game_minutes if isinstance(rot_game_minutes, pd.DataFrame) else pd.DataFrame()
        idmap = espn_id_to_name or {}
        cutoff = pd.to_datetime(ds, errors="coerce") - pd.Timedelta(days=1)
        rot_exp = _compute_rotation_expected_minutes_asof(
            rot_game_minutes=rgm,
            id_to_name=idmap,
            cutoff_date=cutoff,
            lookback_days=int(rotations_lookback_days),
            half_life_days=float(rotations_half_life_days),
        )

        out["_pkey"] = out["player_name"].map(_norm_player_key)
        rot_exp = rot_exp.rename(columns={"team": "team_tri"})
        merged = out.merge(rot_exp, on=["team_tri", "_pkey"], how="left")
        out = merged

        rot_val = pd.to_numeric(out.get("exp_min_mean_rot"), errors="coerce")
        base_val = pd.to_numeric(out.get("_baseline_min"), errors="coerce")
        a = float(rotations_blend_alpha)
        if not np.isfinite(a):
            a = 1.0
        a = float(np.clip(a, 0.0, 1.0))

        out["exp_min_mean"] = rot_val
        if a < 1.0:
            out["exp_min_mean"] = np.where(rot_val.notna(), a * rot_val + (1.0 - a) * base_val, np.nan)
        out["exp_min_mean"] = pd.to_numeric(out["exp_min_mean"], errors="coerce")
        out["exp_min_mean"] = out["exp_min_mean"].where(out["exp_min_mean"].notna(), base_val)
        out["exp_min_mean"] = pd.to_numeric(out["exp_min_mean"], errors="coerce").clip(lower=0.0, upper=float(cap_minutes))
        baseline_src = out.get("_baseline_source", pd.Series([min_col] * len(out), index=out.index)).astype(str)
        out["exp_min_source"] = np.where(
            pd.to_numeric(out.get("exp_min_mean_rot"), errors="coerce").notna(),
            ("rotations_espn_history" if a >= 1.0 else f"rotations_espn_history_blend:{a:.2f}"),
            ("baseline:" + baseline_src),
        )
    else:
        out["exp_min_mean"] = pd.to_numeric(out.get("_baseline_min"), errors="coerce")
        baseline_src = out.get("_baseline_source", pd.Series([min_col] * len(out), index=out.index)).astype(str)
        out["exp_min_source"] = "baseline:" + baseline_src

    if include_uncertainty:
        # Simple default uncertainty; higher for low-minute players.
        sd = np.where(out["exp_min_mean"].fillna(0.0).to_numpy() >= 24.0, 4.0, 6.0)
        out["exp_min_sd"] = sd
        out["exp_min_cap"] = (out["exp_min_mean"] + 2.0 * out["exp_min_sd"]).clip(upper=float(cap_minutes))

    if include_starters:
        out["is_starter"] = False
        try:
            for _, g in out.groupby("team_tri"):
                if not isinstance(g, pd.DataFrame) or g.empty:
                    continue
                gg = g.copy()
                sp = pd.to_numeric(gg.get("starter_prob"), errors="coerce")
                if sp.notna().sum() >= 5:
                    gg["_rank1"] = sp.fillna(-1.0)
                    gg["_rank2"] = pd.to_numeric(gg.get("exp_min_mean"), errors="coerce").fillna(-1.0)
                    top_idx = gg.sort_values(["_rank1", "_rank2", "player_name"], ascending=[False, False, True]).head(5).index
                else:
                    gg["_rank"] = pd.to_numeric(gg.get("exp_min_mean"), errors="coerce").fillna(-1.0)
                    top_idx = gg.sort_values(["_rank", "player_name"], ascending=[False, True]).head(5).index
                out.loc[top_idx, "is_starter"] = True
        except Exception:
            pass

    # Historical placeholder "as-of" timestamp.
    out["exp_asof_ts"] = f"{ds}T16:00:00Z"

    # Drop obviously invalid teams / empty rows.
    out = out[out["team_tri"].astype(str).str.len() >= 2].copy()

    # Dedupe so merge is stable.
    key_cols = ["team_tri", "player_id"] if out["player_id"].notna().any() else ["team_tri", "player_name"]
    try:
        out = out.sort_values(["team_tri", "exp_min_mean"], ascending=[True, False])
        out = out.drop_duplicates(subset=key_cols, keep="first")
    except Exception:
        pass

    out = out.drop(columns=["_pkey", "exp_min_mean_rot", "_baseline_min"], errors="ignore")

    processed.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_fp, index=False)
    return {"date": ds, "status": "ok", "rows": int(len(out)), "min_col": min_col, "source": src, "out": str(out_fp)}


def main() -> int:
    ap = argparse.ArgumentParser(description="Build pregame expected-minutes artifacts over a date range")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--processed", default=str(PROCESSED), help="Processed dir (default: data/processed)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing pregame_expected_minutes files")
    ap.add_argument("--cap-minutes", type=float, default=46.0, help="Per-player minutes cap (default 46)")
    ap.add_argument(
        "--source",
        choices=["props", "rotations"],
        default="rotations",
        help="Source for expected minutes: 'rotations' (derived from rotation_stints_history, as-of) or 'props' (roll-minutes placeholder).",
    )
    ap.add_argument("--rotations-lookback-days", type=int, default=60, help="Lookback days for rotations history (source=rotations)")
    ap.add_argument("--rotations-half-life-days", type=float, default=12.0, help="Half-life (days) for decay weighting (source=rotations)")
    ap.add_argument(
        "--rotations-blend-alpha",
        type=float,
        default=1.0,
        help="Blend rotations-derived minutes with baseline roll-minutes when available (alpha*rot + (1-alpha)*baseline).",
    )
    ap.add_argument(
        "--include-starters",
        action="store_true",
        help="Include is_starter flag (top-5 by expected minutes per team). Default off for placeholder feeds.",
    )
    ap.add_argument(
        "--include-uncertainty",
        action="store_true",
        help="Include exp_min_sd and exp_min_cap columns. Default off for placeholder feeds.",
    )
    args = ap.parse_args()

    processed = Path(str(args.processed)).resolve()
    dates = _date_range(str(args.start), str(args.end))

    rot_game_minutes = None
    espn_id_to_name = None
    if str(args.source).strip().lower() == "rotations":
        espn_id_to_name = _build_espn_id_to_name(processed)
        rot_game_minutes = _load_rotation_minutes_history(processed)

    results: list[dict[str, Any]] = []
    ok = 0
    for ds in dates:
        r = build_for_date(
            ds,
            processed=processed,
            overwrite=bool(args.overwrite),
            cap_minutes=float(args.cap_minutes),
            include_starters=bool(args.include_starters),
            include_uncertainty=bool(args.include_uncertainty),
            source=str(args.source),
            rot_game_minutes=rot_game_minutes,
            espn_id_to_name=espn_id_to_name,
            rotations_lookback_days=int(args.rotations_lookback_days),
            rotations_half_life_days=float(args.rotations_half_life_days),
            rotations_blend_alpha=float(args.rotations_blend_alpha),
        )
        results.append(r)
        if r.get("status") == "ok":
            ok += 1

    out_obj = {
        "start": str(args.start),
        "end": str(args.end),
        "processed": str(processed),
        "ok_days": ok,
        "days": len(dates),
        "ran_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "results": results,
    }
    print(pd.Series({"ok_days": ok, "days": len(dates)}).to_string())
    # Keep a machine-readable log for debugging.
    try:
        fp = processed / f"build_pregame_expected_minutes_{dates[0]}_{dates[-1]}.json"
        fp.write_text(json.dumps(out_obj, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
