from __future__ import annotations

from pathlib import Path
from typing import Any
import re
import unicodedata

import numpy as np
import pandas as pd


_OPENING_COLS = [
    "snapshot_ts",
    "event_id",
    "commence_time",
    "bookmaker",
    "market",
    "outcome_name",
    "player_name",
    "point",
    "price",
]

_HISTORY_COLS = list(_OPENING_COLS)

_MARKET_TO_STAT = {
    "player_points": "pts",
    "player_rebounds": "reb",
    "player_assists": "ast",
    "player_threes": "threes",
    "player_points_rebounds_assists": "pra",
    "player_points_rebounds": "pr",
    "player_points_assists": "pa",
    "player_rebounds_assists": "ra",
    "player_steals": "stl",
    "player_blocks": "blk",
    "player_turnovers": "tov",
    "player_double_double": "dd",
    "player_triple_double": "td",
}

_STAT_TO_MARKET: dict[str, str] = {}
for _market_key, _stat_key in _MARKET_TO_STAT.items():
    _STAT_TO_MARKET.setdefault(str(_stat_key).strip().lower(), str(_market_key).strip().lower())


def _normalize_date_str(target_date: Any) -> str:
    return str(pd.to_datetime(target_date).date())


def _norm_name_key(value: Any) -> str:
    text = str(value or "").strip().upper()
    try:
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = text.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    text = text.replace("-", " ")
    if "(" in text:
        text = text.split("(", 1)[0]
    text = re.sub(r"[^A-Z0-9\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    for suffix in (" JR", " SR", " II", " III", " IV", " V"):
        if text.endswith(suffix):
            text = text[: -len(suffix)].strip()
    return text


def _map_side(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if "OVER" in text:
        return "OVER"
    if "UNDER" in text:
        return "UNDER"
    if text in {"YES", "Y"}:
        return "YES"
    if text in {"NO", "N"}:
        return "NO"
    return None


def _american_to_implied(values: pd.Series) -> pd.Series:
    series = pd.to_numeric(values, errors="coerce")
    implied = pd.Series(np.nan, index=series.index, dtype="float64")
    pos_mask = series > 0
    if pos_mask.any():
        implied.loc[pos_mask] = 100.0 / (series.loc[pos_mask] + 100.0)
    neg_mask = series < 0
    if neg_mask.any():
        implied.loc[neg_mask] = (-series.loc[neg_mask]) / ((-series.loc[neg_mask]) + 100.0)
    return implied


def _column_series(df: pd.DataFrame, column: str, default: Any = np.nan) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series(default, index=df.index)


def _read_frame(path: Path, usecols: list[str] | None = None) -> pd.DataFrame:
    try:
        if not path.exists():
            return pd.DataFrame()
        if str(path).lower().endswith(".parquet"):
            return pd.read_parquet(path, columns=usecols)
        if usecols:
            return pd.read_csv(path, usecols=usecols)
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _filter_to_slate_date(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    if df is None or df.empty or "commence_time" not in df.columns:
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    try:
        dt_utc = pd.to_datetime(df["commence_time"], errors="coerce", utc=True)
        try:
            slate_dates = dt_utc.dt.tz_convert("America/New_York").dt.date
        except Exception:
            try:
                slate_dates = dt_utc.dt.tz_convert("US/Eastern").dt.date
            except Exception:
                slate_dates = dt_utc.dt.date
        return df.loc[slate_dates == pd.to_datetime(date_str).date()].copy()
    except Exception:
        return pd.DataFrame()


def _select_canonical_open(df_in: pd.DataFrame) -> pd.DataFrame:
    if df_in is None or not isinstance(df_in, pd.DataFrame) or df_in.empty:
        return pd.DataFrame(columns=_OPENING_COLS)
    try:
        tmp = df_in[[col for col in _OPENING_COLS if col in df_in.columns]].copy()
        if tmp.empty:
            return pd.DataFrame(columns=_OPENING_COLS)
        tmp["_name_key"] = tmp.get("player_name").astype(str).map(_norm_name_key)
        tmp["_side"] = tmp.get("outcome_name").astype(str).map(_map_side)
        tmp = tmp[
            tmp.get("event_id").notna()
            & tmp.get("bookmaker").notna()
            & tmp.get("market").notna()
            & tmp.get("_name_key").notna()
            & tmp.get("_side").notna()
        ].copy()
        if tmp.empty:
            return pd.DataFrame(columns=_OPENING_COLS)

        tmp["snapshot_ts_dt"] = pd.to_datetime(tmp["snapshot_ts"], errors="coerce", utc=True)
        tmp = tmp[tmp["snapshot_ts_dt"].notna()].copy()
        if tmp.empty:
            return pd.DataFrame(columns=_OPENING_COLS)

        tmp["_price_num"] = pd.to_numeric(tmp.get("price"), errors="coerce")
        implied = _american_to_implied(tmp["_price_num"])
        target_ip = 110.0 / (110.0 + 100.0)
        tmp["_metric"] = (implied - float(target_ip)).abs().fillna(1e9)

        keys = ["event_id", "bookmaker", "market", "_name_key", "_side"]
        first_ts = tmp.groupby(keys)["snapshot_ts_dt"].transform("min")
        first_rows = tmp[tmp["snapshot_ts_dt"] == first_ts].copy()
        if first_rows.empty:
            return pd.DataFrame(columns=_OPENING_COLS)
        try:
            idx = first_rows.groupby(keys)["_metric"].idxmin()
            out = first_rows.loc[idx, [col for col in _OPENING_COLS if col in first_rows.columns]].copy()
        except Exception:
            out = first_rows.sort_values(["snapshot_ts_dt"]).groupby(keys, as_index=False).first()
            out = out[[col for col in _OPENING_COLS if col in out.columns]].copy()
        return out.reindex(columns=_OPENING_COLS)
    except Exception:
        return pd.DataFrame(columns=_OPENING_COLS)


def persist_props_snapshot_tracking(
    snapshot_df: pd.DataFrame,
    target_date: Any,
    *,
    raw_dir: Path,
) -> dict[str, Any]:
    date_str = _normalize_date_str(target_date)
    raw_dir.mkdir(parents=True, exist_ok=True)

    open_pq = raw_dir / f"odds_nba_player_props_opening_{date_str}.parquet"
    open_csv = raw_dir / f"odds_nba_player_props_opening_{date_str}.csv"
    hist_csv = raw_dir / f"odds_nba_player_props_history_{date_str}.csv"

    current = snapshot_df.copy() if isinstance(snapshot_df, pd.DataFrame) else pd.DataFrame()
    current = _filter_to_slate_date(current, date_str) if not current.empty else current
    current_open = _select_canonical_open(current)

    existing_open = _read_frame(open_pq, usecols=_OPENING_COLS)
    if existing_open.empty:
        existing_open = _read_frame(open_csv, usecols=_OPENING_COLS)
    existing_open = _select_canonical_open(existing_open)

    parts = [part for part in (existing_open, current_open) if isinstance(part, pd.DataFrame) and not part.empty]
    combined_source = pd.concat(parts, ignore_index=True, sort=False) if parts else pd.DataFrame(columns=_OPENING_COLS)
    combined_open = _select_canonical_open(combined_source)

    wrote_open = None
    if not combined_open.empty:
        try:
            combined_open.to_parquet(open_pq, index=False)
            wrote_open = open_pq
            try:
                combined_open.to_csv(open_csv, index=False)
            except Exception:
                pass
        except Exception:
            combined_open.to_csv(open_csv, index=False)
            wrote_open = open_csv
            try:
                if open_pq.exists():
                    open_pq.unlink()
            except Exception:
                pass

    appended_rows = 0
    if not current.empty:
        history_rows = current[[col for col in _HISTORY_COLS if col in current.columns]].copy()
        if not history_rows.empty:
            if hist_csv.exists():
                history_rows.to_csv(hist_csv, mode="a", header=False, index=False)
            else:
                history_rows.to_csv(hist_csv, index=False)
            appended_rows = int(len(history_rows))

    return {
        "opening_rows": int(len(combined_open)),
        "opening_path": str(wrote_open) if wrote_open is not None else None,
        "history_appended_rows": int(appended_rows),
        "history_path": str(hist_csv),
    }


def _load_opening_join_frame(target_date: Any, *, raw_dir: Path) -> pd.DataFrame:
    date_str = _normalize_date_str(target_date)
    candidates = [
        raw_dir / f"odds_nba_player_props_opening_{date_str}.parquet",
        raw_dir / f"odds_nba_player_props_opening_{date_str}.csv",
        raw_dir / f"odds_nba_player_props_history_{date_str}.parquet",
        raw_dir / f"odds_nba_player_props_history_{date_str}.csv",
        raw_dir / f"odds_nba_player_props_{date_str}.parquet",
        raw_dir / f"odds_nba_player_props_{date_str}.csv",
    ]

    raw = pd.DataFrame()
    for candidate in candidates:
        raw = _read_frame(candidate, usecols=_OPENING_COLS)
        if raw.empty:
            continue
        raw = _filter_to_slate_date(raw, date_str)
        if not raw.empty:
            break
    if raw.empty:
        return pd.DataFrame(columns=[
            "bookmaker",
            "market",
            "name_key",
            "side",
            "open_line",
            "open_price",
            "open_snapshot_ts",
        ])

    opening = _select_canonical_open(raw)
    if opening.empty:
        return pd.DataFrame(columns=[
            "bookmaker",
            "market",
            "name_key",
            "side",
            "open_line",
            "open_price",
            "open_snapshot_ts",
        ])

    opening = opening.copy()
    opening["name_key"] = opening.get("player_name").astype(str).map(_norm_name_key)
    opening["side"] = opening.get("outcome_name").astype(str).map(_map_side)
    opening["open_line"] = pd.to_numeric(opening.get("point"), errors="coerce")
    opening["open_price"] = pd.to_numeric(opening.get("price"), errors="coerce")
    opening = opening.rename(columns={"snapshot_ts": "open_snapshot_ts"})
    opening["bookmaker"] = opening.get("bookmaker").astype(str).str.strip().str.lower()
    opening["market"] = opening.get("market").astype(str).str.strip().str.lower()
    opening = opening[
        opening["bookmaker"].ne("")
        & opening["market"].ne("")
        & opening["name_key"].ne("")
        & opening["side"].notna()
    ].copy()
    keep_cols = [
        "bookmaker",
        "market",
        "name_key",
        "side",
        "open_line",
        "open_price",
        "open_snapshot_ts",
    ]
    return opening[keep_cols].drop_duplicates(subset=["bookmaker", "market", "name_key", "side"], keep="first")


def enrich_props_edges_with_tracking(
    edges_df: pd.DataFrame,
    target_date: Any,
    *,
    raw_dir: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if edges_df is None or not isinstance(edges_df, pd.DataFrame) or edges_df.empty:
        return pd.DataFrame() if edges_df is None else edges_df, {
            "rows_with_opening": 0,
            "significant_rows": 0,
            "fast_rows": 0,
        }

    opening = _load_opening_join_frame(target_date, raw_dir=raw_dir)
    out = edges_df.copy()
    out["_name_key"] = out.get("player_name", out.get("player", pd.Series("", index=out.index))).astype(str).map(_norm_name_key)
    out["_market_key"] = out.get("stat", out.get("market", pd.Series("", index=out.index))).astype(str).str.strip().str.lower().map(_STAT_TO_MARKET)
    out["_side_key"] = out.get("side", pd.Series("", index=out.index)).astype(str).map(_map_side)
    out["_bookmaker_key"] = out.get("bookmaker", out.get("book", pd.Series("", index=out.index))).astype(str).str.strip().str.lower()

    merged = out
    if not opening.empty:
        opening_join = opening.rename(
            columns={
                "bookmaker": "_bookmaker_key",
                "market": "_market_key",
                "name_key": "_name_key",
                "side": "_side_key",
            }
        )
        merged = out.merge(
            opening_join,
            on=["_bookmaker_key", "_market_key", "_name_key", "_side_key"],
            how="left",
            suffixes=("", "_tracked"),
        )
    else:
        merged = out.copy()
        merged["open_line"] = pd.Series(np.nan, index=merged.index)
        merged["open_price"] = pd.Series(np.nan, index=merged.index)
        merged["open_snapshot_ts"] = pd.Series(np.nan, index=merged.index)

    for col in ("open_line", "open_price", "open_snapshot_ts"):
        tracked_col = f"{col}_tracked"
        if tracked_col in merged.columns:
            merged[col] = merged.get(col).combine_first(merged[tracked_col])
            merged.drop(columns=[tracked_col], inplace=True, errors="ignore")

    current_price = pd.to_numeric(_column_series(merged, "price"), errors="coerce")
    current_implied = pd.to_numeric(_column_series(merged, "implied_prob"), errors="coerce")
    current_implied = current_implied.combine_first(_american_to_implied(current_price))
    open_price = pd.to_numeric(_column_series(merged, "open_price"), errors="coerce")
    open_implied = pd.to_numeric(_column_series(merged, "open_implied_prob"), errors="coerce")
    open_implied = open_implied.combine_first(_american_to_implied(open_price))
    merged["open_implied_prob"] = open_implied

    current_line = pd.to_numeric(_column_series(merged, "line"), errors="coerce")
    open_line = pd.to_numeric(_column_series(merged, "open_line"), errors="coerce")
    existing_line_move = pd.to_numeric(_column_series(merged, "line_move"), errors="coerce")
    derived_line_move = current_line - open_line
    stat_series = _column_series(merged, "stat", _column_series(merged, "market", "")).astype(str).str.strip().str.lower()
    yes_no_mask = stat_series.isin({"dd", "td"})
    derived_line_move.loc[yes_no_mask] = np.nan
    merged["line_move"] = existing_line_move.combine_first(derived_line_move)

    existing_implied_move = pd.to_numeric(_column_series(merged, "implied_move"), errors="coerce")
    derived_implied_move = current_implied - open_implied
    merged["implied_move"] = existing_implied_move.combine_first(derived_implied_move)

    line_move_abs = pd.to_numeric(_column_series(merged, "line_move"), errors="coerce").abs()
    implied_move_abs = pd.to_numeric(_column_series(merged, "implied_move"), errors="coerce").abs()
    significant_mask = (line_move_abs >= 0.5) | (implied_move_abs >= 0.02)
    fast_mask = (line_move_abs >= 1.0) | (implied_move_abs >= 0.05)

    merged["movement_significant"] = significant_mask.fillna(False)
    merged["movement_tier"] = np.where(fast_mask.fillna(False), "fast", np.where(significant_mask.fillna(False), "significant", ""))

    rows_with_opening = int((pd.to_numeric(_column_series(merged, "open_price"), errors="coerce").notna() | pd.to_numeric(_column_series(merged, "open_line"), errors="coerce").notna()).sum())
    significant_rows = int(significant_mask.fillna(False).sum())
    fast_rows = int(fast_mask.fillna(False).sum())

    merged.drop(columns=["_name_key", "_market_key", "_side_key", "_bookmaker_key"], inplace=True, errors="ignore")
    return merged, {
        "rows_with_opening": rows_with_opening,
        "significant_rows": significant_rows,
        "fast_rows": fast_rows,
    }


def write_props_movement_signals(
    edges_df: pd.DataFrame,
    target_date: Any,
    *,
    processed_dir: Path,
) -> dict[str, Any]:
    date_str = _normalize_date_str(target_date)
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_path = processed_dir / f"props_movement_signals_{date_str}.csv"

    source = edges_df.copy() if isinstance(edges_df, pd.DataFrame) else pd.DataFrame()
    if not source.empty:
        line_move_abs = pd.to_numeric(_column_series(source, "line_move"), errors="coerce").abs()
        implied_move_abs = pd.to_numeric(_column_series(source, "implied_move"), errors="coerce").abs()
        signal_mask = _column_series(source, "movement_significant", False).astype(bool)
        signal_mask = signal_mask | (line_move_abs >= 0.5) | (implied_move_abs >= 0.02)
        signals = source.loc[signal_mask].copy()
        if not signals.empty:
            signals["_sort_abs_line_move"] = line_move_abs.loc[signals.index].fillna(0.0)
            signals["_sort_abs_implied_move"] = implied_move_abs.loc[signals.index].fillna(0.0)
            signals = signals.sort_values(["_sort_abs_line_move", "_sort_abs_implied_move", "ev"], ascending=[False, False, False], na_position="last")
            signals.drop(columns=["_sort_abs_line_move", "_sort_abs_implied_move"], inplace=True, errors="ignore")
    else:
        signals = pd.DataFrame()

    signals.to_csv(out_path, index=False)
    return {
        "signals_rows": int(len(signals)),
        "signals_path": str(out_path),
    }


def sync_props_movement_artifacts(
    *,
    date_str: Any,
    snapshot_path: Path,
    edges_path: Path,
    raw_dir: Path,
    processed_dir: Path,
) -> dict[str, Any]:
    snapshot_df = _read_frame(snapshot_path)
    tracking_meta = persist_props_snapshot_tracking(snapshot_df, date_str, raw_dir=raw_dir)

    edges_df = _read_frame(edges_path)
    enriched_edges, enrich_meta = enrich_props_edges_with_tracking(edges_df, date_str, raw_dir=raw_dir)
    edges_path.parent.mkdir(parents=True, exist_ok=True)
    enriched_edges.to_csv(edges_path, index=False)

    signals_meta = write_props_movement_signals(enriched_edges, date_str, processed_dir=processed_dir)

    return {
        **tracking_meta,
        **enrich_meta,
        **signals_meta,
    }