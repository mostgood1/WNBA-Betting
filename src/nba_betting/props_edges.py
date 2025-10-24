from __future__ import annotations

import pandas as pd
import numpy as np
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple

from .config import paths
# from .props_train import predict_props  # MOVED TO CONDITIONAL - requires sklearn
from .props_calibration import compute_biases as _compute_biases, apply_biases as _apply_biases
from .props_features import build_features_for_date
# from .props_train import _load_features as _load_props_features  # MOVED TO CONDITIONAL - requires sklearn
from .odds_api import OddsApiConfig, backfill_player_props, fetch_player_props_current
from .teams import to_tricode as _tri, normalize_team as _norm_team
from .odds_bovada import fetch_bovada_player_props_current


# Map OddsAPI player markets to our prediction columns
MARKET_TO_STAT = {
    "player_points": "pts",
    "player_rebounds": "reb",
    "player_assists": "ast",
    "player_threes": "threes",
    "player_points_rebounds_assists": "pra",
    # Additional OddsAPI markets
    "player_points_rebounds": "pr",
    "player_points_assists": "pa",
    "player_rebounds_assists": "ra",
    "player_steals": "stl",
    "player_blocks": "blk",
    "player_turnovers": "tov",
    "player_double_double": "dd",
    "player_triple_double": "td",
}


def _norm_name(s: str) -> str:
    if s is None:
        return ""
    t = str(s)
    # strip team suffixes in parentheses if present
    if "(" in t:
        t = t.split("(", 1)[0]
    # normalize punctuation/hyphens and quotes
    t = t.replace("-", " ")
    t = t.replace(".", "").replace("'", "").replace(",", " ").strip()
    # remove common suffix tokens
    for suf in [" JR", " SR", " II", " III", " IV"]:
        if t.upper().endswith(suf):
            t = t[: -len(suf)]
    try:
        t = t.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    return t.upper().strip()


def _short_key(s: str) -> str:
    """Lightweight key: LASTNAME + FIRST_INITIAL, uppercase, ascii-only."""
    if not s:
        return ""
    s2 = _norm_name(s)
    parts = [p for p in s2.replace("-", " ").split() if p]
    if not parts:
        return s2
    last = parts[-1]
    first_initial = parts[0][0] if parts and parts[0] else ""
    return f"{last}{first_initial}"


def _american_implied_prob(price: float) -> float:
    try:
        a = float(price)
    except Exception:
        return np.nan
    if a > 0:
        return 100.0 / (a + 100.0)
    else:
        return (-a) / ((-a) + 100.0)


def _ev_per_unit(price: float, win_prob: float) -> float:
    # Expected value per 1 unit stake using American odds
    try:
        a = float(price)
        p = float(win_prob)
    except Exception:
        return np.nan
    if a > 0:
        return p * (a / 100.0) - (1 - p) * 1.0
    else:
        return p * (100.0 / (-a)) - (1 - p) * 1.0


@dataclass
class SigmaConfig:
    pts: float = 7.5
    reb: float = 3.0
    ast: float = 2.5
    threes: float = 1.3
    pra: float = 9.0
    # Optional defaults for additional stats
    stl: float = 1.2
    blk: float = 1.3
    tov: float = 1.5


def _odds_for_date_from_saved(date: datetime) -> pd.DataFrame:
    # Load saved odds and filter to commence_time date
    raw_pq = paths.data_raw / "odds_nba_player_props.parquet"
    raw_csv = paths.data_raw / "odds_nba_player_props.csv"
    df = None
    if raw_pq.exists():
        try:
            df = pd.read_parquet(raw_pq)
        except Exception:
            df = None
    if df is None and raw_csv.exists():
        try:
            df = pd.read_csv(raw_csv)
        except Exception:
            df = None
    if df is None or df.empty:
        return pd.DataFrame()
    # Filter by commence_time date
    if "commence_time" in df.columns:
        dt_utc = pd.to_datetime(df["commence_time"], errors="coerce", utc=True)
        # Convert to US/Eastern date for correct slate filtering
        def _to_et_date(ts):
            try:
                return ts.tz_convert("America/New_York").date()
            except Exception:
                try:
                    return ts.tz_convert("US/Eastern").date()
                except Exception:
                    # Fallback: approximate DST offset by month
                    month = int(ts.month)
                    offset = 4 if 3 <= month <= 11 else 5
                    return (ts - pd.Timedelta(hours=offset)).date()
        et_dates = dt_utc.map(_to_et_date)
        df = df.loc[et_dates == pd.to_datetime(date).date()].copy()
    return df


def _fetch_odds_for_date(date: datetime, mode: str, api_key: Optional[str]) -> pd.DataFrame:
    if not api_key:
        return pd.DataFrame()
    cfg = OddsApiConfig(api_key=api_key)
    # Try historical first with a late timestamp on that date (UTC)
    if mode in ("auto", "historical"):
        # Use a late-evening Eastern timestamp for the snapshot to ensure late games are included
        base = pd.Timestamp(pd.to_datetime(date).date())
        try:
            # Localize to US/Eastern then convert to UTC for the historical API 'date' parameter
            ts_et = base.tz_localize("America/New_York").replace(hour=23, minute=59, second=0)
        except Exception:
            try:
                ts_et = base.tz_localize("US/Eastern").replace(hour=23, minute=59, second=0)
            except Exception:
                # Fallback naive -> approximate by subtracting 4h (DST months) or 5h otherwise
                month = int(base.month)
                offset = 4 if 3 <= month <= 11 else 5
                ts_et = (base.replace(hour=23, minute=59, second=0) - pd.Timedelta(hours=offset)).tz_localize("UTC")
        ts = ts_et.tz_convert("UTC").to_pydatetime()
        try:
            df = backfill_player_props(cfg, ts, verbose=False)
            # Filter to date in commence_time in case file had other days
            if df is not None and not df.empty:
                dt_utc = pd.to_datetime(df["commence_time"], errors="coerce", utc=True)
                def _to_et_date(ts):
                    try:
                        return ts.tz_convert("America/New_York").date()
                    except Exception:
                        try:
                            return ts.tz_convert("US/Eastern").date()
                        except Exception:
                            month = int(ts.month)
                            offset = 4 if 3 <= month <= 11 else 5
                            return (ts - pd.Timedelta(hours=offset)).date()
                et_dates = dt_utc.map(_to_et_date)
                df = df.loc[et_dates == pd.to_datetime(date).date()].copy()
                if not df.empty:
                    return df
        except Exception:
            pass
        if mode == "historical":
            return pd.DataFrame()
    # Fallback to current event odds for the calendar date
    if mode in ("auto", "current"):
        try:
            df = fetch_player_props_current(cfg, pd.to_datetime(date))
            return df if df is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def compute_props_edges(
    date: str,
    sigma: SigmaConfig,
    use_saved: bool = True,
    mode: str = "auto",
    api_key: Optional[str] = None,
    source: str = "auto",
    predictions_path: Optional[str] = None,
    from_file_only: bool = False,
    exclude_injured: bool = True,
) -> pd.DataFrame:
    """Compute model edges/EV against OddsAPI player props for a given date.

    Returns a DataFrame with: date, player_name, stat, side, line, price, implied_prob, model_prob, edge, ev, bookmaker.
    """
    target_date = pd.to_datetime(date)
    # Load predictions for slate, preferring precomputed CSV when available
    preds: pd.DataFrame
    if predictions_path is None:
        # default location written by predict-props CLI
        predictions_path = str(paths.data_processed / f"props_predictions_{pd.to_datetime(target_date).date()}.csv")
    preds = pd.DataFrame()
    try:
        p = Path(predictions_path)
        if not p.is_absolute():
            p = paths.root / p
        if p.exists():
            preds = pd.read_csv(p)
    except Exception:
        preds = pd.DataFrame()
    if preds is None or preds.empty:
        # If we are restricted to file-only mode, do NOT run models server-side
        if from_file_only:
            return pd.DataFrame()
        # Otherwise compute predictions locally using pure ONNX path (no sklearn)
        from .props_onnx_pure import predict_props_pure_onnx  # Pure ONNX inference
        feats = build_features_for_date(target_date)
        preds = predict_props_pure_onnx(feats)
        # Light bias calibration based on recent recon (safe default)
        try:
            biases = _compute_biases(anchor_date=str(pd.to_datetime(target_date).date()), window_days=7)
            preds = _apply_biases(preds, biases)
        except Exception:
            pass
    # Prepare prediction columns
    pred_map = {
        "pts": "pred_pts",
        "reb": "pred_reb",
        "ast": "pred_ast",
        "threes": "pred_threes",
        "pra": "pred_pra",
        # newly supported
        "stl": "pred_stl",
        "blk": "pred_blk",
        "tov": "pred_tov",
    }
    for need in ["player_id", "player_name"] + list(pred_map.values()):
        if need not in preds.columns:
            raise ValueError(f"Predictions missing column: {need}")

    preds = preds.copy()
    preds["name_key"] = preds["player_name"].astype(str).map(_norm_name)
    preds["short_key"] = preds["player_name"].astype(str).map(_short_key)

    # Load odds (OddsAPI or Bovada based on source)
    odds = pd.DataFrame()
    src = (source or "auto").lower()
    if src == "oddsapi":
        if use_saved:
            odds = _odds_for_date_from_saved(target_date)
        if odds is None or odds.empty:
            fetched = _fetch_odds_for_date(target_date, mode=mode, api_key=api_key)
            odds = fetched if fetched is not None else pd.DataFrame()
    elif src == "bovada":
        odds = fetch_bovada_player_props_current(target_date)
    else:
        # auto: try saved/current OddsAPI first, then fall back to Bovada
        if use_saved:
            odds = _odds_for_date_from_saved(target_date)
        if odds is None or odds.empty:
            fetched = _fetch_odds_for_date(target_date, mode=mode, api_key=api_key)
            odds = fetched if fetched is not None else pd.DataFrame()
        if odds is None or odds.empty:
            odds = fetch_bovada_player_props_current(target_date)
    if odds is None or odds.empty:
        return pd.DataFrame()

    # Optional: exclude injured players from consideration before merging
    if exclude_injured:
        try:
            inj_path = paths.data_raw / "injuries.csv"
            if inj_path.exists():
                inj = pd.read_csv(inj_path)
                # Normalize date and take latest status per player/team up to target date
                if "date" in inj.columns:
                    inj["date"] = pd.to_datetime(inj["date"], errors="coerce").dt.date
                    cutoff = pd.to_datetime(target_date).date()
                    inj = inj[inj["date"].notna()]
                    inj = inj[inj["date"] <= cutoff].copy()
                # Keep relevant columns
                keep = [c for c in ["player","team","status","date"] if c in inj.columns]
                inj = inj[keep].copy() if keep else pd.DataFrame()
                if not inj.empty and "player" in inj.columns and "status" in inj.columns:
                    # Latest record per player+team if team exists, else per player
                    sort_cols = [c for c in ["date"] if c in inj.columns]
                    if sort_cols:
                        inj = inj.sort_values(sort_cols)
                    grp_cols = [c for c in ["player","team"] if c in inj.columns]
                    if not grp_cols:
                        grp_cols = ["player"]
                    inj_latest = inj.groupby(grp_cols, as_index=False).tail(1)
                    # Normalize names and statuses
                    def _norm(s: str) -> str:
                        return _norm_name(s)
                    inj_latest["name_key"] = inj_latest["player"].astype(str).map(_norm)
                    inj_latest["short_key"] = inj_latest["player"].astype(str).map(_short_key)
                    inj_latest["status_norm"] = inj_latest["status"].astype(str).str.upper()
                    EXCLUDE_STATUSES = {"OUT","DOUBTFUL","SUSPENDED","INACTIVE","REST"}
                    bad = inj_latest[inj_latest["status_norm"].isin(EXCLUDE_STATUSES)].copy()
                    if not bad.empty:
                        bad_name_keys = set(bad["name_key"].dropna().astype(str))
                        bad_short_keys = set(bad["short_key"].dropna().astype(str))
                        # We'll apply once odds are normalized with name_key/short_key
                        _inj_filter = (bad_name_keys, bad_short_keys)
                    else:
                        _inj_filter = None
                else:
                    _inj_filter = None
            else:
                _inj_filter = None
        except Exception:
            _inj_filter = None

    # Normalize odds
    keep_cols = [
        "bookmaker", "bookmaker_title", "market", "outcome_name", "player_name", "point", "price", "commence_time",
        "home_team", "away_team",
    ]
    odds = odds[[c for c in keep_cols if c in odds.columns]].copy()
    # outcome_name is Over/Under, player_name may be in description
    if "player_name" not in odds.columns and "outcome_name" in odds.columns:
        # Some payloads put the player in outcome_name; try to salvage
        odds["player_name"] = odds["outcome_name"].astype(str)
    odds["name_key"] = odds["player_name"].astype(str).map(_norm_name)
    odds["short_key"] = odds["player_name"].astype(str).map(_short_key)
    # Apply injuries filter now that keys exist
    try:
        if exclude_injured and ('_inj_filter' in locals()) and (_inj_filter is not None):
            bad_name_keys, bad_short_keys = _inj_filter
            if bad_name_keys or bad_short_keys:
                odds = odds[~(odds["name_key"].isin(bad_name_keys) | odds["short_key"].isin(bad_short_keys))].copy()
    except Exception:
        pass
    # Side: OVER/UNDER for most markets; YES/NO for double-double/triple-double
    def _map_side(x: str) -> Optional[str]:
        u = str(x).upper()
        if "OVER" in u:
            return "OVER"
        if "UNDER" in u:
            return "UNDER"
        if u in ("YES", "Y"):
            return "YES"
        if u in ("NO", "N"):
            return "NO"
        return None
    odds["side"] = odds["outcome_name"].astype(str).map(_map_side)
    # Ensure team columns exist for event context
    for col in ("home_team","away_team"):
        if col not in odds.columns:
            odds[col] = None
    # Map markets to stat
    odds["stat"] = odds["market"].map(MARKET_TO_STAT)
    # Keep rows depending on market type: dd/td have no point/line
    def _row_ok(row) -> bool:
        if pd.isna(row.get("name_key")) or pd.isna(row.get("stat")) or pd.isna(row.get("side")):
            return False
        if row.get("stat") in ("dd", "td"):
            return not pd.isna(row.get("price"))
        return (not pd.isna(row.get("point"))) and (not pd.isna(row.get("price")))
    odds = odds[odds.apply(_row_ok, axis=1)].copy()

    # Merge odds with predictions on name_key
    merged = odds.merge(
        preds[["name_key", "player_id", "player_name", "team", pred_map["pts"], pred_map["reb"], pred_map["ast"], pred_map["threes"], pred_map["pra"]]],
        on="name_key", how="left", suffixes=("", "_pred")
    )
    # Second-pass resolve using short key for any unmatched players
    unmatched = merged[merged["player_id"].isna()].copy()
    if not unmatched.empty:
        # Merge by short key; manage name collisions with suffixes and prefer prediction player_name
        alt = odds.merge(
            preds[["short_key", "player_id", "player_name", "team", pred_map["pts"], pred_map["reb"], pred_map["ast"], pred_map["threes"], pred_map["pra"]]].rename(columns={"short_key": "short_key_pred"}),
            left_on="short_key", right_on="short_key_pred", how="left", suffixes=("", "_pred")
        )
        # Consolidate player_name from predictions when available, else keep odds name
        if "player_name_pred" in alt.columns:
            alt["player_name_join"] = alt["player_name_pred"].fillna(alt.get("player_name"))
        else:
            alt["player_name_join"] = alt.get("player_name")
        keep = [
            "short_key", "player_id", "player_name_join", "team",
            pred_map["pts"], pred_map["reb"], pred_map["ast"], pred_map["threes"], pred_map["pra"]
        ]
        keep = [c for c in keep if c in alt.columns]
        alt = alt[keep].copy()
        alt = alt.rename(columns={"player_name_join": "player_name"})
        merged = merged.merge(alt.add_suffix("_alt"), left_on="short_key", right_on="short_key_alt", how="left")
        # Fill missing fields from alt
        for col in ["player_id", "player_name", "team", "model_mean"]:
            if col == "model_mean":
                # will be computed after selecting stat
                continue
            base_col = col
            alt_col = f"{col}_alt"
            if base_col in merged.columns and alt_col in merged.columns:
                merged[base_col] = merged[base_col].fillna(merged[alt_col])

    # Third-pass: roster-assisted resolution by event team context -> join predictions by player_id
    try:
        still_unmatched = merged[merged["player_id"].isna()].copy()
        if not still_unmatched.empty:
            # Load latest rosters file from processed
            roster = pd.DataFrame()
            try:
                proc = paths.data_processed
                # Prefer files like rosters_*.csv; pick most recent by modified time
                cands = sorted(proc.glob("rosters_*.csv"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
                if not cands:
                    cands = sorted(proc.glob("*roster*.csv"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
                if cands:
                    roster = pd.read_csv(cands[0])
            except Exception:
                roster = pd.DataFrame()
            if roster is not None and not roster.empty:
                # Normalize roster columns heuristically
                def _pick(col_opts):
                    for c in col_opts:
                        if c in roster.columns:
                            return c
                    return None
                name_col = _pick(["PLAYER","player","Player","NAME","name"]) or "PLAYER"
                id_col = _pick(["PLAYER_ID","player_id","PlayerID","id","ID"])
                team_col = _pick(["TEAM","team","Team","TEAM_ABBREVIATION","TEAM_ABBR","tricode","TRICODE","TEAM_TRICODE"])            
                cols = {}
                if name_col and name_col in roster.columns:
                    cols["player_name"] = roster[name_col].astype(str)
                if id_col and id_col in roster.columns:
                    cols["player_id"] = pd.to_numeric(roster[id_col], errors="coerce")
                if team_col and team_col in roster.columns:
                    cols["team"] = roster[team_col].astype(str)
                r = pd.DataFrame(cols)
                if not r.empty and ("player_name" in r.columns):
                    r["name_key"] = r["player_name"].astype(str).map(_norm_name)
                    r["short_key"] = r["player_name"].astype(str).map(_short_key)
                    # Join by name_key first
                    enrich = still_unmatched.merge(r[["name_key","player_id","team"]], on="name_key", how="left", suffixes=("","_r"))
                    # For remaining, try short_key
                    rem = enrich[enrich["player_id"].isna()].copy()
                    if not rem.empty:
                        enrich2 = rem.merge(r[["short_key","player_id","team"]].rename(columns={"short_key":"short_key_r"}), left_on="short_key", right_on="short_key_r", how="left")
                        for col in ("player_id","team"):
                            col_r = f"{col}_y"
                            if col in enrich.columns and col_r in enrich2.columns:
                                enrich.loc[rem.index, col] = enrich.loc[rem.index, col].fillna(enrich2[col_r])
                    # Team-tricode alignment: prefer roster match that aligns with event teams
                    def _to_tri_team(x: str | None) -> str:
                        try:
                            return _tri(_norm_team(str(x))) if x is not None else ""
                        except Exception:
                            return (str(x).strip().upper() if x else "")
                    if "home_team" in enrich.columns and "away_team" in enrich.columns:
                        enrich["home_tri"] = enrich["home_team"].astype(str).map(_to_tri_team)
                        enrich["away_tri"] = enrich["away_team"].astype(str).map(_to_tri_team)
                        enrich["team_tri_r"] = enrich.get("team").astype(str).map(_to_tri_team)
                        # In cases where roster team doesn't match either event team, null it out to avoid mis-join
                        ok_mask = (enrich["team_tri_r"] == enrich["home_tri"]) | (enrich["team_tri_r"] == enrich["away_tri"]) | (enrich["team_tri_r"] == "")
                        enrich.loc[~ok_mask, ["player_id","team"]] = np.nan
                    # Fill merged with roster-assisted ids/teams
                    for col in ["player_id","team"]:
                        if col in enrich.columns:
                            merged[col] = merged[col].fillna(enrich[col])
                    # If we resolved player_id, bring in prediction columns via id join
                    need = merged[merged["player_id"].notna()].index
                    if len(need) > 0:
                        pred_cols = ["player_id", "player_name", "team", pred_map["pts"], pred_map["reb"], pred_map["ast"], pred_map["threes"], pred_map["pra"]]
                        pred_cols = [c for c in pred_cols if c in preds.columns]
                        by_id = preds[pred_cols].drop_duplicates("player_id")
                        merged = merged.merge(by_id.add_suffix("_pid"), left_on="player_id", right_on="player_id_pid", how="left")
                        # Backfill any missing prediction fields from the _pid columns
                        for col in ["player_name","team", pred_map["pts"], pred_map["reb"], pred_map["ast"], pred_map["threes"], pred_map["pra"]]:
                            base = col
                            aux = f"{col}_pid"
                            if base in merged.columns and aux in merged.columns:
                                merged[base] = merged[base].fillna(merged[aux])
    except Exception:
        pass
    # Choose model mean based on stat
    def _select_pred(row) -> float:
        stat = row["stat"]
        # Derived combos from base predictions
        if stat == "pr":
            return (row.get(pred_map["pts"], np.nan)) + (row.get(pred_map["reb"], np.nan))
        if stat == "pa":
            return (row.get(pred_map["pts"], np.nan)) + (row.get(pred_map["ast"], np.nan))
        if stat == "ra":
            return (row.get(pred_map["reb"], np.nan)) + (row.get(pred_map["ast"], np.nan))
        col = pred_map.get(stat)
        val = row.get(col, np.nan)
        if pd.isna(val):
            # try alt merged columns
            alt_col = f"{col}_alt"
            return row.get(alt_col, np.nan)
        return val

    merged["model_mean"] = merged.apply(_select_pred, axis=1)
    # Team-consistency guard: ensure the prediction team matches the event's home or away team (by tricode)
    try:
        def _to_tri_team(x: str | None) -> str:
            try:
                return _tri(_norm_team(str(x))) if x is not None else ""
            except Exception:
                return (str(x).strip().upper() if x else "")
        merged["team_tri"] = merged.get("team").astype(str).map(lambda x: _to_tri_team(x))
        merged["home_tri"] = merged.get("home_team").astype(str).map(lambda x: _to_tri_team(x))
        merged["away_tri"] = merged.get("away_team").astype(str).map(lambda x: _to_tri_team(x))
        # Only filter when we have a non-empty team_tri
        mask_ok = (merged["team_tri"] == "") | (merged["team_tri"].isna()) | (
            (merged["team_tri"] == merged["home_tri"]) | (merged["team_tri"] == merged["away_tri"]) )
        merged = merged[mask_ok].copy()
    except Exception:
        pass
    # Sigma by stat; combos derived assuming independence of components
    def _sigma_for(stat: str) -> float:
        if stat == "pts":
            return sigma.pts
        if stat == "reb":
            return sigma.reb
        if stat == "ast":
            return sigma.ast
        if stat == "threes":
            return sigma.threes
        if stat == "pra":
            return sigma.pra
        if stat == "pr":
            return float(np.sqrt(sigma.pts ** 2 + sigma.reb ** 2))
        if stat == "pa":
            return float(np.sqrt(sigma.pts ** 2 + sigma.ast ** 2))
        if stat == "ra":
            return float(np.sqrt(sigma.reb ** 2 + sigma.ast ** 2))
        if stat == "stl":
            return sigma.stl
        if stat == "blk":
            return sigma.blk
        if stat == "tov":
            return sigma.tov
        return np.nan
    merged["sigma"] = merged["stat"].map(_sigma_for)

    # Model probability for Over: P(X > line) under Normal(mean, sigma)
    from math import erf, sqrt

    def _norm_cdf(x):
        return 0.5 * (1.0 + erf(x / sqrt(2.0)))

    def _prob_over(mean, sigma, line):
        if pd.isna(mean) or pd.isna(sigma) or pd.isna(line) or sigma <= 0:
            return np.nan
        z = (line - mean) / sigma
        # P(X > line) = 1 - CDF(line)
        return 1.0 - _norm_cdf(z)

    merged["line"] = pd.to_numeric(merged.get("point"), errors="coerce")
    merged["price"] = pd.to_numeric(merged.get("price"), errors="coerce")
    # Compute model probability; special handling for YES/NO markets (double/triple-double)
    def _calc_model_prob(r) -> float:
        stat = r.get("stat")
        side = r.get("side")
        if stat in ("dd", "td"):
            # Approximate independence on Pts/Reb/Ast reaching 10+
            mean_pts = r.get(pred_map["pts"], np.nan)
            mean_reb = r.get(pred_map["reb"], np.nan)
            mean_ast = r.get(pred_map["ast"], np.nan)
            vals = [(mean_pts, sigma.pts), (mean_reb, sigma.reb), (mean_ast, sigma.ast)]
            p10 = []
            for m, s in vals:
                if pd.isna(m) or s is None or s <= 0:
                    p10.append(np.nan)
                else:
                    z = (10.0 - float(m)) / float(s)
                    p10.append(1.0 - _norm_cdf(z))
            p1, p2, p3 = p10
            if any(pd.isna(x) for x in (p1, p2, p3)):
                return np.nan
            if stat == "td":
                p_yes = float(p1 * p2 * p3)
            else:
                # at least two of three
                p_yes = float(p1 * p2 + p1 * p3 + p2 * p3 - p1 * p2 * p3)
            if side == "YES":
                return p_yes
            if side == "NO":
                return 1.0 - p_yes
            return np.nan
        # Default OVER/UNDER path
        p_over = _prob_over(r.get("model_mean"), r.get("sigma"), r.get("line"))
        if pd.isna(p_over):
            return np.nan
        return (1.0 - p_over) if side == "UNDER" else p_over
    merged["model_prob"] = merged.apply(_calc_model_prob, axis=1)
    merged["implied_prob"] = merged["price"].map(_american_implied_prob)
    merged["edge"] = merged["model_prob"] - merged["implied_prob"]
    merged["ev"] = merged.apply(lambda r: _ev_per_unit(r["price"], r["model_prob"]), axis=1)

    # Ensure we have a player_name column available; prefer odds name, then prediction name
    if "player_name" not in merged.columns:
        if "player_name_pred" in merged.columns:
            merged["player_name"] = merged["player_name_pred"]
        elif "player_name_alt" in merged.columns:
            merged["player_name"] = merged["player_name_alt"]
        else:
            merged["player_name"] = None

    # Default bookmaker_title if missing
    if "bookmaker_title" not in merged.columns and "bookmaker" in merged.columns:
        merged["bookmaker_title"] = merged["bookmaker"].map(lambda b: "Bovada" if str(b).lower()=="bovada" else None)

    desired_cols = [
        "player_id", "player_name", "team", "stat", "side", "line", "price", "implied_prob", "model_prob", "edge", "ev", "bookmaker", "bookmaker_title", "commence_time",
        "home_team", "away_team"
    ]
    out_cols = [c for c in desired_cols if c in merged.columns]
    if not out_cols:
        # If somehow nothing matches, return empty DataFrame to avoid exceptions
        return pd.DataFrame()
    out = merged[out_cols].copy()
    out.insert(0, "date", pd.to_datetime(target_date).date())
    # De-duplicate final edges across identical player/stat/side/line/price/bookmaker
    dedup_keys = [
        "date", "player_id", "player_name", "team", "stat", "side", "line", "price", "bookmaker", "bookmaker_title", "commence_time"
    ]
    keys = [c for c in dedup_keys if c in out.columns]
    if keys:
        out = out.drop_duplicates(subset=keys, keep="first").reset_index(drop=True)
    out.sort_values(["stat", "edge"], ascending=[True, False], inplace=True)
    return out


def calibrate_sigma_for_date(date: str, window_days: int = 30, min_rows: int = 200, defaults: Optional[SigmaConfig] = None) -> SigmaConfig:
    """Estimate sigma per stat from recent residuals in props_features.

    Uses saved features with actual targets (t_*) over [date - window_days, date - 1],
    predicts with current models, and computes stddev of residuals per stat.
    Falls back to defaults if not enough rows.
    """
    if defaults is None:
        defaults = SigmaConfig()
    try:
        from .props_train import _load_features as _load_props_features  # Optional; may not exist without sklearn
        df = _load_props_features().copy()
    except Exception:
        return defaults
    if "date" not in df.columns:
        return defaults
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    end = pd.to_datetime(date)
    start = end - pd.Timedelta(days=window_days)
    hist = df[(df["date"] >= start) & (df["date"] < end)].copy()
    if hist.empty:
        return defaults
    # Predict on this window
    try:
        from .props_onnx_pure import predict_props_pure_onnx
        preds = predict_props_pure_onnx(hist)
    except Exception:
        return defaults
    stats = {
        "pts": ("t_pts", "pred_pts"),
        "reb": ("t_reb", "pred_reb"),
        "ast": ("t_ast", "pred_ast"),
        "threes": ("t_threes", "pred_threes"),
        "pra": ("t_pra", "pred_pra"),
        # Additional stats if available
        "stl": ("t_stl", "pred_stl"),
        "blk": ("t_blk", "pred_blk"),
        "tov": ("t_tov", "pred_tov"),
    }
    sig = {}
    for k, (tgt, pr) in stats.items():
        if tgt in preds.columns and pr in preds.columns:
            y = pd.to_numeric(preds[tgt], errors="coerce")
            p = pd.to_numeric(preds[pr], errors="coerce")
            m = y.notna() & p.notna()
            if m.sum() >= min_rows:
                sig[k] = float((y[m] - p[m]).std(ddof=1))
    return SigmaConfig(
        pts=sig.get("pts", defaults.pts),
        reb=sig.get("reb", defaults.reb),
        ast=sig.get("ast", defaults.ast),
        threes=sig.get("threes", defaults.threes),
        pra=sig.get("pra", defaults.pra),
        stl=sig.get("stl", defaults.stl),
        blk=sig.get("blk", defaults.blk),
        tov=sig.get("tov", defaults.tov),
    )
