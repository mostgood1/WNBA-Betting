from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd

from .config import paths
from .player_names import normalize_player_name_key


def _norm_player_key(x: Any) -> str:
    return normalize_player_name_key(x, case="upper")


def _to_float(x: Any) -> float:
    try:
        v = float(x)
        return float(v) if np.isfinite(v) else 0.0
    except Exception:
        return 0.0


@dataclass(frozen=True)
class PlayerPriorsConfig:
    days_back: int = 21
    min_games: int = 3
    # players with fewer than min_minutes_avg are treated as fringe
    min_minutes_avg: float = 4.0


@dataclass
class PlayerPriors:
    """Per-player priors keyed by (TEAM_TRI, PLAYER_KEY).

    Values represent expected *per-game* totals for a nominal game given minutes.
    We store per-minute rates for robustness and recompute expected totals using sim minutes.
    """

    config: PlayerPriorsConfig
    # (team, player_key) -> {'min_mu':..., 'pts_pm':..., ...}
    rates: Dict[Tuple[str, str], Dict[str, float]]
    # (team, player_key) -> sample size (games)
    games: Dict[Tuple[str, str], int]

    def rate(self, team: str, player_name: str, key: str) -> Dict[str, float]:
        k = (str(team or "").strip().upper(), key or _norm_player_key(player_name))
        return self.rates.get(k, {})

    def games_played(self, team: str, player_name: str, key: str) -> int:
        k = (str(team or "").strip().upper(), key or _norm_player_key(player_name))
        return int(self.games.get(k, 0))


_PLAYER_LOGS_CACHE: Optional[pd.DataFrame] = None


def _load_boxscores_history_as_player_logs() -> pd.DataFrame:
    """Fallback: derive a player-logs-like table from boxscores history.

    Uses data/processed/boxscores_history.parquet (preferred) or boxscores_history.csv.
    Output schema matches the subset used by compute_player_priors.
    """
    p_parquet = paths.data_processed / "boxscores_history.parquet"
    p_csv = paths.data_processed / "boxscores_history.csv"
    try:
        if p_parquet.exists():
            df = pd.read_parquet(p_parquet)
        elif p_csv.exists():
            df = pd.read_csv(p_csv)
        else:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    # Normalize expected columns
    if "date" in df.columns:
        df["GAME_DATE"] = pd.to_datetime(df["date"], errors="coerce")
    elif "GAME_DATE" in df.columns:
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    else:
        df["GAME_DATE"] = pd.NaT

    if "TEAM_ABBREVIATION" in df.columns:
        df["TEAM_ABBREVIATION"] = df["TEAM_ABBREVIATION"].astype(str).str.strip().str.upper()

    if "PLAYER_NAME" in df.columns:
        df["PLAYER_KEY"] = df["PLAYER_NAME"].map(_norm_player_key)

    # Ensure numeric columns used in priors
    for c in ("MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV", "FG3M", "FG3A", "FGA", "FGM", "FTA", "FTM", "PF"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df[c] = 0

    keep = ["GAME_DATE", "TEAM_ABBREVIATION", "PLAYER_NAME", "PLAYER_KEY", "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV", "FG3M", "FG3A", "FGA", "FGM", "FTA", "FTM", "PF"]
    keep = [c for c in keep if c in df.columns]
    return df[keep].copy()


def _load_player_logs() -> pd.DataFrame:
    global _PLAYER_LOGS_CACHE
    if isinstance(_PLAYER_LOGS_CACHE, pd.DataFrame) and not _PLAYER_LOGS_CACHE.empty:
        return _PLAYER_LOGS_CACHE

    p = paths.data_processed / "player_logs.csv"
    if not p.exists():
        # Fallback to boxscores history (ESPN-backed ingestion)
        _PLAYER_LOGS_CACHE = _load_boxscores_history_as_player_logs()
        return _PLAYER_LOGS_CACHE

    df = pd.read_csv(p)
    if not isinstance(df, pd.DataFrame) or df.empty:
        _PLAYER_LOGS_CACHE = _load_boxscores_history_as_player_logs()
        return _PLAYER_LOGS_CACHE

    # Normalize expected columns
    if "GAME_DATE" in df.columns:
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    if "TEAM_ABBREVIATION" in df.columns:
        df["TEAM_ABBREVIATION"] = df["TEAM_ABBREVIATION"].astype(str).str.strip().str.upper()
    if "PLAYER_NAME" in df.columns:
        df["PLAYER_KEY"] = df["PLAYER_NAME"].map(_norm_player_key)

    # Ensure numeric columns
    for c in ("MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV", "FG3M", "FG3A", "FGA", "FGM", "FTA", "FTM", "PF"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    _PLAYER_LOGS_CACHE = df
    return _PLAYER_LOGS_CACHE


def compute_player_priors(date_str: str, cfg: Optional[PlayerPriorsConfig] = None) -> PlayerPriors:
    """Compute recent player priors from league-wide player game logs.

    Uses data/processed/player_logs.csv (nba_api LeagueGameLog).

    Output: per-player per-minute rates for all key counting stats we simulate.
    """
    cfg = cfg or PlayerPriorsConfig()

    cutoff = pd.to_datetime(date_str, errors="coerce")
    if pd.isna(cutoff):
        cutoff = pd.Timestamp.utcnow().normalize()
    start = cutoff - pd.Timedelta(days=int(cfg.days_back))

    df = _load_player_logs()
    if df.empty:
        return PlayerPriors(config=cfg, rates={}, games={})

    if "GAME_DATE" not in df.columns or "TEAM_ABBREVIATION" not in df.columns or "PLAYER_KEY" not in df.columns:
        return PlayerPriors(config=cfg, rates={}, games={})

    use = df[(df["GAME_DATE"].notna()) & (df["GAME_DATE"] >= start) & (df["GAME_DATE"] <= cutoff)].copy()
    if use.empty:
        return PlayerPriors(config=cfg, rates={}, games={})

    # Keep only games where the player actually played.
    if "MIN" in use.columns:
        use = use[use["MIN"].fillna(0.0) > 0.0]
    if use.empty:
        return PlayerPriors(config=cfg, rates={}, games={})

    stat_cols = {
        "min": "MIN",
        "pts": "PTS",
        "reb": "REB",
        "ast": "AST",
        "stl": "STL",
        "blk": "BLK",
        "tov": "TOV",
        "threes": "FG3M",
        "threes_att": "FG3A",
        "fga": "FGA",
        "fgm": "FGM",
        "fta": "FTA",
        "ftm": "FTM",
        "pf": "PF",
    }

    keep = ["TEAM_ABBREVIATION", "PLAYER_KEY"] + [c for c in stat_cols.values() if c in use.columns]
    use = use[keep].copy()

    # Aggregate by player-team.
    g = use.groupby(["TEAM_ABBREVIATION", "PLAYER_KEY"], as_index=False)

    agg: dict[str, str] = {}
    for out_k, c in stat_cols.items():
        if c in use.columns:
            agg[c] = "mean"
    out = g.agg(agg)

    # Games played per player-team in this window.
    # (Avoid reset_index(name=...) for pandas compatibility.)
    games = use.groupby(["TEAM_ABBREVIATION", "PLAYER_KEY"]).size().reset_index()
    games = games.rename(columns={0: "games", "size": "games"})
    out = out.merge(games, on=["TEAM_ABBREVIATION", "PLAYER_KEY"], how="left")

    # Build per-minute rates.
    rates: Dict[Tuple[str, str], Dict[str, float]] = {}
    games_map: Dict[Tuple[str, str], int] = {}

    for _, r in out.iterrows():
        team = str(r.get("TEAM_ABBREVIATION") or "").strip().upper()
        pkey = str(r.get("PLAYER_KEY") or "").strip().upper()
        if not team or not pkey:
            continue

        gp = int(r.get("games") or 0)
        games_map[(team, pkey)] = gp

        min_mu = _to_float(r.get("MIN"))
        if gp < int(cfg.min_games) or min_mu < float(cfg.min_minutes_avg):
            # still store minutes; other rates are too noisy
            rates[(team, pkey)] = {"min_mu": float(max(0.0, min_mu))}
            continue

        denom = max(1e-6, float(min_mu))
        rr: Dict[str, float] = {"min_mu": float(max(0.0, min_mu))}

        # Store per-minute rates for all stat categories.
        for name, col in stat_cols.items():
            if col == "MIN" or col not in out.columns:
                continue
            v_mu = _to_float(r.get(col))
            rr[f"{name}_pm"] = float(max(0.0, v_mu / denom))

        # Derived tendencies
        th_a = rr.get("threes_att_pm")
        fga = rr.get("fga_pm")
        if th_a is not None and fga is not None and fga > 0:
            rr["three_rate"] = float(max(0.0, min(1.0, th_a / fga)))

        rates[(team, pkey)] = rr

    return PlayerPriors(config=cfg, rates=rates, games=games_map)


def write_player_priors_snapshot(priors: PlayerPriors, date_str: str) -> Optional[pd.DataFrame]:
    """Write a processed CSV snapshot for transparency/debugging."""
    try:
        rows = []
        for (team, pkey), rr in priors.rates.items():
            row = {"team": team, "player_key": pkey, "games": int(priors.games.get((team, pkey), 0))}
            row.update({k: float(v) for k, v in rr.items() if v is not None})
            rows.append(row)
        if not rows:
            return None
        df = pd.DataFrame(rows)
        out_dir = paths.data_processed
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"player_priors_{date_str}_w{priors.config.days_back}.csv"
        df.to_csv(out_path, index=False)
        return df
    except Exception:
        return None
