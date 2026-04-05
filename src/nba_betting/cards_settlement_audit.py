from __future__ import annotations

from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Tuple

import pandas as pd

from .config import paths
from .player_names import normalize_player_name_key, short_player_key


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _load_player_logs_date_set() -> set[str]:
    player_logs_path = paths.data_processed / "player_logs.csv"
    if not player_logs_path.exists():
        return set()
    try:
        sample = pd.read_csv(player_logs_path, usecols=lambda col: str(col).strip().lower() in {"game_date", "date"})
    except Exception:
        return set()
    if sample.empty:
        return set()
    for col in ("GAME_DATE", "game_date", "date", "DATE"):
        if col in sample.columns:
            try:
                return set(pd.to_datetime(sample[col], errors="coerce").dt.strftime("%Y-%m-%d").dropna().astype(str))
            except Exception:
                return set()
    return set()


def _load_props_actuals_parquet_date_set() -> set[str]:
    parquet_path = paths.data_processed / "props_actuals.parquet"
    if not parquet_path.exists():
        return set()
    try:
        sample = pd.read_parquet(parquet_path, columns=["date"])
    except Exception:
        return set()


def _load_roster_context(season: int) -> tuple[set[tuple[str, str]], set[tuple[str, int]]]:
    roster_path = paths.data_processed / f"rosters_{int(season)}.csv"
    if not roster_path.exists():
        return set(), set()
    try:
        roster_df = pd.read_csv(roster_path)
    except Exception:
        return set(), set()
    if roster_df.empty:
        return set(), set()

    cols = {str(col).strip().upper(): str(col) for col in roster_df.columns}
    name_col = cols.get("PLAYER") or cols.get("PLAYER_NAME")
    id_col = cols.get("PLAYER_ID")
    team_col = cols.get("TEAM_ABBREVIATION") or cols.get("TEAM")
    if not name_col or not team_col:
        return set(), set()

    roster_name_keys: set[tuple[str, str]] = set()
    roster_id_keys: set[tuple[str, int]] = set()
    for _, row in roster_df.iterrows():
        team_abbr = str(row.get(team_col) or "").strip().upper()
        if not team_abbr:
            continue
        player_key = normalize_player_name_key(row.get(name_col), case="upper")
        if player_key:
            roster_name_keys.add((team_abbr, player_key))
        player_id = _safe_int(row.get(id_col)) if id_col else None
        if player_id is not None:
            roster_id_keys.add((team_abbr, player_id))
    return roster_name_keys, roster_id_keys
    if sample.empty or "date" not in sample.columns:
        return set()
    try:
        return set(pd.to_datetime(sample["date"], errors="coerce").dt.strftime("%Y-%m-%d").dropna().astype(str))
    except Exception:
        return set()


def audit_playable_prop_settlement(
    season: int,
    *,
    requested_date: str | None = None,
) -> Tuple[pd.DataFrame, dict[str, Any]]:
    import app as app_module

    player_logs_dates = _load_player_logs_date_set()
    props_actuals_parquet_dates = _load_props_actuals_parquet_date_set()
    processed_dir = paths.data_processed
    rows: list[dict[str, Any]] = []

    dates = app_module._season_betting_card_candidate_dates(season, requested_date=requested_date)
    for date_str in dates:
        cards_payload = app_module._season_betting_card_fetch_cards_payload(date_str)
        if not isinstance(cards_payload, dict):
            continue

        recon_by_id, recon_by_key = app_module._load_recon_props_lookup(date_str)
        _, source_name = app_module._load_recon_props_frame(date_str)
        recon_csv_path = processed_dir / f"recon_props_{date_str}.csv"
        props_actuals_csv_path = processed_dir / f"props_actuals_{date_str}.csv"

        for game in cards_payload.get("games", []):
            if not isinstance(game, dict):
                continue
            matchup = f"{str(game.get('away_tri') or '').strip().upper()}@{str(game.get('home_tri') or '').strip().upper()}"
            for team_side, team_tri in (
                ("home", str(game.get("home_tri") or "").strip().upper()),
                ("away", str(game.get("away_tri") or "").strip().upper()),
            ):
                for row in ((game.get("prop_recommendations") or {}).get(team_side) or []):
                    if not isinstance(row, dict):
                        continue
                    if str(row.get("card_bucket") or "").strip().lower() != "playable":
                        continue

                    best = row.get("best") if isinstance(row.get("best"), dict) else {}
                    settlement_present = any(
                        value is not None
                        for value in (
                            row.get("result"),
                            row.get("actual"),
                            best.get("result"),
                            best.get("actual"),
                        )
                    )
                    if settlement_present:
                        continue

                    player_name = str(
                        row.get("player")
                        or best.get("player")
                        or best.get("player_name")
                        or row.get("player_name")
                        or ""
                    ).strip()
                    player_key = app_module._norm_player_name(player_name)
                    player_id = app_module._safe_int(best.get("player_id") or row.get("player_id"))
                    lookup_hit_by_id = bool(player_id is not None and player_id in recon_by_id)
                    lookup_hit_by_key = bool(team_tri and player_key and (team_tri, player_key) in recon_by_key)
                    actual_row = None
                    if lookup_hit_by_id and player_id is not None:
                        actual_row = recon_by_id.get(player_id)
                    elif lookup_hit_by_key:
                        actual_row = recon_by_key.get((team_tri, player_key))

                    market = str(best.get("market") or row.get("market") or "").strip().lower()
                    actual_value = app_module._safe_float((actual_row or {}).get(market)) if market else None
                    unresolved_reason = "lookup_miss"
                    if not source_name:
                        unresolved_reason = "missing_source_file"
                    elif actual_row is not None and actual_value is None:
                        unresolved_reason = "market_actual_missing"
                    elif actual_row is not None and actual_value is not None:
                        unresolved_reason = "unsettled_after_actual"

                    rows.append(
                        {
                            "date": date_str,
                            "matchup": matchup,
                            "team_side": team_side,
                            "team_abbr": team_tri,
                            "player_name": player_name,
                            "player_key": player_key,
                            "player_id": player_id,
                            "market": market,
                            "side": str(best.get("side") or row.get("side") or "").strip().lower(),
                            "line": app_module._safe_float(best.get("line") or row.get("line")),
                            "price": app_module._safe_float(best.get("price") or row.get("price") or row.get("odds")),
                            "playable_sleeve": str(row.get("playable_sleeve") or "").strip().lower(),
                            "source_name": source_name,
                            "has_recon_props_csv": recon_csv_path.exists(),
                            "has_props_actuals_csv": props_actuals_csv_path.exists(),
                            "has_props_actuals_parquet_date": date_str in props_actuals_parquet_dates,
                            "has_player_logs_date": date_str in player_logs_dates,
                            "lookup_hit_by_id": lookup_hit_by_id,
                            "lookup_hit_by_key": lookup_hit_by_key,
                            "lookup_status": "id" if lookup_hit_by_id else "team_name" if lookup_hit_by_key else "miss",
                            "actual_value_present": actual_value is not None,
                            "unresolved_reason": unresolved_reason,
                        }
                    )

    audit_df = pd.DataFrame(rows)
    if audit_df.empty:
        return audit_df, {
            "season": int(season),
            "requested_date": requested_date,
            "rows": 0,
            "dates_with_gaps": 0,
            "reasons": {},
            "top_dates": {},
            "top_sleeves": {},
            "source_names": {},
        }

    audit_df = audit_df.sort_values(
        by=["date", "unresolved_reason", "matchup", "team_abbr", "player_name", "market", "side"],
        ascending=[True, True, True, True, True, True, True],
        kind="stable",
    ).reset_index(drop=True)

    reason_counts = Counter(audit_df["unresolved_reason"].astype(str))
    source_counts = Counter(audit_df["source_name"].fillna("NONE").astype(str))
    top_dates = audit_df.groupby("date").size().sort_values(ascending=False).head(10).astype(int).to_dict()
    top_sleeves = audit_df.groupby("playable_sleeve").size().sort_values(ascending=False).head(10).astype(int).to_dict()

    summary = {
        "season": int(season),
        "requested_date": requested_date,
        "rows": int(len(audit_df)),
        "dates_with_gaps": int(audit_df["date"].nunique()),
        "reasons": dict(reason_counts),
        "top_dates": dict(top_dates),
        "top_sleeves": dict(top_sleeves),
        "source_names": dict(source_counts),
    }
    return audit_df, summary


def build_playable_prop_settlement_rollups(audit_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if audit_df is None or audit_df.empty:
        empty = pd.DataFrame()
        return {"players": empty, "dates": empty, "sleeves": empty}

    df = audit_df.copy()
    for col in (
        "player_name",
        "team_abbr",
        "playable_sleeve",
        "unresolved_reason",
        "lookup_status",
        "source_name",
        "date",
    ):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    players = (
        df.groupby(["player_name", "team_abbr"], dropna=False)
        .agg(
            rows=("date", "size"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            sleeves=("playable_sleeve", lambda s: ", ".join(sorted({value for value in s if value}))),
            reasons=("unresolved_reason", lambda s: ", ".join(sorted({value for value in s if value}))),
            lookup_statuses=("lookup_status", lambda s: ", ".join(sorted({value for value in s if value}))),
            source_files=("source_name", lambda s: ", ".join(sorted({value for value in s if value})[:10])),
        )
        .reset_index()
        .sort_values(by=["rows", "last_date", "player_name", "team_abbr"], ascending=[False, False, True, True], kind="stable")
        .reset_index(drop=True)
    )

    dates = (
        df.groupby("date", dropna=False)
        .agg(
            rows=("date", "size"),
            players=("player_name", lambda s: len({value for value in s if value})),
            sleeves=("playable_sleeve", lambda s: ", ".join(sorted({value for value in s if value}))),
            reasons=("unresolved_reason", lambda s: ", ".join(sorted({value for value in s if value}))),
        )
        .reset_index()
        .sort_values(by=["rows", "date"], ascending=[False, False], kind="stable")
        .reset_index(drop=True)
    )

    sleeves = (
        df.groupby("playable_sleeve", dropna=False)
        .agg(
            rows=("date", "size"),
            players=("player_name", lambda s: len({value for value in s if value})),
            dates=("date", lambda s: len({value for value in s if value})),
            reasons=("unresolved_reason", lambda s: ", ".join(sorted({value for value in s if value}))),
        )
        .reset_index()
        .sort_values(by=["rows", "playable_sleeve"], ascending=[False, True], kind="stable")
        .reset_index(drop=True)
    )

    return {"players": players, "dates": dates, "sleeves": sleeves}


def _player_last_name(value: Any) -> str:
    normalized = normalize_player_name_key(value, case="upper")
    if not normalized:
        return ""
    parts = normalized.split()
    return parts[-1] if parts else ""


def _name_similarity(left: Any, right: Any) -> float:
    left_key = normalize_player_name_key(left, case="upper")
    right_key = normalize_player_name_key(right, case="upper")
    if not left_key or not right_key:
        return 0.0
    return float(SequenceMatcher(a=left_key, b=right_key).ratio())


def _rank_alias_candidates(
    *,
    team_abbr: str,
    player_name: str,
    player_key: str,
    player_short_key: str,
    source_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    if source_df is None or source_df.empty:
        return []

    candidates: list[dict[str, Any]] = []
    player_last_name = _player_last_name(player_name)
    seen: set[tuple[str, str]] = set()

    for _, raw_row in source_df.iterrows():
        source_name = str(raw_row.get("player_name") or "").strip()
        if not source_name:
            continue
        source_team = str(raw_row.get("team_abbr") or "").strip().upper()
        source_key = normalize_player_name_key(source_name, case="upper")
        if not source_key:
            continue
        source_short_key = short_player_key(source_name, case="upper")
        if source_team == team_abbr and source_key == player_key:
            continue
        dedupe_key = (source_team, source_key)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        similarity = _name_similarity(player_name, source_name)
        same_team = source_team == team_abbr
        same_short = bool(player_short_key and source_short_key and player_short_key == source_short_key)
        same_last = bool(player_last_name and player_last_name == _player_last_name(source_name))
        exact_norm_other_team = bool(player_key and source_key == player_key and source_team and source_team != team_abbr)

        match_types: list[str] = []
        score = 0.0
        if same_team and same_short:
            match_types.append("same_team_short_key")
            score += 100.0
        if same_team and same_last:
            match_types.append("same_team_last_name")
            score += 30.0
        if same_team and similarity >= 0.88:
            match_types.append("same_team_fuzzy_strong")
            score += 35.0
        elif same_team and similarity >= 0.80:
            match_types.append("same_team_fuzzy")
            score += 20.0
        if exact_norm_other_team:
            match_types.append("exact_name_other_team")
            score += 40.0
        if not same_team and same_short:
            match_types.append("other_team_short_key")
            score += 10.0

        if not match_types:
            continue

        candidates.append(
            {
                "candidate_name": source_name,
                "candidate_team_abbr": source_team,
                "candidate_player_key": source_key,
                "candidate_short_key": source_short_key,
                "candidate_similarity": round(similarity, 4),
                "candidate_score": round(score, 2),
                "candidate_match_types": ", ".join(match_types),
                "same_team": same_team,
                "exact_name_other_team": exact_norm_other_team,
            }
        )

    candidates.sort(
        key=lambda item: (
            float(item.get("candidate_score") or 0.0),
            float(item.get("candidate_similarity") or 0.0),
            bool(item.get("same_team")),
            str(item.get("candidate_name") or ""),
        ),
        reverse=True,
    )
    return candidates[:5]


def build_playable_prop_alias_rollups(audit_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if audit_df is None or audit_df.empty:
        empty = pd.DataFrame()
        return {"pairs": empty, "players": empty}

    df = audit_df.copy()
    for col in (
        "player_name",
        "team_abbr",
        "top_candidate_name",
        "top_candidate_team_abbr",
        "playable_sleeve",
        "date",
        "top_candidate_match_types",
        "audit_classification",
    ):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    pairs_source = df[df["alias_likely"].fillna(False).astype(bool)].copy()
    pairs = pd.DataFrame()
    if not pairs_source.empty:
        pairs = (
            pairs_source.groupby(["player_name", "team_abbr", "top_candidate_name", "top_candidate_team_abbr"], dropna=False)
            .agg(
                rows=("date", "size"),
                first_date=("date", "min"),
                last_date=("date", "max"),
                sleeves=("playable_sleeve", lambda s: ", ".join(sorted({value for value in s if value}))),
                match_types=("top_candidate_match_types", lambda s: ", ".join(sorted({value for value in s if value}))),
            )
            .reset_index()
            .sort_values(by=["rows", "last_date", "player_name"], ascending=[False, False, True], kind="stable")
            .reset_index(drop=True)
        )

    players = (
        df.groupby(["player_name", "team_abbr", "audit_classification"], dropna=False)
        .agg(
            rows=("date", "size"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            sleeves=("playable_sleeve", lambda s: ", ".join(sorted({value for value in s if value}))),
            top_candidates=("top_candidate_name", lambda s: ", ".join(sorted({value for value in s if value})[:5])),
        )
        .reset_index()
        .sort_values(by=["rows", "last_date", "player_name"], ascending=[False, False, True], kind="stable")
        .reset_index(drop=True)
    )

    return {"pairs": pairs, "players": players}


def _rank_provider_anomaly_candidates(
    *,
    team_abbr: str,
    player_name: str,
    player_key: str,
    player_short_key: str,
    player_id: int | None,
    source_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    if source_df is None or source_df.empty:
        return []

    candidates: list[dict[str, Any]] = []
    player_last_name = _player_last_name(player_name)
    seen: set[tuple[str, str, int | None]] = set()

    for _, raw_row in source_df.iterrows():
        source_name = str(raw_row.get("player_name") or "").strip()
        if not source_name:
            continue
        source_team = str(raw_row.get("team_abbr") or "").strip().upper()
        source_key = normalize_player_name_key(source_name, case="upper")
        if not source_key:
            continue
        source_short_key = short_player_key(source_name, case="upper")
        source_player_id = _safe_int(raw_row.get("player_id"))
        dedupe_key = (source_team, source_key, source_player_id)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        same_team = bool(source_team and source_team == team_abbr)
        exact_id = bool(player_id is not None and source_player_id is not None and source_player_id == player_id)
        exact_name = bool(player_key and source_key == player_key)
        short_match = bool(player_short_key and source_short_key and source_short_key == player_short_key)
        last_match = bool(player_last_name and player_last_name == _player_last_name(source_name))
        similarity = _name_similarity(player_name, source_name)

        if same_team and (exact_id or exact_name):
            continue

        match_types: list[str] = []
        score = 0.0
        if exact_id and not same_team:
            match_types.append("exact_player_id_other_team")
            score += 120.0
        if exact_name and not same_team:
            match_types.append("exact_name_other_team")
            score += 90.0
        if short_match and not same_team:
            match_types.append("short_key_other_team")
            score += 55.0
        if last_match and not same_team and similarity >= 0.88:
            match_types.append("last_name_other_team_fuzzy_strong")
            score += 30.0
        elif last_match and not same_team and similarity >= 0.8:
            match_types.append("last_name_other_team_fuzzy")
            score += 15.0

        if not match_types:
            continue

        candidates.append(
            {
                "candidate_name": source_name,
                "candidate_team_abbr": source_team,
                "candidate_player_id": source_player_id,
                "candidate_player_key": source_key,
                "candidate_short_key": source_short_key,
                "candidate_similarity": round(similarity, 4),
                "candidate_score": round(score, 2),
                "candidate_match_types": ", ".join(match_types),
                "exact_id_other_team": bool(exact_id and not same_team),
                "exact_name_other_team": bool(exact_name and not same_team),
                "short_key_other_team": bool(short_match and not same_team),
            }
        )

    candidates.sort(
        key=lambda item: (
            float(item.get("candidate_score") or 0.0),
            float(item.get("candidate_similarity") or 0.0),
            str(item.get("candidate_name") or ""),
        ),
        reverse=True,
    )
    return candidates[:5]


def build_playable_prop_provider_rollups(audit_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if audit_df is None or audit_df.empty:
        empty = pd.DataFrame()
        return {"players": empty, "dates": empty, "pairs": empty}

    df = audit_df.copy()
    for col in (
        "player_name",
        "team_abbr",
        "top_candidate_name",
        "top_candidate_team_abbr",
        "playable_sleeve",
        "date",
        "audit_classification",
        "top_candidate_match_types",
    ):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    players = (
        df.groupby(["player_name", "team_abbr", "audit_classification"], dropna=False)
        .agg(
            rows=("date", "size"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            sleeves=("playable_sleeve", lambda s: ", ".join(sorted({value for value in s if value}))),
            top_candidates=("top_candidate_name", lambda s: ", ".join(sorted({value for value in s if value})[:5])),
        )
        .reset_index()
        .sort_values(by=["rows", "last_date", "player_name"], ascending=[False, False, True], kind="stable")
        .reset_index(drop=True)
    )

    dates = (
        df.groupby(["date", "audit_classification"], dropna=False)
        .agg(
            rows=("date", "size"),
            players=("player_name", lambda s: len({value for value in s if value})),
            sleeves=("playable_sleeve", lambda s: ", ".join(sorted({value for value in s if value}))),
        )
        .reset_index()
        .sort_values(by=["rows", "date", "audit_classification"], ascending=[False, False, True], kind="stable")
        .reset_index(drop=True)
    )

    pairs_source = df[df["provider_anomaly_likely"].fillna(False).astype(bool)].copy()
    pairs = pd.DataFrame()
    if not pairs_source.empty:
        pairs = (
            pairs_source.groupby(["player_name", "team_abbr", "top_candidate_name", "top_candidate_team_abbr"], dropna=False)
            .agg(
                rows=("date", "size"),
                first_date=("date", "min"),
                last_date=("date", "max"),
                match_types=("top_candidate_match_types", lambda s: ", ".join(sorted({value for value in s if value}))),
            )
            .reset_index()
            .sort_values(by=["rows", "last_date", "player_name"], ascending=[False, False, True], kind="stable")
            .reset_index(drop=True)
        )

    return {"players": players, "dates": dates, "pairs": pairs}


def build_playable_prop_coverage_rollups(audit_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if audit_df is None or audit_df.empty:
        empty = pd.DataFrame()
        return {"players": empty, "dates": empty, "teams": empty}

    df = audit_df.copy()
    for col in (
        "player_name",
        "team_abbr",
        "date",
        "playable_sleeve",
        "market",
        "source_name",
        "coverage_subtype",
    ):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    players = (
        df.groupby(["player_name", "team_abbr", "coverage_subtype"], dropna=False)
        .agg(
            rows=("date", "size"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            sleeves=("playable_sleeve", lambda s: ", ".join(sorted({value for value in s if value}))),
            markets=("market", lambda s: ", ".join(sorted({value for value in s if value}))),
            source_files=("source_name", lambda s: ", ".join(sorted({value for value in s if value}))),
        )
        .reset_index()
        .sort_values(by=["rows", "last_date", "player_name"], ascending=[False, False, True], kind="stable")
        .reset_index(drop=True)
    )

    dates = (
        df.groupby(["date", "coverage_subtype"], dropna=False)
        .agg(
            rows=("date", "size"),
            players=("player_name", lambda s: len({value for value in s if value})),
            teams=("team_abbr", lambda s: len({value for value in s if value})),
            source_files=("source_name", lambda s: ", ".join(sorted({value for value in s if value}))),
        )
        .reset_index()
        .sort_values(by=["rows", "date", "coverage_subtype"], ascending=[False, False, True], kind="stable")
        .reset_index(drop=True)
    )

    teams = (
        df.groupby(["team_abbr", "coverage_subtype"], dropna=False)
        .agg(
            rows=("date", "size"),
            players=("player_name", lambda s: len({value for value in s if value})),
            dates=("date", lambda s: len({value for value in s if value})),
            source_files=("source_name", lambda s: ", ".join(sorted({value for value in s if value}))),
        )
        .reset_index()
        .sort_values(by=["rows", "team_abbr", "coverage_subtype"], ascending=[False, True, True], kind="stable")
        .reset_index(drop=True)
    )

    return {"players": players, "dates": dates, "teams": teams}


def audit_playable_prop_provider_anomalies(
    season: int,
    *,
    requested_date: str | None = None,
) -> Tuple[pd.DataFrame, dict[str, Any]]:
    import app as app_module

    settlement_df, settlement_summary = audit_playable_prop_settlement(season, requested_date=requested_date)
    if settlement_df is None or settlement_df.empty:
        return settlement_df, {
            "season": int(season),
            "requested_date": requested_date,
            "rows": 0,
            "provider_anomaly_rows": 0,
            "coverage_missing_rows": 0,
            "classification_counts": {},
            "top_pairs": [],
            "settlement_rows": int(settlement_summary.get("rows") or 0),
        }

    unresolved_df = settlement_df[settlement_df["unresolved_reason"].astype(str) == "lookup_miss"].copy()
    if unresolved_df.empty:
        return unresolved_df, {
            "season": int(season),
            "requested_date": requested_date,
            "rows": 0,
            "provider_anomaly_rows": 0,
            "coverage_missing_rows": 0,
            "classification_counts": {},
            "top_pairs": [],
            "settlement_rows": int(settlement_summary.get("rows") or 0),
        }

    by_date_frames: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    for _, record in unresolved_df.iterrows():
        row = record.to_dict()
        date_str = str(row.get("date") or "").strip()
        if not date_str:
            continue
        if date_str not in by_date_frames:
            source_df, _ = app_module._load_recon_props_frame(date_str)
            by_date_frames[date_str] = source_df if isinstance(source_df, pd.DataFrame) else pd.DataFrame()

        player_name = str(row.get("player_name") or "").strip()
        team_abbr = str(row.get("team_abbr") or "").strip().upper()
        player_key = normalize_player_name_key(player_name, case="upper")
        player_short_key = short_player_key(player_name, case="upper")
        player_id = _safe_int(row.get("player_id"))
        source_df = by_date_frames.get(date_str)
        if source_df is None:
            source_df = pd.DataFrame()

        candidates = _rank_provider_anomaly_candidates(
            team_abbr=team_abbr,
            player_name=player_name,
            player_key=player_key,
            player_short_key=player_short_key,
            player_id=player_id,
            source_df=source_df,
        )
        top_candidate = candidates[0] if candidates else {}

        audit_classification = "coverage_missing"
        if top_candidate:
            if bool(top_candidate.get("exact_id_other_team")):
                audit_classification = "exact_player_id_other_team"
            elif bool(top_candidate.get("exact_name_other_team")):
                audit_classification = "exact_name_other_team"
            elif bool(top_candidate.get("short_key_other_team")):
                audit_classification = "short_key_other_team"
            else:
                audit_classification = "weak_other_team_candidate"

        provider_anomaly_likely = audit_classification in {
            "exact_player_id_other_team",
            "exact_name_other_team",
            "short_key_other_team",
        }

        rows.append(
            {
                **row,
                "player_short_key": player_short_key,
                "player_last_name": _player_last_name(player_name),
                "candidate_count": int(len(candidates)),
                "provider_anomaly_likely": bool(provider_anomaly_likely),
                "audit_classification": audit_classification,
                "top_candidate_name": str(top_candidate.get("candidate_name") or ""),
                "top_candidate_team_abbr": str(top_candidate.get("candidate_team_abbr") or ""),
                "top_candidate_player_id": top_candidate.get("candidate_player_id"),
                "top_candidate_similarity": top_candidate.get("candidate_similarity"),
                "top_candidate_score": top_candidate.get("candidate_score"),
                "top_candidate_match_types": str(top_candidate.get("candidate_match_types") or ""),
                "candidate_names": "; ".join(str(item.get("candidate_name") or "") for item in candidates),
                "candidate_match_types": "; ".join(str(item.get("candidate_match_types") or "") for item in candidates),
            }
        )

    audit_df = pd.DataFrame(rows)
    if audit_df.empty:
        return audit_df, {
            "season": int(season),
            "requested_date": requested_date,
            "rows": 0,
            "provider_anomaly_rows": 0,
            "coverage_missing_rows": 0,
            "classification_counts": {},
            "top_pairs": [],
            "settlement_rows": int(settlement_summary.get("rows") or 0),
        }

    audit_df = audit_df.sort_values(
        by=["provider_anomaly_likely", "candidate_count", "top_candidate_score", "date", "player_name"],
        ascending=[False, False, False, True, True],
        kind="stable",
    ).reset_index(drop=True)

    rollups = build_playable_prop_provider_rollups(audit_df)
    top_pairs = []
    if not rollups["pairs"].empty:
        top_pairs = rollups["pairs"].head(10).to_dict(orient="records")

    summary = {
        "season": int(season),
        "requested_date": requested_date,
        "rows": int(len(audit_df)),
        "provider_anomaly_rows": int(audit_df["provider_anomaly_likely"].fillna(False).astype(bool).sum()),
        "coverage_missing_rows": int((audit_df["audit_classification"] == "coverage_missing").sum()),
        "classification_counts": {
            str(key): int(value)
            for key, value in Counter(audit_df["audit_classification"].fillna("").astype(str)).items()
        },
        "top_pairs": top_pairs,
        "settlement_rows": int(settlement_summary.get("rows") or 0),
    }
    return audit_df, summary


def audit_playable_prop_coverage_gaps(
    season: int,
    *,
    requested_date: str | None = None,
) -> Tuple[pd.DataFrame, dict[str, Any]]:
    provider_df, provider_summary = audit_playable_prop_provider_anomalies(season, requested_date=requested_date)
    if provider_df is None or provider_df.empty:
        return provider_df, {
            "season": int(season),
            "requested_date": requested_date,
            "rows": 0,
            "coverage_subtypes": {},
            "top_players": [],
            "provider_rows": int(provider_summary.get("rows") or 0),
        }

    coverage_df = provider_df[provider_df["audit_classification"].astype(str) == "coverage_missing"].copy()
    if coverage_df.empty:
        return coverage_df, {
            "season": int(season),
            "requested_date": requested_date,
            "rows": 0,
            "coverage_subtypes": {},
            "top_players": [],
            "provider_rows": int(provider_summary.get("rows") or 0),
        }

    roster_name_keys, roster_id_keys = _load_roster_context(season)
    enriched_rows: list[dict[str, Any]] = []
    by_date_frames: dict[str, pd.DataFrame] = {}

    for _, record in coverage_df.iterrows():
        row = record.to_dict()
        date_str = str(row.get("date") or "").strip()
        if not date_str:
            continue
        if date_str not in by_date_frames:
            import app as app_module

            source_df, _ = app_module._load_recon_props_frame(date_str)
            by_date_frames[date_str] = source_df if isinstance(source_df, pd.DataFrame) else pd.DataFrame()

        source_df = by_date_frames.get(date_str)
        if source_df is None:
            source_df = pd.DataFrame()

        team_abbr = str(row.get("team_abbr") or "").strip().upper()
        player_name = str(row.get("player_name") or "").strip()
        player_key = normalize_player_name_key(player_name, case="upper")
        player_id = _safe_int(row.get("player_id"))
        market = str(row.get("market") or "").strip().lower()
        player_last = _player_last_name(player_name)

        source_player_count = 0
        source_team_count = 0
        source_team_player_count = 0
        team_present_in_source = False
        market_supported = bool(market and market in source_df.columns)
        same_last_name_any_team = False
        same_last_name_same_team = False

        if not source_df.empty:
            source_player_keys = source_df.get("player_name", pd.Series(dtype=object)).map(
                lambda value: normalize_player_name_key(value, case="upper")
            )
            source_teams = source_df.get("team_abbr", pd.Series(dtype=object)).map(lambda value: str(value or "").strip().upper())
            source_player_ids = source_df.get("player_id", pd.Series(dtype=object)).map(_safe_int)
            source_last_names = source_df.get("player_name", pd.Series(dtype=object)).map(_player_last_name)

            source_player_count = int(len({value for value in source_player_keys if value}))
            team_mask = source_teams == team_abbr
            source_team_count = int(team_mask.sum()) if len(source_teams) else 0
            team_present_in_source = bool(source_team_count > 0)
            if team_present_in_source:
                source_team_player_count = int(len({value for value in source_player_keys[team_mask] if value}))
            if player_last:
                same_last_name_any_team = bool((source_last_names == player_last).any())
                if team_present_in_source:
                    same_last_name_same_team = bool((source_last_names[team_mask] == player_last).any())
            player_id_present_any_team = bool(player_id is not None and (source_player_ids == player_id).any())
            player_name_present_any_team = bool(player_key and (source_player_keys == player_key).any())
        else:
            player_id_present_any_team = False
            player_name_present_any_team = False

        roster_name_match = bool(team_abbr and player_key and (team_abbr, player_key) in roster_name_keys)
        roster_id_match = bool(team_abbr and player_id is not None and (team_abbr, player_id) in roster_id_keys)

        coverage_subtype = "source_player_gap"
        if not team_present_in_source:
            coverage_subtype = "source_team_gap"
        elif roster_name_match or roster_id_match:
            coverage_subtype = "rostered_player_missing"
        elif same_last_name_same_team:
            coverage_subtype = "same_team_name_collision"
        elif same_last_name_any_team:
            coverage_subtype = "cross_team_name_collision"

        enriched_rows.append(
            {
                **row,
                "coverage_subtype": coverage_subtype,
                "source_row_count": int(len(source_df.index)) if isinstance(source_df, pd.DataFrame) else 0,
                "source_player_count": source_player_count,
                "source_team_row_count": source_team_count,
                "source_team_player_count": source_team_player_count,
                "team_present_in_source": bool(team_present_in_source),
                "market_supported_by_source": bool(market_supported),
                "player_id_present_any_team": bool(player_id_present_any_team),
                "player_name_present_any_team": bool(player_name_present_any_team),
                "same_last_name_any_team": bool(same_last_name_any_team),
                "same_last_name_same_team": bool(same_last_name_same_team),
                "roster_name_match": bool(roster_name_match),
                "roster_id_match": bool(roster_id_match),
            }
        )

    audit_df = pd.DataFrame(enriched_rows)
    if audit_df.empty:
        return audit_df, {
            "season": int(season),
            "requested_date": requested_date,
            "rows": 0,
            "coverage_subtypes": {},
            "top_players": [],
            "provider_rows": int(provider_summary.get("rows") or 0),
        }

    audit_df = audit_df.sort_values(
        by=["coverage_subtype", "date", "team_abbr", "player_name", "market", "side"],
        ascending=[True, True, True, True, True, True],
        kind="stable",
    ).reset_index(drop=True)

    rollups = build_playable_prop_coverage_rollups(audit_df)
    top_players = []
    if not rollups["players"].empty:
        top_players = rollups["players"].head(10).to_dict(orient="records")

    summary = {
        "season": int(season),
        "requested_date": requested_date,
        "rows": int(len(audit_df)),
        "coverage_subtypes": {
            str(key): int(value)
            for key, value in Counter(audit_df["coverage_subtype"].fillna("").astype(str)).items()
        },
        "top_players": top_players,
        "provider_rows": int(provider_summary.get("rows") or 0),
    }
    return audit_df, summary


def audit_playable_prop_aliases(
    season: int,
    *,
    requested_date: str | None = None,
) -> Tuple[pd.DataFrame, dict[str, Any]]:
    import app as app_module

    settlement_df, settlement_summary = audit_playable_prop_settlement(season, requested_date=requested_date)
    if settlement_df is None or settlement_df.empty:
        return settlement_df, {
            "season": int(season),
            "requested_date": requested_date,
            "rows": 0,
            "likely_alias_rows": 0,
            "team_mismatch_rows": 0,
            "no_candidate_rows": 0,
            "top_pairs": [],
            "settlement_rows": int(settlement_summary.get("rows") or 0),
        }

    unresolved_df = settlement_df[settlement_df["unresolved_reason"].astype(str) == "lookup_miss"].copy()
    if unresolved_df.empty:
        return unresolved_df, {
            "season": int(season),
            "requested_date": requested_date,
            "rows": 0,
            "likely_alias_rows": 0,
            "team_mismatch_rows": 0,
            "no_candidate_rows": 0,
            "top_pairs": [],
            "settlement_rows": int(settlement_summary.get("rows") or 0),
        }

    by_date_frames: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    for _, record in unresolved_df.iterrows():
        row = record.to_dict()
        date_str = str(row.get("date") or "").strip()
        if not date_str:
            continue
        if date_str not in by_date_frames:
            source_df, _ = app_module._load_recon_props_frame(date_str)
            by_date_frames[date_str] = source_df if isinstance(source_df, pd.DataFrame) else pd.DataFrame()

        player_name = str(row.get("player_name") or "").strip()
        team_abbr = str(row.get("team_abbr") or "").strip().upper()
        player_key = normalize_player_name_key(player_name, case="upper")
        player_short_key = short_player_key(player_name, case="upper")
        source_df = by_date_frames.get(date_str)
        if source_df is None:
            source_df = pd.DataFrame()
        candidates = _rank_alias_candidates(
            team_abbr=team_abbr,
            player_name=player_name,
            player_key=player_key,
            player_short_key=player_short_key,
            source_df=source_df,
        )

        top_candidate = candidates[0] if candidates else {}
        alias_likely = bool(
            top_candidate
            and bool(top_candidate.get("same_team"))
            and (
                "same_team_short_key" in str(top_candidate.get("candidate_match_types") or "")
                or float(top_candidate.get("candidate_similarity") or 0.0) >= 0.88
            )
        )
        team_mismatch_likely = bool(
            not alias_likely
            and top_candidate
            and bool(top_candidate.get("exact_name_other_team"))
        )
        audit_classification = "no_candidate"
        if alias_likely:
            audit_classification = "likely_alias"
        elif team_mismatch_likely:
            audit_classification = "team_mismatch"
        elif candidates:
            audit_classification = "weak_candidate"

        rows.append(
            {
                **row,
                "player_short_key": player_short_key,
                "player_last_name": _player_last_name(player_name),
                "candidate_count": int(len(candidates)),
                "alias_likely": bool(alias_likely),
                "team_mismatch_likely": bool(team_mismatch_likely),
                "audit_classification": audit_classification,
                "top_candidate_name": str(top_candidate.get("candidate_name") or ""),
                "top_candidate_team_abbr": str(top_candidate.get("candidate_team_abbr") or ""),
                "top_candidate_player_key": str(top_candidate.get("candidate_player_key") or ""),
                "top_candidate_similarity": top_candidate.get("candidate_similarity"),
                "top_candidate_score": top_candidate.get("candidate_score"),
                "top_candidate_match_types": str(top_candidate.get("candidate_match_types") or ""),
                "candidate_names": "; ".join(str(item.get("candidate_name") or "") for item in candidates),
                "candidate_match_types": "; ".join(str(item.get("candidate_match_types") or "") for item in candidates),
            }
        )

    audit_df = pd.DataFrame(rows)
    if audit_df.empty:
        return audit_df, {
            "season": int(season),
            "requested_date": requested_date,
            "rows": 0,
            "likely_alias_rows": 0,
            "team_mismatch_rows": 0,
            "no_candidate_rows": 0,
            "top_pairs": [],
            "settlement_rows": int(settlement_summary.get("rows") or 0),
        }

    audit_df = audit_df.sort_values(
        by=["alias_likely", "team_mismatch_likely", "candidate_count", "top_candidate_score", "date", "player_name"],
        ascending=[False, False, False, False, True, True],
        kind="stable",
    ).reset_index(drop=True)

    rollups = build_playable_prop_alias_rollups(audit_df)
    top_pairs = []
    if not rollups["pairs"].empty:
        top_pairs = rollups["pairs"].head(10).to_dict(orient="records")

    summary = {
        "season": int(season),
        "requested_date": requested_date,
        "rows": int(len(audit_df)),
        "likely_alias_rows": int(audit_df["alias_likely"].fillna(False).astype(bool).sum()),
        "team_mismatch_rows": int(audit_df["team_mismatch_likely"].fillna(False).astype(bool).sum()),
        "no_candidate_rows": int((audit_df["candidate_count"].fillna(0).astype(int) <= 0).sum()),
        "classification_counts": {
            str(key): int(value)
            for key, value in Counter(audit_df["audit_classification"].fillna("").astype(str)).items()
        },
        "top_pairs": top_pairs,
        "settlement_rows": int(settlement_summary.get("rows") or 0),
    }
    return audit_df, summary


def default_playable_prop_settlement_audit_path(*, season: int, requested_date: str | None = None) -> Path:
    suffix = str(requested_date or season).strip()
    safe_suffix = suffix.replace(":", "-")
    return paths.data_processed / f"playable_prop_settlement_audit_{safe_suffix}.csv"


def default_playable_prop_settlement_rollup_path(
    kind: str,
    *,
    season: int,
    requested_date: str | None = None,
) -> Path:
    suffix = str(requested_date or season).strip().replace(":", "-")
    safe_kind = str(kind).strip().lower()
    return paths.data_processed / f"playable_prop_settlement_audit_{safe_kind}_{suffix}.csv"


def default_playable_prop_alias_audit_path(*, season: int, requested_date: str | None = None) -> Path:
    suffix = str(requested_date or season).strip().replace(":", "-")
    return paths.data_processed / f"playable_prop_alias_audit_{suffix}.csv"


def default_playable_prop_alias_rollup_path(
    kind: str,
    *,
    season: int,
    requested_date: str | None = None,
) -> Path:
    suffix = str(requested_date or season).strip().replace(":", "-")
    safe_kind = str(kind).strip().lower()
    return paths.data_processed / f"playable_prop_alias_audit_{safe_kind}_{suffix}.csv"


def default_playable_prop_provider_audit_path(*, season: int, requested_date: str | None = None) -> Path:
    suffix = str(requested_date or season).strip().replace(":", "-")
    return paths.data_processed / f"playable_prop_provider_audit_{suffix}.csv"


def default_playable_prop_provider_rollup_path(
    kind: str,
    *,
    season: int,
    requested_date: str | None = None,
) -> Path:
    suffix = str(requested_date or season).strip().replace(":", "-")
    safe_kind = str(kind).strip().lower()
    return paths.data_processed / f"playable_prop_provider_audit_{safe_kind}_{suffix}.csv"


def default_playable_prop_coverage_audit_path(*, season: int, requested_date: str | None = None) -> Path:
    suffix = str(requested_date or season).strip().replace(":", "-")
    return paths.data_processed / f"playable_prop_coverage_audit_{suffix}.csv"


def default_playable_prop_coverage_rollup_path(
    kind: str,
    *,
    season: int,
    requested_date: str | None = None,
) -> Path:
    suffix = str(requested_date or season).strip().replace(":", "-")
    safe_kind = str(kind).strip().lower()
    return paths.data_processed / f"playable_prop_coverage_audit_{safe_kind}_{suffix}.csv"