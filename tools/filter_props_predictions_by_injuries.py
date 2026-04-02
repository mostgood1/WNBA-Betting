from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(SRC))

from nba_betting.player_names import normalize_player_name_key


def _truthy_mask(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y", "on"})
    try:
        numeric = pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float) > 0.5
        return text | numeric
    except Exception:
        return text


def _tri(value: Any) -> str:
    try:
        text = str(value or "").strip().upper()
    except Exception:
        return ""
    if len(text) == 3 and text.isalpha():
        return text
    return ""


def _player_key(value: Any) -> str:
    try:
        return str(normalize_player_name_key(value, case="upper") or "").strip().upper()
    except Exception:
        return ""


def _protected_pairs_from_props(preds_df: pd.DataFrame) -> set[tuple[str, str]]:
    if preds_df is None or preds_df.empty:
        return set()
    if "player_name" not in preds_df.columns or "team" not in preds_df.columns:
        return set()
    if "playing_today" not in preds_df.columns:
        return set()

    tmp = preds_df[["player_name", "team", "playing_today"]].copy()
    tmp["_keep"] = _truthy_mask(tmp["playing_today"])
    tmp = tmp[tmp["_keep"]].copy()
    if tmp.empty:
        return set()
    tmp["_pkey"] = tmp["player_name"].map(_player_key)
    tmp["_tri"] = tmp["team"].map(_tri)
    return {
        (str(row.get("_pkey") or ""), str(row.get("_tri") or ""))
        for _, row in tmp.iterrows()
        if str(row.get("_pkey") or "") and str(row.get("_tri") or "")
    }


def _protected_pairs_from_league_status(path: Path | None) -> set[tuple[str, str]]:
    if path is None or not path.exists():
        return set()
    try:
        df = pd.read_csv(path)
    except Exception:
        return set()
    if df is None or df.empty:
        return set()
    if "player_name" not in df.columns:
        return set()
    team_col = "team" if "team" in df.columns else ("team_tri" if "team_tri" in df.columns else None)
    if not team_col or "playing_today" not in df.columns:
        return set()

    tmp = df[["player_name", team_col, "playing_today"]].copy()
    if "team_on_slate" in df.columns:
        tmp = tmp[_truthy_mask(df["team_on_slate"]).reindex(tmp.index, fill_value=False)].copy()
    tmp = tmp[_truthy_mask(tmp["playing_today"])].copy()
    if tmp.empty:
        return set()
    tmp["_pkey"] = tmp["player_name"].map(_player_key)
    tmp["_tri"] = tmp[team_col].map(_tri)
    return {
        (str(row.get("_pkey") or ""), str(row.get("_tri") or ""))
        for _, row in tmp.iterrows()
        if str(row.get("_pkey") or "") and str(row.get("_tri") or "")
    }


def filter_props_predictions_by_injuries(
    preds_path: Path,
    injuries_path: Path,
    *,
    league_status_path: Path | None = None,
) -> dict[str, Any]:
    if not preds_path.exists():
        return {"status": "no_predictions"}

    preds_df = pd.read_csv(preds_path)
    before_rows = int(len(preds_df.index))
    if preds_df is None or preds_df.empty:
        preds_df.to_csv(preds_path, index=False)
        return {"status": "ok", "before_rows": before_rows, "after_rows": before_rows, "removed_players": []}

    if not injuries_path.exists():
        preds_df.to_csv(preds_path, index=False)
        return {"status": "ok", "before_rows": before_rows, "after_rows": before_rows, "removed_players": []}

    try:
        inj_df = pd.read_csv(injuries_path)
    except Exception:
        preds_df.to_csv(preds_path, index=False)
        return {"status": "ok", "before_rows": before_rows, "after_rows": before_rows, "removed_players": []}

    if inj_df is None or inj_df.empty or "player" not in inj_df.columns:
        preds_df.to_csv(preds_path, index=False)
        return {"status": "ok", "before_rows": before_rows, "after_rows": before_rows, "removed_players": []}

    protected_pairs = _protected_pairs_from_props(preds_df)
    protected_pairs |= _protected_pairs_from_league_status(league_status_path)

    inj_team_col = "team_tri" if "team_tri" in inj_df.columns else ("team" if "team" in inj_df.columns else None)
    if not inj_team_col:
        preds_df.to_csv(preds_path, index=False)
        return {"status": "ok", "before_rows": before_rows, "after_rows": before_rows, "removed_players": []}

    inj_df = inj_df.copy()
    inj_df["_pkey"] = inj_df["player"].map(_player_key)
    inj_df["_tri"] = inj_df[inj_team_col].map(_tri)
    inj_df = inj_df[(inj_df["_pkey"] != "") & (inj_df["_tri"] != "")].copy()
    if inj_df.empty:
        preds_df.to_csv(preds_path, index=False)
        return {"status": "ok", "before_rows": before_rows, "after_rows": before_rows, "removed_players": []}

    ban_pairs = {
        (str(row.get("_pkey") or ""), str(row.get("_tri") or ""))
        for _, row in inj_df.iterrows()
        if (str(row.get("_pkey") or ""), str(row.get("_tri") or "")) not in protected_pairs
    }
    if not ban_pairs:
        preds_df.to_csv(preds_path, index=False)
        return {"status": "ok", "before_rows": before_rows, "after_rows": before_rows, "removed_players": []}

    filtered_df = preds_df.copy()
    filtered_df["_pkey"] = filtered_df.get("player_name").map(_player_key)
    filtered_df["_tri"] = filtered_df.get("team").map(_tri)
    pair_series = list(zip(filtered_df["_pkey"].astype(str).tolist(), filtered_df["_tri"].astype(str).tolist()))
    remove_mask = pd.Series([pair in ban_pairs for pair in pair_series], index=filtered_df.index)

    removed_players = sorted(
        {
            str(row.get("player_name") or "").strip()
            for _, row in filtered_df[remove_mask].iterrows()
            if str(row.get("player_name") or "").strip()
        }
    )
    filtered_df = filtered_df[~remove_mask].drop(columns=["_pkey", "_tri"], errors="ignore")

    try:
        if "team" in preds_df.columns and "team" in filtered_df.columns:
            before_teams = {
                str(value).strip().upper()
                for value in preds_df["team"].dropna().astype(str).tolist()
                if str(value).strip()
            }
            after_teams = {
                str(value).strip().upper()
                for value in filtered_df["team"].dropna().astype(str).tolist()
                if str(value).strip()
            }
            missing_teams = sorted(before_teams - after_teams)
            if missing_teams:
                restore_rows = preds_df[preds_df["team"].astype(str).str.upper().isin(missing_teams)].copy()
                if not restore_rows.empty:
                    filtered_df = pd.concat([filtered_df, restore_rows], ignore_index=True)
    except Exception:
        pass

    filtered_df.to_csv(preds_path, index=False)
    return {
        "status": "ok",
        "before_rows": before_rows,
        "after_rows": int(len(filtered_df.index)),
        "removed_players": removed_players,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preds", required=True)
    parser.add_argument("--injuries", required=True)
    parser.add_argument("--league-status", dest="league_status", default=None)
    args = parser.parse_args()

    result = filter_props_predictions_by_injuries(
        Path(args.preds),
        Path(args.injuries),
        league_status_path=Path(args.league_status) if args.league_status else None,
    )
    if result.get("status") == "no_predictions":
        print("NO_PREDICTIONS")
        return 0
    print(f"FILTERED:{result.get('before_rows', 0)}->{result.get('after_rows', 0)}")
    removed = result.get("removed_players") or []
    if removed:
        print("REMOVED_PLAYERS:" + ",".join(removed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())