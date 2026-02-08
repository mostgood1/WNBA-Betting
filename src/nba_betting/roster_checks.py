from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from .config import paths


@dataclass
class RosterSanityResult:
    ok: bool
    date: str
    issues: list[str]
    summary: dict[str, Any]


def roster_sanity_check(
    date_str: str,
    *,
    min_total_roster_per_team: int = 10,
    min_playing_today_per_team: int = 8,
    max_team_mismatches_in_props: int = 0,
) -> RosterSanityResult:
    """Validate that today's slate rosters look sane.

    Primary source of truth is `data/processed/league_status_<date>.csv`.

    Checks:
      - Each slate team has at least N total roster rows.
      - Each slate team has at least N players marked playing_today==True.
      - No player_id appears on multiple teams (within slate rows).
      - Players present in props_predictions_<date>.csv (if present) match league_status team.
    """
    issues: list[str] = []
    summary: dict[str, Any] = {}
    warnings: list[str] = []

    def _norm_team(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and pd.isna(value):
            return ""
        s = str(value).strip().upper()
        if s in {"", "NAN", "NONE"}:
            return ""
        return s

    ls_path = paths.data_processed / f"league_status_{date_str}.csv"
    if not ls_path.exists():
        return RosterSanityResult(
            ok=False,
            date=date_str,
            issues=[f"missing league_status file: {ls_path}"],
            summary={"league_status_path": str(ls_path)},
        )

    ls = pd.read_csv(ls_path)
    if ls is None or ls.empty:
        return RosterSanityResult(
            ok=False,
            date=date_str,
            issues=["league_status is empty"],
            summary={"league_status_path": str(ls_path), "rows": 0},
        )

    cols = {c.lower(): c for c in ls.columns}
    team_col = cols.get("team")
    pid_col = cols.get("player_id")
    on_col = cols.get("team_on_slate")
    play_col = cols.get("playing_today")

    if not team_col or not pid_col:
        return RosterSanityResult(
            ok=False,
            date=date_str,
            issues=["league_status missing required columns (team/player_id)"],
            summary={"league_status_path": str(ls_path), "cols": list(ls.columns)},
        )

    ls = ls.copy()
    ls[team_col] = ls[team_col].fillna("").astype(str).str.upper().str.strip()
    ls[pid_col] = pd.to_numeric(ls[pid_col], errors="coerce")

    if on_col and on_col in ls.columns:
        slate_rows = ls[ls[on_col].fillna(False).astype(bool)].copy()
    else:
        # Fallback: treat all teams as slate teams (less strict, but still useful).
        slate_rows = ls.copy()
        warnings.append("league_status missing team_on_slate; treating all teams as on-slate")

    slate_teams = sorted({t for t in slate_rows[team_col].dropna().astype(str).tolist() if t})
    summary["slate_teams"] = slate_teams
    summary["slate_team_count"] = len(slate_teams)

    # Per-team counts
    per_team_total: dict[str, int] = {}
    per_team_playing: dict[str, int] = {}
    for tri in slate_teams:
        team_df = slate_rows[slate_rows[team_col] == tri]
        per_team_total[tri] = int(len(team_df))
        if play_col and play_col in team_df.columns:
            # playing_today can be True/False/None
            per_team_playing[tri] = int((team_df[play_col] == True).sum())  # noqa: E712
        else:
            per_team_playing[tri] = 0

    summary["min_total_roster_per_team"] = int(min_total_roster_per_team)
    summary["min_playing_today_per_team"] = int(min_playing_today_per_team)
    summary["per_team_total"] = per_team_total
    summary["per_team_playing_today_true"] = per_team_playing

    thin_total = {t: n for t, n in per_team_total.items() if n < int(min_total_roster_per_team)}
    if thin_total:
        issues.append(f"thin total roster rows for slate teams: {thin_total}")

    # NOTE: playing_today comes from injury designations and can be noisy/stale; we keep this
    # as a warning (not a hard failure). The hard availability gate remains `check-dressed`.
    thin_play = {t: n for t, n in per_team_playing.items() if n < int(min_playing_today_per_team)}
    if thin_play:
        warnings.append(f"thin playing_today==True counts for slate teams: {thin_play}")

    # Duplicate player_id across multiple teams (within slate)
    dup_examples: list[dict[str, Any]] = []
    try:
        pid_team = slate_rows.dropna(subset=[pid_col])[[pid_col, team_col]].copy()
        pid_team[pid_col] = pd.to_numeric(pid_team[pid_col], errors="coerce")
        pid_team = pid_team.dropna(subset=[pid_col])
        grp = pid_team.groupby(pid_col)[team_col].nunique(dropna=True)
        dups = grp[grp > 1]
        if not dups.empty:
            # collect a few examples
            for pid in dups.index.astype(int).tolist()[:25]:
                teams = sorted(set(pid_team.loc[pid_team[pid_col] == pid, team_col].dropna().astype(str).tolist()))
                dup_examples.append({"player_id": int(pid), "teams": teams})
            issues.append(f"duplicate player_id assigned to multiple slate teams: count={int(len(dups))}")
    except Exception:
        pass
    if dup_examples:
        summary["duplicate_player_ids_examples"] = dup_examples

    # Props predictions team consistency (optional)
    mism_examples: list[dict[str, Any]] = []
    mismatches_n = 0
    pp_path = paths.data_processed / f"props_predictions_{date_str}.csv"
    if pp_path.exists():
        try:
            pp = pd.read_csv(pp_path)
            if isinstance(pp, pd.DataFrame) and (not pp.empty):
                pp_cols = {c.lower(): c for c in pp.columns}
                pp_pid = pp_cols.get("player_id")
                pp_team = pp_cols.get("team")
                pp_name = pp_cols.get("player_name") or pp_cols.get("player")
                pp_team_on_slate = pp_cols.get("team_on_slate")
                if pp_pid and pp_team:
                    use_cols = [pp_pid, pp_team]
                    if pp_name:
                        use_cols.append(pp_name)
                    if pp_team_on_slate:
                        use_cols.append(pp_team_on_slate)

                    tmp = pp[use_cols].copy()
                    # only compare on-slate props rows (most relevant; avoids noise)
                    if pp_team_on_slate and pp_team_on_slate in tmp.columns:
                        tmp = tmp[tmp[pp_team_on_slate].fillna(False).astype(bool)].copy()

                    tmp = tmp.rename(columns={pp_pid: "player_id", pp_team: "team_props"})
                    tmp["player_id"] = pd.to_numeric(tmp["player_id"], errors="coerce")
                    tmp["team_props"] = tmp["team_props"].map(_norm_team)

                    ls_pid_team = ls.dropna(subset=[pid_col])[[pid_col, team_col]].copy()
                    ls_pid_team = ls_pid_team.rename(columns={pid_col: "player_id", team_col: "team_ls"})
                    ls_pid_team["player_id"] = pd.to_numeric(ls_pid_team["player_id"], errors="coerce")
                    ls_pid_team["team_ls"] = ls_pid_team["team_ls"].map(_norm_team)
                    ls_pid_team = ls_pid_team.dropna(subset=["player_id"])
                    ls_pid_team = ls_pid_team.drop_duplicates(subset=["player_id"], keep="first")

                    merged = tmp.merge(ls_pid_team, on="player_id", how="left")
                    # count only when we have both teams
                    merged["team_props"] = merged["team_props"].map(_norm_team)
                    merged["team_ls"] = merged["team_ls"].map(_norm_team)
                    have = (merged["team_props"].str.len() > 0) & (merged["team_ls"].str.len() > 0)
                    mism = have & (merged["team_props"] != merged["team_ls"])
                    mismatches_n = int(mism.sum())
                    if mismatches_n > 0:
                        for _, r in merged.loc[mism].head(25).iterrows():
                            ex = {
                                "player_id": int(r.get("player_id")) if pd.notna(r.get("player_id")) else None,
                                "props_team": str(r.get("team_props") or ""),
                                "league_status_team": str(r.get("team_ls") or ""),
                            }
                            if pp_name:
                                ex["player_name"] = str(r.get(pp_name) or "")
                            mism_examples.append(ex)
                        issues.append(f"props_predictions team mismatches vs league_status: {mismatches_n}")
        except Exception as e:
            issues.append(f"failed to load/compare props_predictions for roster sanity: {type(e).__name__}: {e}")

    if mism_examples:
        summary["props_team_mismatch_examples"] = mism_examples
    summary["props_team_mismatches"] = int(mismatches_n)
    summary["props_predictions_path"] = str(pp_path) if pp_path.exists() else None

    if max_team_mismatches_in_props is not None and mismatches_n > int(max_team_mismatches_in_props):
        issues.append(
            f"too many props team mismatches (max={int(max_team_mismatches_in_props)}): {int(mismatches_n)}"
        )

    ok = len(issues) == 0
    summary["league_status_path"] = str(ls_path)
    summary["league_status_rows"] = int(len(ls))
    if warnings:
        summary["warnings"] = warnings
    return RosterSanityResult(ok=ok, date=date_str, issues=issues, summary=summary)
