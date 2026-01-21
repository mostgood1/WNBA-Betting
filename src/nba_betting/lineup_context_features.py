from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import numpy as np
import pandas as pd

from .config import paths


def _read_hist_any(parquet_path, csv_path) -> pd.DataFrame:
    try:
        if parquet_path.exists():
            return pd.read_parquet(parquet_path)
    except Exception:
        pass
    try:
        if csv_path.exists():
            return pd.read_csv(csv_path)
    except Exception:
        pass
    return pd.DataFrame()


def _clean_id_series(s: pd.Series) -> pd.Series:
    """Normalize IDs that may come back from CSV as floats like '4277961.0'."""
    out = s.astype(str).replace({"nan": "", "None": ""}).str.strip()
    out = out.str.replace(r"^(\d+)\.0$", r"\1", regex=True)
    out = out.replace({"nan": "", "None": ""}).str.strip()
    return out


def _season_start_for_today(today: Optional[pd.Timestamp] = None) -> str:
    t = pd.Timestamp.today().normalize() if today is None else pd.Timestamp(today).normalize()
    yr = int(t.year)
    if int(t.month) < 7:
        yr -= 1
    return f"{yr:04d}-10-01"


def _normalize_date_col(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "date" in df.columns:
        # keep as YYYY-MM-DD string for stable merge/filter
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def _filter_date_range(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "date" not in df.columns:
        return df
    d = _normalize_date_col(df)
    s = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    e = pd.to_datetime(end_date).strftime("%Y-%m-%d")
    m = (d["date"].astype(str) >= s) & (d["date"].astype(str) <= e)
    return d[m].copy()


def _add_acting_lineup(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    need = {"team", "home_team", "away_team", "home_lineup_player_ids", "away_lineup_player_ids"}
    if not need.issubset(set(df.columns)):
        return df

    out = df.copy()
    team = out["team"].astype(str).str.upper()
    home = out["home_team"].astype(str).str.upper()
    away = out["away_team"].astype(str).str.upper()

    acting = np.where(team == home, out["home_lineup_player_ids"], np.where(team == away, out["away_lineup_player_ids"], ""))
    out["acting_lineup_player_ids"] = pd.Series(acting, index=out.index).astype(str)
    out.loc[out["acting_lineup_player_ids"].isin(["nan", "None"]), "acting_lineup_player_ids"] = ""

    # Fallback: ESPN sometimes omits the team field for certain plays (notably rebounds).
    # Infer side by checking whether a participant athlete ID is in the home/away lineup IDs.
    try:
        miss = out["acting_lineup_player_ids"].astype(str).str.len().eq(0)
        if bool(miss.any()):
            # Do a safe row-wise check on only the missing rows (should be small).
            idxs = out.index[miss].tolist()
            for ix in idxs:
                try:
                    home_line = str(out.at[ix, "home_lineup_player_ids"] or "")
                    away_line = str(out.at[ix, "away_lineup_player_ids"] or "")
                    home_team = str(out.at[ix, "home_team"] or "").strip().upper()
                    away_team = str(out.at[ix, "away_team"] or "").strip().upper()
                    p1 = _clean_id_series(pd.Series([out.at[ix, "participant1_id"]])).iloc[0] if "participant1_id" in out.columns else ""
                    p2 = _clean_id_series(pd.Series([out.at[ix, "participant2_id"]])).iloc[0] if "participant2_id" in out.columns else ""
                    home_set = set([x for x in home_line.split(";") if x])
                    away_set = set([x for x in away_line.split(";") if x])
                    if (p1 and p1 in home_set) or (p2 and p2 in home_set):
                        out.at[ix, "acting_lineup_player_ids"] = home_line
                        if not str(out.at[ix, "team"] or "").strip():
                            out.at[ix, "team"] = home_team
                    elif (p1 and p1 in away_set) or (p2 and p2 in away_set):
                        out.at[ix, "acting_lineup_player_ids"] = away_line
                        if not str(out.at[ix, "team"] or "").strip():
                            out.at[ix, "team"] = away_team
                except Exception:
                    continue
    except Exception:
        pass
    return out


def _three_point_attempt_mask(text: pd.Series, points_attempted: pd.Series) -> pd.Series:
    pa = pd.to_numeric(points_attempted, errors="coerce")
    m1 = pa == 3
    # also catch cases where pointsAttempted missing
    t = text.astype(str)
    m2 = t.str.contains(r"three\s*point|3\s*-?point", case=False, regex=True, na=False)
    return (m1 | m2).fillna(False)


def _contains_any(text: pd.Series, patterns: list[str]) -> pd.Series:
    t = text.astype(str)
    m = pd.Series([False] * len(t), index=t.index)
    for p in patterns:
        m = m | t.str.contains(p, case=False, regex=True, na=False)
    return m.fillna(False)


def _event_type_mask(type_text: pd.Series, want: str) -> pd.Series:
    tt = type_text.astype(str).str.strip().str.lower()
    return (tt == str(want).strip().lower()).fillna(False)


def _event_type_contains(type_text: pd.Series, needle: str) -> pd.Series:
    tt = type_text.astype(str)
    return tt.str.contains(str(needle), case=False, regex=False, na=False).fillna(False)


def _build_player_minutes_from_stints(stints: pd.DataFrame) -> pd.DataFrame:
    if stints is None or stints.empty:
        return pd.DataFrame()
    need = {"team", "duration_sec", "lineup_player_ids"}
    if not need.issubset(set(stints.columns)):
        return pd.DataFrame()

    tmp = stints[["team", "duration_sec", "lineup_player_ids"]].copy()
    tmp["team"] = tmp["team"].astype(str).str.upper()
    tmp["duration_sec"] = pd.to_numeric(tmp["duration_sec"], errors="coerce").fillna(0.0)
    tmp["player_id"] = tmp["lineup_player_ids"].astype(str).str.split(";")
    tmp = tmp.explode("player_id")
    tmp["player_id"] = _clean_id_series(tmp["player_id"])
    tmp = tmp[tmp["player_id"].astype(str).str.len() > 0]
    if tmp.empty:
        return pd.DataFrame()

    out = tmp.groupby(["team", "player_id"], as_index=False)["duration_sec"].sum()
    out["minutes"] = out["duration_sec"].astype(float) / 60.0
    return out


def _build_pair_minutes_directional(pairs: pd.DataFrame) -> pd.DataFrame:
    if pairs is None or pairs.empty:
        return pd.DataFrame()
    need = {"team", "player1_id", "player2_id", "sec_together", "min_together"}
    if not need.issubset(set(pairs.columns)):
        return pd.DataFrame()

    p = pairs[["team", "player1_id", "player2_id", "sec_together", "min_together"]].copy()
    p["team"] = p["team"].astype(str).str.upper()
    p["player1_id"] = _clean_id_series(p["player1_id"])
    p["player2_id"] = _clean_id_series(p["player2_id"])

    a = p.rename(columns={"player1_id": "player_id", "player2_id": "teammate_id"})
    b = p.rename(columns={"player2_id": "player_id", "player1_id": "teammate_id"})
    out = pd.concat([a, b], ignore_index=True)
    return out


def build_lineup_teammate_effects(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_minutes_together: float = 25.0,
) -> dict[str, Any]:
    """Build season-range teammate-conditioned rate tables from play context + pair minutes.

    Outputs (data/processed):
      - lineup_player_baselines.(parquet|csv)
      - lineup_teammate_effects.(parquet|csv)

        Notes:
            - This is a first-pass dataset built from play-by-play + on-court context.
            - It’s directional: player_id -> teammate_id.
            - Event attribution is heuristic for non-shooting stats (reb/stl/blk/tov/pf) and may be imperfect,
                but is consistent and stable enough for regularization/conditioning.
    """

    if end_date is None:
        end_date = (pd.Timestamp.today().normalize() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    if start_date is None:
        start_date = _season_start_for_today(pd.Timestamp(end_date))

    pc = _read_hist_any(paths.data_processed / "play_context_history.parquet", paths.data_processed / "play_context_history.csv")
    st = _read_hist_any(paths.data_processed / "rotation_stints_history.parquet", paths.data_processed / "rotation_stints_history.csv")
    pr = _read_hist_any(paths.data_processed / "pair_minutes_history.parquet", paths.data_processed / "pair_minutes_history.csv")

    pc = _filter_date_range(pc, start_date, end_date)
    st = _filter_date_range(st, start_date, end_date)
    pr = _filter_date_range(pr, start_date, end_date)

    if pc is None or pc.empty:
        return {"start": start_date, "end": end_date, "error": "no_play_context_history"}
    if st is None or st.empty:
        return {"start": start_date, "end": end_date, "error": "no_rotation_stints_history"}
    if pr is None or pr.empty:
        return {"start": start_date, "end": end_date, "error": "no_pair_minutes_history"}

    # Minutes denominators
    player_minutes = _build_player_minutes_from_stints(st)
    pair_minutes = _build_pair_minutes_directional(pr)

    pair_minutes = pair_minutes[pair_minutes["min_together"].astype(float) >= float(min_minutes_together)].copy()
    if pair_minutes.empty:
        return {"start": start_date, "end": end_date, "error": "no_pairs_after_min_minutes", "min_minutes_together": float(min_minutes_together)}

    pc = _add_acting_lineup(pc)
    pc = pc[pc["acting_lineup_player_ids"].astype(str).str.len() > 0].copy()
    if pc.empty:
        return {"start": start_date, "end": end_date, "error": "no_acting_lineups"}

    # Normalize columns
    pc["team"] = pc["team"].astype(str).str.upper()
    pc["participant1_id"] = _clean_id_series(pc.get("participant1_id"))
    pc["participant2_id"] = _clean_id_series(pc.get("participant2_id"))

    score_value = pd.to_numeric(pc.get("score_value"), errors="coerce").fillna(0.0)
    points_attempted = pd.to_numeric(pc.get("points_attempted"), errors="coerce")

    scoring_play = pc.get("scoring_play")
    if scoring_play is None:
        scoring_play = pd.Series([False] * len(pc), index=pc.index)
    scoring_play = scoring_play.fillna(False).astype(bool)

    shooting_play = pc.get("shooting_play")
    if shooting_play is None:
        shooting_play = pd.Series([False] * len(pc), index=pc.index)
    shooting_play = shooting_play.fillna(False).astype(bool)

    text = pc.get("text")
    if text is None:
        text = pd.Series([""] * len(pc), index=pc.index)

    type_text = pc.get("type")
    if type_text is None:
        type_text = pd.Series([""] * len(pc), index=pc.index)

    is_3pa = _three_point_attempt_mask(text, points_attempted)
    is_fg_attempt = shooting_play & points_attempted.isin([2, 3])
    is_ft_attempt = shooting_play & (points_attempted == 1)

    pts = (score_value.where(scoring_play, 0.0)).astype(float)
    fga = is_fg_attempt.astype(int)
    three_pa = is_3pa.astype(int) * fga.astype(int)
    fgm = ((scoring_play) & (points_attempted.isin([2, 3]))).astype(int)
    three_pm = ((scoring_play) & (is_3pa) & (points_attempted.fillna(3).astype(float) >= 3)).astype(int)
    fta = is_ft_attempt.astype(int)
    ftm = ((scoring_play) & (points_attempted == 1)).astype(int)

    shooter_id = pc["participant1_id"]

    # Additional event attribution (heuristic but consistent):
    # - Rebounds credited to participant1 on Rebound plays or text mentions.
    # - Turnovers credited to participant1 on Turnover plays.
    # - Steals credited to participant2 on Turnover plays when text mentions steal.
    # - Blocks credited to participant2 when text mentions block; otherwise participant1 if no participant2.
    # - Fouls credited to participant1 on Foul plays.
    # ESPN play type text is often very specific (e.g. "Defensive Rebound", "Shooting Foul").
    is_reb = (_event_type_contains(type_text, "Rebound") | _contains_any(text, [r"\\brebound\\b"]))
    is_tov = (_event_type_contains(type_text, "Turnover") | _contains_any(text, [r"\\bturnover\\b"]))
    is_pf = (_event_type_contains(type_text, "Foul") | _contains_any(text, [r"\\bfoul\\b"]))  # includes offensive/personal
    has_steal = _event_type_contains(type_text, "Steal") | _contains_any(text, [r"steal"])  # catches steals/stolen
    has_block = _event_type_contains(type_text, "Block") | _contains_any(text, [r"block", r"blocked"]) 


    reb_player_id = pc["participant1_id"]
    tov_player_id = pc["participant1_id"]
    pf_player_id = pc["participant1_id"]
    stl_player_id = pc["participant2_id"]
    blk_player_id = pc["participant2_id"].where(pc["participant2_id"].astype(str).str.len().gt(0), pc["participant1_id"])

    # Team attribution: ESPN's 'team' for turnovers/blocks is typically the offense.
    # For steals/blocks (defense events), assign to the opponent team when we can.
    try:
        t = pc["team"].astype(str).str.upper()
        h = pc.get("home_team").astype(str).str.upper() if "home_team" in pc.columns else t
        a = pc.get("away_team").astype(str).str.upper() if "away_team" in pc.columns else t
        opp = np.where(t == h, a, np.where(t == a, h, t))
        stl_team = pd.Series(opp, index=pc.index).astype(str).str.upper()
        blk_team = pd.Series(opp, index=pc.index).astype(str).str.upper()
    except Exception:
        stl_team = pc["team"].astype(str).str.upper()
        blk_team = pc["team"].astype(str).str.upper()

    reb = (is_reb.astype(int)).astype(int)
    tov = (is_tov.astype(int)).astype(int)
    pf = (is_pf.astype(int)).astype(int)
    stl = ((is_tov & has_steal & pc["participant2_id"].astype(str).str.len().gt(0)).astype(int)).astype(int)
    blk = ((has_block & blk_player_id.astype(str).str.len().gt(0)).astype(int)).astype(int)

    # Shooter totals per-player (baseline)
    shooter_totals = pd.DataFrame(
        {
            "team": pc["team"],
            "player_id": shooter_id,
            "pts": pts,
            "fga": fga,
            "fgm": fgm,
            "three_pa": three_pa,
            "three_pm": three_pm,
            "fta": fta,
            "ftm": ftm,
        }
    )
    shooter_totals = shooter_totals[shooter_totals["player_id"].astype(str).str.len() > 0].copy()
    player_events = shooter_totals.groupby(["team", "player_id"], as_index=False)[["pts", "fga", "fgm", "three_pa", "three_pm", "fta", "ftm"]].sum()

    # Non-shooting events per player
    other_events_rows: list[pd.DataFrame] = []
    def _agg_simple(team_s: pd.Series, pid_s: pd.Series, val_s: pd.Series, col: str) -> pd.DataFrame:
        tmp = pd.DataFrame({"team": team_s, "player_id": pid_s, col: val_s})
        tmp["player_id"] = _clean_id_series(tmp["player_id"])
        tmp = tmp[tmp["player_id"].astype(str).str.len() > 0]
        if tmp.empty:
            return pd.DataFrame(columns=["team", "player_id", col])
        return tmp.groupby(["team", "player_id"], as_index=False)[col].sum()

    other_events_rows.append(_agg_simple(pc["team"], reb_player_id, reb, "reb"))
    other_events_rows.append(_agg_simple(pc["team"], tov_player_id, tov, "tov"))
    other_events_rows.append(_agg_simple(pc["team"], pf_player_id, pf, "pf"))
    other_events_rows.append(_agg_simple(stl_team, stl_player_id, stl, "stl"))
    other_events_rows.append(_agg_simple(blk_team, blk_player_id, blk, "blk"))
    other_events = None
    for fr in other_events_rows:
        if fr is None or fr.empty:
            continue
        if other_events is None:
            other_events = fr
        else:
            other_events = other_events.merge(fr, on=["team", "player_id"], how="outer")
    if other_events is None:
        other_events = pd.DataFrame(columns=["team", "player_id", "reb", "tov", "pf", "stl", "blk"])

    # Assists: infer from text + participant2
    assist_mask = scoring_play & pc["participant2_id"].astype(str).str.len().gt(0) & text.astype(str).str.contains("assist", case=False, na=False)
    assists = pd.DataFrame({"team": pc.loc[assist_mask, "team"], "player_id": pc.loc[assist_mask, "participant2_id"], "ast": 1})
    assists = assists.groupby(["team", "player_id"], as_index=False)["ast"].sum() if not assists.empty else pd.DataFrame(columns=["team", "player_id", "ast"])

    # Baselines
    baselines = player_minutes.merge(player_events, on=["team", "player_id"], how="left")
    baselines = baselines.merge(assists, on=["team", "player_id"], how="left")
    baselines = baselines.merge(other_events, on=["team", "player_id"], how="left")
    for c in ["pts", "fga", "fgm", "three_pa", "three_pm", "fta", "ftm", "ast", "reb", "tov", "pf", "stl", "blk"]:
        if c in baselines.columns:
            baselines[c] = pd.to_numeric(baselines[c], errors="coerce").fillna(0.0)
    baselines["pts_per36"] = np.where(baselines["minutes"] > 0, baselines["pts"] / baselines["minutes"] * 36.0, np.nan)
    baselines["fga_per36"] = np.where(baselines["minutes"] > 0, baselines["fga"] / baselines["minutes"] * 36.0, np.nan)
    baselines["three_pa_per36"] = np.where(baselines["minutes"] > 0, baselines["three_pa"] / baselines["minutes"] * 36.0, np.nan)
    baselines["ast_per36"] = np.where(baselines["minutes"] > 0, baselines["ast"] / baselines["minutes"] * 36.0, np.nan)
    baselines["reb_per36"] = np.where(baselines["minutes"] > 0, baselines["reb"] / baselines["minutes"] * 36.0, np.nan)
    baselines["tov_per36"] = np.where(baselines["minutes"] > 0, baselines["tov"] / baselines["minutes"] * 36.0, np.nan)
    baselines["pf_per36"] = np.where(baselines["minutes"] > 0, baselines["pf"] / baselines["minutes"] * 36.0, np.nan)
    baselines["stl_per36"] = np.where(baselines["minutes"] > 0, baselines["stl"] / baselines["minutes"] * 36.0, np.nan)
    baselines["blk_per36"] = np.where(baselines["minutes"] > 0, baselines["blk"] / baselines["minutes"] * 36.0, np.nan)

    # Teammate-conditioned shooter events: explode acting lineups to teammate_id
    shooter_ctx = pc[["team", "acting_lineup_player_ids"]].copy()
    shooter_ctx["player_id"] = shooter_id
    shooter_ctx["pts"] = pts
    shooter_ctx["fga"] = fga
    shooter_ctx["fgm"] = fgm
    shooter_ctx["three_pa"] = three_pa
    shooter_ctx["three_pm"] = three_pm
    shooter_ctx["fta"] = fta
    shooter_ctx["ftm"] = ftm

    shooter_ctx = shooter_ctx[shooter_ctx["player_id"].astype(str).str.len() > 0].copy()
    shooter_ctx["teammate_id"] = shooter_ctx["acting_lineup_player_ids"].astype(str).str.split(";")
    shooter_ctx = shooter_ctx.explode("teammate_id")
    shooter_ctx["teammate_id"] = _clean_id_series(shooter_ctx["teammate_id"])
    shooter_ctx = shooter_ctx[shooter_ctx["teammate_id"].astype(str).str.len() > 0]
    shooter_ctx = shooter_ctx[shooter_ctx["teammate_id"] != shooter_ctx["player_id"]]

    shooter_pair = shooter_ctx.groupby(["team", "player_id", "teammate_id"], as_index=False)[["pts", "fga", "fgm", "three_pa", "three_pm", "fta", "ftm"]].sum()

    # Other teammate-conditioned events: explode acting lineups to teammate_id
    other_ctx = pc[["team", "acting_lineup_player_ids"]].copy()
    other_ctx["player_id"] = reb_player_id
    other_ctx["reb"] = reb
    other_ctx["tov_player_id"] = tov_player_id
    other_ctx["tov"] = tov
    other_ctx["pf_player_id"] = pf_player_id
    other_ctx["pf"] = pf
    other_ctx["stl_player_id"] = stl_player_id
    other_ctx["stl"] = stl
    other_ctx["blk_player_id"] = blk_player_id
    other_ctx["blk"] = blk

    other_ctx["stl_team"] = stl_team
    other_ctx["blk_team"] = blk_team

    # Build per-stat frames so player_id attribution matches the correct participant.
    def _ctx_for(col: str, pid_col: str, team_col: str = "team") -> pd.DataFrame:
        tmp = other_ctx[[team_col, "acting_lineup_player_ids", pid_col, col]].copy()
        tmp = tmp.rename(columns={team_col: "team"})
        tmp = tmp.rename(columns={pid_col: "player_id"})
        tmp["player_id"] = _clean_id_series(tmp["player_id"])
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce").fillna(0).astype(int)
        tmp = tmp[tmp["player_id"].astype(str).str.len() > 0]
        tmp = tmp[tmp[col].astype(int) != 0]
        if tmp.empty:
            return pd.DataFrame(columns=["team", "player_id", "teammate_id", col])
        tmp["teammate_id"] = tmp["acting_lineup_player_ids"].astype(str).str.split(";")
        tmp = tmp.explode("teammate_id")
        tmp["teammate_id"] = _clean_id_series(tmp["teammate_id"])
        tmp = tmp[tmp["teammate_id"].astype(str).str.len() > 0]
        tmp = tmp[tmp["teammate_id"] != tmp["player_id"]]
        return tmp.groupby(["team", "player_id", "teammate_id"], as_index=False)[col].sum()

    reb_pair = _ctx_for("reb", "player_id")
    tov_pair = _ctx_for("tov", "tov_player_id")
    pf_pair = _ctx_for("pf", "pf_player_id")
    stl_pair = _ctx_for("stl", "stl_player_id", team_col="stl_team")
    blk_pair = _ctx_for("blk", "blk_player_id", team_col="blk_team")

    # Teammate-conditioned assists
    if assist_mask.any():
        assist_ctx = pc.loc[assist_mask, ["team", "acting_lineup_player_ids", "participant2_id"]].copy()
        assist_ctx["player_id"] = _clean_id_series(assist_ctx["participant2_id"])
        assist_ctx["ast"] = 1
        assist_ctx["teammate_id"] = assist_ctx["acting_lineup_player_ids"].astype(str).str.split(";")
        assist_ctx = assist_ctx.explode("teammate_id")
        assist_ctx["teammate_id"] = _clean_id_series(assist_ctx["teammate_id"])
        assist_ctx = assist_ctx[assist_ctx["player_id"].astype(str).str.len() > 0]
        assist_ctx = assist_ctx[assist_ctx["teammate_id"].astype(str).str.len() > 0]
        assist_ctx = assist_ctx[assist_ctx["teammate_id"] != assist_ctx["player_id"]]
        assist_pair = assist_ctx.groupby(["team", "player_id", "teammate_id"], as_index=False)["ast"].sum()
    else:
        assist_pair = pd.DataFrame(columns=["team", "player_id", "teammate_id", "ast"])

    teammate = shooter_pair.merge(assist_pair, on=["team", "player_id", "teammate_id"], how="left")
    teammate["ast"] = pd.to_numeric(teammate.get("ast"), errors="coerce").fillna(0.0)

    for extra in (reb_pair, tov_pair, pf_pair, stl_pair, blk_pair):
        try:
            if extra is not None and not extra.empty:
                teammate = teammate.merge(extra, on=["team", "player_id", "teammate_id"], how="left")
        except Exception:
            continue
    for c in ["reb", "tov", "pf", "stl", "blk"]:
        if c not in teammate.columns:
            teammate[c] = 0.0
        teammate[c] = pd.to_numeric(teammate[c], errors="coerce").fillna(0.0)

    # Add denominators
    teammate = teammate.merge(pair_minutes[["team", "player_id", "teammate_id", "min_together"]], on=["team", "player_id", "teammate_id"], how="inner")

    teammate["pts_per36_with"] = np.where(teammate["min_together"] > 0, teammate["pts"] / teammate["min_together"] * 36.0, np.nan)
    teammate["fga_per36_with"] = np.where(teammate["min_together"] > 0, teammate["fga"] / teammate["min_together"] * 36.0, np.nan)
    teammate["three_pa_per36_with"] = np.where(teammate["min_together"] > 0, teammate["three_pa"] / teammate["min_together"] * 36.0, np.nan)
    teammate["ast_per36_with"] = np.where(teammate["min_together"] > 0, teammate["ast"] / teammate["min_together"] * 36.0, np.nan)
    teammate["reb_per36_with"] = np.where(teammate["min_together"] > 0, teammate["reb"] / teammate["min_together"] * 36.0, np.nan)
    teammate["tov_per36_with"] = np.where(teammate["min_together"] > 0, teammate["tov"] / teammate["min_together"] * 36.0, np.nan)
    teammate["pf_per36_with"] = np.where(teammate["min_together"] > 0, teammate["pf"] / teammate["min_together"] * 36.0, np.nan)
    teammate["stl_per36_with"] = np.where(teammate["min_together"] > 0, teammate["stl"] / teammate["min_together"] * 36.0, np.nan)
    teammate["blk_per36_with"] = np.where(teammate["min_together"] > 0, teammate["blk"] / teammate["min_together"] * 36.0, np.nan)

    # Persist
    base_pq = paths.data_processed / "lineup_player_baselines.parquet"
    base_csv = paths.data_processed / "lineup_player_baselines.csv"
    tm_pq = paths.data_processed / "lineup_teammate_effects.parquet"
    tm_csv = paths.data_processed / "lineup_teammate_effects.csv"

    wrote_base = None
    wrote_tm = None
    try:
        baselines.to_parquet(base_pq, index=False)
        wrote_base = str(base_pq)
    except Exception:
        baselines.to_csv(base_csv, index=False)
        wrote_base = str(base_csv)

    try:
        teammate.to_parquet(tm_pq, index=False)
        wrote_tm = str(tm_pq)
    except Exception:
        teammate.to_csv(tm_csv, index=False)
        wrote_tm = str(tm_csv)

    return {
        "start": str(start_date),
        "end": str(end_date),
        "pairs_min_minutes": float(min_minutes_together),
        "rows_baselines": int(len(baselines)),
        "rows_teammate": int(len(teammate)),
        "wrote_baselines": wrote_base,
        "wrote_teammate": wrote_tm,
    }
