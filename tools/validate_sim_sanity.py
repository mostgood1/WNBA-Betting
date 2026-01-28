"""Sanity checks for simulated + processed NBA artifacts.

Validates, for a given date:
- Quarter/half/game arithmetic and consistency in recon_quarters.
- Finals totals vs recon_quarters totals.
- Boxscore player points sum to team final points (by game_id mapping).
- smart_sim JSON internal consistency (period additivity, prob bounds).
- Optional: smart_sim vs actual score error summary.

Usage:
  python tools/validate_sim_sanity.py --date 2026-01-22

Exit code:
  0 if all checks pass within tolerances
  2 if any check fails
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
PROC = REPO_ROOT / "data" / "processed"


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    details: str

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "ok": self.ok, "details": self.details}


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def _dedupe_boxscores(box: pd.DataFrame) -> pd.DataFrame:
    """Drop accidental duplicated player rows in processed boxscores.

    Some pipelines can append the same boxscore rows multiple times; for sanity checks we want
    one row per (game_id, team, player_id).
    """
    if box is None or box.empty:
        return box
    cols = set(box.columns)
    subset = []
    for c in ("game_id", "TEAM_ABBREVIATION", "PLAYER_ID"):
        if c in cols:
            subset.append(c)
    if len(subset) < 2:
        return box
    try:
        return box.drop_duplicates(subset=subset, keep="last")
    except Exception:
        return box


def _load_smart_sim_players(date_str: str) -> dict[int, dict[str, Any]]:
    """Return {game_id: {home, away, player_ids_home, player_ids_away}} for a date."""
    out: dict[int, dict[str, Any]] = {}

    # Optional mapping from (home_tri, away_tri) -> game_id from game_cards.
    tri_to_gid: dict[tuple[str, str], int] = {}
    try:
        cards_p = PROC / f"game_cards_{date_str}.csv"
        if cards_p.exists():
            from nba_betting.teams import to_tricode  # type: ignore

            cdf = _read_csv(cards_p)
            if cdf is not None and not cdf.empty:
                cdf = cdf.copy()
                cdf["home_tri"] = cdf.get("home_team", "").astype(str).map(to_tricode)
                cdf["away_tri"] = cdf.get("visitor_team", "").astype(str).map(to_tricode)
                cdf["game_id"] = pd.to_numeric(cdf.get("game_id"), errors="coerce")
                cdf = cdf.dropna(subset=["home_tri", "away_tri", "game_id"])
                for rr in cdf.itertuples(index=False):
                    try:
                        ht = str(getattr(rr, "home_tri") or "").upper().strip()
                        at = str(getattr(rr, "away_tri") or "").upper().strip()
                        gid = int(float(getattr(rr, "game_id")))
                    except Exception:
                        continue
                    if ht and at:
                        tri_to_gid[(ht, at)] = gid
    except Exception:
        tri_to_gid = {}

    for fp in sorted(PROC.glob(f"smart_sim_{date_str}_*.json")):
        try:
            obj = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        gid = obj.get("game_id")
        home_tri = str(obj.get("home") or "").upper().strip()
        away_tri = str(obj.get("away") or "").upper().strip()
        gid_i = None
        try:
            if gid is not None and str(gid) != "nan":
                gid_i = int(float(gid))
        except Exception:
            gid_i = None
        if gid_i is None and home_tri and away_tri:
            gid_i = tri_to_gid.get((home_tri, away_tri))
        if gid_i is None:
            continue

        players = obj.get("players", {}) if isinstance(obj.get("players", {}), dict) else {}
        home_ids: set[int] = set()
        away_ids: set[int] = set()
        for pr in players.get("home", []) or []:
            try:
                home_ids.add(int(pr.get("player_id")))
            except Exception:
                pass
        for pr in players.get("away", []) or []:
            try:
                away_ids.add(int(pr.get("player_id")))
            except Exception:
                pass

        out[int(gid_i)] = {
            "file": fp.name,
            "home": home_tri,
            "away": away_tri,
            "home_ids": home_ids,
            "away_ids": away_ids,
        }
    return out


def check_props_predictions_sanity(date_str: str) -> CheckResult:
    p = PROC / f"props_predictions_{date_str}.csv"
    if not p.exists():
        return CheckResult("props_predictions sanity", True, f"missing {p} (skipped)")

    df = _read_csv(p)
    if df.empty:
        return CheckResult("props_predictions sanity", False, "file exists but empty")

    required = {
        "player_id",
        "player_name",
        "team",
        "asof_date",
        "opponent",
        "home",
        "pred_pts",
        "pred_reb",
        "pred_ast",
        "pred_pra",
        "sd_pts",
        "sd_reb",
        "sd_ast",
        "sd_pra",
    }
    missing_cols = sorted(list(required - set(df.columns)))
    if missing_cols:
        return CheckResult("props_predictions sanity", False, f"missing columns: {missing_cols}")

    # Duplicates (same player/team/date) are usually a pipeline bug.
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
    dup = df[df.duplicated(subset=["asof_date", "team", "player_id"], keep=False)]
    dup_n = int(len(dup))

    # PRA additivity and sd bounds.
    for c in ["pred_pts", "pred_reb", "pred_ast", "pred_pra", "sd_pts", "sd_reb", "sd_ast", "sd_pra"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    pra_err = (df["pred_pra"] - (df["pred_pts"] + df["pred_reb"] + df["pred_ast"]))
    # We now enforce PRA as a derived stat in outputs; allow only tiny floating error.
    pra_bad = int(pra_err.abs().gt(1e-6).sum())

    sd_bad = int(((df[["sd_pts", "sd_reb", "sd_ast", "sd_pra"]] < 0).any(axis=1)).sum())

    # Extremely small negatives due to numerical issues are ok; big negatives are not.
    pred_neg = int(((df[["pred_pts", "pred_reb", "pred_ast", "pred_pra"]] < -1e-6).any(axis=1)).sum())

    # Optional injury consistency if columns exist
    injury_inconsistent = 0
    if "injury_status" in df.columns and "playing_today" in df.columns:
        st = df["injury_status"].astype(str).str.upper().str.strip()
        playing = df["playing_today"].astype(bool)
        injury_inconsistent = int(((st.isin({"OUT", "O"})) & playing).sum())

    ok = (dup_n == 0) and (pra_bad == 0) and (sd_bad == 0) and (pred_neg == 0) and (injury_inconsistent == 0)
    details = f"rows={len(df)} dup_rows={dup_n} pra_bad={pra_bad} sd_bad={sd_bad} pred_negative={pred_neg} injury_inconsistent={injury_inconsistent}"
    return CheckResult("props_predictions sanity", ok, details)


def check_props_vs_smart_sim_coverage(date_str: str) -> CheckResult:
    props_p = PROC / f"props_predictions_{date_str}.csv"
    if not props_p.exists():
        return CheckResult("props vs smart_sim coverage", True, "missing props_predictions (skipped)")

    sims = _load_smart_sim_players(date_str)
    if not sims:
        return CheckResult("props vs smart_sim coverage", True, "no smart_sim files found (skipped)")

    props = _read_csv(props_p).copy()
    if props.empty:
        return CheckResult("props vs smart_sim coverage", False, "props_predictions empty")

    props["player_id"] = pd.to_numeric(props["player_id"], errors="coerce")
    props["team"] = props["team"].astype(str).str.upper().str.strip()

    # Only consider players marked as playing today if that signal exists.
    if "playing_today" in props.columns:
        try:
            props = props[props["playing_today"].astype(bool)].copy()
        except Exception:
            pass

    # For each sim game, count props players that belong to the game teams but are missing in sim.
    issues = []
    total_considered = 0
    total_missing = 0

    for gid, meta in sims.items():
        home = meta["home"]
        away = meta["away"]
        home_ids: set[int] = meta["home_ids"]
        away_ids: set[int] = meta["away_ids"]

        p_g = props[props["team"].isin([home, away])].dropna(subset=["player_id"]).copy()
        if p_g.empty:
            continue

        p_g["player_id_i"] = p_g["player_id"].astype(int)
        in_sim = set(home_ids) | set(away_ids)
        ids = set(p_g["player_id_i"].tolist())
        missing = ids - in_sim

        total_considered += len(ids)
        total_missing += len(missing)
        if missing:
            issues.append({"game_id": gid, "home": home, "away": away, "missing_n": len(missing)})

    miss_rate = (total_missing / max(1, total_considered))
    ok = miss_rate <= 0.05
    details = f"games={len(sims)} props_players_considered={total_considered} missing={total_missing} miss_rate={miss_rate:.3f}" + (f"; examples={issues[:5]}" if issues else "")
    return CheckResult("props vs smart_sim coverage", ok, details)


def check_props_vs_boxscores_coverage(date_str: str, min_minutes: float = 10.0) -> CheckResult:
    props_p = PROC / f"props_predictions_{date_str}.csv"
    box_p = PROC / f"boxscores_{date_str}.csv"
    if not props_p.exists() or not box_p.exists():
        return CheckResult("props vs boxscores coverage", True, "missing inputs (skipped)")

    props = _read_csv(props_p).copy()
    box = _dedupe_boxscores(_read_csv(box_p)).copy()
    if props.empty or box.empty:
        return CheckResult("props vs boxscores coverage", False, "props or boxscores empty")

    props["player_id"] = pd.to_numeric(props["player_id"], errors="coerce")
    prop_ids = set(props.dropna(subset=["player_id"])["player_id"].astype(int).tolist())

    box["PLAYER_ID"] = pd.to_numeric(box["PLAYER_ID"], errors="coerce")
    box["MIN"] = pd.to_numeric(box["MIN"], errors="coerce").fillna(0.0)
    played = box[box["MIN"] >= float(min_minutes)].dropna(subset=["PLAYER_ID"]).copy()
    played_ids = set(played["PLAYER_ID"].astype(int).tolist())

    missing = played_ids - prop_ids
    miss_rate = (len(missing) / max(1, len(played_ids)))

    # If you intended props_predictions to cover only teams on slate, missing can be legitimate.
    # Still, for a completed date file, large gaps suggest a data/join issue.
    ok = miss_rate <= 0.20
    details = f"played_min>={min_minutes:g} played_players={len(played_ids)} missing_in_props={len(missing)} miss_rate={miss_rate:.3f}"
    return CheckResult("props vs boxscores coverage", ok, details)


def check_recon_quarters_arithmetic(date_str: str) -> CheckResult:
    p = PROC / f"recon_quarters_{date_str}.csv"
    if not p.exists():
        return CheckResult("recon_quarters arithmetic", True, f"missing {p} (skipped)")

    df = _read_csv(p)
    if df.empty:
        return CheckResult("recon_quarters arithmetic", False, "file exists but empty")

    # Basic arithmetic: q1+q2=h1, q3+q4=h2, h1+h2=game
    def _violations(prefix: str) -> int:
        q1 = df[f"{prefix}_q1_total"]
        q2 = df[f"{prefix}_q2_total"]
        q3 = df[f"{prefix}_q3_total"]
        q4 = df[f"{prefix}_q4_total"]
        h1 = df[f"{prefix}_h1_total"]
        h2 = df[f"{prefix}_h2_total"]
        g = df[f"{prefix}_game_total"]

        v = 0
        v += int(((q1 + q2) - h1).abs().fillna(0).gt(1e-6).sum())
        v += int(((q3 + q4) - h2).abs().fillna(0).gt(1e-6).sum())
        v += int(((h1 + h2) - g).abs().fillna(0).gt(1e-6).sum())
        v += int(((q1 + q2 + q3 + q4) - g).abs().fillna(0).gt(1e-6).sum())
        return v

    actual_v = _violations("actual")
    pred_v = 0
    if {"pred_q1_total", "pred_q2_total", "pred_q3_total", "pred_q4_total", "pred_h1_total", "pred_h2_total", "pred_game_total"}.issubset(
        df.columns
    ):
        pred_v = _violations("pred")

    ok = (actual_v == 0) and (pred_v == 0)
    details = f"rows={len(df)} actual_violations={actual_v} pred_violations={pred_v}"
    return CheckResult("recon_quarters arithmetic", ok, details)


def check_finals_vs_recon_totals(date_str: str) -> CheckResult:
    recon_p = PROC / f"recon_quarters_{date_str}.csv"
    finals_p = PROC / f"finals_{date_str}.csv"
    pbp_recon_p = PROC / f"pbp_reconcile_{date_str}.csv"
    pbp_p = PROC / f"pbp_{date_str}.csv"

    if not recon_p.exists() or not finals_p.exists():
        return CheckResult(
            "finals vs recon totals",
            True,
            f"missing recon={recon_p.exists()} finals={finals_p.exists()} (skipped)",
        )

    recon = _read_csv(recon_p)
    finals = _read_csv(finals_p)

    if recon.empty or finals.empty:
        return CheckResult("finals vs recon totals", False, "recon or finals empty")

    finals["total"] = pd.to_numeric(finals.get("home_pts"), errors="coerce") + pd.to_numeric(finals.get("visitor_pts"), errors="coerce")

    j = recon.merge(
        finals[["date", "home_tri", "away_tri", "total"]],
        how="left",
        on=["date", "home_tri", "away_tri"],
        suffixes=("", "_finals"),
    )

    missing = int(j["total"].isna().sum())
    if missing > 0:
        return CheckResult("finals vs recon totals", False, f"missing finals matches for {missing}/{len(j)} rows")

    j["actual_game_total_num"] = pd.to_numeric(j["actual_game_total"], errors="coerce")
    j["total_num"] = pd.to_numeric(j["total"], errors="coerce")
    j["delta"] = j["total_num"] - j["actual_game_total_num"]

    mism_df = j[j["delta"].abs().gt(1e-6)].copy()
    mism = int(len(mism_df))
    if mism == 0:
        return CheckResult("finals vs recon totals", True, f"rows={len(j)} mismatches=0")

    # If the only reason for mismatch is overtime, recon_quarters will often reflect
    # regulation totals while finals reflect full-game totals.
    ot_reconciled = 0
    if pbp_recon_p.exists() and pbp_p.exists():
        try:
            game_map = _read_csv(pbp_recon_p)[["game_id", "home_team", "visitor_team"]].dropna().drop_duplicates().copy()
            game_map["home_team"] = game_map["home_team"].astype(str).str.upper().str.strip()
            game_map["visitor_team"] = game_map["visitor_team"].astype(str).str.upper().str.strip()

            pbp = pd.read_csv(pbp_p, usecols=["game_id", "period", "actionNumber", "pointsTotal", "scoreHome", "scoreAway"])  # type: ignore[arg-type]
            pbp = pbp[pbp.scoreHome.notna() & pbp.scoreAway.notna()].copy()

            # Build a small cache of (game_id -> (reg_total, final_total))
            totals = {}
            for gid, g in pbp.groupby("game_id"):
                g = g.sort_values(["period", "actionNumber"])
                # final total
                final_total = float(g[["pointsTotal"]].iloc[-1]["pointsTotal"])
                # regulation total at end of period 4 (if present)
                g4 = g[g["period"] == 4]
                if g4.empty:
                    continue
                reg_total = float(g4[["pointsTotal"]].iloc[-1]["pointsTotal"])
                totals[int(gid)] = (reg_total, final_total)

            def _is_ot_only(row) -> bool:
                home = str(row.get("home_tri") or "").upper().strip()
                away = str(row.get("away_tri") or "").upper().strip()
                mm = game_map[(game_map.home_team == home) & (game_map.visitor_team == away)]
                if mm.empty:
                    return False
                gid = int(mm.iloc[0]["game_id"])
                if gid not in totals:
                    return False
                reg_total, final_total = totals[gid]
                recon_total = float(row.get("actual_game_total_num") or 0.0)
                finals_total = float(row.get("total_num") or 0.0)
                # Treat as OT-only mismatch if recon matches regulation and finals matches final.
                return (abs(recon_total - reg_total) < 1e-6) and (abs(finals_total - final_total) < 1e-6) and (final_total > reg_total)

            ot_flags = mism_df.apply(_is_ot_only, axis=1)
            ot_reconciled = int(ot_flags.sum())
            mism_df.loc[ot_flags, "ot_only"] = True
            mism_df.loc[~ot_flags, "ot_only"] = False
        except Exception:
            # If OT reconciliation fails for any reason, fall back to strict mismatch reporting.
            ot_reconciled = 0

    remaining = mism - ot_reconciled
    ok = remaining == 0

    examples = mism_df[["home_tri", "away_tri", "actual_game_total_num", "total_num", "delta"]].head(5).to_dict("records")
    details = (
        f"rows={len(j)} mismatches={mism} ot_only={ot_reconciled} remaining={remaining}"
        + (f"; examples={examples}" if examples else "")
    )
    return CheckResult("finals vs recon totals", ok, details)


def check_boxscores_team_points(date_str: str) -> CheckResult:
    box_p = PROC / f"boxscores_{date_str}.csv"
    finals_p = PROC / f"finals_{date_str}.csv"
    map_p = PROC / f"pbp_reconcile_{date_str}.csv"

    if not box_p.exists() or not finals_p.exists() or not map_p.exists():
        return CheckResult(
            "boxscores team points",
            True,
            f"missing box={box_p.exists()} finals={finals_p.exists()} map={map_p.exists()} (skipped)",
        )

    box = _dedupe_boxscores(_read_csv(box_p))
    finals = _read_csv(finals_p)
    m = _read_csv(map_p)

    if box.empty:
        return CheckResult("boxscores team points", False, "boxscores empty")

    # Map game_id -> (home, away)
    m = m[["game_id", "home_team", "visitor_team"]].dropna().drop_duplicates(subset=["game_id", "home_team", "visitor_team"]).copy()
    m["home_team"] = m["home_team"].astype(str).str.upper().str.strip()
    m["visitor_team"] = m["visitor_team"].astype(str).str.upper().str.strip()

    # Finals per-team points
    finals["home_tri"] = finals["home_tri"].astype(str).str.upper().str.strip()
    finals["away_tri"] = finals["away_tri"].astype(str).str.upper().str.strip()

    # Aggregate boxscore points by game/team
    box["TEAM_ABBREVIATION"] = box["TEAM_ABBREVIATION"].astype(str).str.upper().str.strip()
    box_pts = (
        box.groupby(["game_id", "TEAM_ABBREVIATION"], as_index=False)["PTS"]
        .sum()
        .rename(columns={"TEAM_ABBREVIATION": "tri", "PTS": "box_pts"})
    )

    # Join to finals via mapped home/away
    j = box_pts.merge(m, on="game_id", how="left")
    # attach expected points for each team
    j = j.merge(
        finals[["date", "home_tri", "away_tri", "home_pts", "visitor_pts"]],
        left_on=["home_team", "visitor_team"],
        right_on=["home_tri", "away_tri"],
        how="left",
    )

    # Determine expected per row
    def _expected(row):
        tri = str(row.get("tri") or "").upper().strip()
        if tri and tri == str(row.get("home_team") or "").upper().strip():
            return row.get("home_pts")
        if tri and tri == str(row.get("visitor_team") or "").upper().strip():
            return row.get("visitor_pts")
        return None

    j["expected_pts"] = j.apply(_expected, axis=1)

    missing_map = int(j["home_team"].isna().sum())
    missing_finals = int(j["home_pts"].isna().sum())

    comp = j.dropna(subset=["expected_pts"]).copy()
    comp["expected_pts"] = pd.to_numeric(comp["expected_pts"], errors="coerce")
    comp["box_pts"] = pd.to_numeric(comp["box_pts"], errors="coerce")
    mism = int((comp["expected_pts"] - comp["box_pts"]).abs().gt(0.5).sum())

    ok = (missing_map == 0) and (missing_finals == 0) and (mism == 0)
    details = f"rows={len(box_pts)} missing_game_map={missing_map} missing_finals_join={missing_finals} point_mismatches={mism}"
    return CheckResult("boxscores team points", ok, details)


def check_smart_sim_internal(date_str: str) -> CheckResult:
    files = sorted(PROC.glob(f"smart_sim_{date_str}_*.json"))
    if not files:
        return CheckResult("smart_sim internal", True, "no smart_sim files found (skipped)")

    tol = 1e-6
    bad = []

    for fp in files:
        obj = json.loads(fp.read_text(encoding="utf-8"))
        score = obj.get("score", {})
        periods = obj.get("periods", {})

        # Additivity checks
        def g(d, k):
            v = d.get(k)
            return float(v) if v is not None else None

        home_mean = g(score, "home_mean")
        away_mean = g(score, "away_mean")
        total_mean = g(score, "total_mean")
        margin_mean = g(score, "margin_mean")

        if None in (home_mean, away_mean, total_mean, margin_mean):
            bad.append((fp.name, "missing score means"))
            continue

        if abs((home_mean + away_mean) - total_mean) > 1e-3:
            bad.append((fp.name, "home+away != total"))
        if abs((home_mean - away_mean) - margin_mean) > 1e-3:
            bad.append((fp.name, "home-away != margin"))

        # Period additivity (if q1-q4 exist)
        if all(k in periods for k in ["q1", "q2", "q3", "q4", "h1", "h2"]):
            q1t = _safe_float(periods["q1"].get("total_mean"))
            q2t = _safe_float(periods["q2"].get("total_mean"))
            q3t = _safe_float(periods["q3"].get("total_mean"))
            q4t = _safe_float(periods["q4"].get("total_mean"))
            h1t = _safe_float(periods["h1"].get("total_mean"))
            h2t = _safe_float(periods["h2"].get("total_mean"))

            if None not in (q1t, q2t, h1t) and abs((q1t + q2t) - h1t) > 1e-3:
                bad.append((fp.name, "q1+q2 != h1 (total_mean)"))
            if None not in (q3t, q4t, h2t) and abs((q3t + q4t) - h2t) > 1e-3:
                bad.append((fp.name, "q3+q4 != h2 (total_mean)"))
            # With small n_sims or rounding, tiny drift is expected. Keep this check as a guardrail
            # for big structural mismatches.
            if None not in (h1t, h2t):
                delta = abs((h1t + h2t) - total_mean)
                if delta > 0.5:
                    bad.append((fp.name, f"h1+h2 != score.total_mean (delta={delta:.2f})"))

        # Probability bounds
        for pk in ["p_home_win", "p_away_win", "p_home_cover", "p_total_over"]:
            pv = score.get(pk)
            if pv is None:
                continue
            try:
                pvf = float(pv)
            except Exception:
                bad.append((fp.name, f"{pk} non-numeric"))
                continue
            if not (0.0 - tol <= pvf <= 1.0 + tol):
                bad.append((fp.name, f"{pk} out of [0,1]"))

        # Player stat sanity
        players = obj.get("players", {})
        # Team points should roughly align with the sum of player mean points.
        # This isn't guaranteed (some sims may omit deep bench or include adjustments),
        # but extreme mismatches are a strong red flag.
        pts_sum_by_side = {}
        for side in ["home", "away"]:
            arr = players.get(side, []) if isinstance(players, dict) else []
            pts_sum = 0.0
            for pr in arr:
                # Non-negative means
                for mk in ["pts_mean", "reb_mean", "ast_mean", "pra_mean", "stl_mean", "blk_mean", "tov_mean", "threes_mean"]:
                    if mk in pr and pr[mk] is not None:
                        try:
                            if float(pr[mk]) < -1e-6:
                                bad.append((fp.name, f"player {mk} < 0"))
                                break
                        except Exception:
                            bad.append((fp.name, f"player {mk} non-numeric"))
                            break

                # PRA additivity
                try:
                    pts = float(pr.get("pts_mean", 0.0))
                    reb = float(pr.get("reb_mean", 0.0))
                    ast = float(pr.get("ast_mean", 0.0))
                    pra = float(pr.get("pra_mean", pts + reb + ast))
                    if abs((pts + reb + ast) - pra) > 1e-3:
                        bad.append((fp.name, "pra_mean != pts+reb+ast"))
                except Exception:
                    pass

                try:
                    pts_sum += float(pr.get("pts_mean", 0.0) or 0.0)
                except Exception:
                    pass

            pts_sum_by_side[side] = pts_sum

        # Compare player pts sums to score means (only if we have at least some players).
        if pts_sum_by_side.get("home", 0.0) > 0 and pts_sum_by_side.get("away", 0.0) > 0:
            home_diff = abs(pts_sum_by_side["home"] - home_mean)
            away_diff = abs(pts_sum_by_side["away"] - away_mean)
            # Flag only large diffs (avoid noise).
            if home_diff > max(25.0, 0.20 * max(home_mean, 1.0)):
                bad.append((fp.name, f"sum(home pts_mean) off by {home_diff:.1f}"))
            if away_diff > max(25.0, 0.20 * max(away_mean, 1.0)):
                bad.append((fp.name, f"sum(away pts_mean) off by {away_diff:.1f}"))

    ok = len(bad) == 0
    details = f"files={len(files)} issues={len(bad)}" + ("; examples=" + ", ".join([f"{a}:{b}" for a, b in bad[:5]]) if bad else "")
    return CheckResult("smart_sim internal", ok, details)


def check_smart_sim_game_id_present(date_str: str) -> CheckResult:
    files = sorted(PROC.glob(f"smart_sim_{date_str}_*.json"))
    if not files:
        return CheckResult("smart_sim game_id present", True, "no smart_sim files found (skipped)")

    missing: list[str] = []
    for fp in files:
        try:
            obj = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            missing.append(fp.name)
            continue
        gid = obj.get("game_id")
        try:
            if gid is None or str(gid).strip() == "" or str(gid).lower() == "nan":
                missing.append(fp.name)
            else:
                int(float(gid))
        except Exception:
            missing.append(fp.name)

    ok = len(missing) == 0
    details = f"files={len(files)} missing_game_id={len(missing)}" + (f"; missing={missing[:10]}" if missing else "")
    return CheckResult("smart_sim game_id present", ok, details)


def check_smart_sim_vs_actual_scores(date_str: str) -> CheckResult:
    # Optional: compare smart_sim score means vs finals using pbp_reconcile mapping
    files = sorted(PROC.glob(f"smart_sim_{date_str}_*.json"))
    finals_p = PROC / f"finals_{date_str}.csv"
    map_p = PROC / f"pbp_reconcile_{date_str}.csv"

    if not files or not finals_p.exists() or not map_p.exists():
        return CheckResult("smart_sim vs actual", True, "missing inputs (skipped)")

    finals = _read_csv(finals_p).copy()
    finals["home_tri"] = finals["home_tri"].astype(str).str.upper().str.strip()
    finals["away_tri"] = finals["away_tri"].astype(str).str.upper().str.strip()

    mapping = _read_csv(map_p)[["game_id", "home_team", "visitor_team"]].dropna().copy()
    mapping["home_team"] = mapping["home_team"].astype(str).str.upper().str.strip()
    mapping["visitor_team"] = mapping["visitor_team"].astype(str).str.upper().str.strip()

    # Build quick finals lookup
    finals_key = {(r.home_tri, r.away_tri): (float(r.home_pts), float(r.visitor_pts)) for r in finals.itertuples(index=False)}

    rows = []
    for fp in files:
        obj = json.loads(fp.read_text(encoding="utf-8"))
        home = str(obj.get("home") or "").upper().strip()
        away = str(obj.get("away") or "").upper().strip()
        score = obj.get("score", {})
        hm = _safe_float(score.get("home_mean"))
        am = _safe_float(score.get("away_mean"))
        tm = _safe_float(score.get("total_mean"))
        if (home, away) not in finals_key or None in (hm, am, tm):
            continue
        ah, aa = finals_key[(home, away)]
        rows.append({
            "home": home,
            "away": away,
            "pred_home": hm,
            "pred_away": am,
            "pred_total": tm,
            "act_home": ah,
            "act_away": aa,
            "act_total": ah + aa,
        })

    if not rows:
        return CheckResult("smart_sim vs actual", True, "no joinable games (skipped)")

    d = pd.DataFrame(rows)
    mae_total = (d["pred_total"] - d["act_total"]).abs().mean()
    mae_home = (d["pred_home"] - d["act_home"]).abs().mean()
    mae_away = (d["pred_away"] - d["act_away"]).abs().mean()

    return CheckResult(
        "smart_sim vs actual",
        True,
        f"games={len(d)} MAE(total)={mae_total:.2f} MAE(home)={mae_home:.2f} MAE(away)={mae_away:.2f}",
    )


def check_smart_sim_vs_actual_players(date_str: str, min_minutes: float = 10.0) -> CheckResult:
    """Sanity-check player projections against actual boxscores for completed games.

    This is *not* a model-accuracy test. It aims to catch structural issues like:
    - mismatched games or teams
    - broken player-id joins
    - missing large chunks of played players
    """

    files = sorted(PROC.glob(f"smart_sim_{date_str}_*.json"))
    box_p = PROC / f"boxscores_{date_str}.csv"

    if not files or not box_p.exists():
        return CheckResult("smart_sim vs boxscores (players)", True, "missing inputs (skipped)")

    box = _dedupe_boxscores(_read_csv(box_p)).copy()
    if box.empty:
        return CheckResult("smart_sim vs boxscores (players)", False, "boxscores empty")

    # Ensure numeric types
    box["PLAYER_ID"] = pd.to_numeric(box["PLAYER_ID"], errors="coerce")
    box["MIN"] = pd.to_numeric(box["MIN"], errors="coerce").fillna(0.0)
    for c in ["PTS", "REB", "AST"]:
        box[c] = pd.to_numeric(box[c], errors="coerce").fillna(0.0)
    box["PRA"] = box["PTS"] + box["REB"] + box["AST"]

    # Aggregate stats across all joinable player rows
    rows: list[dict[str, Any]] = []
    game_summaries: list[dict[str, Any]] = []

    for fp in files:
        obj = json.loads(fp.read_text(encoding="utf-8"))
        gid = obj.get("game_id")
        try:
            gid_i = int(gid)
        except Exception:
            continue

        b = box[box["game_id"] == gid_i].copy()
        if b.empty:
            continue

        b_played = b[b["MIN"] > 0].copy()
        played_ids = set(b_played["PLAYER_ID"].dropna().astype(int).tolist())

        players = obj.get("players", {}) if isinstance(obj.get("players", {}), dict) else {}
        sim_ids: set[int] = set()
        sim_pts_by_team = {"home": 0.0, "away": 0.0}

        for side in ["home", "away"]:
            for pr in players.get(side, []) or []:
                pid = pr.get("player_id")
                try:
                    pid_i = int(pid)
                except Exception:
                    continue
                sim_ids.add(pid_i)
                try:
                    sim_pts_by_team[side] += float(pr.get("pts_mean", 0.0) or 0.0)
                except Exception:
                    pass

                # Join by player_id
                act = b_played[b_played["PLAYER_ID"] == pid_i]
                if act.empty:
                    continue
                act_row = act.iloc[0]

                def f(k, default=0.0):
                    try:
                        return float(pr.get(k, default) or default)
                    except Exception:
                        return float(default)

                rows.append(
                    {
                        "game_id": gid_i,
                        "side": side,
                        "player_id": pid_i,
                        "player_name": pr.get("player_name"),
                        "min": float(act_row["MIN"]),
                        "pred_pts": f("pts_mean"),
                        "pred_reb": f("reb_mean"),
                        "pred_ast": f("ast_mean"),
                        "pred_pra": f("pra_mean"),
                        "act_pts": float(act_row["PTS"]),
                        "act_reb": float(act_row["REB"]),
                        "act_ast": float(act_row["AST"]),
                        "act_pra": float(act_row["PRA"]),
                    }
                )

        matched_ids = sim_ids.intersection(played_ids)
        match_rate = (len(matched_ids) / max(1, len(played_ids)))
        missing_played = len(played_ids - sim_ids)

        # Compare team point sums (pred sum of player means vs actual team totals)
        # Determine actual home/away team totals from boxscore TEAM_ABBREVIATION.
        home_tri = str(obj.get("home") or "").upper().strip()
        away_tri = str(obj.get("away") or "").upper().strip()
        act_home_pts = float(b_played[b_played["TEAM_ABBREVIATION"].astype(str).str.upper().str.strip() == home_tri]["PTS"].sum())
        act_away_pts = float(b_played[b_played["TEAM_ABBREVIATION"].astype(str).str.upper().str.strip() == away_tri]["PTS"].sum())

        game_summaries.append(
            {
                "game_id": gid_i,
                "home": home_tri,
                "away": away_tri,
                "played_players": len(played_ids),
                "match_rate": match_rate,
                "missing_played": missing_played,
                "pred_home_pts_sum": sim_pts_by_team["home"],
                "pred_away_pts_sum": sim_pts_by_team["away"],
                "act_home_pts": act_home_pts,
                "act_away_pts": act_away_pts,
                "home_pts_diff": abs(sim_pts_by_team["home"] - act_home_pts),
                "away_pts_diff": abs(sim_pts_by_team["away"] - act_away_pts),
            }
        )

    if not game_summaries:
        return CheckResult("smart_sim vs boxscores (players)", True, "no joinable games (skipped)")

    gs = pd.DataFrame(game_summaries)
    overall_match_rate = float(gs["match_rate"].mean())

    # Player-level error summary (focus on meaningful-minute players)
    if rows:
        d = pd.DataFrame(rows)
        d2 = d[d["min"] >= float(min_minutes)].copy()
        if not d2.empty:
            mae_pts = float((d2["pred_pts"] - d2["act_pts"]).abs().mean())
            mae_reb = float((d2["pred_reb"] - d2["act_reb"]).abs().mean())
            mae_ast = float((d2["pred_ast"] - d2["act_ast"]).abs().mean())
            mae_pra = float((d2["pred_pra"] - d2["act_pra"]).abs().mean())

            outliers = (
                d2.assign(err_pts=(d2["pred_pts"] - d2["act_pts"]).abs())
                .sort_values("err_pts", ascending=False)
                .head(5)
            )
            out_ex = outliers[["game_id", "player_name", "min", "pred_pts", "act_pts", "pred_pra", "act_pra"]].to_dict("records")
        else:
            mae_pts = mae_reb = mae_ast = mae_pra = float("nan")
            out_ex = []
    else:
        mae_pts = mae_reb = mae_ast = mae_pra = float("nan")
        out_ex = []

    # Heuristics for "sanity" (not accuracy):
    # - match rate should generally be decent if IDs line up
    # - team sum diffs shouldn't be absurdly large
    low_match_games = int((gs["match_rate"] < 0.30).sum())
    huge_sum_games = int(((gs["home_pts_diff"] > 100) | (gs["away_pts_diff"] > 100)).sum())
    ok = (overall_match_rate >= 0.60) and (low_match_games == 0) and (huge_sum_games == 0)

    details = (
        f"games={len(gs)} overall_match_rate={overall_match_rate:.2f} low_match_games={low_match_games} huge_sum_games={huge_sum_games}"
        + (f"; MAE@min>={min_minutes:g} pts={mae_pts:.2f} reb={mae_reb:.2f} ast={mae_ast:.2f} pra={mae_pra:.2f}" if mae_pts == mae_pts else "")
        + (f"; top_outliers={out_ex}" if out_ex else "")
    )
    return CheckResult("smart_sim vs boxscores (players)", ok, details)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--json-out", default=None, help="Optional path to write JSON report")
    args = ap.parse_args()

    date_str = args.date.strip()

    checks = [
        check_recon_quarters_arithmetic(date_str),
        check_finals_vs_recon_totals(date_str),
        check_boxscores_team_points(date_str),
        check_smart_sim_game_id_present(date_str),
        check_smart_sim_internal(date_str),
        check_smart_sim_vs_actual_scores(date_str),
        check_smart_sim_vs_actual_players(date_str),
        check_props_predictions_sanity(date_str),
        check_props_vs_smart_sim_coverage(date_str),
        check_props_vs_boxscores_coverage(date_str),
    ]

    any_fail = False
    print(f"Sanity audit for {date_str}  (processed dir: {PROC})")
    for c in checks:
        status = "OK" if c.ok else "FAIL"
        print(f"- {status}: {c.name}: {c.details}")
        if not c.ok:
            any_fail = True

    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "date": date_str,
            "processed_dir": str(PROC),
            "any_fail": any_fail,
            "checks": [c.to_dict() for c in checks],
        }
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Use exit code 1 for failures to match common CLI conventions.
    return 1 if any_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
