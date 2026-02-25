#!/usr/bin/env python3
"""Daily Live Lens ROI report: JSONL signals -> settled rows + markdown summary.

Reads (for a given date):
- data/processed/live_lens_signals_<date>.jsonl
- data/processed/recon_games_<date>.csv (game totals + spreads)
- data/processed/recon_quarters_<date>.csv (half/quarter totals)
- data/processed/recon_props_<date>.csv (player props actuals)

Writes:
- data/processed/reports/live_lens_roi_<date>.md
- data/processed/reports/live_lens_roi_scored_<date>.csv

This report settles logged *signals* (BET/WATCH/NONE) into realized outcomes.
It is intentionally conservative:
- Uses 1u risk units.
- Uses logged prices when available (player props), otherwise assumes -110.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
LIVE_LENS_DIR = Path((os.getenv("NBA_LIVE_LENS_DIR") or os.getenv("LIVE_LENS_DIR") or "").strip() or str(PROCESSED))
REPORTS = PROCESSED / "reports"


def _parse_date(s: str) -> _date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _iso(d: _date) -> str:
    return d.isoformat()


def _n(x: Any) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


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


def _norm_player_name(s: str) -> str:
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


def _live_stat_key(x: Any) -> str:
    s = str(x or "").strip().lower()
    m = {
        "points": "pts",
        "point": "pts",
        "pts": "pts",
        "rebounds": "reb",
        "rebound": "reb",
        "reb": "reb",
        "assists": "ast",
        "assist": "ast",
        "ast": "ast",
        "3pt": "threes",
        "3pm": "threes",
        "threes": "threes",
        "threes_made": "threes",
        "pra": "pra",
        "points+rebounds+assists": "pra",
        "pr": "pr",
        "points+rebounds": "pr",
        "pa": "pa",
        "points+assists": "pa",
        "ra": "ra",
        "rebounds+assists": "ra",
    }
    return m.get(s, s)


def _parse_iso_ts(s: Any) -> Optional[datetime]:
    try:
        t = str(s or "").strip()
        if not t:
            return None
        # pandas handles many variants; keep this local/simple
        dt = pd.to_datetime(t, errors="coerce", utc=True)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


def _american_profit(price: float, win: bool) -> float:
    """Return profit for 1u risk at American odds, excluding stake (loss is -1)."""
    if not win:
        return -1.0
    try:
        p = float(price)
    except Exception:
        return float("nan")
    if p == 0:
        return float("nan")
    if p > 0:
        return p / 100.0
    return 100.0 / abs(p)


def _settle_over_under(actual: float, line: float, side: str) -> tuple[str, bool | None]:
    s = (side or "").strip().upper()
    if s not in {"OVER", "UNDER"}:
        return "", None
    if actual == line:
        return "PUSH", None
    if s == "OVER":
        return ("WIN", True) if actual > line else ("LOSS", False)
    return ("WIN", True) if actual < line else ("LOSS", False)


def _settle_ats(actual_margin_home: float, side: str) -> tuple[str, bool | None, float | None, str | None]:
    """Settle ATS given final home margin and a side string like 'BOS -3.5'."""
    s = str(side or "").strip().upper()
    if not s:
        return "", None, None, None

    # Extract first token as team code, and first signed float as spread
    m_team = re.match(r"^([A-Z]{2,4})\b", s)
    m_spread = re.search(r"([+-]?\d+(?:\.\d+)?)", s)
    if not m_team or not m_spread:
        return "", None, None, None

    team = m_team.group(1)
    try:
        spread = float(m_spread.group(1))
    except Exception:
        return "", None, None, team

    # If the side includes something like 'BOS -3.5', spread is relative to that team.
    # To settle, we need that team's final margin (team pts - opp pts).
    # Caller provides only home margin, so we return team and spread; caller decides sign.
    return "", None, spread, team


def _rem_bucket(market: str, horizon: str | None, remaining_min: float | None) -> str | None:
    if remaining_min is None:
        return None
    rm = float(remaining_min)

    # Bucket shapes: simple + stable (good for dashboarding)
    if market == "quarter_total":
        # 12 minute quarter
        if rm >= 6:
            return "6+"
        if rm >= 4:
            return "4-6"
        if rm >= 2:
            return "2-4"
        return "<2"

    if market == "half_total":
        # 24 minute half
        if rm >= 12:
            return "12+"
        if rm >= 8:
            return "8-12"
        if rm >= 4:
            return "4-8"
        return "<4"

    # Full game totals, ATS, and player props bucket on game minutes remaining
    if rm >= 24:
        return "24+"
    if rm >= 18:
        return "18-24"
    if rm >= 12:
        return "12-18"
    if rm >= 6:
        return "6-12"
    return "<6"


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                out.append(obj)
    return out


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _prep_recon_games(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "game_id" in out.columns:
        out["_gid"] = out["game_id"].map(_canon_nba_game_id)
    else:
        out["_gid"] = ""
    for c in ("home_tri", "away_tri"):
        if c in out.columns:
            out[c] = out[c].astype(str).str.strip().str.upper()
    return out


def _prep_recon_quarters(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "game_id" in out.columns:
        out["_gid"] = out["game_id"].map(_canon_nba_game_id)
    else:
        out["_gid"] = ""
    for c in ("home_tri", "away_tri"):
        if c in out.columns:
            out[c] = out[c].astype(str).str.strip().str.upper()
    return out


def _prep_recon_props(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "game_id" in out.columns:
        out["_gid"] = out["game_id"].map(_canon_nba_game_id)
    else:
        out["_gid"] = ""

    if "player_name" in out.columns:
        out["_name_key"] = out["player_name"].astype(str).map(_norm_player_name)
    else:
        out["_name_key"] = ""

    # Ensure numeric stat cols
    for c in ("pts", "reb", "ast", "threes"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    # Derive combos if absent
    if all(c in out.columns for c in ("pts", "reb")) and "pr" not in out.columns:
        out["pr"] = out["pts"] + out["reb"]
    if all(c in out.columns for c in ("pts", "ast")) and "pa" not in out.columns:
        out["pa"] = out["pts"] + out["ast"]
    if all(c in out.columns for c in ("reb", "ast")) and "ra" not in out.columns:
        out["ra"] = out["reb"] + out["ast"]
    if all(c in out.columns for c in ("pts", "reb", "ast")) and "pra" not in out.columns:
        out["pra"] = out["pts"] + out["reb"] + out["ast"]

    return out


def _actual_total(market: str, horizon: str | None, gid: str | None, home: str | None, away: str | None, rg: pd.DataFrame, rq: pd.DataFrame) -> float | None:
    if market == "total":
        if rg.empty:
            return None
        if gid:
            hit = rg[rg.get("_gid") == gid]
            if not hit.empty:
                return _n(hit.iloc[0].get("total_actual"))
        if home and away:
            hit = rg[(rg.get("home_tri") == home) & (rg.get("away_tri") == away)]
            if not hit.empty:
                return _n(hit.iloc[0].get("total_actual"))
        return None

    if market in {"half_total", "quarter_total"}:
        if rq.empty:
            return None
        hit = pd.DataFrame()
        if gid and "_gid" in rq.columns:
            hit = rq[rq.get("_gid") == gid]
        if hit.empty and home and away:
            hit = rq[(rq.get("home_tri") == home) & (rq.get("away_tri") == away)]
        if hit.empty:
            return None

        row = hit.iloc[0]
        if market == "half_total":
            if horizon == "h1":
                return _n(row.get("actual_h1_total"))
            if horizon == "h2":
                return _n(row.get("actual_h2_total"))
            return None

        if market == "quarter_total":
            hz = horizon or ""
            if hz in {"q1", "q2", "q3", "q4"}:
                return _n(row.get(f"actual_{hz}_total"))
            return None

    return None


def _actual_margin_home(gid: str | None, home: str | None, away: str | None, rg: pd.DataFrame) -> float | None:
    if rg.empty:
        return None
    hit = pd.DataFrame()
    if gid:
        hit = rg[rg.get("_gid") == gid]
    if hit.empty and home and away:
        hit = rg[(rg.get("home_tri") == home) & (rg.get("away_tri") == away)]
    if hit.empty:
        return None
    # recon_games uses actual_margin (home - away)
    return _n(hit.iloc[0].get("actual_margin"))


def _actual_prop(name_key: str | None, stat_key: str, rp: pd.DataFrame) -> float | None:
    if rp.empty:
        return None
    nk = (name_key or "").strip().upper()
    if not nk:
        return None
    hit = rp[rp.get("_name_key") == nk]
    if hit.empty:
        return None
    col = stat_key
    if col not in hit.columns:
        return None
    return _n(hit.iloc[0].get(col))


@dataclass(frozen=True)
class Scored:
    date: str
    market: str
    horizon: str | None
    klass: str | None
    game_id: str | None
    home: str | None
    away: str | None
    player: str | None
    stat: str | None
    side: str | None
    line: float | None
    actual: float | None
    price: float | None
    outcome: str | None
    profit_u: float | None
    remaining: float | None
    rem_bucket: str | None
    received_at: str | None
    strength: float | None


def _score_rows(ds: str, assumed_juice: float, include_watch: bool) -> list[Scored]:
    sig_path = LIVE_LENS_DIR / f"live_lens_signals_{ds}.jsonl"
    sigs = _load_jsonl(sig_path)
    if not sigs:
        return []

    rg = _prep_recon_games(_load_csv(PROCESSED / f"recon_games_{ds}.csv"))
    rq = _prep_recon_quarters(_load_csv(PROCESSED / f"recon_quarters_{ds}.csv"))
    rp = _prep_recon_props(_load_csv(PROCESSED / f"recon_props_{ds}.csv"))

    out: list[Scored] = []

    for obj in sigs:
        market = str(obj.get("market") or "").strip().lower()
        if market not in {"total", "half_total", "quarter_total", "ats", "player_prop"}:
            continue

        klass = str(obj.get("klass") or "").strip().upper() or None
        if not include_watch and klass != "BET":
            continue

        gid = _canon_nba_game_id(obj.get("game_id")) or None
        home = str(obj.get("home") or "").strip().upper() or None
        away = str(obj.get("away") or "").strip().upper() or None
        horizon = str(obj.get("horizon") or "").strip().lower() or None
        side = str(obj.get("side") or "").strip().upper() or None
        remaining = _n(obj.get("remaining"))
        rem_bucket = _rem_bucket(market, horizon, remaining)
        strength = _n(obj.get("strength"))

        received_at = None
        for k in ("received_at", "ts", "created_at"):
            v = obj.get(k)
            if v:
                received_at = str(v)
                break

        line = None
        if market == "player_prop":
            line = _n(obj.get("line"))
        else:
            line = _n(obj.get("live_line"))

        actual = None
        outcome = None
        win = None
        price = None
        profit_u = None

        if market in {"total", "half_total", "quarter_total"}:
            if line is not None and side is not None:
                actual = _actual_total(market, horizon, gid, home, away, rg, rq)
                if actual is not None:
                    outcome, win = _settle_over_under(float(actual), float(line), str(side))

            if outcome == "PUSH":
                profit_u = 0.0
            elif win is not None:
                price = -float(abs(assumed_juice))
                profit_u = _american_profit(price, bool(win))

        elif market == "ats":
            margin_home = _actual_margin_home(gid, home, away, rg)
            if margin_home is not None and side is not None:
                _, _, spread, team = _settle_ats(float(margin_home), str(side))
                # Determine which team was picked relative to home/away
                team_margin = None
                if spread is not None and team is not None:
                    if home and team == home:
                        team_margin = float(margin_home)
                    elif away and team == away:
                        team_margin = -float(margin_home)
                    # else: cannot determine
                if team_margin is not None and spread is not None:
                    v = float(team_margin) + float(spread)
                    if v == 0:
                        outcome, win = "PUSH", None
                    elif v > 0:
                        outcome, win = "WIN", True
                    else:
                        outcome, win = "LOSS", False

            if outcome == "PUSH":
                profit_u = 0.0
            elif win is not None:
                price = -float(abs(assumed_juice))
                profit_u = _american_profit(price, bool(win))

        elif market == "player_prop":
            player = str(obj.get("player") or "").strip() or None
            stat = str(obj.get("stat") or "").strip() or None
            stat_key = _live_stat_key(stat)
            name_key = str(obj.get("name_key") or "").strip().upper() or _norm_player_name(player or "")

            if line is not None and side is not None:
                actual = _actual_prop(name_key, stat_key, rp)
                if actual is not None:
                    outcome, win = _settle_over_under(float(actual), float(line), str(side))

            # Price from context (if present)
            ctx = obj.get("context")
            ctx_price = None
            if isinstance(ctx, dict):
                if side == "OVER":
                    ctx_price = _n(ctx.get("price_over"))
                elif side == "UNDER":
                    ctx_price = _n(ctx.get("price_under"))
                if ctx_price is None:
                    ctx_price = _n(ctx.get("price"))

            if ctx_price is not None:
                price = float(ctx_price)
            else:
                price = -float(abs(assumed_juice))

            if outcome == "PUSH":
                profit_u = 0.0
            elif win is not None and price is not None:
                profit_u = _american_profit(float(price), bool(win))

            out.append(
                Scored(
                    date=str(obj.get("date") or ds),
                    market=market,
                    horizon=horizon,
                    klass=klass,
                    game_id=gid,
                    home=home,
                    away=away,
                    player=player,
                    stat=stat_key,
                    side=side,
                    line=line,
                    actual=actual,
                    price=price,
                    outcome=outcome,
                    profit_u=profit_u,
                    remaining=remaining,
                    rem_bucket=rem_bucket,
                    received_at=received_at,
                    strength=strength,
                )
            )
            continue

        out.append(
            Scored(
                date=str(obj.get("date") or ds),
                market=market,
                horizon=horizon,
                klass=klass,
                game_id=gid,
                home=home,
                away=away,
                player=None,
                stat=None,
                side=side,
                line=line,
                actual=actual,
                price=price,
                outcome=outcome,
                profit_u=profit_u,
                remaining=remaining,
                rem_bucket=rem_bucket,
                received_at=received_at,
                strength=strength,
            )
        )

    return out


def _summary_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["market", "horizon", "klass", "n", "settled", "wins", "losses", "pushes", "profit_u", "roi_u_per_bet", "win_rate"])

    def _agg(g: pd.DataFrame) -> pd.Series:
        n = int(len(g))
        settled = int(g["outcome"].isin(["WIN", "LOSS", "PUSH"]).sum())
        wins = int((g["outcome"] == "WIN").sum())
        losses = int((g["outcome"] == "LOSS").sum())
        pushes = int((g["outcome"] == "PUSH").sum())
        profit = float(pd.to_numeric(g["profit_u"], errors="coerce").fillna(0.0).sum())
        denom = max(1, (wins + losses + pushes))
        roi = profit / float(denom)
        wr_denom = max(1, (wins + losses))
        wr = wins / float(wr_denom)
        return pd.Series(
            {
                "n": n,
                "settled": settled,
                "wins": wins,
                "losses": losses,
                "pushes": pushes,
                "profit_u": round(profit, 4),
                "roi_u_per_bet": round(roi, 4),
                "win_rate": round(wr, 4),
            }
        )

    group_cols = ["market", "horizon", "klass"]
    out = df.groupby(group_cols, dropna=False, as_index=False).apply(_agg).reset_index()
    # pandas adds extra index columns sometimes
    out = out.drop(columns=["level_0", "level_1", "index"], errors="ignore")
    out = out.sort_values(["market", "horizon", "klass"], ascending=[True, True, True])
    return out


def _df_to_md_table(df: pd.DataFrame, max_rows: int = 60) -> str:
    if df is None or df.empty:
        return "(no rows)"
    d = df.copy().head(max_rows)

    def _is_nan(x: Any) -> bool:
        try:
            return x is None or (isinstance(x, float) and math.isnan(x))
        except Exception:
            return x is None

    def _fmt(x: Any) -> str:
        if _is_nan(x):
            return ""
        if isinstance(x, float):
            # Keep reports compact and stable
            return str(round(x, 6))
        return str(x)

    def _esc(x: Any) -> str:
        return _fmt(x).replace("|", "\\|")

    cols = [str(c) for c in d.columns]
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = ["| " + " | ".join(_esc(v) for v in row) + " |" for row in d.itertuples(index=False, name=None)]
    return "\n".join([header, sep] + body)


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily Live Lens ROI report")
    ap.add_argument("--date", default=None, help="Target date YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--assumed-juice", type=float, default=110.0, help="Assume -juice when a price is not logged")
    ap.add_argument(
        "--include-watch",
        action="store_true",
        help="Include WATCH rows (default: BET only)",
    )
    args = ap.parse_args()

    if args.date:
        ds = _parse_date(args.date).isoformat()
    else:
        ds = (datetime.now().date() - timedelta(days=1)).isoformat()

    REPORTS.mkdir(parents=True, exist_ok=True)

    scored = _score_rows(ds, assumed_juice=float(args.assumed_juice), include_watch=bool(args.include_watch))
    if not scored:
        print(f"No scored rows for {ds} (missing logs or no settled markets)")
        return 2

    df = pd.DataFrame([r.__dict__ for r in scored])

    # Keep only settled rows for summary
    settled = df[df["outcome"].isin(["WIN", "LOSS", "PUSH"])].copy()

    out_csv = REPORTS / f"live_lens_roi_scored_{ds}.csv"
    try:
        df.to_csv(out_csv, index=False)
    except Exception:
        pass

    sum_all = _summary_table(settled)
    sum_bucket = _summary_table(settled[settled["rem_bucket"].notna()].rename(columns={"rem_bucket": "horizon"})) if False else None

    # Bucket summary: group by market + rem_bucket + klass
    bucket_df = pd.DataFrame()
    if not settled.empty:
        tmp = settled.copy()
        tmp["rem_bucket"] = tmp["rem_bucket"].fillna("(missing)")

        def _agg_b(g: pd.DataFrame) -> pd.Series:
            wins = int((g["outcome"] == "WIN").sum())
            losses = int((g["outcome"] == "LOSS").sum())
            pushes = int((g["outcome"] == "PUSH").sum())
            profit = float(pd.to_numeric(g["profit_u"], errors="coerce").fillna(0.0).sum())
            denom = max(1, (wins + losses + pushes))
            roi = profit / float(denom)
            wr_denom = max(1, (wins + losses))
            wr = wins / float(wr_denom)
            return pd.Series(
                {
                    "n": int(len(g)),
                    "wins": wins,
                    "losses": losses,
                    "pushes": pushes,
                    "profit_u": round(profit, 4),
                    "roi_u_per_bet": round(roi, 4),
                    "win_rate": round(wr, 4),
                }
            )

        bucket_df = (
            tmp.groupby(["market", "rem_bucket", "klass"], dropna=False)
            .apply(_agg_b)
            .reset_index()
            .drop(columns=["index"], errors="ignore")
            .sort_values(["market", "klass", "rem_bucket"], ascending=[True, True, False])
        )

    out_md = REPORTS / f"live_lens_roi_{ds}.md"

    md = []
    md.append(f"# Live Lens ROI report ({ds})")
    md.append("")
    md.append(f"- include_watch: {bool(args.include_watch)}")
    md.append(f"- assumed_juice: -{abs(float(args.assumed_juice))}")
    md.append("")
    md.append("## Summary (by market / horizon / klass)")
    md.append("")
    md.append(_df_to_md_table(sum_all))
    md.append("")
    md.append("## Buckets (by market / minutes remaining / klass)")
    md.append("")
    md.append(_df_to_md_table(bucket_df, max_rows=120))
    md.append("")
    md.append("## Notes")
    md.append("")
    md.append("- Totals/period totals and ATS assume -110 unless a price is logged.")
    md.append("- Player props use logged prices when available (context.price_over/price_under), else assume -110.")

    try:
        out_md.write_text("\n".join(md) + "\n", encoding="utf-8")
    except Exception:
        pass

    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
