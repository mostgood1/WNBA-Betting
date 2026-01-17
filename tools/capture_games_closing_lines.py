import os
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd

# Allow importing project-local modules (if needed)
import sys
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "src"))

try:
    from nba_betting.odds_api import fetch_game_odds_current, OddsApiConfig
except Exception:
    fetch_game_odds_current = None
    OddsApiConfig = None


def american_to_implied_prob(american: Optional[float]) -> Optional[float]:
    try:
        if american is None:
            return None
        a = float(american)
        if pd.isna(a):
            return None
        if a > 0:
            return 100.0 / (a + 100.0)
        elif a < 0:
            return (-a) / ((-a) + 100.0)
        else:
            return 0.5
    except Exception:
        return None


def consensus_last(odds: pd.DataFrame) -> pd.DataFrame:
    # Build per-game consensus of closing lines from available books at runtime
    rows = []
    games = odds[["home_team", "away_team"]].drop_duplicates()
    for _, g in games.iterrows():
        home = g["home_team"]; away = g["away_team"]
        sub = odds[(odds["home_team"] == home) & (odds["away_team"] == away)]
        out = {"home_team": home, "away_team": away}
        # Spreads: use home outcome where available; average line and price
        try:
            spreads = sub[sub["market"] == "spreads"]
            home_spreads = spreads[spreads["outcome_name"] == home]
            if not home_spreads.empty:
                out["spread_home"] = float(home_spreads["point"].astype(float).mean())
                out["spread_home_price"] = float(home_spreads["price"].astype(float).mean())
        except Exception:
            pass
        # Totals: use Over lines; average
        try:
            totals = sub[sub["market"] == "totals"]
            overs = totals[totals["outcome_name"].str.lower() == "over"]
            if not overs.empty:
                out["total_line"] = float(overs["point"].astype(float).mean())
                out["total_over_price"] = float(overs["price"].astype(float).mean())
        except Exception:
            pass
        # Moneyline: average prices for each side
        try:
            ml = sub[sub["market"] == "h2h"]
            home_ml = ml[ml["outcome_name"] == home]
            away_ml = ml[ml["outcome_name"] == away]
            if not home_ml.empty:
                out["home_ml"] = float(home_ml["price"].astype(float).mean())
                out["home_implied_prob"] = american_to_implied_prob(out.get("home_ml"))
            if not away_ml.empty:
                out["away_ml"] = float(away_ml["price"].astype(float).mean())
                out["away_implied_prob"] = american_to_implied_prob(out.get("away_ml"))
        except Exception:
            pass
        rows.append(out)
    return pd.DataFrame(rows)


def _try_load_existing_odds(target: date) -> Optional[pd.DataFrame]:
    proc = BASE_DIR / "data" / "processed"
    # Prefer whatever the app expects/creates today
    for name in [
        f"game_odds_{target:%Y-%m-%d}.csv",
        f"odds_{target:%Y-%m-%d}.csv",
        f"market_{target:%Y-%m-%d}.csv",
    ]:
        fp = proc / name
        if fp.exists():
            try:
                df = pd.read_csv(fp)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    return df
            except Exception:
                continue
    return None


def main():
    # Env and args
    api_key = os.environ.get("ODDS_API_KEY")
    # Date arg via env or today
    ds = os.environ.get("NBA_DATE")
    if ds:
        target = datetime.strptime(ds, "%Y-%m-%d").date()
    else:
        target = date.today()

    odds_df = None
    # Primary: Odds API (if available and key present)
    if api_key and fetch_game_odds_current is not None and OddsApiConfig is not None:
        try:
            cfg = OddsApiConfig(api_key=api_key, markets="h2h,spreads,totals")
            odds_df = fetch_game_odds_current(cfg, datetime.combine(target, datetime.min.time()), verbose=False)
        except Exception:
            odds_df = None
    # Fallback: use existing odds snapshots if present
    if odds_df is None or (isinstance(odds_df, pd.DataFrame) and odds_df.empty):
        odds_df = _try_load_existing_odds(target)
    out_dir = BASE_DIR / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    # Frontend + API expect closing_lines_<date>.csv
    out_path = out_dir / f"closing_lines_{target:%Y-%m-%d}.csv"

    if odds_df is None or (isinstance(odds_df, pd.DataFrame) and odds_df.empty):
        pd.DataFrame(columns=[
            "date","home_team","visitor_team",
            "home_ml","away_ml","home_spread","away_spread","home_spread_price","away_spread_price",
            "total","total_over_price","total_under_price","bookmaker","commence_time",
        ]).to_csv(out_path, index=False)
        print(f"{{\"ok\": false, \"reason\": \"no-odds\", \"path\": \"{out_path}\"}}")
        return

    # If odds_df is already in game_odds_* format, normalize to expected closing_lines_* schema.
    # Otherwise, compute consensus from Odds API raw pulls.
    if set(["market","outcome_name","point","price"]).issubset(set(odds_df.columns)):
        clos = consensus_last(odds_df)
        clos.insert(0, "date", f"{target:%Y-%m-%d}")
        # Align to historical schema used by frontend
        out = pd.DataFrame({
            "date": clos.get("date"),
            "home_team": clos.get("home_team"),
            "visitor_team": clos.get("away_team"),
            "home_ml": clos.get("home_ml"),
            "away_ml": clos.get("away_ml"),
            "home_spread": clos.get("spread_home"),
            "away_spread": None,
            "home_spread_price": clos.get("spread_home_price"),
            "away_spread_price": None,
            "total": clos.get("total_line"),
            "total_over_price": clos.get("total_over_price"),
            "total_under_price": None,
            "bookmaker": "consensus",
            "commence_time": None,
        })
    else:
        df = odds_df.copy()
        # Ensure required columns exist and match naming expected by frontend
        if "visitor_team" not in df.columns and "away_team" in df.columns:
            df = df.rename(columns={"away_team": "visitor_team"})
        if "date" not in df.columns:
            df.insert(0, "date", f"{target:%Y-%m-%d}")
        # Keep only known columns
        keep = [
            "date","home_team","visitor_team",
            "home_ml","away_ml","home_spread","away_spread",
            "home_spread_price","away_spread_price",
            "total","total_over_price","total_under_price",
            "bookmaker","commence_time",
        ]
        for c in keep:
            if c not in df.columns:
                df[c] = None
        out = df[keep]
    out.to_csv(out_path, index=False)
    print(f"{{\"ok\": true, \"rows\": {len(out)}, \"path\": \"{out_path}\"}}")


if __name__ == "__main__":
    main()
