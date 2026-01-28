from __future__ import annotations

import pandas as pd

from pathlib import Path


def main() -> None:
    ds = "2026-01-22"
    home = "PHI"
    away = "HOU"

    proc = Path("data/processed")
    pp = proc / f"props_predictions_{ds}.csv"
    if not pp.exists():
        raise SystemExit(f"Missing {pp}")

    props = pd.read_csv(pp)
    team_u = props["team"].astype(str).str.upper()
    opp_u = props["opponent"].astype(str).str.upper()
    sub = props[((team_u == away) & (opp_u == home)) | ((team_u == home) & (opp_u == away))].copy()
    print("props rows", len(sub), "players", int(sub["player_name"].nunique()))

    from nba_betting.sim.quarters import GameInputs, TeamContext, simulate_quarters
    from nba_betting.sim.connected_game import simulate_connected_game

    # Use neutral team contexts for quarters sampling (we only care about boxscore realism guardrails here).
    inp = GameInputs(
        date=ds,
        home=TeamContext(team=home, pace=98.0, off_rating=114.0, def_rating=114.0),
        away=TeamContext(team=away, pace=98.0, off_rating=114.0, def_rating=114.0),
    )
    qsum = simulate_quarters(inp, n_samples=350)
    sim = simulate_connected_game(
        qsum.quarters,
        home_tri=home,
        away_tri=away,
        props_df=sub,
        n_samples=350,
        seed=1,
        date_str=ds,
        use_event_level_sim=True,
    )

    rep = sim.get("rep") or {}
    for side in ("home", "away"):
        box = rep.get(f"{side}_box") or {}
        players = box.get("players") or []
        top_pts = sorted(players, key=lambda r: int(r.get("pts") or 0), reverse=True)[:5]
        top_reb = sorted(players, key=lambda r: int(r.get("reb") or 0), reverse=True)[:3]
        top_ast = sorted(players, key=lambda r: int(r.get("ast") or 0), reverse=True)[:3]

        print("\n", side, "team_total", box.get("team_total_pts"))
        print("  top pts:", [(p.get("player_name"), p.get("pts")) for p in top_pts])
        print("  top reb:", [(p.get("player_name"), p.get("reb")) for p in top_reb])
        print("  top ast:", [(p.get("player_name"), p.get("ast")) for p in top_ast])

    diag = sim.get("diagnostics", {}) or {}
    print("\nwarnings:", diag.get("warnings"))
    print("event_diag:", diag.get("event_level"))


if __name__ == "__main__":
    main()
