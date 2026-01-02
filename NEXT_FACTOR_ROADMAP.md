# Next-Factor Roadmap (Q1 2026)

This document prioritizes additional features to improve recommendations across Games (ML/ATS/TOTAL), Props, and PBP-derived markets (First Basket, Early Threes). Each item includes scope, quick win, data source, and integration notes.

## Priority 1: Games
- Rolling Form Windows: Use 7/14/30-day splits for offense/defense efficiency ($\mu$, $\sigma$) to capture trend momentum; blend with season priors.
- Travel & Time Zone: Flight distance and ET shift last 48h; penalize back-to-back with long-haul travel; source schedule venues + simple city mapping.
- Altitude Factor: Elevation adjustments for Denver/Utah/Mexico City; small pace/fatigue impact on totals and fourth-quarter performance.
- Blowout Risk: Combine spread, bench depth, and garbage-time propensity; down-weight player props and adjust totals bias.
- Refined Schedule Density: Extend 3-in-4 to sliding 5-in-7 with exponential decay; separate home/away fatigue asymmetry.
- Referee Tempo/Fouls (optional): FTA/game, pace deltas; integrate only if reliable public assignments are available before tip.

## Priority 2: Props
- Minutes Projections: Blend injury context with rotation history; cap upside when coach patterns limit minutes.
- Usage Shift Modeling: On/Off and teammate outs → expected delta in `USG%`, `AST%`, `REB%`; add “Usage uptick expected” scoring weight by stat.
- Shot Profile Alignment: Player 3PA rate vs opponent allowed-3s rank; drive/paint attempts vs allowed-PTS-in-paint.
- Opponent Matchups: Primary defender archetype (length/foot speed) as a categorical dampener/boost where available.
- Market Liquidity Strength: Number of books publishing a line and price dispersion; confidence boost for tight consensus.
- Late Injury Volatility: Flag fragile lines (Q tags within 2h of tip); reduce confidence or suppress display for unstable markets.

## Priority 3: First Basket / Early Threes
- Tip-Off Win Odds: Model center matchups from historical head-to-head; improve first-possession probability.
- Early Pace & Scheme: Team first-quarter pace vs opponent defensive set frequency; boost/depress early threes.
- Shooter Heat Check: Rolling first-quarter 3PA/FG% within 7 games; cautious boost capped to prevent overfitting.

## System-Level Enhancements
- Dynamic Weighting: Auto-tune factor weights monthly using out-of-sample validation; guardrails via isotonic calibration.
- Robust Slate Gating: Already added; extend to tip-aware late refresh (schedule-min start time − 90m).
- Explainability: Expand `why_explain` to name factor families (trend, travel, altitude, usage, liquidity) with concise thresholds.

## Data & Integration Notes
- Sources: Local schedule (venues, dates), OddsAPI consensus, boxscores/PBP history, injuries cache.
- Storage: Add small lookup tables (cities→time zones, cities→altitude). Keep everything in `data/processed`.
- Implementation: Stage changes behind feature flags; emit validation CSVs per factor to monitor impact.

## Initial Implementation Order (2–3 weeks)
1) Rolling windows 7/14/30 for games+props (trend weights).
2) Market liquidity consensus strength for props/games.
3) Minutes projections + usage shift under injuries.
4) Tip-off odds + early pace for first basket/early threes.
5) Travel/time zone + altitude tweaks for games totals.

## KPIs
- Games: Brier/logloss improvements; calibrated reliability curve flattening.
- Props: Hit rate at fixed EV thresholds; ROI uplift with liquidity filter.
- PBP Markets: First basket top-1/top-3 accuracy; early threes MAE.
