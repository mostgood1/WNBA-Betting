# Props Engine Rebuild Plan

## Goal

Replace the current heuristic prop recommendation path with a market-specific engine that produces canonical candidates, calibrated probabilities, and sleeve-aware selection rules.

This rebuild stays rooted in the existing SmartSim stack. The sim engine remains the primary source of player expectation, variance, and matchup context; the rebuild changes how prop candidates are normalized, calibrated, filtered, and promoted.

This is a targeted rebuild of the props engine only. The rest of the application should remain intact:

- Keep the game-market recommendation path.
- Keep card manifest generation, settlement, and frontend rendering.
- Keep the locked vs playable board structure.

## Why A Rebuild Is Needed

The current prop path in [app.py](app.py#L15557), [app.py](app.py#L15959), and [app.py](app.py#L14521) combines mixed-source rows and then applies a generic ranking policy. Recent scans showed:

- Broad prop pools are negative.
- High-score and high-edge prop subsets are still negative.
- Profitable behavior is market-shape specific, not threshold specific.
- Positive sleeves currently cluster in `blk under` and `ast over`, while other sleeves such as steals and rebounds-over are structurally weak.

That means tuning thresholds on the existing engine is unlikely to produce durable hit-rate improvement.

## Current State

### Current runtime sources

- `props_recommendations_<date>.csv` style artifacts loaded through `api_cards`
- SmartSim-derived market comparisons in `_sim_vs_line_prop_recommendations`
- top-play wrappers with inconsistent field placement on `row`, `best`, and `top_play`

### Core principle

The rebuilt selector should answer: "given SmartSim's view of this player and this market, is this sleeve historically trustworthy enough to bet?"

It should not answer: "which wrapper row has the highest generic blended score?"

### Current failure mode

- Candidate schema is inconsistent.
- Score composition is cross-market and heuristic.
- Selection is mostly threshold-and-rank based.
- Historical evaluation is portfolio-level, not sleeve-first.

## Target Architecture

### 1. Canonical candidate layer

Every prop recommendation candidate should be represented by one normalized record regardless of source.

Required canonical fields:

- identity: date, game key, player, player id, team, opponent, home/away
- market: market, side, line, price, bookmaker, implied probability
- model: mean, sd, win probability, push probability, expected value, calibrated probability
- context: projected minutes, minutes confidence, starter flag, injury state, opponent position context, pace/total context
- provenance: source path, source priority, market sleeve key, feature completeness flags
- evaluation: actual, result, profit_u when settled

### 2. Market-specific sleeves

Instead of a single prop policy, maintain separate sleeves:

- points
- rebounds
- assists
- threes
- steals
- blocks
- combo markets (`pra`, `pr`, `pa`, `ra`)

Each sleeve should have:

- its own feature set
- its own calibration
- its own acceptance rules
- its own holdout evaluation

Each sleeve should still be driven by the same SmartSim core outputs:

- projected mean
- projected variance / uncertainty
- projected minutes confidence
- opponent and pace context

The sleeve-specific logic decides whether a SmartSim signal is actionable for that market shape.

### 3. Selection layer

Selection should not be “highest score wins”.

It should be:

- sleeve eligible or ineligible
- confidence gated by calibration and sample stability
- portfolio capped by sleeve and by game
- optionally promoted from playable to official only after sleeve-level reliability clears a threshold

### 4. Portfolio layer

The engine should output:

- `discard`
- `playable`
- `official`

Promotion logic should be portfolio-aware, not row-only. Example:

- allow multiple playable props from profitable sleeves
- keep official card game-market led until a prop sleeve proves stable enough for promotion

## Implementation Phases

## Phase 1: Canonicalization

Deliverables:

- new canonical candidate dataclass and extractor
- one evaluation script that writes canonical settled prop rows for a season window
- replacement of ad hoc `best` / `top_play` field reading in analysis code

Success criteria:

- every settled prop row resolves to one canonical market and side
- no field ambiguity between wrapper row and effective row

## Phase 2: Sleeve evaluation

Deliverables:

- sleeve-level backtest report by market, side, price bucket, line bucket, and confidence bucket
- reliability report for each sleeve
- allowlist and denylist configuration for live usage

Success criteria:

- each sleeve can be turned on or off independently
- worst sleeves are blocked by default

## Phase 3: Runtime selector replacement

Deliverables:

- new prop runtime selector using canonical candidates
- live bucketing into playable and official using sleeve rules
- fallback compatibility layer for existing UI payload shape

Success criteria:

- `api_cards` emits the same frontend contract
- prop selection no longer depends on mixed-source wrapper score ordering

## Phase 4: Official-card promotion

Deliverables:

- promotion policy from playable sleeve to official sleeve
- guardrails based on recent holdout ROI and calibration

Success criteria:

- official props are only enabled when sleeve-level evidence justifies it

## Recommended First Slice

Build the canonical candidate layer first, then backtest these sleeves explicitly:

- `blk under`
- `ast over`
- `threes under`
- `stl under`
- `reb over`

Reason:

- the first three provide positive or near-positive anchors
- the last two are currently negative control sleeves and should prove the filter is actually discriminating
- all should be evaluated with explicit SmartSim coverage checks so we know whether performance comes from model-backed rows or fallback wrapper rows

## Near-Term Runtime Policy

Until the rebuilt engine is live:

- keep official props disabled
- keep props on the playable board only
- treat `blk under` and `ast over` as the first candidate sleeves for selective re-entry

## Code Touch Points

Primary current touch points for replacement:

- [app.py](app.py#L15557)
- [app.py](app.py#L15959)
- [app.py](app.py#L14521)
- [app.py](app.py#L15497)

Primary existing data/model modules to reuse:

- [src/nba_betting/props_edges.py](src/nba_betting/props_edges.py)
- [src/nba_betting/props_backtest.py](src/nba_betting/props_backtest.py)
- [src/nba_betting/props_calibration.py](src/nba_betting/props_calibration.py)
- [src/nba_betting/sim/smart_sim.py](src/nba_betting/sim/smart_sim.py)
- [src/nba_betting/props/minutes_forecaster.py](src/nba_betting/props/minutes_forecaster.py)

## Immediate Next Tasks

1. Land canonical candidate extraction in `src/nba_betting/props/recommendation_engine.py`.
2. Add a canonical settled-sleeve evaluation script using that extractor.
3. Define live sleeve config with allowlist, denylist, and promotion flags.