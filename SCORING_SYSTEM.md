# Scoring System (0–100)

This repo now uses a **single 0–100 score** for both **game picks** and **prop picks**.

The goal is:
- A **stable, deterministic** daily ranking signal
- A score that can be **backtested and optimized** against reconciliation (actuals)
- A score that is **explainable** (each pick can carry `score_explain` and `score_components`)

## 1) Philosophy

We score picks using a small set of inputs that exist in your processed artifacts:

### Games
- `market` (ML / ATS / TOTAL)
- `ev` (for ML)
- `edge` (for ATS/TOTAL)
- `price` (optional; used as a small modifier)

### Props
- `ev`
- probability edge (`edge` if present; otherwise `model_prob - implied_prob`)
- `price`

Each signal is transformed with a **sigmoid** so:
- outliers don’t dominate
- scores change smoothly
- optimization is easier (weights/scales matter, but the function is stable)

## 2) Game score

Implemented in `src/nba_betting/scoring.py::score_game_pick_0_100`.

### Moneyline (ML)

We use a weighted combination of:
- **EV component**: `sigmoid(ev / ev_scale)`
- **PriceQuality**: favors roughly-standard prices (e.g. -110)

Default form:

$$\text{Score} = 100 \cdot \frac{w_{ev}\,\sigma(ev/ev\_scale) + w_{price}\,PriceQuality}{w_{ev}+w_{price}}$$

Defaults:
- `w_ev = 0.85`
- `w_price = 0.04`
- `ev_scale = 0.04`

### ATS / TOTAL

We use:
- **Edge component**: `sigmoid((|edge_pts| - edge_center) / edge_scale)`
- **EV component**: `sigmoid(ev / ev_scale)`
- **PriceQuality**: small modifier

Default form:

$$\text{Score} = 100 \cdot \frac{w_{edge}\,\sigma((|edge|-edge\_center)/edge\_scale) + w_{ev}\,\sigma(ev/ev\_scale) + w_{price}\,PriceQuality}{w_{edge}+w_{ev}+w_{price}}$$

Defaults:
- `w_edge_pts = 0.86`
- `w_ev_non_ml = 0.10`
- `w_price = 0.04`
- `edge_center = 1.5`
- `edge_scale = 2.0`

## 3) Prop score

Implemented in `src/nba_betting/scoring.py::score_prop_pick_0_100`.

We combine:
- **EV component**: `sigmoid(ev / ev_scale)`
- **Prob edge component**: `sigmoid((prob_edge - prob_edge_center) / prob_edge_scale)`
- **PriceQuality**: small modifier

Where `prob_edge` is:
- `edge` if present, else `model_prob - implied_prob`

Default form:

$$\text{Score} = 100 \cdot \frac{w_{ev}\,\sigma(ev/ev\_scale) + w_{prob}\,\sigma((prob\_edge-center)/scale) + w_{price}\,PriceQuality}{w_{ev}+w_{prob}+w_{price}}$$

Defaults:
- `w_ev = 0.58`
- `w_prob_edge = 0.34`
- `w_price = 0.07`
- `ev_scale = 0.06`
- `prob_edge_center = 0.02`
- `prob_edge_scale = 0.03`

## 4) Explainability fields

Daily best-edges snapshots now include:
- `score` (0–100 integer)
- `score_explain` (human-readable formula summary)
- `score_components` (JSON string with raw inputs + components)

These are designed so you can later:
- audit why a pick scored high
- backtest score buckets (e.g. 80+)
- optimize weights/scales systematically

## 5) Optimization note

The score is intentionally weight/scale-driven so you can run an optimizer:
- objective = maximize ROI, maximize accuracy, or a blend
- constraint = minimum number of bets / days

The optimizer should only use dates where reconciliation files exist.
