# Modeling Roadmap (Games, Totals, Props, First Basket)

Goal: World-class prediction engine across game outcomes, totals and derivatives, player props, and first-basket. Focus on calibrated probabilistic outputs, robust evaluation, and repeatable training + deployment.

## Objectives
- Probabilistic forecasts: well-calibrated probabilities and intervals for all markets.
- Sharpness + skill: maximize information content (low Brier/logloss; low MAE/RMSE for totals).
- Operational: daily robustness, quick retraining, ONNX export, NPU-friendly inference.
- Financial realism: evaluation includes odds/vig and portfolio-level metrics.

## Core Tracks
- Games (side/win):
  - Features: team strength (Elo, RAPM-like proxies), schedule/rest, injuries/league_status, market priors, matchup synergies, TS% differentials.
  - Models: calibrated logistic (baseline), gradient boosting/GBDT, blending with market priors.
  - Metrics: Brier, logloss, calibration curves, reliability by bucket; ROI vs moneyline baselines.
- Totals (full/periods):
  - Features: pace, TS%, 3PA rate, FT rate, opponent-adjusted offensive/defensive ratings, referee/arena/altitude effects.
  - Models: quantile regression (Q10/Q50/Q90), Gaussian head with learned variance, conformal intervals.
  - Metrics: MAE/RMSE, interval coverage, WIS (weighted interval score), market comparison.
- Player Props:
  - Features: role/usage, minutes projections (injuries/rotations), opponent propensity, synergy, pace; per-stat calibration.
  - Models: per-stat regressors (GBDT/linear baselines), distributional heads (Poisson/NB for counts), lightweight deep for interaction effects.
  - Metrics: MAE/RMSE per-stat, calibration vs actuals, ROI vs posted lines.
- First Basket / PBP markets:
  - Features: opening lineup, jump-ball win probs, first-play tendencies, set frequency by team/coach.
  - Models: multiclass classifier with calibration; constrained to on-court players.
  - Metrics: top-1/top-5 hit rates, brier/logloss, ROI under top-N pick strategies.

## Cross-Cutting
- Data quality: strict schema checks for processed files; missingness handling and imputation policies.
- Calibration: isotonic/Platt for classifiers; conformal for regression intervals.
- Ensembling: blend model and market; stack multiple learners; snapshot weekly + seasonal baselines.
- Model registry: versioned artifacts (ONNX + metadata JSON), reproducible training configs.
- Backtesting: rolling-origin CV aligned with NBA calendar; include odds/closing lines where available.
- Monitoring: daily metric dashboards, drift detection, alarm thresholds.

## Phases
1) Evaluation Harness (now):
   - Aggregate metrics from existing processed outputs by date-range.
   - Baseline dashboards for games/totals/PBP.
2) Baselines + Calibration:
   - Add calibrated logistic for sides; quantile baseline for totals; per-stat linear for props; isotonic calibration for probs.
3) Feature Enrichment:
   - TS% features, lineup/rotation minutes priors; pace/3PA/FTA; schedule/rest; injuries.
4) Ensembling + Intervals:
   - Blend with market priors; conformal intervals for totals/props.
5) Registry + Automation:
   - Versioned models, scheduled weekly retrain, ONNX export and inference validation.

## Near-Term Deliverables
- `tools/evaluate_models.py`: roll-up metrics for games, totals, PBP, and basic props.
- CLI/Tasks: VS Code tasks to run evaluations over last 30/60 days.
- Add env-driven thresholds and reporting CSVs under `data/processed/metrics_*`.

---

This roadmap evolves; we will iterate with data findings and market changes.
