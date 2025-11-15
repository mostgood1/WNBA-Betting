# Per-Player Calibration Tuner

This project now includes a CLI command to auto-tune per-player calibration parameters per stat over a trailing date range and write a config that `predict-props` will consume automatically.

## What it does
- Grid-searches shrinkage K and minimum pairs (n) per stat: PTS, REB, AST, THREES, PRA
- Evaluates predictions vs actuals for each date in the range using global+per-player calibration
- Selects the best (K, n) per stat by the chosen criterion (MAE or RMSE)
- Writes `data/processed/props_player_calibration_config.json`
- `predict-props` will auto-load this config unless you pass explicit `--player-*-by-stat` flags

## Usage

- Tune over the last N days (yesterday inclusive):
  
  nba-betting tune-props-player-calibration --days 14 --criterion mae

- Tune over an explicit date range:
  
  nba-betting tune-props-player-calibration --start 2025-11-01 --end 2025-11-14 --criterion rmse

- Optional knobs:
  - `--slate-only/--no-slate-only` (default: `--slate-only`) restricts predictions to the day’s scoreboard slate
  - `--k-grid "6,8,10,12"` candidate shrinkage values (floats allowed)
  - `--min-grid "6,8,10"` candidate min-pairs values (ints)
  - `--criterion mae|rmse` selection metric (mean across days)

## Output
- Best parameters are printed per stat
- Config is saved to:
  
  data/processed/props_player_calibration_config.json

Example content:

{
  "updated_at": "2025-11-15",
  "window_days": 14,
  "criterion": "mae",
  "per_stat": {
    "pts": {"K": 8, "min_pairs": 6},
    "reb": {"K": 12, "min_pairs": 8},
    "ast": {"K": 12, "min_pairs": 8}
  }
}

`predict-props` will read this file automatically and apply the per-stat overrides when `--calibrate-player` is enabled and explicit `--player-*-by-stat` flags are not provided.

## Notes
- The tuner generates temporary prediction files in `data/processed` with names like `_tune_pts_K8_N6_YYYY-MM-DD.csv`.
- Ensure `props_actuals.parquet` (or daily CSV snapshots) exist for the tuning range; otherwise the tuner skips those days.
- Global calibration is applied during tuning (7-day window) to match daily behavior.

## Next steps
- Add a weekly scheduled task to run the tuner before the trailing evaluator, so daily predictions use fresh per-stat settings.
- Optionally widen or narrow the grids for higher-fidelity tuning once runtime constraints are known.
