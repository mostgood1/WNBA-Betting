# Props Calibration: Global vs Per-Player (14-day evaluation)

Range: 2025-11-01 … 2025-11-13 (inclusive)
Slate filter: none (max overlap)
Artifacts:
- data/processed/props_eval_compare_daily_2025-11-01_2025-11-13.csv
- data/processed/props_eval_compare_summary_2025-11-01_2025-11-13.csv

## Summary metrics (per-stat)
- AST: RMSE +0.006 | MAE +0.002 (slight degradation)
- PRA: RMSE -0.199 | MAE -0.224 (improvement)
- PTS: RMSE -0.097 | MAE -0.118 (improvement)
- REB: RMSE +0.031 | MAE +0.014 (slight degradation)
- THREES: RMSE -0.0149 | MAE -0.0207 (small improvement)

n per stat = 2232 joined rows across the range.

## Interpretation
- Per-player calibration is a net positive on composite PRA and on PTS and THREES.
- Small regressions on REB and AST suggest the current per-player shrinkage K=8 and min_pairs=6 may be slightly too permissive for those stats (higher variance, fewer attempts).

## Recommendations
1. Keep per-player calibration enabled broadly (we see wins on PRA/PTS/THREES).
2. Tune by stat:
   - Increase shrinkage for REB, AST (e.g., K≈10–12), or require min_pairs≈8–10.
   - Keep current K=8 for PTS, PRA, THREES.
3. Add a guardrail: if player_n < min_pairs or bias magnitude > cap, fall back to global-only for that player-stat (already partially implemented; consider tighter caps for REB/AST).
4. Add a weekly scheduled evaluation to regenerate this report over the trailing 14 days and track trend.

## Notes
- Joins now normalize `player_id` dtype and fall back to trimmed `player_name` if needed.
- Metrics computed via NumPy, avoiding sklearn dependency.
- Diagnostic counts are printed per date in the evaluator logs.

