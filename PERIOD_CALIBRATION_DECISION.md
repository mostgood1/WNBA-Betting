# Period calibration: default blend weight

Decision: Set CalibrationConfig.totals_blend_weight = 0.8 (overridable via env NBA_CALIB_TOTALS_WEIGHT).

Rationale:
- November 2024 sweep (n=464 quarter rows) across weights [0.0, 0.3, 0.6, 0.8, 1.0] showed small but consistent MAE improvements as weight increased, with a slight minimum near 0.8.
- Average quarter MAE per weight (lower is better):
  - 0.0 → ~8.541
  - 0.3 → ~8.534
  - 0.6 → ~8.530
  - 0.8 → ~8.529 (min)
  - 1.0 → ~8.529
- RMSE trends similarly for Q1–Q3; Q4 showed a small RMSE increase at 1.0, favoring 0.8 as a balanced choice.

Notes:
- The improvement is modest; the calibration primarily enforces consistency with game totals/margins and stabilizes quarter splits using team/league shares.
- Override at runtime with: NBA_CALIB_TOTALS_WEIGHT=0.0..1.0
- Next: run broader backtests (2023-01..06, 2024-10..present) to confirm stability across windows.
