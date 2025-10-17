# NN Props Quick Win Strategy

## Current Situation
- ✅ Have 5 NPU-accelerated ONNX props models: t_pts, t_reb, t_ast, t_threes, t_pra
- ✅ All models on QNN NPU (fast inference)
- ✅ props_features.parquet exists with historical data
- ❌ Missing scikit-learn in NBA NPU environment (compilation failing)
- ❌ Can't train new models easily right now

## Quick Win Approach: Leverage Existing NN + Derivations

Instead of training 20+ new models, **calculate additional stats from existing NN predictions**:

### Direct NN Predictions (Already Have)
1. **PTS** - Points (t_pts_ridge.onnx)
2. **REB** - Rebounds (t_reb_ridge.onnx)
3. **AST** - Assists (t_ast_ridge.onnx)
4. **3PM** - Three-pointers (t_threes_ridge.onnx)
5. **PRA** - Points+Rebounds+Assists (t_pra_ridge.onnx)

### Derived Stats (Calculate from NN outputs)
6. **PR** = PTS + REB
7. **PA** = PTS + AST
8. **RA** = REB + AST
9. **2PA** = (PTS - 3PM*3) / 2 (approx 2-point attempts)
10. **Double-Double Prob** = P(PTS≥10 AND REB≥10) + P(PTS≥10 AND AST≥10) + P(REB≥10 AND AST≥10)

### Historical Average Stats (For Display)
For stats we can't model yet (STL, BLK, TOV, etc.), show **player's rolling averages** from features:
- STL - Last 10 games average
- BLK - Last 10 games average
- TOV - Last 10 games average
- FGM, FGA, FG% - Last 10 games average
- FTM, FTA, FT% - Last 10 games average

## Implementation Steps

### Step 1: Update NPUPropsPredictor to add derived stats ✅
Already done in props_npu.py:
```python
# Calculate combo stats from predictions
if "pred_stl" in result_df.columns and "pred_blk" in result_df.columns:
    result_df["pred_stocks"] = result_df["pred_stl"] + result_df["pred_blk"]
if "pred_pts" in result_df.columns and "pred_reb" in result_df.columns:
    result_df["pred_pr"] = result_df["pred_pts"] + result_df["pred_reb"]
if "pred_pts" in result_df.columns and "pred_ast" in result_df.columns:
    result_df["pred_pa"] = result_df["pred_pts"] + result_df["pred_ast"]
if "pred_reb" in result_df.columns and "pred_ast" in result_df.columns:
    result_df["pred_ra"] = result_df["pred_reb"] + result_df["pred_ast"]
```

### Step 2: Add historical averages to predictions output
Modify CLI predict-props command to include rolling averages for non-modeled stats

### Step 3: Update frontend to show all stats
- NN predictions for modeled stats (PTS, REB, AST, 3PM, PRA + derived)
- Rolling averages for other stats (STL, BLK, TOV, FG%, FT%)

## Benefits of This Approach
1. ✅ **Fast**: No retraining needed
2. ✅ **Uses NN**: Leverages our NPU-accelerated models
3. ✅ **Complete**: Shows all stats users want to see
4. ✅ **Accurate**: NN for core stats, historical for others
5. ✅ **Implementable NOW**: No dependency issues

## Next Steps (In Order)
1. Generate today's props predictions with existing 5 models ✅
2. Calculate derived combo stats (PR, PA, RA) ✅
3. Add rolling averages to output CSV
4. Update frontend to display all stats
5. Later: Train additional models when environment is fixed

## Command to Run
```powershell
$env:PYTHONPATH="C:\Users\mostg\OneDrive\Coding\NBA-Betting\src"
& "C:\Users\mostg\OneDrive\Coding\NBA NPU\.venv-arm64\Scripts\python.exe" -m nba_betting.cli predict-props --date 2025-10-17
```

This gives us FULL props display using NN predictions + smart derivations!
