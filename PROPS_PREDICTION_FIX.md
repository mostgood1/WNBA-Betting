# Props Prediction Fix - Summary

## Changes Made ✅

### 1. Removed `--slate-only` Flag

**Files Changed:**
- `scripts/daily_update.ps1` (line 163)
- `app.py` (line 1571)

**Before:**
```powershell
predict-props --date 2025-10-21 --slate-only --calibrate --use-pure-onnx
```

**After:**
```powershell
predict-props --date 2025-10-21 --no-slate-only --calibrate --use-pure-onnx
```

**Impact:**
- Props predictions will now attempt to generate for ALL players (not just today's slate)
- This populates the full props table at `/props`
- Edges are still calculated separately and shown at `/props_recommendations`

## Current Architecture ✅

### Data Flow:
```
1. predict-props (NPU ONNX) → props_predictions_YYYY-MM-DD.csv
   └─ ALL players with historical data
   
2. props-edges (Combine predictions + odds) → props_edges_YYYY-MM-DD.csv
   └─ Only players where both model prediction AND betting line exist
   
3. export-props-recommendations (Filter by criteria) → props_recommendations_YYYY-MM-DD.csv
   └─ Best bets only (high edge, high EV)
```

### Frontend Pages:
- **`/props`** → Shows `props_predictions_*.csv` (ALL model predictions)
- **`/props_recommendations`** → Shows `props_edges_*.csv` or `props_recommendations_*.csv` (filtered edges only)

## Remaining Issue ⚠️

### Problem: Missing `player_logs.csv`

**Current State:**
- `player_logs.parquet` exists (1.1 MB)
- `player_logs.csv` does NOT exist
- Pure feature builder tries CSV first, then falls back to parquet
- Parquet requires `pyarrow` which can't be installed on ARM64 Windows

**Error When Running:**
```
Building features with pure method (no sklearn)...
WARNING: Pure feature builder failed: Unable to find a usable engine; 
tried using: 'pyarrow', 'fastparquet'.
```

## Solution Options

### Option 1: Export Parquet to CSV (Recommended)

**On a system with pyarrow installed** (x64 Windows, Linux, or Mac):

```powershell
# Export player_logs from parquet to CSV
python -c "import pandas as pd; df = pd.read_parquet('data/processed/player_logs.parquet'); df.to_csv('data/processed/player_logs.csv', index=False); print(f'Exported {len(df)} rows')"
```

Then commit the CSV to git:
```powershell
git add data/processed/player_logs.csv
git commit -m "Add player_logs.csv export for ARM64 compatibility"
git push origin main
```

**Benefits:**
- One-time export
- Works on ARM64 Windows without pyarrow
- CSV file is only ~3-4 MB (still manageable in git)

### Option 2: Use Game Slate to Predict Only Today's Players

Modify `props_features_pure.py` to:
1. Read today's game predictions (`predictions_YYYY-MM-DD.csv`)
2. Extract the teams playing today
3. Only build features for players on those teams

**Code Change Needed:**
```python
def build_features_for_date_pure(date: str, ...):
    # Get today's slate from game predictions
    preds_file = paths.data_processed / f"predictions_{date}.csv"
    if preds_file.exists():
        preds = pd.read_csv(preds_file)
        teams_today = set(preds['home_team'].tolist() + preds['visitor_team'].tolist())
        # Filter logs to only these teams...
```

**Benefits:**
- No CSV export needed
- Only predicts for relevant players
- Smaller dataset to process

### Option 3: Wait for Game Day

The scoreboard API returns data closer to tipoff, so `--slate-only` would work automatically on game day.

**When:**
- Day of the game (Oct 21)
- A few hours before tipoff

**Command:**
```powershell
# This will work on game day
predict-props --date 2025-10-21 --slate-only --calibrate --use-pure-onnx
```

## Testing the Fix

Once `player_logs.csv` exists, test with:

```powershell
# Test props predictions for Oct 21
$env:PYTHONPATH="C:\Users\mostg\OneDrive\Coding\WNBA-Betting\src"
& "C:\Users\mostg\OneDrive\Coding\NBA NPU\.venv-arm64\Scripts\python.exe" `
  -m nba_betting.cli predict-props `
  --date 2025-10-21 `
  --no-slate-only `
  --calibrate `
  --use-pure-onnx
```

**Expected Output:**
```
Building features with pure method (no sklearn)...
📅 Found XXX player entries for 2025-10-21
✅ Built features for XXX players
Using pure ONNX models with NPU acceleration...
✅ PTS model loaded (🚀 NPU)
✅ REB model loaded (🚀 NPU)
✅ AST model loaded (🚀 NPU)
✅ PRA model loaded (🚀 NPU)
✅ THREES model loaded (🚀 NPU)
Pure ONNX predictions generated for XXX players
Saved props predictions to data/processed/props_predictions_2025-10-21.csv
```

Then run full daily update:
```powershell
powershell -ExecutionPolicy Bypass -File scripts\daily_update.ps1 -Date "2025-10-21" -GitPush
```

## Expected Results After Fix

### Files Generated:
1. `props_predictions_2025-10-21.csv` (200-400 players, all stats)
2. `props_edges_2025-10-21.csv` (subset with betting lines available)
3. `props_recommendations_2025-10-21.csv` (filtered best bets)

### Frontend Display:
- **`/props`** → Table with 200-400 rows (all players, all predictions)
- **`/props_recommendations`** → Table with 20-50 rows (only edges > threshold)

## Summary

**What Was Fixed:**
✅ Removed `--slate-only` from daily update and Flask API
✅ Props predictions will now attempt ALL players instead of just slate

**What's Needed:**
⚠️ Export `player_logs.parquet` to `player_logs.csv` (requires system with pyarrow)
  OR
⚠️ Modify feature builder to use game predictions to filter teams

**Recommendation:**
Export the CSV once from an x64 system and commit it to git. This is the simplest and most reliable solution.

---

**Commit:** `3d0c112` - "Change props predictions to use --no-slate-only for ALL players (not just slate)"
