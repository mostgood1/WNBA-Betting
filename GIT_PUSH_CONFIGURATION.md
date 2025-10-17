# Git Push Configuration - Essential CSVs

**Last Updated:** October 17, 2025  
**Status:** ✅ All essential CSVs tracked and will be pushed by daily updater

---

## Daily Updater Git Push Behavior

### Script: `scripts/daily_update.ps1`

The daily updater **automatically pushes** when run with the `-GitPush` flag:

```powershell
# Example: Run with git push enabled
powershell -ExecutionPolicy Bypass -File scripts/daily_update.ps1 -Date "2025-10-17" -GitPush
```

### What Gets Pushed

**Line 188-189 in daily_update.ps1:**
```powershell
& git add -- data data\processed
if (Test-Path 'predictions.csv') { git add -- predictions.csv | Out-Null }
```

This stages:
1. ✅ **Entire `data/` directory** (includes raw data if needed)
2. ✅ **Entire `data/processed/` directory** (all CSVs)
3. ✅ **Root `predictions.csv`** (legacy, if exists)

### Commit Message Format
```
local daily: {DATE} (predictions/odds/props)
```

### Git Workflow
1. Stage files (`git add`)
2. Commit changes (`git commit`)
3. Pull with rebase (`git pull --rebase`)
4. Push to origin (`git push`)

---

## Essential CSVs Being Tracked

### ✅ **Game Predictions** (Generated Daily)
```
predictions_{DATE}.csv           - 45-feature NPU model predictions
predictions_{DATE}_calibrated.csv - Calibrated win probabilities
```

**Current Files:**
- predictions_2025-10-17.csv ✅
- predictions_2025-10-17_calibrated.csv ✅
- Plus historical: 2025-10-02 through 2025-10-21

### ✅ **Game Odds** (Fetched Daily from Bovada)
```
game_odds_{DATE}.csv - Spread, Total, Moneyline with prices
```

**Current Files:**
- game_odds_2025-10-17.csv ✅
- Plus historical: 2025-10-03 through 2025-10-21

### ✅ **Props Edges** (Calculated Daily)
```
props_edges_{DATE}.csv - Edge calculations for all available props
```

**Current Files:**
- props_edges_2025-10-17.csv ✅ (173 props, 38 players)
- Plus historical: 2025-10-03 through 2025-10-17

### ✅ **Props Predictions** (Generated Daily)
```
props_predictions_{DATE}.csv - NPU ONNX model predictions
```

**Current Files:**
- props_predictions_2025-10-17.csv ✅
- Plus historical: 2025-10-02 through 2025-10-17

### ✅ **Recommendations & Edges** (Exported Daily)
```
recommendations_{DATE}.csv       - Game recommendations export
props_recommendations_{DATE}.csv - Props recommendations export
edges_{DATE}.csv                 - Game edges calculation
```

**Current Files:**
- recommendations_2025-10-17.csv ✅
- props_recommendations_2025-10-17.csv ✅ (28.97 KB)
- edges_2025-10-17.csv ✅ (16 edges)

### ✅ **Reconciliation Data** (Generated After Games)
```
recon_games_{DATE}.csv - Actual results vs predictions (games)
recon_props_{DATE}.csv - Actual results vs predictions (props)
```

**Current Files:**
- recon_games_2025-10-02 through 2025-10-16 ✅
- recon_props_2025-10-02 through 2025-10-16 ✅

### ✅ **Props Actuals** (Fetched Daily)
```
props_actuals_{DATE}.csv - Actual player stats from completed games
```

**Current Files:**
- props_actuals_2025-10-02 through 2025-10-16 ✅

### ✅ **Static Data** (Updated Seasonally)
```
schedule_2025_26.csv        - Full season schedule
rosters_2025-26.csv         - Team rosters
player_logs.csv             - Historical player stats
team_advanced_stats_2025.csv - Team statistics
```

---

## Daily Update Workflow with Git Push

### 1. Generate Predictions
```powershell
python -m nba_betting.cli predict-date --date {DATE}
```
**Creates:** `predictions_{DATE}.csv` (staged for commit)

### 2. Fetch Game Odds
```powershell
python scripts/fetch_bovada_game_odds.py {DATE}
```
**Creates:** `game_odds_{DATE}.csv` (staged for commit)

### 3. Generate Props Predictions
```powershell
python -m nba_betting.cli predict-props --date {DATE} --use-pure-onnx
```
**Creates:** `props_predictions_{DATE}.csv` (staged for commit)

### 4. Calculate Props Edges
```powershell
python -m nba_betting.cli props-edges --date {DATE} --source auto
```
**Creates:** `props_edges_{DATE}.csv` (staged for commit)

### 5. Export Recommendations
```powershell
python -m nba_betting.cli export-recommendations --date {DATE}
python -m nba_betting.cli export-props-recommendations --date {DATE}
```
**Creates:** 
- `recommendations_{DATE}.csv` (staged for commit)
- `props_recommendations_{DATE}.csv` (staged for commit)
- `edges_{DATE}.csv` (staged for commit)

### 6. Reconcile Previous Day
```powershell
python -m nba_betting.cli reconcile-date --date {YESTERDAY}
python -m nba_betting.cli fetch-prop-actuals --date {YESTERDAY}
```
**Creates:**
- `recon_games_{YESTERDAY}.csv` (staged for commit)
- `recon_props_{YESTERDAY}.csv` (staged for commit)
- `props_actuals_{YESTERDAY}.csv` (staged for commit)

### 7. Git Push (if `-GitPush` flag used)
```powershell
git add -- data data\processed
git commit -m "local daily: {DATE} (predictions/odds/props)"
git pull --rebase
git push
```

---

## Verification

### Check What Will Be Pushed
```powershell
cd "C:\Users\mostg\OneDrive\Coding\NBA-Betting"
git add -- data data\processed
git diff --cached --name-only
```

### Check Git Tracking Status
```powershell
# List all tracked CSVs in data/processed
git ls-files data/processed/*.csv | Sort-Object

# Current count: 103 CSV files tracked ✅
```

### Manual Push (if needed)
```powershell
cd "C:\Users\mostg\OneDrive\Coding\NBA-Betting"
git add data/processed/*.csv
git commit -m "Update predictions and props for {DATE}"
git push origin main
```

---

## ✅ Summary

**All essential CSVs are configured to be pushed automatically:**

1. ✅ **Daily Predictions** - Game and props predictions with NPU models
2. ✅ **Daily Odds** - Bovada game odds (spread, total, ML)
3. ✅ **Daily Edges** - Props edges and game recommendations
4. ✅ **Daily Reconciliation** - Actual results vs predictions
5. ✅ **Exports** - Recommendations CSVs for frontend consumption

**The daily updater with `-GitPush` flag handles everything automatically:**
- ✅ Generates all daily CSVs
- ✅ Stages changes in `data/processed/`
- ✅ Commits with descriptive message
- ✅ Pulls with rebase to sync
- ✅ Pushes to GitHub

**No manual intervention needed for CSV tracking!** 🎯

---

*Generated: October 17, 2025*  
*Script: scripts/daily_update.ps1*
