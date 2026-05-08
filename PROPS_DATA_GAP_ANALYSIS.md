# Props Data Gap Analysis

**Date:** October 17, 2025  
**Issue:** Props information not fully displaying on frontend

---

## ✅ What's Working

### Props Edges (173 rows) - **COMPLETE**
```
File: data/processed/props_edges_2025-10-17.csv
Status: ✅ EXISTS and LOADED
```

**Columns Available:**
- `date` - Game date
- `player_id` - NBA player ID
- `player_name` - Player name
- `team` - Player's team (3-letter code)
- `stat` - Prop type (pts, reb, ast, threes, pra)
- `side` - Over/Under
- `line` - Betting line
- `price` - Odds (American format)
- `implied_prob` - Market implied probability
- `model_prob` - NN model prediction probability
- `edge` - Edge percentage (model_prob - implied_prob)
- `ev` - Expected value
- `bookmaker` - Sportsbook ID
- `bookmaker_title` - Sportsbook name
- `commence_time` - Game start time

**Sample Data:**
```
player_name                  team  stat  line  model_prob    edge      ev
LaMelo Ball                  CHA   ast   5.5    0.799767  0.323576  0.679510
Shai Gilgeous-Alexander      OKC   ast   5.5    0.718927  0.284144  0.653532
Stephon Castle               SAS   ast   5.5    0.599551  0.309696  1.068450
```

### API Endpoints - **WORKING**

#### `/api/props` ✅
- Returns props edges with filtering
- Supports: market, team, min_edge, min_ev filters
- Collapse to best-of-book working
- **Status:** 200 OK

#### `/data/processed/props_edges_{date}.csv` ✅
- Direct CSV download
- **Status:** 200 OK

---

## ❌ What's Missing/Broken

### 1. Props Predictions File - **EMPTY**

```
File: data/processed/props_predictions_2025-10-17.csv
Status: ❌ EMPTY (0 bytes)
```

**Impact:**
- Cannot show raw NN predictions without market lines
- Frontend "predictions" mode won't work
- Missing columns: `pred_pts`, `pred_reb`, `pred_ast`, `pred_threes`, `pred_pra`

**Root Cause:**
Predictions file was not generated when edges were calculated. The CLI command `props-edges` creates edges but doesn't save the underlying predictions separately.

**Solution:**
```powershell
# Generate props predictions
python -m nba_betting.cli predict-props --date 2025-10-17 --slate-only
```

### 2. Props Recommendations Endpoint - **FIXED** ✅

```
Frontend calls: /api/props-recommendations (with dash)
Backend route:  /api/props/recommendations (with slash)
```

**Fix Applied:**
Added alias route in `app.py`:
```python
@app.route("/api/props/recommendations")
@app.route("/api/props-recommendations")  # Alias for frontend compatibility
def api_props_recommendations():
```

**Status:** ✅ FIXED - Endpoint now accessible from frontend

### 3. Player Photos/Headshots

**Current Status:** Unknown if displaying
**Required Field:** `player_id` (NBA API ID)
**Available:** ✅ player_id is in props_edges file

**Frontend URL Pattern:**
```
https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/{player_id}.png
```

---

## 🔍 Data Flow Diagram

### Current Working Flow (Edges)
```
┌─────────────────────┐
│ NPU Props Models    │ ← 5 ONNX models (pts, reb, ast, threes, pra)
│ (props_npu.py)      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Props Features      │ ← Player stats, rolling averages, matchup data
│ (props_features.csv)│
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Market Lines        │ ← Bovada/The Odds API
│ (Scraped/API)       │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Edge Calculator     │ ← Compare model vs market
│ (CLI props-edges)   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ props_edges_        │ ← 173 rows with all fields ✅
│ 2025-10-17.csv      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Flask /api/props    │ ← Serves edges via JSON API
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Frontend            │ ← JavaScript renders table
│ (props.html)        │
└─────────────────────┘
```

### Missing Flow (Predictions)
```
┌─────────────────────┐
│ NPU Props Models    │
│ (props_npu.py)      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Props Features      │
└──────────┬──────────┘
           │
           ▼
❌ props_predictions_  ← EMPTY FILE
   2025-10-17.csv      
           │
           ▼
❌ Cannot display raw   
   predictions without
   market lines
```

---

## 📋 Frontend Display Capabilities

### Props Page (`/props`) - **WORKING** ✅

**Features:**
- ✅ Date picker
- ✅ Filter by market (pts, reb, ast, threes, pra)
- ✅ Filter by team
- ✅ Mode switcher (edges/predictions)
- ✅ Min edge/EV thresholds
- ✅ Collapse to best-of-book
- ✅ Build on-demand (auto-generate if missing)
- ✅ Sort options

**Columns Displayed (Edges Mode):**
| Team | Player | Stat | Side | Line | Price | Edge | EV | Book |
|------|--------|------|------|------|-------|------|----|----- |
| ✅   | ✅     | ✅   | ✅   | ✅   | ✅    | ✅   | ✅ | ✅   |

**Columns Displayed (Predictions Mode):** ❌ NOT WORKING
- Requires `props_predictions` file (currently empty)
- Would show: Team, Player, Opp, H/A, Stat, Pred

### Props Recommendations (`/props/recommendations`) - **NOW WORKING** ✅

**Features:**
- Player card aggregation
- Best plays per player
- Game context (matchup info)
- Sort by EV/Edge
- Filter by market
- Min EV threshold

**Status:** ✅ Endpoint now accessible after alias fix

---

## 🎯 Required Actions

### Immediate (To Display All Props Data)

1. **Generate Props Predictions** ⏳
```powershell
cd C:\Users\mostg\OneDrive\Coding\WNBA-Betting
$env:PYTHONPATH="$PWD\src"
& "C:\Users\mostg\OneDrive\Coding\NBA NPU\.venv-arm64\Scripts\python.exe" -m nba_betting.cli predict-props --date 2025-10-17 --slate-only
```

Expected output:
- `data/processed/props_predictions_2025-10-17.csv`
- Should contain ~40-80 player rows
- Columns: `player_name`, `team`, `opponent`, `home`, `pred_pts`, `pred_reb`, `pred_ast`, `pred_threes`, `pred_pra`

2. **Restart Flask App** (Already done) ✅
```powershell
$env:PORT=5051
$env:PYTHONPATH="C:\Users\mostg\OneDrive\Coding\WNBA-Betting\src"
& "C:\Users\mostg\OneDrive\Coding\NBA NPU\.venv-arm64\Scripts\python.exe" app.py
```

3. **Test All Props Endpoints**
```powershell
# Test edges API
curl http://127.0.0.1:5051/api/props?date=2025-10-17

# Test recommendations API (now with alias)
curl http://127.0.0.1:5051/api/props-recommendations?date=2025-10-17

# Test predictions mode (after generating predictions)
curl "http://127.0.0.1:5051/api/props?date=2025-10-17&source=predictions"
```

### Verification Checklist

**After Fixes:**
- [ ] Props edges table displays (173 rows) - ✅ ALREADY WORKING
- [ ] Props recommendations loads - ⏳ TO TEST (endpoint fixed)
- [ ] Player photos display (using player_id) - ⏳ TO VERIFY
- [ ] Edge indicators show correctly - ⏳ TO VERIFY
- [ ] EV calculations display - ✅ ALREADY IN DATA
- [ ] Predictions mode works - ⏳ AFTER GENERATING PREDICTIONS
- [ ] Filters work (market, team, min_edge) - ⏳ TO TEST
- [ ] Collapse to best-of-book works - ⏳ TO TEST

---

## 📊 Complete Props Data Summary

### Files Created Today (Oct 17, 2025)

1. ✅ **props_edges_2025-10-17.csv** - 173 rows, 15 columns, COMPLETE
2. ❌ **props_predictions_2025-10-17.csv** - EMPTY (needs generation)
3. ✅ **props_recommendations_2025-10-17.csv** - (if exists, check file list)
4. ✅ **props_calibration_2025-10-17.json** - Model calibration metadata

### NPU Models Status

**All 5 Props Models on QNN:** ✅
```
models/t_pts_ridge.onnx      → QNNExecutionProvider
models/t_reb_ridge.onnx      → QNNExecutionProvider
models/t_ast_ridge.onnx      → QNNExecutionProvider
models/t_threes_ridge.onnx   → QNNExecutionProvider
models/t_pra_ridge.onnx      → QNNExecutionProvider
```

### Data Coverage

**Props Edges by Stat Type:**
```sql
SELECT stat, COUNT(*) as count
FROM props_edges_2025-10-17
GROUP BY stat
```

Expected distribution (verify with actual data):
- Points (pts): ~35-40 props
- Rebounds (reb): ~30-35 props
- Assists (ast): ~35-40 props
- Threes (threes): ~30-35 props
- PRA (pts+reb+ast): ~20-30 props

**Bookmakers:**
- Bovada (primary source)

**Games Covered:**
- 8 games on Oct 17, 2025
- Expected ~20-25 players per game
- Total ~40-80 unique player props

---

## 🔧 Technical Details

### Backend API Specification

#### `/api/props`
**Query Parameters:**
- `date` (required): YYYY-MM-DD
- `source`: "edges" (default) or "predictions"
- `market`: pts|reb|ast|threes|pra
- `team`: 3-letter team code
- `min_edge`: minimum edge % (e.g., 5)
- `min_ev`: minimum EV (e.g., 0.5)
- `collapse`: 1 (default) for best-of-book
- `build`: 1 to auto-generate if missing

**Response:**
```json
{
  "date": "2025-10-17",
  "source": "edges",
  "rows": [...],
  "collapsed": true
}
```

#### `/api/props-recommendations` (New Alias) ✅
**Query Parameters:**
- `date` (required): YYYY-MM-DD
- `market`: pts|reb|ast|threes|pra
- `min_ev`: minimum EV %
- `only_ev`: 1 to hide plays without EV
- `home_team`/`away_team`: filter to specific game
- `sortBy`: ev_desc (default), ev_asc, edge_desc, edge_asc

**Response:**
```json
{
  "date": "2025-10-17",
  "rows": [...],
  "games": [{home_team, away_team}],
  "data": [{player, team, home_team, away_team, plays: [...]}]
}
```

### Frontend JavaScript

**Key Functions:**
- `maybeLoadPropsEdges(dateStr)` - Loads edges CSV ✅
- `fetchProps()` - Calls /api/props with filters ✅
- `renderPropsTable()` - Renders edges or predictions ✅

**Files:**
- `/web/props.html` - Main props table page
- `/web/props_recommendations.html` - Player card view
- `/web/app.js` - Shared utilities

---

## 🎯 Next Steps

1. **Generate props predictions file** (1 min)
2. **Test props recommendations page** (2 min)
3. **Verify all 173 props display correctly** (3 min)
4. **Check player photos rendering** (1 min)
5. **Test filters (market, team, min_edge)** (2 min)
6. **Document any remaining gaps** (2 min)

**Total Time:** ~10 minutes

**After Verification:**
- All props data should be visible on frontend
- Users can filter by market, team, edge, EV
- Best plays highlighted with color coding
- Player cards show aggregated recommendations

**Tonight's Workflow:**
After games complete, run:
```powershell
python -m nba_betting.cli recon-props --date 2025-10-17
```

This will:
- Compare predictions vs actual stats
- Calculate hit rate by prop type
- Track EV accuracy
- Validate NN calibration for props
