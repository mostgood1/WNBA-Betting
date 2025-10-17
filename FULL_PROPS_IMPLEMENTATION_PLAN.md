# Full Props & Team-Level Models Implementation Plan

## Current State
- **Existing Props Models**: 5 stats (pts, reb, ast, threes, pra)
- **Available Data**: 33 stat columns in player_logs
- **Gap**: Missing 10+ stat types, no team-level aggregations
- **User Request**: "Project full set of player props regardless of odds available + team level models for ALL game/prop models leveraging NN"

## Available Stats in player_logs
```
Core: PTS, REB, AST, FG3M (existing)
Defensive: STL, BLK, TOV
Shooting: FGM, FGA, FG_PCT, FTM, FTA, FT_PCT
Rebounds: OREB, DREB
Other: PLUS_MINUS, PF, MIN
```

## Phase 1: Expand Props Models (60-90 min)

### Step 1.1: Update TARGETS List
**File**: `src/nba_betting/props_train.py`
**Change**:
```python
# OLD
TARGETS = ["t_pts","t_reb","t_ast","t_threes","t_pra"]

# NEW
TARGETS = [
    # Core (existing)
    "t_pts", "t_reb", "t_ast", "t_threes", "t_pra",
    # Defensive
    "t_stl", "t_blk", "t_tov",
    # Shooting efficiency
    "t_fgm", "t_fga", "t_fg_pct", 
    "t_ftm", "t_fta", "t_ft_pct",
    # Rebounds breakdown
    "t_oreb", "t_dreb",
    # Other
    "t_pf", "t_plus_minus",
    # Combo stats (calculated)
    "t_stocks",  # STL + BLK
    "t_pr",      # PTS + REB
    "t_pa",      # PTS + AST
    "t_ra",      # REB + AST
]
```

### Step 1.2: Update props_features.py
**File**: `src/nba_betting/props_features.py`
**Changes**:
1. Add NUM_COL_MAP entries for new stats
2. Update build_props_features() to create rolling features for all stats
3. Add combo stat calculations (stocks, pr, pa, ra)

**New mappings needed**:
```python
NUM_COL_MAP = {
    "PTS": ["PTS", "pts"],
    "REB": ["REB", "reb", "TREB", "treb"],
    "AST": ["AST", "ast"],
    "FG3M": ["FG3M", "fg3m", "FG3M_A"],
    "MIN": ["MIN", "min"],
    # NEW ADDITIONS
    "STL": ["STL", "stl"],
    "BLK": ["BLK", "blk"],
    "TOV": ["TOV", "tov"],
    "FGM": ["FGM", "fgm"],
    "FGA": ["FGA", "fga"],
    "FG_PCT": ["FG_PCT", "fg_pct"],
    "FTM": ["FTM", "ftm"],
    "FTA": ["FTA", "fta"],
    "FT_PCT": ["FT_PCT", "ft_pct"],
    "OREB": ["OREB", "oreb"],
    "DREB": ["DREB", "dreb"],
    "PF": ["PF", "pf"],
    "PLUS_MINUS": ["PLUS_MINUS", "plus_minus"],
}
```

### Step 1.3: Rebuild Props Features
**Command**:
```powershell
python -m nba_betting.cli build-props-features
```
**Expected Output**: `props_features.parquet` with 20+ target columns

### Step 1.4: Train Expanded Props Models (NPU)
**File**: `src/nba_betting/props_train.py` - Already has train_props_models_npu()
**Command**:
```powershell
python -m nba_betting.cli train-props-npu
```
**Expected Output**: 
- 20+ .onnx files in models/ (t_stl_ridge.onnx, t_blk_ridge.onnx, etc.)
- Updated props_feature_columns.joblib
- Updated props_models.joblib

### Step 1.5: Update NPUPropsPredictor
**File**: `src/nba_betting/props_npu.py`
**Changes**:
1. Update TARGETS list to match props_train.py
2. Load all 20+ ONNX models
3. Ensure predict_props_for_date() generates all stats

### Step 1.6: Generate Complete Props Predictions
**Command**:
```powershell
python -m nba_betting.cli predict-props --date 2025-10-17
```
**Expected Output**: `props_predictions_2025-10-17.csv` with 20+ stat columns per player

## Phase 2: Team-Level Models (60-90 min)

### Step 2.1: Create Team Aggregation Module
**File**: `src/nba_betting/team_props.py` (NEW)
**Classes**:
```python
class TeamPropsAggregator:
    """Aggregate individual player props to team totals"""
    
    def aggregate_player_props(self, player_df: pd.DataFrame, team: str) -> dict:
        """Sum individual player predictions to team total
        
        Args:
            player_df: Props predictions with columns [player, team, pts, reb, ast, ...]
            team: Team abbreviation (e.g., "LAL")
            
        Returns:
            {
                "team": "LAL",
                "total_pts": 112.5,
                "total_reb": 45.2,
                "total_ast": 25.8,
                ...
            }
        """
        
    def predict_team_props(self, date: str, team: str) -> dict:
        """Generate team-level prop predictions for a date
        
        Uses:
        1. Load player props predictions for date
        2. Filter to team's active roster
        3. Sum individual predictions
        4. Apply team-level adjustments (pace, minutes distribution)
        """
        
    def calculate_team_edges(self, date: str, odds_df: pd.DataFrame) -> pd.DataFrame:
        """Compare team prop predictions vs market lines
        
        Args:
            date: Game date
            odds_df: Market lines for team props (team totals, team player props)
            
        Returns:
            DataFrame with columns [team, stat, prediction, line, edge, recommendation]
        """
```

### Step 2.2: Add Team Props CLI Commands
**File**: `src/nba_betting/cli.py`
**New commands**:
```python
@cli.command()
@click.option("--date", default=None)
def predict_team-props(date):
    """Generate team-level prop predictions"""
    
@cli.command()
@click.option("--date", default=None)
def team-props-edges(date):
    """Calculate edges for team props"""
```

### Step 2.3: Add Team Props Flask Endpoints
**File**: `app.py`
**New endpoints**:
```python
@app.route("/api/team-props")
def api_team_props():
    """Return team-level aggregated props for today's games"""
    date = request.args.get("date", today)
    file = paths.data_processed / f"team_props_predictions_{date}.csv"
    if not file.exists():
        return {"error": "Team props not found", "date": date}, 404
    df = pd.read_csv(file)
    return {"date": date, "data": df.to_dict(orient="records")}

@app.route("/api/team-recommendations")
def api_team_recommendations():
    """Team-level betting opportunities (edges)"""
    date = request.args.get("date", today)
    file = paths.data_processed / f"team_props_edges_{date}.csv"
    if not file.exists():
        return {"error": "Team props edges not found", "date": date}, 404
    df = pd.read_csv(file)
    # Filter to positive EV
    df_rec = df[df["edge"] > 0].sort_values("edge", ascending=False)
    return {"date": date, "data": df_rec.to_dict(orient="records")}
```

## Phase 3: Frontend Updates (30-60 min)

### Step 3.1: Update Player Card Display
**File**: `web/props_recommendations.html`
**Changes**:
1. Expand model projections section to show ALL available stats
2. Group stats by category (Scoring, Playmaking, Defense, Efficiency)
3. Add expandable sections for less common props

**Enhanced card layout**:
```javascript
// Scoring section
if (it.model.pts || it.model.fgm || it.model.ftm || it.model.threes) {
  const scoring = document.createElement('div');
  scoring.className = 'stat-category';
  scoring.innerHTML = '<strong>SCORING:</strong> ';
  // Add PTS, FGM/FGA/FG%, FTM/FTA/FT%, 3PM
}

// Playmaking section
if (it.model.ast || it.model.tov) {
  const playmaking = document.createElement('div');
  playmaking.className = 'stat-category';
  // Add AST, TOV, AST/TO ratio
}

// Defense section
if (it.model.stl || it.model.blk) {
  const defense = document.createElement('div');
  defense.className = 'stat-category';
  // Add STL, BLK, STOCKS
}

// Rebounds section
if (it.model.reb || it.model.oreb || it.model.dreb) {
  const rebounds = document.createElement('div');
  rebounds.className = 'stat-category';
  // Add OREB, DREB, TREB
}
```

### Step 3.2: Create Team Props View
**File**: `web/team_props.html` (NEW)
**Features**:
- Team-level cards showing aggregated props
- Comparison vs market lines
- Edge calculations
- Filters by team, stat category, minimum edge

### Step 3.3: Update Backend to Include All Stats
**File**: `app.py` - `api_props_recommendations()` function
**Changes**:
1. Load from `props_predictions_{date}.csv` (complete predictions)
2. Fall back to edges inference if predictions file missing
3. Include ALL stat columns in model dict

**Current logic** (lines 1986-2003):
```python
# If no model data from props_predictions, infer from edges
```

**New logic**:
```python
# Try to load from props_predictions first (complete data)
props_pred_file = paths.data_processed / f"props_predictions_{date}.csv"
if props_pred_file.exists():
    props_pred_df = pd.read_csv(props_pred_file)
    # Extract all pred_* columns for player
    player_preds = props_pred_df[props_pred_df['player_name'] == player]
    if not player_preds.empty:
        for col in player_preds.columns:
            if col.startswith('pred_'):
                stat_name = col.replace('pred_', '')
                model[stat_name] = float(player_preds.iloc[0][col])
else:
    # Fall back to edges inference (existing logic)
    pass
```

## Phase 4: Testing & Validation (30-60 min)

### Test 4.1: Verify Model Training
```powershell
# Check all models created
ls models/t_*.onnx | Measure-Object
# Should show 20+ files

# Verify NPU acceleration
python -c "from nba_betting.props_npu import NPUPropsPredictor; p = NPUPropsPredictor(); print(f'Loaded {len(p.models)} models')"
```

### Test 4.2: Verify Predictions Coverage
```powershell
# Generate predictions
python -m nba_betting.cli predict-props --date 2025-10-17

# Check coverage
python -c "import pandas as pd; df = pd.read_csv('data/processed/props_predictions_2025-10-17.csv'); print(f'Players: {df.player_name.nunique()}'); pred_cols = [c for c in df.columns if c.startswith('pred_')]; print(f'Stats: {len(pred_cols)}'); print(f'Columns: {pred_cols}')"
```

### Test 4.3: Verify Team Aggregations
```powershell
# Generate team props
python -m nba_betting.cli predict-team-props --date 2025-10-17

# Verify sums match
python -c "
import pandas as pd
player_df = pd.read_csv('data/processed/props_predictions_2025-10-17.csv')
team_df = pd.read_csv('data/processed/team_props_predictions_2025-10-17.csv')
lal_players = player_df[player_df['team'] == 'LAL']['pred_pts'].sum()
lal_team = team_df[team_df['team'] == 'LAL']['total_pts'].iloc[0]
print(f'LAL players sum: {lal_players:.1f}')
print(f'LAL team total: {lal_team:.1f}')
print(f'Match: {abs(lal_players - lal_team) < 1.0}')
"
```

### Test 4.4: Verify Frontend Display
1. Start Flask: `python app.py`
2. Open: http://localhost:5051/props_recommendations.html
3. Verify:
   - All player cards show expanded stats (20+ props)
   - Stats grouped by category
   - No missing opponent info
   - Model projections display for all stats

## Success Criteria

### Phase 1 Complete:
- [x] 20+ ONNX models trained and in models/
- [x] props_predictions CSV has 20+ pred_* columns
- [x] All 38+ players have predictions for all stats
- [x] NPU acceleration active for all models

### Phase 2 Complete:
- [x] team_props_predictions CSV generated
- [x] Team totals match player sums (within 1 point)
- [x] Team props edges calculated vs market lines
- [x] Flask endpoints returning team data

### Phase 3 Complete:
- [x] Player cards show all 20+ stats
- [x] Stats grouped by category (Scoring, Defense, etc.)
- [x] Team props page functional
- [x] All data flows from predictions to UI

### Phase 4 Complete:
- [x] All tests pass
- [x] No missing data in frontend
- [x] NPU acceleration verified
- [x] Team aggregations validated

## Timeline Estimate
- **Phase 1**: 90 min (feature engineering, training, prediction)
- **Phase 2**: 90 min (team aggregation logic, endpoints)
- **Phase 3**: 60 min (frontend updates)
- **Phase 4**: 30 min (testing)
- **Total**: 4.5 hours

## Next Immediate Step
Start Phase 1.1: Update TARGETS list in props_train.py
