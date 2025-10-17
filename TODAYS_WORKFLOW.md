# 🏀 End-to-End Workflow for Today's Games (Oct 17, 2025)

**Goal**: Generate predictions for today's NBA games using enhanced 45-feature models

---

## 📋 Complete Workflow

### Step 1: Update Data (5 min)

**Update injury reports and advanced stats:**
```powershell
python -m nba_betting.cli run-all-improvements
```

**What this does:**
- ✅ Fetches latest injury reports (ESPN)
- ✅ Updates team advanced stats (Basketball Reference)
- ✅ Generates performance report (if data available)

---

### Step 2: Generate Predictions (2 min)

**Generate predictions for today:**
```powershell
python -m nba_betting.cli predict-date --date-str 2025-10-17
```

**Output:**
- File: `data/processed/predictions_2025-10-17.csv`
- Contains: Win probability, spread, total, halves, quarters predictions
- Uses: Enhanced 45-feature models with NPU acceleration

**Optional - specify custom output:**
```powershell
python -m nba_betting.cli predict-date --date-str 2025-10-17 --out-path "my_predictions.csv"
```

---

### Step 3: Fetch Betting Odds (3 min)

**Get current lines from Bovada:**
```powershell
python -m nba_betting.cli fetch-bovada-game-odds --date 2025-10-17
```

**Output:**
- File: `data/processed/game_odds_2025-10-17.csv`
- Contains: Spread, total, moneyline odds from Bovada

---

### Step 4: Calculate Edges (1 min)

**Compare predictions to market lines:**
```powershell
python -m nba_betting.cli calculate-edges --date 2025-10-17
```

**What this does:**
- Compares your predictions to market odds
- Identifies positive expected value (EV+) opportunities
- Shows where you have edge over bookmakers

---

### Step 5: Generate Recommendations (1 min)

**Create betting recommendations:**
```powershell
python -m nba_betting.cli recommendations --date 2025-10-17
```

**Output:**
- File: `data/processed/recommendations_2025-10-17.csv`
- Contains: Only bets with positive edge
- Includes: Recommended stake sizes (Kelly criterion)

---

### Step 6: View in Browser (Optional)

**Start Flask app to view interactive dashboard:**
```powershell
.\start-local.ps1
```

**Then open in browser:**
- Main: http://localhost:5051/
- Recommendations: http://localhost:5051/recommendations.html
- Reconciliation: http://localhost:5051/reconciliation.html

---

### Step 7: After Games Complete

**Reconcile actual results with predictions:**
```powershell
python -m nba_betting.cli recon-games --date 2025-10-17
```

**Output:**
- File: `data/processed/recon_games_2025-10-17.csv`
- Shows: Predicted vs actual, errors, accuracy

**Check performance:**
```powershell
python -m nba_betting.cli performance-report --days 7
```

**Calculate ROI:**
```powershell
python -m nba_betting.cli calculate-roi --days 7
```

---

## 🚀 Quick Start (One Command)

For today's games, just run:

```powershell
# All-in-one: Update data + predictions + odds + recommendations
python -m nba_betting.cli run-all-improvements; python -m nba_betting.cli predict-date --date-str 2025-10-17; python -m nba_betting.cli fetch-bovada-game-odds --date 2025-10-17; python -m nba_betting.cli recommendations --date 2025-10-17
```

---

## 📊 What to Look For

### In Predictions File (`predictions_2025-10-17.csv`)

**Key columns:**
- `home_team`, `visitor_team`: Teams playing
- `win_prob`: Home team win probability (0.0-1.0)
- `spread_margin`: Predicted point differential (positive = home wins)
- `total`: Predicted total points scored
- `halves_*`: First/second half predictions
- `quarters_*`: Quarter-by-quarter predictions

### In Recommendations File (`recommendations_2025-10-17.csv`)

**Key columns:**
- `bet_type`: Type of bet (spread, total, moneyline)
- `recommendation`: Which side to bet
- `edge`: Your expected value (positive = good bet)
- `kelly_stake`: Recommended bet size
- `confidence`: Model confidence level

**Betting criteria** (you can adjust these):
- Only show bets with edge > 2%
- Only show high-confidence picks
- Kelly stakes sized based on edge and confidence

---

## ⚠️ Important Notes

### If Today is Pre-Season or No Games

The schedule might show no games if:
- NBA season hasn't started
- It's an off-day
- Schedule data not updated

**Check schedule:**
```powershell
python -m nba_betting.cli show-schedule --date 2025-10-17
```

### If Predictions Look Wrong

**Common issues:**
1. **NaN values**: Check if games are too far in future (no ELO history)
2. **Zero probabilities**: Data quality issue - check feature engineering
3. **Unrealistic spreads**: Model might need recalibration

**Debug:**
```powershell
# Check feature data
python -m nba_betting.cli build-features

# Verify model files exist
dir models\*.onnx

# Check if using 45 features
python -c "import joblib; print(len(joblib.load('models/feature_columns.joblib')))"
```

---

## 📈 Expected Output

### Typical Game Prediction

```
Game: BOS @ LAL
Win Probability: 62.3% (Boston favored)
Predicted Spread: -5.2 (Boston by 5.2)
Predicted Total: 223.5 points

Halves:
  H1: Boston by 2.8, Total 112.1
  H2: Boston by 2.4, Total 111.4

Quarters:
  Q1: Boston by 0.8, Total 56.2
  Q2: Boston by 2.0, Total 55.9
  Q3: Boston by 1.2, Total 55.8
  Q4: Boston by 1.2, Total 55.6
```

### Typical Recommendation

```
Bet Type: Spread
Game: BOS @ LAL
Recommendation: Bet Boston -4.5 (market line)
Edge: +3.2% (model predicts -5.2, market at -4.5)
Kelly Stake: 1.2% of bankroll
Confidence: High (62% win probability)
```

---

## 🎯 Today's Action Items

**Right now:**
1. ✅ Run: `python -m nba_betting.cli predict-date --date-str 2025-10-17`
2. ✅ Check output file: `data\processed\predictions_2025-10-17.csv`
3. ✅ Review predictions (if games exist today)

**If games exist today:**
4. Fetch odds: `python -m nba_betting.cli fetch-bovada-game-odds --date 2025-10-17`
5. Generate recommendations: `python -m nba_betting.cli recommendations --date 2025-10-17`
6. Review and decide on bets

**After games complete (tonight):**
7. Reconcile: `python -m nba_betting.cli recon-games --date 2025-10-17`
8. Check accuracy: How did predictions perform?
9. Update tracking spreadsheet

---

**Let's start! Run the prediction command now.** 🚀
