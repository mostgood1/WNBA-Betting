# NBA Betting Model - Improvement Roadmap

## Phase 1: High-ROI Data Enhancements ⭐⭐⭐⭐⭐

### 1.1 Injury Data Integration
**Impact**: 30-40% predictive power improvement  
**Effort**: Medium  
**Priority**: 🔥 HIGHEST

**Implementation:**
- [ ] Create injury scraper (ESPN/NBA.com)
- [ ] Build injury database with player impact weights
- [ ] Add injury features to training data
- [ ] Retrain models with injury indicators

**Features to Add:**
- `home_key_injuries`: Count of starters/key players out
- `visitor_key_injuries`: Count of starters/key players out
- `home_injury_minutes_lost`: Minutes per game lost to injuries
- `visitor_injury_minutes_lost`: Minutes per game lost to injuries

### 1.2 Lineup & Roster Data
**Impact**: 25-35% predictive power improvement  
**Effort**: High  
**Priority**: 🔥 HIGH

**Implementation:**
- [ ] Scrape starting lineups (RotoWire/ESPN)
- [ ] Calculate lineup strength metrics
- [ ] Add bench depth indicators
- [ ] Track player availability

**Features to Add:**
- `home_starting_5_net_rating`: Net rating of starting lineup
- `visitor_starting_5_net_rating`: Net rating of starting lineup
- `home_bench_depth`: Bench strength metric
- `visitor_bench_depth`: Bench strength metric

### 1.3 Advanced Team Statistics
**Impact**: 15-20% predictive power improvement  
**Effort**: Low-Medium  
**Priority**: 🟡 MEDIUM-HIGH

**Implementation:**
- [ ] Scrape pace data (possessions per game)
- [ ] Get offensive/defensive efficiency ratings
- [ ] Calculate Four Factors (eFG%, TOV%, REB%, FTR)
- [ ] Add to feature engineering pipeline

**Features to Add:**
- `home_pace`: Possessions per 48 minutes
- `visitor_pace`: Possessions per 48 minutes
- `home_off_rating`: Points per 100 possessions
- `visitor_off_rating`: Points per 100 possessions
- `home_def_rating`: Points allowed per 100 possessions
- `visitor_def_rating`: Points allowed per 100 possessions
- `home_efg_pct`: Effective field goal %
- `visitor_efg_pct`: Effective field goal %

## Phase 2: Feature Engineering Improvements ⭐⭐⭐⭐

### 2.1 Quarter-Specific Features
**Impact**: 20-30% improvement for quarter predictions  
**Effort**: Medium  
**Priority**: 🟡 MEDIUM

**Implementation:**
- [ ] Calculate quarter-specific ELO ratings
- [ ] Track scoring patterns by quarter (Q1-Q4)
- [ ] Identify team tendencies by quarter
- [ ] Add quarter momentum features

**Features to Add:**
- `home_q1_avg_margin`: Average Q1 margin last 10 games
- `home_q2_avg_margin`: Average Q2 margin last 10 games
- `home_q3_avg_margin`: Average Q3 margin last 10 games
- `home_q4_avg_margin`: Average Q4 margin last 10 games
- (Same for visitor)

### 2.2 Opponent Adjustments
**Impact**: 10-15% predictive power improvement  
**Effort**: Medium  
**Priority**: 🟢 MEDIUM-LOW

**Implementation:**
- [ ] Head-to-head historical records
- [ ] Style matchup indicators (pace vs pace)
- [ ] Defensive scheme vs offensive strength
- [ ] Recent matchup results

### 2.3 Home/Away Splits
**Impact**: 5-10% predictive power improvement  
**Effort**: Low  
**Priority**: 🟢 LOW-MEDIUM

**Implementation:**
- [ ] Separate home/away ELO ratings
- [ ] Calculate travel distance
- [ ] Time zone adjustments (East Coast → West Coast)
- [ ] Altitude factors (Denver)

## Phase 3: Model Sophistication ⭐⭐⭐

### 3.1 Ensemble Models
**Impact**: 5-10% predictive power improvement  
**Effort**: High  
**Priority**: 🟢 LOW

**Implementation:**
- [ ] Train Random Forest models
- [ ] Train XGBoost models
- [ ] Train LightGBM models
- [ ] Weight by historical performance
- [ ] Ensemble predictions

### 3.2 Bayesian Approaches
**Impact**: 3-7% predictive power improvement  
**Effort**: High  
**Priority**: 🟢 LOW

**Implementation:**
- [ ] Implement Bayesian Ridge Regression
- [ ] Prior distributions from historical data
- [ ] Update beliefs as season progresses
- [ ] Uncertainty quantification

### 3.3 Deep Learning
**Impact**: 10-15% predictive power improvement  
**Effort**: Very High  
**Priority**: 🟢 LOW (Future)

**Implementation:**
- [ ] LSTM for time-series patterns
- [ ] Attention mechanisms for key features
- [ ] Transfer learning from other sports
- [ ] Requires more compute and data

## Quick Wins (Implement First) 🚀

### A. Add More Features to Existing Models
**Effort**: LOW | **Impact**: MEDIUM | **Time**: 1-2 hours

1. **Pace Metrics** (from Basketball Reference)
   - Average possessions per game
   - Offensive/Defensive pace ratings

2. **Shooting Efficiency** (from NBA.com stats)
   - Effective field goal percentage
   - Three-point attempt rate
   - Free throw rate

3. **Advanced Box Score Stats**
   - Offensive rebound rate
   - Turnover rate
   - Assist-to-turnover ratio

**Code Changes:**
```python
# In feature engineering:
base_feats = [
    # ... existing features ...
    "home_pace", "visitor_pace",
    "home_efg", "visitor_efg",
    "home_tov_rate", "visitor_tov_rate",
]
```

### B. Improve Data Pipeline
**Effort**: LOW | **Impact**: MEDIUM | **Time**: 2-3 hours

1. **Automated Data Updates**
   - Daily scraping schedule
   - Injury report updates
   - Lineup confirmations
   - Stats refresh

2. **Data Quality Checks**
   - Missing value detection
   - Outlier identification
   - Data validation rules

3. **Feature Store**
   - Pre-computed features
   - Faster prediction generation
   - Versioning and rollback

### C. Better Model Validation
**Effort**: MEDIUM | **Impact**: HIGH | **Time**: 3-4 hours

1. **Walk-Forward Validation**
   - Train on all data up to date D
   - Test on date D+1
   - Simulate real-world deployment

2. **Stratified Splits**
   - Ensure balanced outcomes
   - Test on different scenarios
   - Home favorites, road underdogs, etc.

3. **Performance Metrics Dashboard**
   - Accuracy by bet type
   - ROI by confidence tier
   - Calibration curves
   - Profit/Loss tracking

## Implementation Priority Queue

### Week 1: Quick Wins (Do Now!)
1. ✅ Add pace metrics from Basketball Reference
2. ✅ Add shooting efficiency stats
3. ✅ Implement walk-forward validation
4. ✅ Create performance dashboard

### Week 2: Data Enhancement
1. ⏳ Build injury scraper (ESPN API)
2. ⏳ Create injury impact model
3. ⏳ Integrate injury data into features
4. ⏳ Retrain all models

### Week 3: Advanced Features
1. ⏳ Scrape lineup data (RotoWire)
2. ⏳ Calculate lineup strength metrics
3. ⏳ Add opponent adjustment features
4. ⏳ Quarter-specific feature engineering

### Week 4: Model Improvements
1. ⏳ Implement ensemble models
2. ⏳ Add uncertainty quantification
3. ⏳ Deploy improved models
4. ⏳ Track performance vs old models

## Expected Improvements

### Current Performance (Estimated)
- Game Winner: 53-56% accuracy
- Spread: ±10-12 points RMSE
- Totals: ±12-15 points RMSE

### After Phase 1 (High-ROI Data)
- Game Winner: **55-59% accuracy** (+2-3%)
- Spread: **±8-10 points RMSE** (-2 points)
- Totals: **±10-12 points RMSE** (-2 points)

### After All Phases
- Game Winner: **57-61% accuracy** (+4-5%)
- Spread: **±7-9 points RMSE** (-3-4 points)
- Totals: **±9-11 points RMSE** (-3-4 points)

## Cost-Benefit Analysis

| Improvement | Effort | Impact | ROI | Priority |
|-------------|--------|--------|-----|----------|
| Injury Data | Medium | Very High | ⭐⭐⭐⭐⭐ | Do First |
| Pace/Efficiency | Low | High | ⭐⭐⭐⭐⭐ | Do First |
| Lineup Data | High | High | ⭐⭐⭐⭐ | Week 2 |
| Quarter Features | Medium | Medium | ⭐⭐⭐ | Week 3 |
| Ensemble Models | High | Medium | ⭐⭐ | Week 4 |
| Deep Learning | Very High | Medium | ⭐ | Future |

## Next Steps

**Immediate Actions (Today):**
1. ✅ Run full assessment (DONE)
2. 🔄 Create improvement roadmap (IN PROGRESS)
3. ⏳ Implement pace/efficiency features
4. ⏳ Retrain models with new features
5. ⏳ Test on holdout data
6. ⏳ Deploy if performance improves

**This Week:**
- Focus on Quick Wins (Section A & B)
- Add 5-10 new features from existing data sources
- Improve validation methodology
- Create performance tracking dashboard

**This Month:**
- Complete Phase 1 (High-ROI Data Enhancements)
- Injury data integration (highest impact)
- Lineup data scraping
- Advanced statistics pipeline

---

*Roadmap Created: October 17, 2025*  
*Priority: Maximize ROI with minimal effort*  
*Goal: Increase accuracy by 2-5% in next 30 days*
