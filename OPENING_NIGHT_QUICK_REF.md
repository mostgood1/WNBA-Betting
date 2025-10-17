# 🏀 Opening Night Quick Reference Card
**October 21, 2025 | Regular Season Opener**

---

## 🎯 GAME SCHEDULE

### Game 1: Thunder @ Rockets
- **Time:** 7:30 PM ET
- **Location:** Paycom Center, Oklahoma City
- **TV:** National broadcast expected

### Game 2: Lakers vs Warriors  
- **Time:** 10:00 PM ET
- **Location:** Crypto.com Arena, Los Angeles
- **TV:** TNT expected

---

## 📊 PREDICTIONS AT A GLANCE

### OKC Thunder vs Houston Rockets

**Winner:** Thunder (75% probability)  
**Spread:** Thunder -8 ✅ (Model agrees)  
**Total:** 233.2 (Model says OVER 225.5) ⬆️  
**Best Bet:** OVER 225.5 (7.7 point edge)

**Quarter Breakdown:**
- Q1: OKC leads by ~2.4 (58 pts total)
- Q2: OKC leads by ~2.4 (59 pts total)
- Q3: OKC leads by ~1.8 (58 pts total)
- Q4: OKC leads by ~1.5 (57 pts total)

---

### LA Lakers vs Golden State Warriors

**Winner:** Lakers (54% probability)  
**Spread:** Pick'em to LAL -2  
**Total:** 224.7 points  
**Analysis:** Very tight game, slight Lakers edge

**Quarter Breakdown:**
- Q1: Even game (~56 pts total)
- Q2: Lakers slight edge (~57 pts total)
- Q3: Warriors slight edge (~56 pts total)
- Q4: Lakers slight edge (~55 pts total)

---

## ✅ DATA STATUS

| File | Status |
|------|--------|
| Game Predictions | ✅ READY |
| Quarter Data | ✅ READY |
| Game Odds | ✅ READY (OKC only) |
| Props Predictions | ⏳ Generate game day |
| Props Edges | ⏳ Generate game day |

---

## 🔄 GAME DAY COMMANDS

### Morning Check (10 AM)
```powershell
# Verify scheduled task ran
Get-ScheduledTask | Where-Object {$_.TaskName -like "*NBA*"}
```

### Afternoon Update (2 PM)
```powershell
# Run full pipeline
.\scripts\daily_update.ps1 -Date "2025-10-21" -GitPush
```

### Pre-Game Check (6 PM)
```powershell
# Verify all files exist
Get-ChildItem "data\processed\*2025-10-21*"
```

### Odds Refresh (6:30 PM)
```powershell
# Update latest odds
python scripts/fetch_bovada_game_odds.py 2025-10-21
```

---

## 💰 BETTING RECOMMENDATIONS

### High Confidence Bets

**1. Thunder/Rockets OVER 225.5**
- Model: 233.2 points
- Line: 225.5
- Edge: +7.7 points
- Confidence: ⭐⭐⭐⭐

**2. Thunder -8**
- Model: -8.1 spread
- Line: -8.0
- Edge: +0.1 points (minimal)
- Confidence: ⭐⭐⭐

### Moderate Confidence

**3. Lakers ML (when posted)**
- Model: 54% win probability
- Expected line: -115 to -130
- Wait for actual line to assess value
- Confidence: ⭐⭐⭐

---

## 📱 QUICK ACCESS

### Web Dashboard
```
http://127.0.0.1:5050
```

### Key Files
```
predictions: data/processed/predictions_2025-10-21.csv
game_odds:   data/processed/game_odds_2025-10-21.csv
props:       data/processed/props_predictions_2025-10-21.csv (TBD)
```

### Logs
```
logs/local_daily_update_*.log
```

---

## 🚨 TROUBLESHOOTING

**No predictions?**
→ Run `.\scripts\daily_update.ps1 -Date "2025-10-21"`

**No odds?**
→ Run `python scripts/fetch_bovada_game_odds.py 2025-10-21`

**Props missing?**
→ Normal until game day morning

**Web dashboard down?**
→ Run task "Run Flask app"

---

## 📞 CONTACTS

**OddsAPI Status:** https://the-odds-api.com/
**NBA Schedule:** https://www.nba.com/schedule
**Injury Reports:** https://www.rotowire.com/basketball/nba-lineups.php

---

**System Status:** ✅ OPERATIONAL  
**Data Status:** ✅ 95% READY  
**Confidence:** ✅ HIGH

*Last Updated: Oct 17, 2025, 9:45 AM*
