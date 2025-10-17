# Game Models: Current Status Report

**Date:** October 17, 2025  
**Question:** Are game models fully using neural networks?  
**Answer:** ❌ **NO - Currently using sklearn models, but ONNX models exist!**

## Current State

### ✅ What Exists:

**ONNX Neural Network Models (NPU-ready):**
- `models/win_prob.onnx` (954 bytes)
- `models/spread_margin.onnx` (599 bytes)
- `models/totals.onnx` (599 bytes)

**sklearn Models (Currently Active):**
- `models/win_prob.joblib` (2,401 bytes)
- `models/spread_margin.joblib` (2,089 bytes)
- `models/totals.joblib` (2,089 bytes)
- `models/halves_models.joblib` (for H1/H2)
- `models/quarters_models.joblib` (for Q1-Q4)

**NPU Module:**
- ✅ `src/nba_betting/games_npu.py` exists with full NPU support

### ❌ What's Currently Used:

The `predict-date` command (lines 990-1100 in cli.py) loads models like this:

```python
# CURRENT CODE - Uses sklearn joblib models:
win_model = joblib.load(paths.models / "win_prob.joblib")
spread_model = joblib.load(paths.models / "spread_margin.joblib")
total_model = joblib.load(paths.models / "totals.joblib")
halves = joblib.load(paths.models / "halves_models.joblib")
quarters = joblib.load(paths.models / "quarters_models.joblib")

# Then makes predictions:
res["home_win_prob"] = win_model.predict_proba(X)[:, 1]
res["pred_margin"] = spread_model.predict(X)
res["pred_total"] = total_model.predict(X)
```

**This is using sklearn models, NOT the ONNX neural networks!**

## The Problem

Just like with props predictions, you have **TWO parallel systems**:

1. **Active Path (CPU):** `predict-date` → sklearn joblib models
2. **Dormant Path (NPU):** `games_npu.py` → ONNX models with NPU acceleration

The daily update currently uses the old sklearn path!

## The Solution

### Option 1: Create Pure ONNX Game Predictor (Same as Props)

Create a parallel pure ONNX system for games:
- `src/nba_betting/games_onnx_pure.py` - Pure ONNX game predictor
- No sklearn dependency
- Always uses ONNX neural networks
- NPU accelerated

### Option 2: Modify Existing predict-date Command

Update the `_predict_from_matchups` function to use ONNX models instead of joblib:

```python
# NEW CODE - Use ONNX models:
if ONNX_AVAILABLE:
    win_session = ort.InferenceSession('models/win_prob.onnx', 
                                       providers=['QNNExecutionProvider', 'CPUExecutionProvider'])
    spread_session = ort.InferenceSession('models/spread_margin.onnx',
                                          providers=['QNNExecutionProvider', 'CPUExecutionProvider'])
    total_session = ort.InferenceSession('models/totals.onnx',
                                         providers=['QNNExecutionProvider', 'CPUExecutionProvider'])
    
    # Run inference
    X_array = X.values.astype(np.float32)
    res["home_win_prob"] = win_session.run(None, {win_session.get_inputs()[0].name: X_array})[0][:, 1]
    res["pred_margin"] = spread_session.run(None, {spread_session.get_inputs()[0].name: X_array})[0].flatten()
    res["pred_total"] = total_session.run(None, {total_session.get_inputs()[0].name: X_array})[0].flatten()
```

## Comparison with Props System

| Aspect | Props | Games |
|--------|-------|-------|
| **ONNX Models Exist** | ✅ Yes (5 models) | ✅ Yes (3 models) |
| **NPU Module Exists** | ✅ Yes (props_npu.py) | ✅ Yes (games_npu.py) |
| **Pure ONNX Implementation** | ✅ Done (props_onnx_pure.py) | ❌ TODO |
| **Currently Used** | sklearn joblib | sklearn joblib |
| **Daily Update Uses NN** | ❌ No (but ready) | ❌ No |

## What Needs to Happen

### For Full Neural Network Deployment:

**Props:**
1. ✅ Pure ONNX predictor created
2. ✅ Pure feature builder created
3. ✅ Tested successfully (299 players)
4. ⏳ Add CLI command
5. ⏳ Update daily_update.ps1

**Games:**
1. ⏳ Create pure ONNX game predictor (or adapt games_npu.py)
2. ⏳ Modify _predict_from_matchups to use ONNX
3. ⏳ Test with game predictions
4. ⏳ Update predict-date command
5. ⏳ Update daily_update.ps1

## Recommendation

Since you already have:
- ✅ ONNX game models (win_prob.onnx, spread_margin.onnx, totals.onnx)
- ✅ NPU infrastructure working (tested with props)
- ✅ Experience creating pure ONNX predictors

**I recommend creating a `games_onnx_pure.py` module** (similar to props_onnx_pure.py) that:
- Loads the 3 ONNX game models
- Uses NPU acceleration
- Has no sklearn dependency
- Can replace the joblib model loading in `_predict_from_matchups`

This would make **BOTH props AND games always use neural networks** with NPU acceleration!

## Next Steps

1. **Create games_onnx_pure.py** - Pure ONNX game predictor
2. **Modify _predict_from_matchups** - Use ONNX instead of joblib
3. **Test game predictions** - Verify ONNX models work
4. **Deploy both systems** - Props + Games fully neural network

Would you like me to create the pure ONNX game predictor now?
