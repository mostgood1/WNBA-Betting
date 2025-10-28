# Enhanced Training + ONNX (x86_64 friendly)

This repo is optimized for Windows ARM64 runtime with ONNX Runtime + QNN (NPU). Training uses scikit-learn and is best run on an x86_64 machine where wheels are readily available. Use the provided GitHub Actions workflow to train and export ONNX models.

## One-time setup
- Ensure your `data/raw/games_nba_api.csv` is present in the repo (optional). If missing, the workflow can fetch 2 seasons automatically.

## How to run the workflow
1. In GitHub, go to Actions → "Train Enhanced Models + ONNX".
2. Click "Run workflow".
   - Keep "Fetch data" checked to fetch two seasons before training, or uncheck if your repo already includes `data/raw/games_nba_api.csv`.
3. Wait for the run to finish. It will:
   - Install training dependencies (scikit-learn, skl2onnx, onnxruntime).
   - (Optionally) fetch two seasons of games.
   - Run `python -m nba_betting.cli train-games-enhanced-onnx`.
   - Upload artifacts: `models/*_enhanced.onnx` (main + halves + quarters) and `feature_columns_enhanced.joblib`.

## Install the trained models locally (ARM64 runtime)
1. Download the artifact from the workflow run (enhanced-onnx-models).
2. Extract into your repo's `models/` folder:
   - `models/win_prob_enhanced.onnx`
   - `models/spread_margin_enhanced.onnx`
   - `models/totals_enhanced.onnx`
   - `models/halves_{h1,h2}_{win,margin,total}_enhanced.onnx`
   - `models/quarters_{q1..q4}_{win,margin,total}_enhanced.onnx`
   - `models/feature_columns_enhanced.joblib`
3. No code changes are required. The runtime automatically prefers `*_enhanced.onnx` files.

## Daily predictions on ARM64 (NPU)
- Use the NPU command as usual:
  ```powershell
  .\.venv\Scripts\python.exe -m nba_betting.cli predict-games-npu --date YYYY-MM-DD --periods --calibrate-periods
  ```
- The loader will report which ONNX files were loaded with QNN acceleration and how many models are NPU-backed.

## Notes
- We keep scikit-learn out of the ARM64 venv to avoid wheel issues. Training is separate; runtime is pure ONNX/QNN.
- If you later re-train, just replace the files in `models/`. The loader will pick them up automatically.
