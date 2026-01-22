from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def _to_num(x):
    try:
        return pd.to_numeric(x, errors="coerce")
    except Exception:
        return np.nan


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--top", type=int, default=5)
    ap.add_argument("--skip-onnx", action="store_true")
    args = ap.parse_args()

    repo = Path(__file__).resolve().parents[1]
    proc = repo / "data" / "processed"

    pred_path = proc / f"props_predictions_{args.date}.csv"
    if not pred_path.exists():
        raise SystemExit(f"Missing {pred_path}")

    df = pd.read_csv(pred_path)
    print(f"pred file: {pred_path}")
    print(f"pred shape: {df.shape}")

    if "pred_pts" not in df.columns:
        raise SystemExit("pred_pts missing")

    out = df.sort_values("pred_pts", ascending=False).head(args.top).copy()
    show = [c for c in [
        "player_name","team","player_id","pred_pts","pred_reb","pred_ast","pred_threes","pred_pra",
        "lag1_min","roll3_min","roll5_min","roll10_min",
        "lag1_pts","roll3_pts","roll5_pts","roll10_pts",
        "lag1_threes","roll10_threes",
    ] if c in out.columns]
    print("\nTOP OUTLIERS (by pred_pts)")
    print(out[show].to_string(index=False))

    # Load features for date
    from nba_betting.props_features import build_features_for_date

    feats = build_features_for_date(args.date)
    feats = feats.copy()
    feats["player_id"] = pd.to_numeric(feats.get("player_id"), errors="coerce")

    print(f"\nfeatures shape: {feats.shape}")

    # Feature magnitude diagnostics for the top outlier
    pid = _to_num(out.iloc[0].get("player_id"))
    print(f"\ntop outlier player_id: {pid}")
    rowf = feats.loc[feats["player_id"] == pid]
    print(f"matched feature rows: {len(rowf)}")

    if len(rowf):
        r = rowf.iloc[0]
        focus_cols = [c for c in [
            "lag1_min","roll10_min","lag1_pts","roll10_pts","lag1_threes","roll10_threes","b2b"
        ] if c in rowf.columns]
        focus = {c: (None if pd.isna(r[c]) else float(r[c])) for c in focus_cols}
        print("focus features:")
        print(focus)

        feat_cols = [c for c in rowf.columns if c.startswith("roll") or c.startswith("lag1_") or c == "b2b"]
        vals = pd.to_numeric(rowf[feat_cols].iloc[0], errors="coerce").to_numpy(dtype=float)
        max_abs = np.nanmax(np.abs(vals)) if np.isfinite(vals).any() else np.nan
        print(f"max |feature|: {max_abs}")

        s = pd.Series(vals, index=feat_cols)
        s = s.replace([np.inf, -np.inf], np.nan).dropna().abs().sort_values(ascending=False).head(15)
        print("top |features|:")
        print(s.to_string())

    # Compare sklearn vs ONNX on the same features row if possible
    try:
        import joblib
        from nba_betting.config import paths

        feat_list = joblib.load(paths.models / "props_feature_columns.joblib")
        models_store = joblib.load(paths.models / "props_models.joblib")
        X = feats[feat_list].fillna(0.0).to_numpy(dtype=float)
        idxs = feats.index[feats["player_id"] == pid].tolist()
        if idxs:
            x1 = X[idxs[0]: idxs[0] + 1]
            sk = float(models_store["t_pts"].predict(x1)[0])
            print(f"\nsklearn pred_pts (t_pts model): {sk}")
        else:
            print("\nsklearn compare skipped (pid not in feats)")
    except Exception as e:
        print(f"\nsklearn compare failed: {e}")

    if not args.skip_onnx:
        try:
            from nba_betting.props_onnx_pure import PureONNXPredictor

            pr = PureONNXPredictor()
            row = feats.loc[feats["player_id"] == pid].copy()
            if row.empty:
                print("\nonnx compare skipped (pid not in feats)")
            else:
                pred_df = pr.predict(row)
                print(f"onnx pred_pts: {float(pred_df['pred_pts'].iloc[0])}")
        except Exception as e:
            print(f"\nonnx compare failed: {e}")

    # Inspect SmartSim outputs (these can override pred_* columns)
    try:
        smart_paths = sorted(proc.glob(f"smart_sim_{args.date}_*.json"))
        print(f"\nSmartSim files found: {len(smart_paths)}")
        found = False
        for sp in smart_paths:
            try:
                j = pd.read_json(sp)
                # Sometimes JSON is a dict; pd.read_json may return frame; fall back to json module
                if isinstance(j, pd.DataFrame) and j.shape[0] == 1 and j.shape[1] == 1:
                    raise ValueError("ambiguous pd.read_json")
            except Exception:
                import json
                with open(sp, "r", encoding="utf-8") as f:
                    j = json.load(f)
            players = None
            if isinstance(j, dict):
                players = j.get("players") or j.get("player_stats") or j.get("results")
            if not players or not isinstance(players, list):
                continue
            for pr in players:
                try:
                    pr_pid = _to_num(pr.get("player_id") or pr.get("PLAYER_ID") or pr.get("id"))
                except Exception:
                    pr_pid = np.nan
                if pd.notna(pr_pid) and float(pr_pid) == float(pid):
                    # Print key stats (names vary)
                    key_stats = {}
                    for k in [
                        "pts_mean","reb_mean","ast_mean","threes_mean","pra_mean",
                        "pts","reb","ast","threes","pra",
                        "minutes_mean","min_mean","min",
                        "n_sims","sims","num_sims",
                    ]:
                        if k in pr:
                            key_stats[k] = pr.get(k)
                    key_stats["file"] = str(sp.name)
                    key_stats["player_name"] = pr.get("player_name") or pr.get("PLAYER_NAME")
                    print("\nSmartSim match:")
                    print(key_stats)
                    found = True
                    break
            if found:
                break
        if not found:
            print("No matching player_id found in SmartSim outputs.")
    except Exception as e:
        print(f"\nSmartSim inspection failed: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
