from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Dict, List
from pathlib import Path
import json

from .config import paths


def _load_feature_columns() -> List[str]:
    """Load feature column names saved during props model training."""
    import pickle
    p = paths.models / "props_feature_columns.joblib"
    with open(p, 'rb') as f:
        cols = pickle.load(f)
    return list(cols)


def _load_training_features() -> pd.DataFrame:
    """Load historical props features for training linear models."""
    pq = paths.data_processed / "props_features.parquet"
    if pq.exists():
        try:
            return pd.read_parquet(pq)
        except Exception:
            pass
    csvs = sorted(paths.data_processed.glob("props_features_*.csv"))
    if csvs:
        # Use the latest dated snapshot
        return pd.read_csv(csvs[-1])
    raise FileNotFoundError("props_features not found; run build-props-features")


def train_linear_props_models(targets: List[str] = ["t_stl","t_blk","t_tov"], alpha: float = 1.0) -> Path:
    """Train simple ridge (closed-form) linear models for specified targets using numpy.

    Saves an .npz file with coefficients and intercepts for each target.
    """
    df = _load_training_features()
    feat_cols = _load_feature_columns()
    # Filter rows with all required targets present
    have_targets = [t for t in targets if t in df.columns]
    if not have_targets:
        raise ValueError("No target columns available in features for training")
    df = df.dropna(subset=have_targets)
    X = df[feat_cols].fillna(0.0).to_numpy(dtype=float)

    # Precompute matrices
    X_mean = X.mean(axis=0)
    Xc = X - X_mean
    XtX = Xc.T @ Xc
    # Ridge regularization (no penalty on intercept as we work centered)
    XtX_reg = XtX + (alpha * np.eye(XtX.shape[0]))
    XtX_inv = np.linalg.pinv(XtX_reg)

    coefs: Dict[str, np.ndarray] = {}
    intercepts: Dict[str, float] = {}
    for tgt in have_targets:
        y = pd.to_numeric(df[tgt], errors="coerce").to_numpy(dtype=float)
        y = np.where(np.isfinite(y), y, 0.0)
        y_mean = y.mean()
        yc = y - y_mean
        w = XtX_inv @ (Xc.T @ yc)
        b = float(y_mean - X_mean @ w)
        coefs[tgt] = w
        intercepts[tgt] = b

    # Save
    out_path = paths.models / "pure_linear_props_models.npz"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        out_path,
        feature_cols=np.array(feat_cols, dtype=object),
        **{f"coef_{k}": v for k, v in coefs.items()},
        **{f"intercept_{k}": v for k, v in intercepts.items()},
    )
    return out_path


def load_linear_props_models() -> Dict[str, Dict[str, np.ndarray | float]]:
    """Load previously trained linear props models (.npz)."""
    p = paths.models / "pure_linear_props_models.npz"
    if not p.exists():
        raise FileNotFoundError(str(p))
    data = np.load(p, allow_pickle=True)
    feature_cols = list(data["feature_cols"].tolist())
    result: Dict[str, Dict[str, np.ndarray | float]] = {"feature_cols": feature_cols}
    for k in data.files:
        if k.startswith("coef_"):
            tgt = k[len("coef_"):]
            result.setdefault(tgt, {})["coef"] = data[k]
        elif k.startswith("intercept_"):
            tgt = k[len("intercept_"):]
            result.setdefault(tgt, {})["intercept"] = float(data[k])
    return result


def predict_with_linear_models(features_df: pd.DataFrame, models: Dict[str, Dict[str, np.ndarray | float]]) -> pd.DataFrame:
    """Add pred_* columns to features_df using loaded linear models."""
    feat_cols = models.get("feature_cols")  # type: ignore
    if not feat_cols:
        return features_df
    X = features_df[list(feat_cols)].fillna(0.0).to_numpy(dtype=float)
    out = features_df.copy()
    for tgt, spec in models.items():
        if tgt == "feature_cols":
            continue
        w = spec.get("coef")
        b = spec.get("intercept")
        if w is None or b is None:
            continue
        yhat = X @ w + float(b)
        out[tgt.replace("t_","pred_")] = yhat
    return out


def export_linear_to_onnx(targets: List[str] = ["t_stl","t_blk","t_tov"]) -> Dict[str, Path]:
    """Export pure-linear models to simple ONNX graphs (MatMul + Add).

    Returns a mapping from target -> saved ONNX path.
    """
    import onnx
    from onnx import helper, TensorProto
    from onnx import numpy_helper

    lin = load_linear_props_models()
    feat_cols = lin.get("feature_cols")  # type: ignore
    if not feat_cols:
        raise ValueError("feature_cols missing in linear models store")
    n_features = len(feat_cols)

    out: Dict[str, Path] = {}
    for tgt in targets:
        spec = lin.get(tgt)
        if not spec or spec.get("coef") is None or spec.get("intercept") is None:
            # Skip if not trained
            continue
        w = np.asarray(spec["coef"], dtype=np.float32).reshape(n_features, 1)
        b = np.asarray([spec["intercept"]], dtype=np.float32)  # shape (1,)

        # Build graph: y = X @ W + b
        X = helper.make_tensor_value_info("input", TensorProto.FLOAT, [None, n_features])
        Y = helper.make_tensor_value_info("output", TensorProto.FLOAT, [None, 1])

        W_const = numpy_helper.from_array(w, name="W")
        b_const = numpy_helper.from_array(b, name="b")

        node_matmul = helper.make_node("MatMul", inputs=["input", "W"], outputs=["mm_out"], name="MatMul")
        node_add = helper.make_node("Add", inputs=["mm_out", "b"], outputs=["output"], name="Add")

        graph = helper.make_graph(
            nodes=[node_matmul, node_add],
            name=f"linear_{tgt}",
            inputs=[X],
            outputs=[Y],
            initializer=[W_const, b_const],
        )
        model = helper.make_model(graph, producer_name="nba_betting_linear_export")
        # Set opset for broad compatibility
        model.opset_import[0].version = 13
        # Force an older IR version for compatibility with onnxruntime 1.23.x (max IR version 11)
        # Our graph only uses basic operators (MatMul, Add), so lowering IR is safe.
        model.ir_version = 11
        onnx.checker.check_model(model)

        save_path = paths.models / f"{tgt}_ridge.onnx"
        onnx.save(model, save_path)
        out[tgt] = save_path
    return out
