"""
Enhanced model training with 45 features (base + advanced + injuries).
Supports both baseline (17 features) and enhanced (45 features) training.
"""

from __future__ import annotations

import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import log_loss, mean_squared_error
import numpy as np
from pathlib import Path
import joblib

from .config import paths
from .features_enhanced import get_enhanced_feature_columns


def _time_series_cv(X, y, n_splits=5):
    """Time-series cross-validation generator."""
    tscv = TimeSeriesSplit(n_splits=n_splits)
    for tr, te in tscv.split(X):
        yield tr, te


def _recency_sample_weights(df: pd.DataFrame, half_life_days: float = 30.0) -> np.ndarray:
    if "date" not in df.columns or df.empty:
        return np.ones(len(df), dtype=float)
    dates = pd.to_datetime(df["date"], errors="coerce")
    if dates.notna().sum() <= 1:
        return np.ones(len(df), dtype=float)
    max_date = dates.max()
    age_days = (max_date - dates).dt.days.fillna(0).astype(float)
    weights = np.power(0.5, age_days / max(float(half_life_days), 1.0))
    return np.clip(weights.to_numpy(dtype=float), 0.35, 1.0)


def train_models_enhanced(df: pd.DataFrame, use_enhanced_features: bool = True):
    """
    Train models with enhanced features (45 features) or baseline (17 features).
    
    Args:
        df: DataFrame with features and targets
        use_enhanced_features: If True, use 45 features; if False, use 17 baseline features
    
    Returns:
        Dictionary with cross-validation metrics
    """
    print("\n" + "="*70)
    print("MODEL TRAINING")
    print("="*70)
    
    # Select features
    if use_enhanced_features:
        print("\nUsing ENHANCED features (45 features)")
        all_features = get_enhanced_feature_columns()
    else:
        print("\nUsing BASELINE features (17 features)")
        all_features = [
            "elo_diff",
            "home_rest_days", "visitor_rest_days", "home_b2b", "visitor_b2b",
            "home_form_off_5", "home_form_def_5", "visitor_form_off_5", "visitor_form_def_5",
            "home_games_last3", "visitor_games_last3", "home_games_last5", "visitor_games_last5",
            "home_3in4", "visitor_3in4", "home_4in6", "visitor_4in6",
        ]
    
    # Filter to complete games only
    df = df.dropna(subset=["target_home_win", "target_margin", "target_total"])
    
    # Keep only features that exist in dataset
    use_feats = [c for c in all_features if c in df.columns]
    missing_feats = [c for c in all_features if c not in df.columns]
    
    print(f"\nFeature Status:")
    print(f"   Available: {len(use_feats)}/{len(all_features)}")
    if missing_feats:
        print(f"   Missing: {missing_feats[:5]}{'...' if len(missing_feats) > 5 else ''}")
    
    X = df[use_feats].fillna(0)
    sample_weights = _recency_sample_weights(df)
    print(f"\nTraining Data:")
    print(f"   Games: {len(X):,}")
    print(f"   Features: {len(use_feats)}")
    print(f"   Missing values filled: {X.isnull().sum().sum()}")
    print(f"   Recency weights: min={sample_weights.min():.3f} max={sample_weights.max():.3f}")
    
    # ========================================================================
    # 1. WIN PROBABILITY MODEL
    # ========================================================================
    print("\n[1/3] Training Win Probability Model...")
    y_win = df["target_home_win"].astype(int)
    
    # Hyperparameter tuning: C parameter for LogisticRegression
    # Using SMALLER C values (0.01-0.5) for stronger regularization to prevent overconfidence
    Cs = [0.01, 0.05, 0.1, 0.25, 0.5]
    c_losses: dict[float, list[float]] = {c: [] for c in Cs}
    
    print("   Testing regularization strengths (smaller C = stronger regularization)...")
    for tr, te in _time_series_cv(X, y_win):
        Xtr, Xte = X.iloc[tr], X.iloc[te]
        ytr, yte = y_win.iloc[tr], y_win.iloc[te]
        wtr = sample_weights[tr]
        for c in Cs:
            pipe = Pipeline([
                ("scaler", StandardScaler()),
                ("logreg", LogisticRegression(
                    solver="saga",
                    max_iter=5000,
                    C=c,
                    class_weight='balanced'  # Handle any class imbalance
                ))
            ])
            pipe.fit(Xtr, ytr, logreg__sample_weight=wtr)
            p = pipe.predict_proba(Xte)[:, 1]
            c_losses[c].append(log_loss(yte, p, labels=[0, 1]))
    
    mean_c = {c: float(np.mean(v)) for c, v in c_losses.items()}
    best_c = min(mean_c, key=mean_c.get)
    
    print(f"   Best C: {best_c} (LogLoss: {mean_c[best_c]:.4f})")
    print(f"   All C results: {mean_c}")
    
    # Train final model with best C
    clf = Pipeline([
        ("scaler", StandardScaler()),
        ("logreg", LogisticRegression(
            solver="saga",
            max_iter=5000,
            C=best_c,
            class_weight='balanced'
        ))
    ])
    clf.fit(X, y_win, logreg__sample_weight=sample_weights)
    cv_losses = list(c_losses[best_c])
    
    # Check probability distribution to detect overconfidence
    train_probs = clf.predict_proba(X)[:, 1]
    print(f"\n   Probability calibration check:")
    print(f"   - Mean probability: {train_probs.mean():.3f}")
    print(f"   - Std probability: {train_probs.std():.3f}")
    print(f"   - Probabilities < 0.05: {(train_probs < 0.05).sum()} ({(train_probs < 0.05).mean()*100:.1f}%)")
    print(f"   - Probabilities > 0.95: {(train_probs > 0.95).sum()} ({(train_probs > 0.95).mean()*100:.1f}%)")
    extreme_ratio = ((train_probs < 0.05) | (train_probs > 0.95)).mean()
    if extreme_ratio > 0.30:
        print(f"   ⚠️  WARNING: {extreme_ratio*100:.1f}% of predictions are extreme (< 5% or > 95%)")
        print(f"   Consider using even smaller C values for stronger regularization")
    
    # ========================================================================
    # 2. SPREAD MARGIN MODEL
    # ========================================================================
    print("\n[2/3] Training Spread Margin Model...")
    y_margin = df["target_margin"].astype(float)
    
    # Hyperparameter tuning: alpha parameter for Ridge
    ridge_alphas = [0.5, 1.0, 2.0, 5.0, 10.0]
    alpha_rmse_m: dict[float, list[float]] = {a: [] for a in ridge_alphas}
    
    for tr, te in _time_series_cv(X, y_margin):
        Xtr, Xte = X.iloc[tr], X.iloc[te]
        ytr, yte = y_margin.iloc[tr], y_margin.iloc[te]
        wtr = sample_weights[tr]
        for a in ridge_alphas:
            pipe = Pipeline([
                ("scaler", StandardScaler()),
                ("ridge", Ridge(alpha=a))
            ])
            pipe.fit(Xtr, ytr, ridge__sample_weight=wtr)
            pred = pipe.predict(Xte)
            rmse = float(np.sqrt(mean_squared_error(yte, pred)))
            alpha_rmse_m[a].append(rmse)
    
    mean_rmse_m = {a: float(np.mean(v)) for a, v in alpha_rmse_m.items()}
    best_alpha_m = min(mean_rmse_m, key=mean_rmse_m.get)
    
    print(f"   Best alpha: {best_alpha_m} (RMSE: {mean_rmse_m[best_alpha_m]:.2f} points)")
    
    # Train final model with best alpha
    reg_margin = Pipeline([
        ("scaler", StandardScaler()),
        ("ridge", Ridge(alpha=best_alpha_m))
    ])
    reg_margin.fit(X, y_margin, ridge__sample_weight=sample_weights)
    cv_rmse_m = list(alpha_rmse_m[best_alpha_m])
    
    # ========================================================================
    # 3. TOTALS MODEL
    # ========================================================================
    print("\n[3/3] Training Totals Model...")
    y_total = df["target_total"].astype(float)
    
    alpha_rmse_t: dict[float, list[float]] = {a: [] for a in ridge_alphas}
    
    for tr, te in _time_series_cv(X, y_total):
        Xtr, Xte = X.iloc[tr], X.iloc[te]
        ytr, yte = y_total.iloc[tr], y_total.iloc[te]
        wtr = sample_weights[tr]
        for a in ridge_alphas:
            pipe = Pipeline([
                ("scaler", StandardScaler()),
                ("ridge", Ridge(alpha=a))
            ])
            pipe.fit(Xtr, ytr, ridge__sample_weight=wtr)
            pred = pipe.predict(Xte)
            rmse = float(np.sqrt(mean_squared_error(yte, pred)))
            alpha_rmse_t[a].append(rmse)
    
    mean_rmse_t = {a: float(np.mean(v)) for a, v in alpha_rmse_t.items()}
    best_alpha_t = min(mean_rmse_t, key=mean_rmse_t.get)
    
    print(f"   Best alpha: {best_alpha_t} (RMSE: {mean_rmse_t[best_alpha_t]:.2f} points)")
    
    # Train final model with best alpha
    reg_total = Pipeline([
        ("scaler", StandardScaler()),
        ("ridge", Ridge(alpha=best_alpha_t))
    ])
    reg_total.fit(X, y_total, ridge__sample_weight=sample_weights)
    cv_rmse_t = list(alpha_rmse_t[best_alpha_t])
    
    # ========================================================================
    # 4. HALVES MODELS
    # ========================================================================
    print("\n[4/5] Training Halves Models (H1, H2)...")
    models_halves = {}
    
    for half in ("h1", "h2"):
        cols = [f"target_{half}_home_win", f"target_{half}_margin", f"target_{half}_total"]
        if not all(c in df.columns for c in cols):
            print(f"   {half.upper()}: Skipping (no data)")
            continue
        
        mask = df[cols].notna().all(axis=1)
        if not mask.any():
            print(f"   {half.upper()}: Skipping (all NaN)")
            continue
        
        Xh = X.loc[mask]
        y_hw = df.loc[mask, f"target_{half}_home_win"].astype(int)
        y_hm = df.loc[mask, f"target_{half}_margin"].astype(float)
        y_ht = df.loc[mask, f"target_{half}_total"].astype(float)
        
        # Use best hyperparameters from main models (with class balancing)
        clf_h = Pipeline([
            ("scaler", StandardScaler()),
            ("logreg", LogisticRegression(
                solver="saga",
                max_iter=5000,
                C=best_c,
                class_weight='balanced'
            ))
        ]).fit(Xh, y_hw, logreg__sample_weight=sample_weights[mask.to_numpy()])
        
        reg_hm = Pipeline([
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=best_alpha_m))
        ]).fit(Xh, y_hm, ridge__sample_weight=sample_weights[mask.to_numpy()])
        
        reg_ht = Pipeline([
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=best_alpha_t))
        ]).fit(Xh, y_ht, ridge__sample_weight=sample_weights[mask.to_numpy()])
        
        models_halves[half] = {"win": clf_h, "margin": reg_hm, "total": reg_ht}
        print(f"   {half.upper()}: Trained on {len(Xh):,} games")
    
    # ========================================================================
    # 5. QUARTERS MODELS
    # ========================================================================
    print("\n[5/5] Training Quarters Models (Q1-Q4)...")
    models_quarters = {}
    
    for q in ("q1", "q2", "q3", "q4"):
        cols = [f"target_{q}_home_win", f"target_{q}_margin", f"target_{q}_total"]
        if not all(c in df.columns for c in cols):
            print(f"   {q.upper()}: Skipping (no data)")
            continue
        
        mask = df[cols].notna().all(axis=1)
        if not mask.any():
            print(f"   {q.upper()}: Skipping (all NaN)")
            continue
        
        Xq = X.loc[mask]
        y_qw = df.loc[mask, f"target_{q}_home_win"].astype(int)
        y_qm = df.loc[mask, f"target_{q}_margin"].astype(float)
        y_qt = df.loc[mask, f"target_{q}_total"].astype(float)
        
        # Use best hyperparameters from main models (with class balancing)
        clf_q = Pipeline([
            ("scaler", StandardScaler()),
            ("logreg", LogisticRegression(
                solver="saga",
                max_iter=5000,
                C=best_c,
                class_weight='balanced'
            ))
        ]).fit(Xq, y_qw, logreg__sample_weight=sample_weights[mask.to_numpy()])
        
        reg_qm = Pipeline([
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=best_alpha_m))
        ]).fit(Xq, y_qm, ridge__sample_weight=sample_weights[mask.to_numpy()])
        
        reg_qt = Pipeline([
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=best_alpha_t))
        ]).fit(Xq, y_qt, ridge__sample_weight=sample_weights[mask.to_numpy()])
        
        models_quarters[q] = {"win": clf_q, "margin": reg_qm, "total": reg_qt}
        print(f"   {q.upper()}: Trained on {len(Xq):,} games")
    
    # ========================================================================
    # SAVE MODELS
    # ========================================================================
    print("\nSaving models...")
    paths.models.mkdir(parents=True, exist_ok=True)
    
    # Save main models
    model_suffix = "_enhanced" if use_enhanced_features else ""
    joblib.dump(clf, paths.models / f"win_prob{model_suffix}.joblib")
    joblib.dump(reg_margin, paths.models / f"spread_margin{model_suffix}.joblib")
    joblib.dump(reg_total, paths.models / f"totals{model_suffix}.joblib")
    
    # Save feature list
    joblib.dump(use_feats, paths.models / f"feature_columns{model_suffix}.joblib")
    
    # Save period models
    joblib.dump(models_halves, paths.models / f"halves_models{model_suffix}.joblib")
    joblib.dump(models_quarters, paths.models / f"quarters_models{model_suffix}.joblib")
    
    print(f"   Saved to models/ directory")
    
    # ========================================================================
    # SUMMARY METRICS
    # ========================================================================
    metrics = {
        "features_used": len(use_feats),
        "games_trained": len(X),
        "win_logloss_cv_mean": float(pd.Series(cv_losses).mean()) if cv_losses else None,
        "margin_rmse_cv_mean": float(pd.Series(cv_rmse_m).mean()) if cv_rmse_m else None,
        "total_rmse_cv_mean": float(pd.Series(cv_rmse_t).mean()) if cv_rmse_t else None,
        "best_C": best_c,
        "best_alpha_margin": best_alpha_m,
        "best_alpha_total": best_alpha_t,
        "halves_models": len(models_halves),
        "quarters_models": len(models_quarters),
        "sample_weight_min": float(sample_weights.min()) if len(sample_weights) else None,
    }
    
    print("\n" + "="*70)
    print("TRAINING COMPLETE")
    print("="*70)
    print(f"\nMetrics:")
    print(f"   Features: {metrics['features_used']}")
    print(f"   Games: {metrics['games_trained']:,}")
    print(f"   Win LogLoss: {metrics['win_logloss_cv_mean']:.4f}")
    print(f"   Margin RMSE: {metrics['margin_rmse_cv_mean']:.2f} points")
    print(f"   Total RMSE: {metrics['total_rmse_cv_mean']:.2f} points")
    print(f"   Halves models: {metrics['halves_models']}")
    print(f"   Quarters models: {metrics['quarters_models']}")
    print("="*70 + "\n")
    
    return metrics


# Example usage
if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    from nba_betting.features_enhanced import build_features_enhanced
    from nba_betting.config import paths
    
    # Load raw games
    games_file = paths.data_raw / "games_nba_api.csv"
    if not games_file.exists():
        print(f"Games file not found: {games_file}")
        sys.exit(1)
    
    print(f"Loading games from {games_file}...")
    games = pd.read_csv(games_file)
    
    # Rename columns
    if 'home_team_tri' in games.columns:
        games = games.rename(columns={
            'home_team_tri': 'home_team',
            'visitor_team_tri': 'visitor_team',
            'date_est': 'date',
            'home_score': 'home_pts',
            'visitor_score': 'visitor_pts'
        })
    
    # Build enhanced features
    print("\nBuilding enhanced features...")
    df = build_features_enhanced(games, include_advanced_stats=True, include_injuries=True)
    
    # Train with enhanced features
    print("\nTraining models with ENHANCED features...")
    metrics_enhanced = train_models_enhanced(df, use_enhanced_features=True)
    
    print("\n" + "="*70)
    print("DONE! Models saved to models/ directory")
    print("="*70)
