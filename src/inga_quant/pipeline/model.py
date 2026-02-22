"""Model training: Ridge / ElasticNet with forward_return_5d target."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet, Ridge
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

TARGET_COL = "forward_return_5d"


@dataclass
class ModelConfig:
    model_type: str = "Ridge"
    alpha: float = 1.0
    l1_ratio: float = 0.5  # ElasticNet only
    target: str = TARGET_COL


@dataclass
class TrainResult:
    model: Any
    scaler: StandardScaler
    coef: dict[str, float]
    feature_names: list[str]
    train_rows: int
    train_ic: float  # in-sample Spearman IC


def _make_model(cfg: ModelConfig) -> Any:
    if cfg.model_type == "ElasticNet":
        return ElasticNet(alpha=cfg.alpha, l1_ratio=cfg.l1_ratio, max_iter=10000)
    return Ridge(alpha=cfg.alpha)


def add_forward_return(
    df: pd.DataFrame,
    price_col: str = "close",
    periods: int = 5,
) -> pd.DataFrame:
    """
    Add forward_return_5d column per ticker (realised future return).
    Only rows where the forward price is known get a non-NaN value.
    """
    df = df.copy()
    result_parts = []
    for ticker, g in df.groupby("ticker", sort=False):
        g = g.sort_values("as_of").copy()
        pc = g[price_col].to_numpy(dtype=float, na_value=np.nan)
        fwd = np.full(len(g), np.nan)
        for i in range(len(g) - periods):
            p0, p5 = pc[i], pc[i + periods]
            if p0 > 0 and not np.isnan(p0) and not np.isnan(p5):
                fwd[i] = (p5 - p0) / p0
        g[TARGET_COL] = fwd
        result_parts.append(g)
    return pd.concat(result_parts, ignore_index=True)


def train_model(
    features: pd.DataFrame,
    feature_names: list[str],
    cfg: ModelConfig | None = None,
) -> TrainResult:
    """
    Train Ridge/ElasticNet on features with known forward_return_5d.

    Parameters
    ----------
    features : DataFrame containing feature columns + TARGET_COL
    feature_names : ordered list of feature columns to use
    cfg : ModelConfig
    """
    if cfg is None:
        cfg = ModelConfig()

    assert cfg.target == TARGET_COL, "target must be forward_return_5d (Phase 1 contract)"

    # Keep only rows with target + all features present
    cols_needed = feature_names + [cfg.target]
    present = [c for c in feature_names if c in features.columns]
    if len(present) < len(feature_names):
        missing = set(feature_names) - set(present)
        logger.warning("Missing feature columns (will drop): %s", missing)
        feature_names = present

    train_df = features[features[cfg.target].notna()].copy()
    # Fill NaN features with column mean (simple imputation)
    for col in feature_names:
        if col in train_df.columns:
            mean_val = train_df[col].mean()
            train_df[col] = train_df[col].fillna(mean_val)
        else:
            train_df[col] = 0.0

    X = train_df[feature_names].to_numpy(dtype=float)
    y = train_df[cfg.target].to_numpy(dtype=float)

    if len(X) == 0:
        raise ValueError("No training rows with known forward_return_5d")

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = _make_model(cfg)
    model.fit(X_scaled, y)

    coef = {f: float(c) for f, c in zip(feature_names, model.coef_)}

    # In-sample Spearman IC
    y_pred = model.predict(X_scaled)
    train_ic = _spearman_ic(y_pred, y)

    logger.info(
        "Model trained: %s alpha=%.3f features=%d rows=%d IS-IC=%.4f",
        cfg.model_type, cfg.alpha, len(feature_names), len(X), train_ic,
    )
    return TrainResult(
        model=model,
        scaler=scaler,
        coef=coef,
        feature_names=feature_names,
        train_rows=len(X),
        train_ic=train_ic,
    )


def predict(
    result: TrainResult,
    features: pd.DataFrame,
) -> pd.Series:
    """Generate predictions for a feature DataFrame. Returns Series aligned to df index."""
    feat_df = features.copy()
    for col in result.feature_names:
        if col not in feat_df.columns:
            feat_df[col] = 0.0
        else:
            feat_df[col] = feat_df[col].fillna(feat_df[col].mean())
    X = feat_df[result.feature_names].to_numpy(dtype=float)
    X_scaled = result.scaler.transform(X)
    preds = result.model.predict(X_scaled)
    return pd.Series(preds, index=features.index)


def _spearman_ic(y_pred: np.ndarray, y_true: np.ndarray) -> float:
    """Spearman rank correlation between predictions and actual returns."""
    from scipy.stats import spearmanr
    if len(y_pred) < 3:
        return 0.0
    corr, _ = spearmanr(y_pred, y_true)
    return float(corr) if not np.isnan(corr) else 0.0
