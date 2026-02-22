"""Watchlist builder: score tickers, apply rotation constraints."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class WatchlistConfig:
    size: int = 50
    max_new: int = 20
    min_retained: int = 30
    turnover_penalty: float = 0.01


@dataclass
class WatchlistEntry:
    ticker: str
    name: str
    score: float
    reason_short: str
    is_new: bool
    turnover_penalty: float


def _compute_scores(
    features: pd.DataFrame,
    signal_features: list[str],
    model_coef: dict[str, float],
    market_regime: str = "risk_on",
    regime_multipliers: dict[str, float] | None = None,
) -> pd.Series:
    """Compute per-ticker composite score from model coefficients."""
    if regime_multipliers is None:
        regime_multipliers = {"risk_on": 1.0, "risk_off": 0.5}

    multiplier = regime_multipliers.get(market_regime, 1.0)

    scores = pd.Series(0.0, index=features.index)
    for feat in signal_features:
        if feat in features.columns and feat in model_coef:
            col = features[feat].fillna(0.0)
            scores += model_coef[feat] * col

    return scores * multiplier


def build_watchlist(
    features: pd.DataFrame,
    as_of_date: Any,
    model_coef: dict[str, float],
    signal_features: list[str],
    prev_watchlist: list[str] | None = None,
    cfg: WatchlistConfig | None = None,
    regime_multipliers: dict[str, float] | None = None,
) -> list[WatchlistEntry]:
    """
    Build watchlist for a given as_of date.

    Parameters
    ----------
    features : DataFrame with features for the scoring date
    as_of_date : date to filter features on
    model_coef : {feature_name: coefficient}
    signal_features : ordered list of feature names to use
    prev_watchlist : list of tickers in yesterday's watchlist (for rotation)
    cfg : WatchlistConfig
    regime_multipliers : multiplier per market_regime value

    Returns
    -------
    List of WatchlistEntry sorted by score descending.
    """
    if cfg is None:
        cfg = WatchlistConfig()

    # Get snapshot for as_of_date
    day_df = features[features["as_of"] == as_of_date].copy()
    if day_df.empty:
        logger.warning("No features for as_of=%s", as_of_date)
        return []

    # Determine market regime for this day
    regime = "risk_on"
    if "market_regime" in day_df.columns and not day_df["market_regime"].isna().all():
        regime = day_df["market_regime"].iloc[0]

    scores = _compute_scores(day_df, signal_features, model_coef, regime, regime_multipliers)
    day_df = day_df.copy()
    day_df["_score"] = scores.values

    # Drop tickers with NaN scores (missing critical features)
    day_df = day_df.dropna(subset=["_score"])

    prev_set = set(prev_watchlist or [])

    # Apply turnover penalty to new entries
    day_df["_is_new"] = ~day_df["ticker"].isin(prev_set)
    day_df["_turnover_penalty"] = day_df["_is_new"].astype(float) * cfg.turnover_penalty
    day_df["_adj_score"] = day_df["_score"] - day_df["_turnover_penalty"]

    # Sort by adjusted score
    day_df = day_df.sort_values("_adj_score", ascending=False)

    # Rotation constraint: if prev_watchlist exists and has enough entries
    if prev_set and len(prev_set) >= cfg.min_retained:
        retained = day_df[day_df["ticker"].isin(prev_set)].head(cfg.min_retained)
        new_candidates = day_df[~day_df["ticker"].isin(prev_set)].head(cfg.max_new)
        pool = pd.concat([retained, new_candidates]).sort_values("_adj_score", ascending=False)
        selected = pool.head(cfg.size)
    else:
        selected = day_df.head(cfg.size)

    entries: list[WatchlistEntry] = []
    for _, row in selected.iterrows():
        entries.append(WatchlistEntry(
            ticker=str(row["ticker"]),
            name=str(row.get("name", row["ticker"])),
            score=float(row["_adj_score"]),
            reason_short=_reason_short(row, signal_features),
            is_new=bool(row["_is_new"]),
            turnover_penalty=float(row["_turnover_penalty"]),
        ))

    logger.info(
        "Watchlist built: %d entries (%d new, %d retained) for %s",
        len(entries),
        sum(e.is_new for e in entries),
        sum(not e.is_new for e in entries),
        as_of_date,
    )
    return entries


def _reason_short(row: pd.Series, features: list[str]) -> str:
    """Produce a short human-readable reason based on top contributing feature."""
    best_feat = None
    best_val = -1e9
    for f in features:
        if f in row and not pd.isna(row[f]):
            if abs(float(row[f])) > best_val:
                best_val = abs(float(row[f]))
                best_feat = f
    if best_feat is None:
        return "composite"
    return best_feat.replace("_", " ")
