"""Quality gates: walk-forward, ticker CV, cost test, stability, leak detection."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from inga_quant.pipeline.model import (
    ModelConfig,
    TARGET_COL,
    _make_model,
    _spearman_ic,
    train_model,
    predict,
)

logger = logging.getLogger(__name__)


@dataclass
class GateResult:
    name: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)
    reason: str = ""


@dataclass
class AllGatesResult:
    all_passed: bool
    gates: dict[str, GateResult]
    rejection_reasons: list[str]
    missing_rate: float
    n_eligible: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "all_passed": self.all_passed,
            "missing_rate": self.missing_rate,
            "n_eligible": self.n_eligible,
            "gates": {
                name: {
                    "passed": r.passed,
                    "reason": r.reason,
                    **r.details,
                }
                for name, r in self.gates.items()
            },
            "rejection_reasons": self.rejection_reasons,
        }


# ---------------------------------------------------------------------------
# Individual gates
# ---------------------------------------------------------------------------

def gate_walk_forward(
    features: pd.DataFrame,
    feature_names: list[str],
    cfg: ModelConfig,
    threshold: float = 0.01,
    n_splits: int = 3,
) -> GateResult:
    """
    Rolling walk-forward: train on first K folds, validate on (K+1)th fold.
    Gate passes if median Spearman IC across folds > threshold.
    """
    df = features[features[TARGET_COL].notna()].copy()
    dates = sorted(df["as_of"].unique())
    if len(dates) < n_splits * 2:
        return GateResult(
            name="walk_forward",
            passed=False,
            details={"ic": None, "threshold": threshold, "n_splits_available": len(dates)},
            reason="insufficient data for walk-forward splits",
        )

    fold_size = len(dates) // (n_splits + 1)
    ics: list[float] = []

    for k in range(n_splits):
        train_end_idx = (k + 1) * fold_size
        test_end_idx = (k + 2) * fold_size
        train_dates = dates[:train_end_idx]
        test_dates = dates[train_end_idx:test_end_idx]
        if not test_dates:
            continue

        train_df = df[df["as_of"].isin(train_dates)]
        test_df = df[df["as_of"].isin(test_dates)]

        try:
            result = train_model(train_df, feature_names, cfg)
            preds = predict(result, test_df)
            ic = _spearman_ic(preds.to_numpy(), test_df[TARGET_COL].to_numpy())
            ics.append(ic)
        except Exception as exc:
            logger.warning("WF fold %d failed: %s", k, exc)
            ics.append(0.0)

    median_ic = float(np.median(ics)) if ics else 0.0
    passed = median_ic > threshold
    return GateResult(
        name="walk_forward",
        passed=passed,
        details={"ic": round(median_ic, 6), "threshold": threshold, "fold_ics": [round(x, 6) for x in ics]},
        reason="" if passed else f"WF IC {median_ic:.4f} <= threshold {threshold}",
    )


def gate_ticker_split_cv(
    features: pd.DataFrame,
    feature_names: list[str],
    cfg: ModelConfig,
    threshold: float = 0.00,
    test_frac: float = 0.2,
) -> GateResult:
    """
    Ticker-split cross-validation: hold out test_frac of tickers.
    Gate passes if held-out Spearman IC > threshold.
    """
    df = features[features[TARGET_COL].notna()].copy()
    tickers = df["ticker"].unique()
    if len(tickers) < 5:
        return GateResult(
            name="ticker_split_cv",
            passed=False,
            details={"ic": None, "threshold": threshold},
            reason=f"too few tickers ({len(tickers)}) for ticker-split CV",
        )

    rng = np.random.default_rng(42)
    n_test = max(1, int(len(tickers) * test_frac))
    test_tickers = set(rng.choice(tickers, n_test, replace=False))
    train_tickers = set(tickers) - test_tickers

    train_df = df[df["ticker"].isin(train_tickers)]
    test_df = df[df["ticker"].isin(test_tickers)]

    try:
        result = train_model(train_df, feature_names, cfg)
        preds = predict(result, test_df)
        ic = _spearman_ic(preds.to_numpy(), test_df[TARGET_COL].to_numpy())
    except Exception as exc:
        return GateResult(
            name="ticker_split_cv",
            passed=False,
            details={"ic": None},
            reason=f"ticker CV failed: {exc}",
        )

    passed = ic > threshold
    return GateResult(
        name="ticker_split_cv",
        passed=passed,
        details={"ic": round(ic, 6), "threshold": threshold, "n_test_tickers": n_test},
        reason="" if passed else f"ticker CV IC {ic:.4f} <= threshold {threshold}",
    )


def gate_cost_test(
    features: pd.DataFrame,
    feature_names: list[str],
    cfg: ModelConfig,
    cost_bps_list: list[int] | None = None,
) -> dict[str, GateResult]:
    """
    Cost-tolerance test: simulate long-top-decile strategy with costs.
    Gate passes if cumulative net return > 0 at each cost level.
    Returns a dict: {'cost_5bps': GateResult, 'cost_15bps': GateResult, ...}
    """
    if cost_bps_list is None:
        cost_bps_list = [5, 15]

    df = features[features[TARGET_COL].notna()].copy()
    try:
        result = train_model(df, feature_names, cfg)
        preds = predict(result, df)
    except Exception as exc:
        reason = f"model train failed for cost test: {exc}"
        return {
            f"cost_{bps}bps": GateResult(f"cost_{bps}bps", False, {}, reason)
            for bps in cost_bps_list
        }

    df = df.copy()
    df["_pred"] = preds.values

    results: dict[str, GateResult] = {}
    for bps in cost_bps_list:
        cost = bps / 10000.0
        # Per day: long top decile, compute average return, subtract cost
        daily_returns: list[float] = []
        for as_of, g in df.groupby("as_of"):
            if len(g) < 5:
                continue
            q90 = g["_pred"].quantile(0.90)
            top = g[g["_pred"] >= q90]
            if top.empty:
                continue
            gross = top[TARGET_COL].mean()
            net = gross - cost
            daily_returns.append(net)

        cum_ret = sum(daily_returns)
        passed = cum_ret > 0
        name = f"cost_{bps}bps"
        results[name] = GateResult(
            name=name,
            passed=passed,
            details={"net_return": round(cum_ret, 6), "cost_bps": bps, "n_days": len(daily_returns)},
            reason="" if passed else f"net return {cum_ret:.4f} <= 0 at {bps}bps cost",
        )
    return results


def gate_param_stability(
    features: pd.DataFrame,
    feature_names: list[str],
    cfg: ModelConfig,
    threshold: float = 0.70,
    n_windows: int = 3,
) -> GateResult:
    """
    Train model on N sub-windows and compute cosine similarity of coefficients.
    Gate passes if mean cosine similarity > threshold.
    """
    df = features[features[TARGET_COL].notna()].copy()
    dates = sorted(df["as_of"].unique())
    window = len(dates) // n_windows
    if window < 10:
        return GateResult(
            name="param_stability",
            passed=False,
            details={"cosine_sim": None, "threshold": threshold},
            reason="insufficient data for parameter stability test",
        )

    coef_vectors: list[np.ndarray] = []
    for i in range(n_windows):
        window_dates = dates[i * window: (i + 1) * window]
        sub_df = df[df["as_of"].isin(window_dates)]
        try:
            result = train_model(sub_df, feature_names, cfg)
            vec = np.array([result.coef.get(f, 0.0) for f in feature_names])
            coef_vectors.append(vec)
        except Exception as exc:
            logger.warning("Stability window %d failed: %s", i, exc)

    if len(coef_vectors) < 2:
        return GateResult(
            name="param_stability",
            passed=False,
            details={"cosine_sim": None},
            reason="not enough windows trained successfully",
        )

    sims: list[float] = []
    for i in range(len(coef_vectors)):
        for j in range(i + 1, len(coef_vectors)):
            sims.append(_cosine_similarity(coef_vectors[i], coef_vectors[j]))

    mean_sim = float(np.mean(sims))
    passed = mean_sim > threshold
    return GateResult(
        name="param_stability",
        passed=passed,
        details={"cosine_sim": round(mean_sim, 6), "threshold": threshold, "n_windows": len(coef_vectors)},
        reason="" if passed else f"param stability {mean_sim:.4f} <= threshold {threshold}",
    )


def gate_leak_detection(
    features: pd.DataFrame,
    feature_names: list[str],
    as_of: Any,
) -> GateResult:
    """
    Check for common data leakage patterns.

    Checks:
    1. No feature with |corr| > 0.99 with same-day TARGET_COL
       (would indicate label leakage)
    2. as_of column has no values strictly after `as_of`
    """
    issues: list[str] = []

    # Check 1: future date check
    dates = features["as_of"]
    if hasattr(as_of, "date"):
        as_of = as_of.date()
    future_rows = (dates > as_of).sum()
    if future_rows > 0:
        issues.append(f"{future_rows} rows have as_of > cutoff ({as_of})")

    # Check 2: suspicious correlation with target
    if TARGET_COL in features.columns:
        df_notna = features[features[TARGET_COL].notna()]
        for feat in feature_names:
            if feat not in df_notna.columns:
                continue
            col = df_notna[feat].fillna(0)
            tgt = df_notna[TARGET_COL]
            if col.std() > 0 and tgt.std() > 0:
                corr = float(col.corr(tgt))
                if abs(corr) > 0.99:
                    issues.append(f"feature '{feat}' has suspicious corr={corr:.4f} with target")

    passed = len(issues) == 0
    return GateResult(
        name="leak_detection",
        passed=passed,
        details={"issues": issues},
        reason="; ".join(issues) if issues else "",
    )


# ---------------------------------------------------------------------------
# Run all gates
# ---------------------------------------------------------------------------

def run_all_gates(
    features: pd.DataFrame,
    feature_names: list[str],
    as_of: Any,
    cfg: ModelConfig | None = None,
    gate_cfg: dict[str, Any] | None = None,
) -> AllGatesResult:
    """
    Run all quality gates and return an AllGatesResult.
    NO_TRADE is triggered if any gate fails or additional conditions are not met.
    """
    if cfg is None:
        cfg = ModelConfig()
    if gate_cfg is None:
        gate_cfg = {}

    wf_threshold = gate_cfg.get("wf_ic_threshold", 0.01)
    cv_threshold = gate_cfg.get("ticker_cv_ic_threshold", 0.00)
    stability_threshold = gate_cfg.get("param_stability_threshold", 0.70)
    cost_bps = gate_cfg.get("cost_bps", [5, 15])
    missing_threshold = gate_cfg.get("missing_rate_threshold", 0.20)
    min_eligible = gate_cfg.get("min_eligible_stocks", 5)
    confidence_threshold = gate_cfg.get("confidence_threshold", 0.005)

    gates: dict[str, GateResult] = {}
    rejection_reasons: list[str] = []

    # Compute missing rate
    day_features = features[features["as_of"] == as_of]
    n_eligible = len(day_features)
    if n_eligible > 0:
        n_missing = day_features[feature_names].isna().any(axis=1).sum()
        missing_rate = float(n_missing) / n_eligible
    else:
        missing_rate = 1.0

    # Pre-flight checks
    if n_eligible < min_eligible:
        rejection_reasons.append(f"n_eligible={n_eligible} < {min_eligible}")
    if missing_rate > missing_threshold:
        rejection_reasons.append(f"missing_rate={missing_rate:.2%} > {missing_threshold:.0%}")

    # Gate: walk-forward
    wf = gate_walk_forward(features, feature_names, cfg, threshold=wf_threshold)
    gates["walk_forward"] = wf
    if not wf.passed:
        rejection_reasons.append(f"gate:walk_forward — {wf.reason}")

    # Gate: ticker split CV
    cv = gate_ticker_split_cv(features, feature_names, cfg, threshold=cv_threshold)
    gates["ticker_split_cv"] = cv
    if not cv.passed:
        rejection_reasons.append(f"gate:ticker_split_cv — {cv.reason}")

    # Gate: cost tests
    cost_results = gate_cost_test(features, feature_names, cfg, cost_bps_list=cost_bps)
    for name, cr in cost_results.items():
        gates[name] = cr
        if not cr.passed:
            rejection_reasons.append(f"gate:{name} — {cr.reason}")

    # Gate: parameter stability
    stab = gate_param_stability(features, feature_names, cfg, threshold=stability_threshold)
    gates["param_stability"] = stab
    if not stab.passed:
        rejection_reasons.append(f"gate:param_stability — {stab.reason}")

    # Gate: leak detection
    leak = gate_leak_detection(features, feature_names, as_of)
    gates["leak_detection"] = leak
    if not leak.passed:
        rejection_reasons.append(f"gate:leak_detection — {leak.reason}")

    # Confidence check (WF IC)
    wf_ic = wf.details.get("ic") or 0.0
    if wf_ic is not None and wf_ic < confidence_threshold:
        rejection_reasons.append(f"confidence={wf_ic:.4f} < threshold {confidence_threshold}")

    all_passed = len(rejection_reasons) == 0
    return AllGatesResult(
        all_passed=all_passed,
        gates=gates,
        rejection_reasons=list(dict.fromkeys(rejection_reasons)),  # deduplicate
        missing_rate=missing_rate,
        n_eligible=n_eligible,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    norm_a, norm_b = np.linalg.norm(a), np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))
