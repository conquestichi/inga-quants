"""Test quality gates: pass/fail logic for each gate."""
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from inga_quant.pipeline.gates import (
    AllGatesResult,
    GateResult,
    gate_cost_test,
    gate_leak_detection,
    gate_param_stability,
    gate_ticker_split_cv,
    gate_walk_forward,
    run_all_gates,
)
from inga_quant.pipeline.model import ModelConfig, TARGET_COL


def _make_clean_dataset(
    n_tickers: int = 10,
    n_days: int = 120,
    seed: int = 0,
) -> pd.DataFrame:
    """
    Build a synthetic dataset where a linear signal actually predicts returns.
    This ensures gates that check IC > 0 can potentially pass.
    """
    rng = np.random.default_rng(seed)
    start = date(2025, 6, 1)
    tickers = [f"T{i:02d}" for i in range(n_tickers)]
    rows = []
    for ticker in tickers:
        price = 1000.0
        for i in range(n_days + 5):
            d = date.fromordinal(start.toordinal() + i)
            price *= (1 + rng.normal(0, 0.01))
            rows.append({
                "as_of": d,
                "ticker": ticker,
                "ret_1d": rng.normal(0, 0.01),
                "ret_20d": rng.normal(0, 0.05),
                "liq_score": float(rng.random()),
            })

    df = pd.DataFrame(rows)

    # Add a synthetic predictive signal and forward return
    # forward_return = 0.5 * signal + noise → signal has IC
    signal = df["ret_1d"].copy()
    df[TARGET_COL] = 0.3 * signal + rng.normal(0, 0.05, len(df))

    # Set last 5 rows per ticker to NaN (no forward return)
    for ticker, g in df.groupby("ticker"):
        last5 = g.sort_values("as_of").tail(5).index
        df.loc[last5, TARGET_COL] = np.nan

    return df


_FEATURES = ["ret_1d", "ret_20d", "liq_score"]
_CFG = ModelConfig(alpha=0.1)
# _make_clean_dataset(n_days=120) produces 120+5=125 rows per ticker, last date = day 124
_AS_OF = date(2025, 6, 1) + timedelta(days=124)


def _make_monotonic_dataset(
    n_tickers: int = 10,
    n_days: int = 120,
) -> pd.DataFrame:
    """
    Dataset where a ticker's rank is constant over time and perfectly predicts
    its forward-return rank.  Ridge regression learns coef > 0 for ret_1d,
    so corr(pred, actual) > 0 in every walk-forward fold.
    """
    rng = np.random.default_rng(0)
    start = date(2024, 1, 1)
    tickers = [f"T{i:02d}" for i in range(n_tickers)]
    rows = []
    for i, ticker in enumerate(tickers):
        signal = (i + 1) / n_tickers          # constant, monotonically increasing per ticker
        for d_idx in range(n_days + 5):
            d = date.fromordinal(start.toordinal() + d_idx)
            rows.append({
                "as_of": d,
                "ticker": ticker,
                "ret_1d": signal,
                "ret_20d": signal,
                "liq_score": signal,
                TARGET_COL: signal + rng.normal(0, 1e-4),  # nearly perfect target
            })
    df = pd.DataFrame(rows)
    for ticker, g in df.groupby("ticker"):
        last5 = g.sort_values("as_of").tail(5).index
        df.loc[last5, TARGET_COL] = np.nan
    return df


class TestGateWalkForward:
    def test_passes_on_clean_data(self):
        df = _make_clean_dataset()
        result = gate_walk_forward(df, _FEATURES, _CFG, threshold=0.0)
        assert result.passed is True
        assert result.details["ic"] is not None

    def test_fails_on_noise_only(self):
        rng = np.random.default_rng(1)
        df = _make_clean_dataset()
        # Completely randomise the target — IC should be near 0
        df[TARGET_COL] = rng.normal(0, 1, len(df))
        result = gate_walk_forward(df, _FEATURES, _CFG, threshold=10.0)  # impossible threshold
        assert result.passed is False

    def test_fails_with_insufficient_data(self):
        df = _make_clean_dataset(n_days=4)
        result = gate_walk_forward(df, _FEATURES, _CFG)
        assert result.passed is False

    def test_details_have_ic_key(self):
        df = _make_clean_dataset()
        result = gate_walk_forward(df, _FEATURES, _CFG, threshold=0.0)
        assert "ic" in result.details
        assert "threshold" in result.details

    def test_wf_ic_positive_on_monotonic_data(self):
        """
        WF_IC = corr(pred_score, realized_forward_return) must be > 0 when
        the signal perfectly ranks future returns (higher signal → higher return).
        """
        df = _make_monotonic_dataset()
        result = gate_walk_forward(df, _FEATURES, _CFG, threshold=0.0)
        assert result.details["ic"] is not None
        assert result.details["ic"] > 0, (
            f"WF_IC should be positive on monotonic data, got {result.details['ic']}"
        )


class TestGateTickerSplitCV:
    def test_passes_on_clean_data(self):
        df = _make_clean_dataset(n_tickers=15)
        result = gate_ticker_split_cv(df, _FEATURES, _CFG, threshold=-999.0)
        assert result.passed is True

    def test_fails_with_too_few_tickers(self):
        df = _make_clean_dataset(n_tickers=2)
        result = gate_ticker_split_cv(df, _FEATURES, _CFG)
        assert result.passed is False

    def test_details_have_ic_key(self):
        df = _make_clean_dataset(n_tickers=15)
        result = gate_ticker_split_cv(df, _FEATURES, _CFG, threshold=-999.0)
        assert "ic" in result.details


class TestGateCostTest:
    def test_returns_both_cost_levels(self):
        df = _make_clean_dataset()
        results = gate_cost_test(df, _FEATURES, _CFG, cost_bps_list=[5, 15])
        assert "cost_5bps" in results
        assert "cost_15bps" in results

    def test_each_result_has_net_return(self):
        df = _make_clean_dataset()
        results = gate_cost_test(df, _FEATURES, _CFG)
        for r in results.values():
            assert "net_return" in r.details


class TestGateParamStability:
    def test_passes_on_trivial_threshold(self):
        # With threshold=-1.0 (always satisfiable), gate must pass
        df = _make_clean_dataset(n_days=180)
        result = gate_param_stability(df, _FEATURES, _CFG, threshold=-1.0)
        assert result.passed is True

    def test_details_have_cosine_sim(self):
        df = _make_clean_dataset(n_days=180)
        result = gate_param_stability(df, _FEATURES, _CFG, threshold=0.0)
        assert "cosine_sim" in result.details

    def test_fails_with_insufficient_data(self):
        df = _make_clean_dataset(n_days=5)
        result = gate_param_stability(df, _FEATURES, _CFG)
        assert result.passed is False

    def test_cosine_sim_in_0_1_range(self):
        """
        param_stability cosine_sim must be in [0, 1] even when model coefficients
        flip sign across windows.  We use abs() so that consistently-signed and
        consistently-opposite-signed coefficients both score near 1.0.
        """
        df = _make_clean_dataset(n_days=180, seed=42)
        result = gate_param_stability(df, _FEATURES, _CFG, threshold=0.0)
        sim = result.details.get("cosine_sim")
        if sim is not None:  # None only when insufficient data
            assert 0.0 <= sim <= 1.0, f"cosine_sim={sim} is outside [0, 1]"


class TestGateLeakDetection:
    def test_passes_on_clean_data(self):
        df = _make_clean_dataset()
        result = gate_leak_detection(df, _FEATURES, _AS_OF)
        assert result.passed is True
        assert result.details["issues"] == []

    def test_detects_future_rows(self):
        df = _make_clean_dataset()
        future_row = df.iloc[0:1].copy()
        future_row["as_of"] = date(2099, 1, 1)
        df2 = pd.concat([df, future_row], ignore_index=True)
        result = gate_leak_detection(df2, _FEATURES, _AS_OF)
        assert result.passed is False
        assert len(result.details["issues"]) > 0

    def test_detects_near_perfect_correlation(self):
        df = _make_clean_dataset()
        # Inject a "leaky" feature that is almost identical to the target
        df["leaky"] = df[TARGET_COL].fillna(0) + np.random.default_rng(0).normal(0, 1e-10, len(df))
        result = gate_leak_detection(df, _FEATURES + ["leaky"], _AS_OF)
        assert result.passed is False
        assert any("leaky" in issue for issue in result.details["issues"])


class TestRunAllGates:
    def test_returns_all_gates_result(self):
        df = _make_clean_dataset()
        result = run_all_gates(df, _FEATURES, _AS_OF, cfg=_CFG,
                               gate_cfg={"wf_ic_threshold": 0.0, "ticker_cv_ic_threshold": -999.0,
                                         "param_stability_threshold": 0.0, "confidence_threshold": 0.0})
        assert isinstance(result, AllGatesResult)
        assert "walk_forward" in result.gates
        assert "ticker_split_cv" in result.gates
        assert "cost_5bps" in result.gates
        assert "cost_15bps" in result.gates
        assert "param_stability" in result.gates
        assert "leak_detection" in result.gates

    def test_no_trade_when_gate_fails(self):
        df = _make_clean_dataset()
        result = run_all_gates(df, _FEATURES, _AS_OF, cfg=_CFG,
                               gate_cfg={"wf_ic_threshold": 999.0})  # impossible threshold
        assert result.all_passed is False
        assert len(result.rejection_reasons) > 0

    def test_to_dict_keys(self):
        df = _make_clean_dataset()
        result = run_all_gates(df, _FEATURES, _AS_OF, cfg=_CFG,
                               gate_cfg={"wf_ic_threshold": 0.0, "confidence_threshold": 0.0})
        d = result.to_dict()
        assert "all_passed" in d
        assert "gates" in d
        assert "rejection_reasons" in d
        assert "missing_rate" in d

    def test_confidence_in_decision_card_non_negative(self, tmp_path):
        """
        decision_card 'confidence' must be >= 0 even when WF_IC is negative.
        The raw 'wf_ic' value is preserved; only 'confidence' is clipped.
        """
        import json
        from datetime import date as _date
        from inga_quant.pipeline.output import write_outputs

        # Simulate negative WF_IC (e.g. model predicts backwards on small dataset)
        wf_gate = GateResult(
            name="walk_forward",
            passed=False,
            details={"ic": -0.15, "threshold": 0.01, "fold_ics": [-0.15]},
            reason="WF IC -0.1500 <= threshold 0.0100",
        )
        gate_result = AllGatesResult(
            all_passed=False,
            gates={"walk_forward": wf_gate},
            rejection_reasons=["gate:walk_forward — WF IC -0.1500 <= threshold 0.0100"],
            missing_rate=0.0,
            n_eligible=10,
        )
        paths = write_outputs(
            out_dir=tmp_path,
            trade_date=_date(2026, 2, 24),
            run_id="test-run",
            gate_result=gate_result,
            watchlist=[],
            manifest={},
            wf_ic=-0.15,
        )
        card = json.loads(paths["decision_card"].read_text())
        assert card["key_metrics"]["confidence"] >= 0.0, (
            f"confidence must be >= 0, got {card['key_metrics']['confidence']}"
        )
        assert card["key_metrics"]["wf_ic"] == pytest.approx(-0.15), (
            "raw wf_ic must be preserved in key_metrics"
        )
