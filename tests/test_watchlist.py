"""Test watchlist rotation constraints and scoring."""
from datetime import date

import numpy as np
import pandas as pd
import pytest

from inga_quant.pipeline.watchlist import WatchlistConfig, build_watchlist


def _make_features(n_tickers: int, as_of: date) -> pd.DataFrame:
    """Build minimal features DataFrame for watchlist tests."""
    rng = np.random.default_rng(99)
    tickers = [f"T{i:03d}" for i in range(n_tickers)]
    rows = []
    for t in tickers:
        rows.append({
            "as_of": as_of,
            "ticker": t,
            "ret_1d": float(rng.normal(0, 0.01)),
            "ret_20d": float(rng.normal(0, 0.05)),
            "vol_20": float(abs(rng.normal(0.01, 0.005))),
            "liq_score": float(rng.random()),
            "rs_20d": float(rng.normal(0, 0.02)),
            "market_regime": "risk_on",
        })
    return pd.DataFrame(rows)


_FEATURES = ["ret_1d", "ret_20d", "liq_score", "rs_20d"]
_COEF = {"ret_1d": 1.0, "ret_20d": 0.5, "liq_score": 0.3, "rs_20d": 0.2}
_AS_OF = date(2026, 2, 10)


class TestWatchlistBasic:
    def test_returns_at_most_size_entries(self):
        features = _make_features(100, _AS_OF)
        cfg = WatchlistConfig(size=50)
        result = build_watchlist(features, _AS_OF, _COEF, _FEATURES, cfg=cfg)
        assert len(result) <= 50

    def test_all_tickers_unique(self):
        features = _make_features(60, _AS_OF)
        result = build_watchlist(features, _AS_OF, _COEF, _FEATURES)
        tickers = [e.ticker for e in result]
        assert len(tickers) == len(set(tickers))

    def test_empty_features_returns_empty(self):
        features = pd.DataFrame(columns=["as_of", "ticker"] + _FEATURES)
        result = build_watchlist(features, _AS_OF, _COEF, _FEATURES)
        assert result == []


class TestWatchlistRotation:
    def test_max_new_entries_respected(self):
        features = _make_features(100, _AS_OF)
        prev_watchlist = [f"T{i:03d}" for i in range(50)]  # full prior watchlist
        cfg = WatchlistConfig(size=50, max_new=20, min_retained=30)
        result = build_watchlist(features, _AS_OF, _COEF, _FEATURES,
                                 prev_watchlist=prev_watchlist, cfg=cfg)
        new_count = sum(e.is_new for e in result)
        assert new_count <= cfg.max_new, f"Too many new entries: {new_count} > {cfg.max_new}"

    def test_min_retained_respected_when_possible(self):
        features = _make_features(100, _AS_OF)
        prev_watchlist = [f"T{i:03d}" for i in range(50)]
        cfg = WatchlistConfig(size=50, max_new=20, min_retained=30)
        result = build_watchlist(features, _AS_OF, _COEF, _FEATURES,
                                 prev_watchlist=prev_watchlist, cfg=cfg)
        retained_count = sum(not e.is_new for e in result)
        assert retained_count >= cfg.min_retained, (
            f"Too few retained: {retained_count} < {cfg.min_retained}"
        )

    def test_no_rotation_limit_when_no_prev(self):
        features = _make_features(100, _AS_OF)
        cfg = WatchlistConfig(size=50, max_new=20, min_retained=30)
        result = build_watchlist(features, _AS_OF, _COEF, _FEATURES,
                                 prev_watchlist=None, cfg=cfg)
        # All entries are "new" with no prev
        assert all(e.is_new for e in result)

    def test_is_new_flag_correctly_set(self):
        features = _make_features(60, _AS_OF)
        prev = [f"T{i:03d}" for i in range(20)]  # first 20 tickers are old
        result = build_watchlist(features, _AS_OF, _COEF, _FEATURES, prev_watchlist=prev)
        for e in result:
            if e.ticker in prev:
                assert not e.is_new, f"{e.ticker} should not be is_new"
            else:
                assert e.is_new, f"{e.ticker} should be is_new"

    def test_turnover_penalty_applied(self):
        features = _make_features(60, _AS_OF)
        prev = [f"T{i:03d}" for i in range(30)]
        cfg = WatchlistConfig(turnover_penalty=0.05)
        result = build_watchlist(features, _AS_OF, _COEF, _FEATURES, prev_watchlist=prev, cfg=cfg)
        for e in result:
            if e.is_new:
                assert e.turnover_penalty == pytest.approx(0.05)
            else:
                assert e.turnover_penalty == pytest.approx(0.0)
