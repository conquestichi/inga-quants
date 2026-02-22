"""Test that build_features returns all required columns."""
import json
from pathlib import Path

import pandas as pd
import pytest

from inga_quant.utils.io import load_bars
from inga_quant.features.build_features import build_features

BARS_PATH = Path(__file__).parent / "fixtures" / "bars_small.parquet"
AS_OF = "2026-02-10"

REQUIRED_COLS = [
    "as_of", "ticker",
    "avg_traded_value_20d", "liq_score",
    "ret_1d", "ret_3d", "ret_5d", "ret_20d", "ret_60d", "absret_1d",
    "hh_20d",
    "volume_z_20d",
    "vol_20", "vol_60", "vol_z_20d", "vol_z_60d",
    "prev_close", "gap_1d", "range", "close_to_high_1d", "close_pos_in_range_1d",
    "trend_20d", "trend_60d",
    "up_streak_3",
    "market_ret_20d", "market_ret_60d", "rs_20d", "rs_60d",
    "earnings_react_1d", "earnings_drift_5d", "earnings_quality_z",
    "data_stale_flag", "market_regime",
    "op_margin_yoy", "guidance_up_flag", "event_bullish_count_60d",
    "quality_flags",
]


@pytest.fixture(scope="module")
def features_df():
    bars = load_bars(BARS_PATH, as_of=AS_OF)
    return build_features(bars)


def test_all_required_columns_present(features_df):
    missing = [c for c in REQUIRED_COLS if c not in features_df.columns]
    assert missing == [], f"Missing columns: {missing}"


def test_row_count_positive(features_df):
    assert len(features_df) > 0


def test_no_duplicate_keys(features_df):
    dupes = features_df.duplicated(subset=["as_of", "ticker"]).sum()
    assert dupes == 0, f"{dupes} duplicate (as_of, ticker) rows"


def test_quality_flags_is_valid_json(features_df):
    for val in features_df["quality_flags"]:
        parsed = json.loads(val)
        assert isinstance(parsed, list), f"quality_flags must be a JSON array, got: {val!r}"


def test_data_stale_flag_is_zero(features_df):
    assert (features_df["data_stale_flag"] == 0).all()


def test_market_regime_values(features_df):
    valid = {"risk_on", "risk_off"}
    actual = set(features_df["market_regime"].unique())
    assert actual.issubset(valid), f"Unexpected regime values: {actual - valid}"


def test_op_margin_yoy_is_nan(features_df):
    assert features_df["op_margin_yoy"].isna().all()


def test_guidance_up_flag_is_zero(features_df):
    assert (features_df["guidance_up_flag"] == 0).all()


def test_liq_score_in_0_1(features_df):
    valid = features_df["liq_score"].dropna()
    assert (valid >= 0).all() and (valid <= 1).all()


def test_absret_1d_nonnegative(features_df):
    valid = features_df["absret_1d"].dropna()
    assert (valid >= 0).all()


def test_no_events_flag_present(features_df):
    """Without events, every row should have no_events in quality_flags."""
    for val in features_df["quality_flags"]:
        flags = json.loads(val)
        assert "no_events" in flags, f"Expected no_events flag, got: {flags}"
