"""Spot-check computed feature values against manual calculations."""
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from inga_quant.utils.io import load_bars
from inga_quant.features.build_features import build_features

BARS_PATH = Path(__file__).parent / "fixtures" / "bars_small.parquet"
AS_OF = "2026-02-10"
TICKER = "AAA"


@pytest.fixture(scope="module")
def features_aaa():
    bars = load_bars(BARS_PATH, as_of=AS_OF)
    df = build_features(bars)
    return df[df["ticker"] == TICKER].reset_index(drop=True)


@pytest.fixture(scope="module")
def raw_aaa():
    bars = load_bars(BARS_PATH, as_of=AS_OF)
    return bars[bars["ticker"] == TICKER].sort_values("as_of").reset_index(drop=True)


def test_ret_1d_value(features_aaa, raw_aaa):
    """ret_1d for row i == (close[i] - close[i-1]) / close[i-1]."""
    i = 50  # arbitrary mid-series row
    row_f = features_aaa.iloc[i]
    expected = (raw_aaa["close"].iloc[i] - raw_aaa["close"].iloc[i - 1]) / raw_aaa["close"].iloc[i - 1]
    assert abs(row_f["ret_1d"] - expected) < 1e-8


def test_avg_traded_value_20d(features_aaa, raw_aaa):
    """avg_traded_value_20d at row 30 == mean of close*vol for rows 11..30 (20-window)."""
    i = 30
    price = raw_aaa["close"]
    vol = raw_aaa["volume"]
    tv = price * vol
    expected = tv.iloc[max(0, i - 19): i + 1].mean()
    assert abs(features_aaa.iloc[i]["avg_traded_value_20d"] - expected) < 1e-3


def test_hh_20d_excludes_current_day(features_aaa, raw_aaa):
    """hh_20d[i] == max(close[i-20..i-1]), i.e., excludes close[i]."""
    i = 40
    price = raw_aaa["close"]
    shifted = price.shift(1)
    expected = shifted.iloc[max(0, i - 19): i + 1].max()
    assert abs(features_aaa.iloc[i]["hh_20d"] - expected) < 1e-8


def test_up_streak_3(features_aaa, raw_aaa):
    """up_streak_3: manually check a row with known consecutive increases."""
    price = raw_aaa["close"]
    feat = features_aaa

    for i in range(3, len(feat)):
        expected_streak = int(
            price.iloc[i] > price.iloc[i - 1]
            and price.iloc[i - 1] > price.iloc[i - 2]
            and price.iloc[i - 2] > price.iloc[i - 3]
        )
        actual = feat.iloc[i]["up_streak_3"]
        if pd.isna(actual):
            continue
        assert int(actual) == expected_streak, (
            f"Row {i}: expected up_streak_3={expected_streak}, got {actual}"
        )
        # Spot-check only first 20 rows to keep test fast
        if i > 20:
            break


def test_trend_aliases(features_aaa):
    """trend_20d == ret_20d and trend_60d == ret_60d."""
    df = features_aaa
    pd.testing.assert_series_equal(
        df["trend_20d"].reset_index(drop=True),
        df["ret_20d"].reset_index(drop=True),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        df["trend_60d"].reset_index(drop=True),
        df["ret_60d"].reset_index(drop=True),
        check_names=False,
    )


def test_insufficient_history_flags(features_aaa):
    """First rows should carry insufficient_history_20."""
    flags_row0 = json.loads(features_aaa.iloc[0]["quality_flags"])
    assert "insufficient_history_20" in flags_row0


def test_rs_20d_definition(features_aaa):
    """rs_20d == ret_20d - market_ret_20d for all non-NaN rows."""
    df = features_aaa.dropna(subset=["rs_20d", "ret_20d", "market_ret_20d"])
    diff = (df["rs_20d"] - (df["ret_20d"] - df["market_ret_20d"])).abs()
    assert (diff < 1e-10).all()


def test_close_pos_in_range_bounds(features_aaa):
    """close_pos_in_range_1d ∈ [0, 1] when range > 0."""
    valid = features_aaa["close_pos_in_range_1d"].dropna()
    assert (valid >= -1e-9).all() and (valid <= 1 + 1e-9).all()


def test_volume_z_no_nan_when_volume_ok(features_aaa):
    """After row 20, volume_z_20d should not be NaN (fixture has no zero-volume days)."""
    late_rows = features_aaa.iloc[25:]
    # If std is zero the flag is set and value=0 (not NaN), so no NaNs expected
    assert late_rows["volume_z_20d"].isna().sum() == 0
