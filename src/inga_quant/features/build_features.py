"""Feature Store v1 MVP — builds features_daily from bars_daily + optional events."""
from __future__ import annotations

import json
from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cs_rank(series: pd.Series) -> pd.Series:
    """Cross-sectional percentile rank in [0, 1], method='average'."""
    return series.rank(method="average", pct=True)


def _cs_zscore(series: pd.Series) -> pd.Series:
    """Cross-sectional z-score; returns 0 everywhere if std == 0 or NaN."""
    mu = series.mean()
    sigma = series.std(ddof=1)
    if pd.isna(sigma) or sigma == 0:
        return pd.Series(0.0, index=series.index)
    return (series - mu) / sigma


def _flags_json(flags: list[str]) -> str:
    return json.dumps(sorted(set(flags)))


# ---------------------------------------------------------------------------
# Per-ticker rolling calculations
# ---------------------------------------------------------------------------

def _build_ticker_features(
    g: pd.DataFrame,
    price_col: str,
) -> pd.DataFrame:
    """
    Compute all per-ticker time-series features.
    g is sorted ascending by as_of for one ticker.
    Returns g augmented with new columns + _flags list per row.
    """
    g = g.copy().reset_index(drop=True)
    n = len(g)

    # Accumulate per-row flag lists
    flag_lists: list[list[str]] = [[] for _ in range(n)]

    price = g[price_col].to_numpy(dtype=float, na_value=np.nan)
    vol_arr = g["volume"].to_numpy(dtype=float, na_value=np.nan)
    open_arr = g["open"].to_numpy(dtype=float, na_value=np.nan)
    high_arr = g["high"].to_numpy(dtype=float, na_value=np.nan)
    low_arr = g["low"].to_numpy(dtype=float, na_value=np.nan)
    close_arr = g["close"].to_numpy(dtype=float, na_value=np.nan)

    # --- Row-level quality flags ---
    has_suspended = "is_suspended" in g.columns

    for i in range(n):
        if np.isnan(price[i]):
            flag_lists[i].append("missing_price")
        if np.isnan(vol_arr[i]):
            flag_lists[i].append("missing_volume")
        if any(np.isnan(v) for v in [close_arr[i], open_arr[i], high_arr[i], low_arr[i]]):
            flag_lists[i].append("missing_ohlc")
        rng_i = high_arr[i] - low_arr[i]
        if not np.isnan(rng_i) and rng_i == 0:
            flag_lists[i].append("zero_range")
        if has_suspended:
            val = g["is_suspended"].iloc[i]
            if val is True or val == 1:
                flag_lists[i].append("suspended")
        # Insufficient history
        rows_so_far = i + 1
        if rows_so_far < 20:
            flag_lists[i].append("insufficient_history_20")
        elif rows_so_far < 60:
            flag_lists[i].append("insufficient_history_60")

    # --- Group 1: Liquidity ---
    traded_value = pd.Series(close_arr * vol_arr, dtype=float)
    g["avg_traded_value_20d"] = traded_value.rolling(20, min_periods=1).mean().to_numpy()

    # --- Group 2: Returns ---
    price_s = pd.Series(price, dtype=float)
    for periods, col in [(1, "ret_1d"), (3, "ret_3d"), (5, "ret_5d"),
                         (20, "ret_20d"), (60, "ret_60d")]:
        g[col] = price_s.pct_change(periods).to_numpy()
    g["absret_1d"] = np.abs(g["ret_1d"].to_numpy(dtype=float, na_value=np.nan))

    # --- Group 3: High watermark (shift=1, excludes current day) ---
    g["hh_20d"] = price_s.shift(1).rolling(20, min_periods=1).max().to_numpy()

    # --- Group 4: Volume Z-score ---
    vol_s = pd.Series(vol_arr, dtype=float)
    vol_mean20 = vol_s.rolling(20, min_periods=1).mean()
    vol_std20 = vol_s.rolling(20, min_periods=2).std(ddof=1)
    vz_arr = np.where(
        vol_std20.to_numpy() > 0,
        (vol_arr - vol_mean20.to_numpy()) / vol_std20.to_numpy(),
        np.nan,
    )
    # Where std == 0 exactly (not NaN), set to 0 and flag
    for i in range(n):
        std_i = vol_std20.iloc[i]
        if not pd.isna(std_i) and std_i == 0:
            vz_arr[i] = 0.0
            flag_lists[i].append("volume_std_zero")
    g["volume_z_20d"] = vz_arr

    # --- Group 5: Volatility (time-series part; cross-sectional done later) ---
    ret1d_s = pd.Series(g["ret_1d"].to_numpy(dtype=float, na_value=np.nan), dtype=float)
    g["vol_20"] = ret1d_s.rolling(20, min_periods=2).std(ddof=1).to_numpy()
    g["vol_60"] = ret1d_s.rolling(60, min_periods=2).std(ddof=1).to_numpy()

    # --- Group 6: Gap & shape ---
    prev_close_arr = np.roll(price, 1)
    prev_close_arr[0] = np.nan  # first row has no previous
    g["prev_close"] = prev_close_arr

    # gap_1d: NaN + flag if prev_close <= 0
    gap_arr = np.where(
        (prev_close_arr > 0) & ~np.isnan(prev_close_arr),
        (open_arr - prev_close_arr) / prev_close_arr,
        np.nan,
    )
    for i in range(n):
        pc = prev_close_arr[i]
        if not np.isnan(pc) and pc <= 0:
            flag_lists[i].append("nonpositive_prev_close")

    rng_arr = high_arr - low_arr
    cth_arr = np.where(rng_arr > 0, (close_arr - high_arr) / rng_arr, np.nan)
    cpr_arr = np.where(rng_arr > 0, (close_arr - low_arr) / rng_arr, np.nan)

    g["gap_1d"] = gap_arr
    g["range"] = rng_arr
    g["close_to_high_1d"] = cth_arr
    g["close_pos_in_range_1d"] = cpr_arr

    # --- Group 7: Trend (aliases) ---
    g["trend_20d"] = g["ret_20d"].to_numpy()
    g["trend_60d"] = g["ret_60d"].to_numpy()

    # --- Group 8: Up streak (3 consecutive days including today) ---
    prev1 = np.roll(price, 1); prev1[0] = np.nan
    prev2 = np.roll(price, 2); prev2[:2] = np.nan
    prev3 = np.roll(price, 3); prev3[:3] = np.nan

    streak_arr = np.where(
        np.isnan(price) | np.isnan(prev1) | np.isnan(prev2) | np.isnan(prev3),
        np.nan,
        ((price > prev1) & (prev1 > prev2) & (prev2 > prev3)).astype(float),
    )
    g["up_streak_3"] = streak_arr

    g["_flags"] = flag_lists
    return g


# ---------------------------------------------------------------------------
# Earnings features (per-ticker, requires events)
# ---------------------------------------------------------------------------

def _build_earnings_features(
    g: pd.DataFrame,
    events_ticker: pd.DataFrame,
    price_col: str,
) -> pd.DataFrame:
    """Add earnings_react_1d and earnings_drift_5d to g (single ticker)."""
    g = g.copy()
    react = np.full(len(g), np.nan)
    drift = np.full(len(g), np.nan)

    earnings = events_ticker[
        events_ticker["event_type"].str.lower() == "earnings"
    ]["date"].unique()

    if len(earnings) > 0:
        dates = list(g["as_of"])
        date_to_idx = {d: i for i, d in enumerate(dates)}
        price_arr = g[price_col].to_numpy(dtype=float, na_value=np.nan)
        ret1d_arr = g["ret_1d"].to_numpy(dtype=float, na_value=np.nan)

        for ev_date in earnings:
            if ev_date not in date_to_idx:
                continue
            idx = date_to_idx[ev_date]
            react[idx] = ret1d_arr[idx]
            if idx + 5 < len(g):
                p0 = price_arr[idx]
                p5 = price_arr[idx + 5]
                if p0 > 0 and not np.isnan(p0) and not np.isnan(p5):
                    drift[idx] = (p5 - p0) / p0

    g["earnings_react_1d"] = react
    g["earnings_drift_5d"] = drift
    # Forward-fill react
    g["earnings_react_1d"] = g["earnings_react_1d"].ffill()
    return g


def _build_bullish_count(
    g: pd.DataFrame,
    events_ticker: pd.DataFrame,
) -> pd.DataFrame:
    """Rolling 60-day count of bullish events per ticker."""
    g = g.copy()
    bullish_dates = events_ticker[
        events_ticker["event_type"].str.lower() == "bullish"
    ]["date"].tolist()

    if not bullish_dates:
        g["event_bullish_count_60d"] = 0
        return g

    bullish_ts = [pd.Timestamp(d) for d in bullish_dates]
    counts = []
    for d in g["as_of"]:
        ts = pd.Timestamp(d)
        window_start = ts - pd.Timedelta(days=60)
        cnt = sum(1 for ev in bullish_ts if window_start <= ev <= ts)
        counts.append(cnt)
    g["event_bullish_count_60d"] = counts
    return g


# ---------------------------------------------------------------------------
# Cross-sectional zscore on an arbitrary aligned series
# ---------------------------------------------------------------------------

def _apply_cs_zscore_series(df: pd.DataFrame, raw: pd.Series) -> pd.Series:
    """Apply cross-sectional zscore to a series aligned with df index."""
    tmp_col = "__tmp_ez__"
    df2 = df[["as_of"]].copy()
    df2[tmp_col] = raw.values
    return df2.groupby("as_of")[tmp_col].transform(_cs_zscore)


# ---------------------------------------------------------------------------
# Main build function
# ---------------------------------------------------------------------------

def build_features(
    bars: pd.DataFrame,
    events: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Build features_daily from bars (and optionally events).

    Returns a DataFrame with (as_of, ticker) as logical key,
    all required feature columns, and quality_flags (JSON string).
    """
    # Determine price column
    price_col = "adj_close" if "adj_close" in bars.columns else "close"

    bars = bars.sort_values(["ticker", "as_of"]).reset_index(drop=True)

    ticker_dfs: list[pd.DataFrame] = []

    for ticker, g in bars.groupby("ticker", sort=True):
        g = g.sort_values("as_of").reset_index(drop=True)
        g = _build_ticker_features(g, price_col)

        if events is not None:
            ev_t = events[events["ticker"] == ticker]
            g = _build_earnings_features(g, ev_t, price_col)
            g = _build_bullish_count(g, ev_t)
            has_earnings = (ev_t["event_type"].str.lower() == "earnings").any()
            if not has_earnings:
                for fl in g["_flags"]:
                    fl.append("no_events")
        else:
            g["earnings_react_1d"] = np.nan
            g["earnings_drift_5d"] = np.nan
            g["event_bullish_count_60d"] = 0
            for fl in g["_flags"]:
                fl.append("no_events")

        ticker_dfs.append(g)

    df = pd.concat(ticker_dfs, ignore_index=True)

    # --- Cross-sectional features (per as_of) ---
    df["liq_score"] = df.groupby("as_of")["avg_traded_value_20d"].transform(_cs_rank)
    df["vol_z_20d"] = df.groupby("as_of")["vol_20"].transform(_cs_zscore)
    df["vol_z_60d"] = df.groupby("as_of")["vol_60"].transform(_cs_zscore)

    # Market returns & relative strength
    df["market_ret_20d"] = df.groupby("as_of")["ret_20d"].transform("mean")
    df["market_ret_60d"] = df.groupby("as_of")["ret_60d"].transform("mean")
    df["rs_20d"] = df["ret_20d"] - df["market_ret_20d"]
    df["rs_60d"] = df["ret_60d"] - df["market_ret_60d"]

    # Market vol & regime
    df["_market_vol"] = df.groupby("as_of")["vol_20"].transform("median")
    global_median_vol = df.groupby("as_of")["_market_vol"].first().median()

    mret = df["market_ret_20d"].to_numpy(dtype=float, na_value=np.nan)
    mvol = df["_market_vol"].to_numpy(dtype=float, na_value=np.nan)
    regime = np.where(
        (~np.isnan(mret)) & (~np.isnan(mvol)) & (mret >= 0) & (mvol <= global_median_vol),
        "risk_on",
        "risk_off",
    )
    df["market_regime"] = regime

    # Earnings quality z-score
    raw_eq = 0.6 * df["earnings_react_1d"] + 0.4 * df["earnings_drift_5d"]
    df["earnings_quality_z"] = _apply_cs_zscore_series(df, raw_eq)

    # --- Dummies ---
    df["data_stale_flag"] = 0
    df["op_margin_yoy"] = np.nan
    df["guidance_up_flag"] = 0

    # --- Finalise quality_flags ---
    df["quality_flags"] = df["_flags"].apply(_flags_json)

    # --- Select and order output columns ---
    output_cols = [
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

    df = df.drop(columns=[c for c in ["_flags", "_market_vol"] if c in df.columns])
    return df[output_cols]
