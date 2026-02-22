# SPEC.md — Feature Store v1 (Phase 1)

This document is the authoritative contract for the inga-quant Feature Store MVP.
Tests and code must conform to this spec, not the other way around.

---

## 1. Data Contract (Input)

### 1.1 bars_daily (required)

| Column       | Type    | Required | Notes                                      |
|--------------|---------|----------|--------------------------------------------|
| as_of        | date    | ✓        | ISO-8601 date, e.g. 2026-02-10             |
| ticker       | str     | ✓        | Security identifier                        |
| open         | float   | ✓        |                                            |
| high         | float   | ✓        |                                            |
| low          | float   | ✓        |                                            |
| close        | float   | ✓        |                                            |
| volume       | float   | ✓        |                                            |
| adj_close    | float   | —        | If present, used as price_col over close   |
| adj_factor   | float   | —        |                                            |
| is_suspended | bool    | —        | Adds `suspended` to quality_flags if true  |
| src          | str     | —        | Ignored                                    |
| revised_at   | datetime| —        | Ignored                                    |

- Format: Parquet or CSV (detected by extension).
- Rows are filtered to `as_of <= --as-of` before computation; the full history up to --as-of is used for rolling windows.
- `price_col = adj_close if adj_close column exists else close`

### 1.2 events (optional)

| Column       | Type | Required | Notes                          |
|--------------|------|----------|--------------------------------|
| date         | date | ✓        |                                |
| ticker       | str  | ✓        |                                |
| event_type   | str  | ✓        | e.g. "earnings", "bullish"     |
| payload_json | str  | —        | JSON string, ignored in MVP    |

If absent, earnings_* columns are NaN and `no_events` is noted in quality_flags per ticker.

---

## 2. Eval Contract (Feature Definitions)

All rolling windows are computed per-ticker, sorted ascending by as_of.
All cross-sectional operations (rank, zscore) are computed per as_of date across eligible tickers.

### price_col
`adj_close` if column present, else `close`.

### Required Columns (Phase 1)

#### Group 1 — Liquidity
- `avg_traded_value_20d`: rolling 20-day mean of `close * volume`
- `liq_score`: cross-sectional percentile rank of `avg_traded_value_20d` per day (0..1, method='average')

#### Group 2 — Returns
- `ret_1d, ret_3d, ret_5d, ret_20d, ret_60d`: `price_col.pct_change(N)` per ticker
- `absret_1d = abs(ret_1d)`

#### Group 3 — High-Water Mark
- `hh_20d`: rolling max 20 of `price_col.shift(1)` — excludes current day

#### Group 4 — Volume Z-score
- `volume_z_20d = (volume - mean20(volume)) / std20(volume)`
  - If std == 0: value = 0, flag `volume_std_zero`

#### Group 5 — Volatility (cross-sectional)
- `vol_20 = rolling std of ret_1d (window=20)`
- `vol_60 = rolling std of ret_1d (window=60)`
- `vol_z_20d = cross-sectional zscore(vol_20) per day`
- `vol_z_60d = cross-sectional zscore(vol_60) per day`
  - zscore std==0 → 0; flag `vol_cs_std_zero` if it occurs

#### Group 6 — Gap & Shape
- `prev_close = price_col.shift(1)`
- `gap_1d = (open - prev_close) / prev_close` — NaN + flag `nonpositive_prev_close` if prev_close <= 0
- `range = high - low`
- `close_to_high_1d = (close - high) / range` — NaN + flag `zero_range` if range == 0
- `close_pos_in_range_1d = (close - low) / range` — NaN if range == 0

#### Group 7 — Trend (aliases)
- `trend_20d = ret_20d`
- `trend_60d = ret_60d`

#### Group 8 — Consecutive Up
- `up_streak_3 = 1` if price_col > price_col.shift(1) for each of the last 3 days (including today), else 0
  - NaN when history < 3

#### Group 9 — Relative Strength
- `market_ret_20d`: mean of `ret_20d` across all eligible tickers per as_of
- `market_ret_60d`: mean of `ret_60d` across all eligible tickers per as_of
- `rs_20d = ret_20d - market_ret_20d`
- `rs_60d = ret_60d - market_ret_60d`

#### Group 10 — Earnings (events required)
- `earnings_react_1d`: on earnings event date, value = `ret_1d`; forward-filled to subsequent dates
- `earnings_drift_5d`: 5-day cumulative return starting from day after earnings event; NaN if not computable
- `earnings_quality_z`: raw = `0.6 * earnings_react_1d + 0.4 * earnings_drift_5d`; cross-sectional zscore per day
- All NaN if no events provided.

#### Group 11 — Regime & Stale
- `data_stale_flag = 0` (fixed)
- `market_regime`: `'risk_on'` if `market_ret_20d >= 0 AND market_vol <= median(market_vol series)`, else `'risk_off'`
  - `market_vol = median(vol_20) across tickers per day`

#### Group 12 — Mid-term Dummies
- `op_margin_yoy`: NaN (placeholder)
- `guidance_up_flag`: 0 (placeholder)
- `event_bullish_count_60d`: rolling 60-day count of `event_type == 'bullish'` per ticker; 0 if no events

---

## 3. Output Contract

### features_daily.parquet

| Column         | Type   | Notes                                      |
|----------------|--------|--------------------------------------------|
| as_of          | date   | Key                                        |
| ticker         | str    | Key                                        |
| quality_flags  | str    | JSON array string, e.g. `["missing_price"]`|
| ... all feature columns above ...           |        |                                            |

- Written with `pyarrow`. Index is NOT saved (index=False).
- Path: `<--out>/features_daily.parquet`
- Rows: one per (ticker, as_of) pair where as_of <= --as-of argument.
- Missing values: NaN for floats, null for strings — never silently zero-filled unless spec says "0 fixed".
- `quality_flags` is a JSON-array string per row, containing zero or more flag names from the defined vocabulary.

### quality_flags vocabulary

| Flag                  | Trigger                                           |
|-----------------------|---------------------------------------------------|
| missing_price         | price_col is NaN                                  |
| missing_volume        | volume is NaN                                     |
| missing_ohlc          | any of open/high/low/close is NaN                 |
| zero_range            | high - low == 0                                   |
| nonpositive_prev_close| prev_close <= 0                                   |
| suspended             | is_suspended == True                              |
| insufficient_history_20| fewer than 20 prior rows for this ticker          |
| insufficient_history_60| fewer than 20 prior rows but <60 for this ticker  |
| volume_std_zero       | std of volume over 20-day window is 0             |
| no_events             | events file not provided (earnings features = NaN)|

---

## 4. CLI Contract

```
python -m inga_quant.cli build-features \
    --as-of YYYY-MM-DD \
    --bars <path>      \
    [--events <path>]  \
    --out <dir>
```

- `--as-of`: cutoff date (inclusive). Only rows with `as_of <= cutoff` are used.
- `--bars`: path to bars_daily CSV or Parquet.
- `--events`: optional path to events CSV or Parquet.
- `--out`: output directory. Created if not exists. Output file: `<dir>/features_daily.parquet`.
- Exit code 0 on success, non-zero on error.

---

## 5. Reproducibility

- Given identical input files and `--as-of`, output is deterministic.
- No external data fetches in Phase 1.
- No random seeds needed (no randomness used).

---

## Phase 2+ (not implemented — see docs/spec_future.md)
- Scheduled daily pipeline, trade_date calculation via J-Quants calendar
- Watchlist, decision cards, Slack integration
- Quality gates (WF/overfitting/cost/stability/leakage)
