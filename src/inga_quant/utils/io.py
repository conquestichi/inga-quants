"""I/O utilities: load bars/events, schema validation, parquet save."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BARS_REQUIRED = {"as_of", "ticker", "open", "high", "low", "close", "volume"}
EVENTS_REQUIRED = {"date", "ticker", "event_type"}


def load_bars(path: str | Path, as_of: str | None = None) -> pd.DataFrame:
    """Load bars_daily from CSV or Parquet, parse dates, optionally filter."""
    path = Path(path)
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)

    missing = BARS_REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"bars missing required columns: {sorted(missing)}")

    df["as_of"] = pd.to_datetime(df["as_of"]).dt.date
    df["ticker"] = df["ticker"].astype(str)

    if as_of is not None:
        cutoff = pd.to_datetime(as_of).date()
        df = df[df["as_of"] <= cutoff].copy()

    return df


def load_events(path: str | Path) -> pd.DataFrame:
    """Load events from CSV or Parquet."""
    path = Path(path)
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)

    missing = EVENTS_REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"events missing required columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["ticker"] = df["ticker"].astype(str)
    return df


def save_parquet(df: pd.DataFrame, path: str | Path) -> None:
    """Save DataFrame as Parquet without index using pyarrow."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path)
