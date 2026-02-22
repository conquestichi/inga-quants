"""Minute-bar cache management: prune files older than N business days."""
from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import jpholiday

logger = logging.getLogger(__name__)


def _is_business_day(d: date) -> bool:
    return d.weekday() < 5 and not jpholiday.is_holiday(d)


def _business_days_before(reference: date, n: int) -> date:
    """Return the date that is exactly n business days before reference."""
    count = 0
    current = reference
    while count < n:
        current = date.fromordinal(current.toordinal() - 1)
        if _is_business_day(current):
            count += 1
    return current


def prune_minute_cache(
    cache_dir: str | Path,
    keep_days: int,
    reference_date: date | None = None,
) -> list[Path]:
    """
    Delete minute-bar cache files older than `keep_days` business days.

    Cache files are expected to be named <ticker>/<YYYYMMDD>.parquet
    (or any file where the parent directory is the ticker and the stem is YYYYMMDD).

    Returns list of deleted file paths.
    """
    cache_dir = Path(cache_dir)
    if not cache_dir.exists():
        return []

    if reference_date is None:
        reference_date = date.today()

    cutoff = _business_days_before(reference_date, keep_days)
    deleted: list[Path] = []

    for f in cache_dir.rglob("*.parquet"):
        stem = f.stem
        try:
            file_date = datetime.strptime(stem, "%Y%m%d").date()
        except ValueError:
            continue  # skip files not matching YYYYMMDD pattern
        if file_date < cutoff:
            logger.info("Pruning cache file: %s (date=%s < cutoff=%s)", f, file_date, cutoff)
            f.unlink()
            deleted.append(f)

    return deleted
