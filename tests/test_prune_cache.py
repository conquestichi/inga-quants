"""Test prune_minute_cache: removes files older than N business days."""
from datetime import date, datetime
from pathlib import Path

import pytest

from inga_quant.utils.cache import prune_minute_cache


def _make_cache_file(cache_dir: Path, ticker: str, file_date: date) -> Path:
    """Create a dummy parquet-named cache file."""
    ticker_dir = cache_dir / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    path = ticker_dir / f"{file_date.strftime('%Y%m%d')}.parquet"
    path.write_bytes(b"dummy")
    return path


class TestPruneMinuteCache:
    def test_deletes_old_files(self, tmp_path):
        cache_dir = tmp_path / "cache"
        # reference = 2026-02-10, keep_days=5
        # 5 business days before 2026-02-10:
        #   2026-02-09 (Mon), -08 (missing: 2026-02-05 Fri), ...
        #   Actually: 2026-02-10 back 5 biz days = 2026-02-03 (Tue)
        reference = date(2026, 2, 10)

        old_file = _make_cache_file(cache_dir, "AAA", date(2026, 1, 5))   # old (30 days back)
        recent_file = _make_cache_file(cache_dir, "AAA", date(2026, 2, 9))  # recent

        deleted = prune_minute_cache(cache_dir, keep_days=5, reference_date=reference)

        assert old_file in deleted
        assert recent_file not in deleted
        assert not old_file.exists()
        assert recent_file.exists()

    def test_empty_cache_dir_returns_empty(self, tmp_path):
        cache_dir = tmp_path / "nonexistent"
        result = prune_minute_cache(cache_dir, keep_days=5, reference_date=date(2026, 2, 10))
        assert result == []

    def test_all_recent_files_kept(self, tmp_path):
        cache_dir = tmp_path / "cache"
        reference = date(2026, 2, 10)
        files = [
            _make_cache_file(cache_dir, "AAA", date(2026, 2, 9)),
            _make_cache_file(cache_dir, "BBB", date(2026, 2, 9)),
            _make_cache_file(cache_dir, "AAA", date(2026, 2, 10)),
        ]
        deleted = prune_minute_cache(cache_dir, keep_days=5, reference_date=reference)
        assert deleted == []
        for f in files:
            assert f.exists()

    def test_multiple_tickers(self, tmp_path):
        cache_dir = tmp_path / "cache"
        reference = date(2026, 2, 10)
        old1 = _make_cache_file(cache_dir, "AAA", date(2025, 12, 1))
        old2 = _make_cache_file(cache_dir, "BBB", date(2025, 12, 1))
        new1 = _make_cache_file(cache_dir, "AAA", date(2026, 2, 9))

        deleted = prune_minute_cache(cache_dir, keep_days=5, reference_date=reference)
        assert old1 in deleted
        assert old2 in deleted
        assert new1 not in deleted

    def test_non_date_files_ignored(self, tmp_path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        weird = cache_dir / "not_a_date.parquet"
        weird.write_bytes(b"x")
        deleted = prune_minute_cache(cache_dir, keep_days=5, reference_date=date(2026, 2, 10))
        assert weird not in deleted
        assert weird.exists()
