"""Tests for run-lock helpers: stale recovery, active detection, --force."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path

import pytest

from inga_quant.cli import _acquire_run_lock, _pid_exists, _read_lock_info


# ---------------------------------------------------------------------------
# _read_lock_info
# ---------------------------------------------------------------------------

class TestReadLockInfo:
    def test_valid_json(self, tmp_path):
        lock = tmp_path / "run.lock"
        lock.write_text(json.dumps({"pid": 12345, "started_at": "2026-01-01T00:00:00+09:00"}))
        info = _read_lock_info(lock)
        assert info is not None
        assert info["pid"] == 12345
        assert info["started_at"] == "2026-01-01T00:00:00+09:00"

    def test_missing_file_returns_none(self, tmp_path):
        assert _read_lock_info(tmp_path / "nonexistent.lock") is None

    def test_corrupt_content_returns_none(self, tmp_path):
        lock = tmp_path / "run.lock"
        lock.write_bytes(b"not valid json \xff\xfe")
        assert _read_lock_info(lock) is None

    def test_empty_file_returns_none(self, tmp_path):
        lock = tmp_path / "run.lock"
        lock.write_text("")
        assert _read_lock_info(lock) is None


# ---------------------------------------------------------------------------
# _pid_exists
# ---------------------------------------------------------------------------

class TestPidExists:
    def test_own_pid_exists(self):
        assert _pid_exists(os.getpid()) is True

    def test_absurd_pid_does_not_exist(self):
        # PID 9_999_999 is above the Linux default max (4_194_304)
        assert _pid_exists(9_999_999) is False


# ---------------------------------------------------------------------------
# _acquire_run_lock: clean / stale / active
# ---------------------------------------------------------------------------

class TestAcquireRunLock:
    def _make_lock(self, tmp_path: Path, pid: int, started: str = "2026-01-01T00:00:00+09:00") -> Path:
        lock_path = tmp_path / "run.lock"
        lock_path.write_text(json.dumps({"pid": pid, "started_at": started}))
        return lock_path

    # -- clean acquisition ---------------------------------------------------

    def test_clean_acquire_writes_pid(self, tmp_path):
        """No existing lock → flock acquired and PID written to file."""
        lock_path = tmp_path / "run.lock"
        fh = _acquire_run_lock(lock_path)
        try:
            assert fh is not None
            info = json.loads(lock_path.read_text())
            assert info["pid"] == os.getpid()
            assert "started_at" in info
        finally:
            if fh:
                fcntl.flock(fh, fcntl.LOCK_UN)
                fh.close()

    # -- stale lock (dead PID) -----------------------------------------------

    def test_stale_lock_auto_recovery(self, tmp_path, monkeypatch, capsys):
        """Stale lock (non-existent PID) → auto-delete → lock acquired on retry."""
        lock_path = self._make_lock(tmp_path, pid=9_999_999)

        # Simulate: first flock attempt fails (as if a dead process held it),
        # second attempt (after stale-delete) passes through to the real flock.
        real_flock = fcntl.flock
        call_count = {"n": 0}

        def mock_flock(fh, op):
            if op == (fcntl.LOCK_EX | fcntl.LOCK_NB):
                call_count["n"] += 1
                if call_count["n"] == 1:
                    raise BlockingIOError("simulated: lock held by dead process")
            return real_flock(fh, op)

        monkeypatch.setattr(fcntl, "flock", mock_flock)

        fh = _acquire_run_lock(lock_path)
        try:
            assert fh is not None, "Lock must be acquired after stale recovery"
            # Lock file should now contain our PID
            info = json.loads(lock_path.read_text())
            assert info["pid"] == os.getpid()
        finally:
            if fh:
                real_flock(fh, fcntl.LOCK_UN)
                fh.close()

        captured = capsys.readouterr()
        assert "stale" in captured.err.lower() or "自動復旧" in captured.err, (
            f"Expected stale-recovery message in stderr, got: {captured.err!r}"
        )

    # -- active lock (live PID) ----------------------------------------------

    def test_active_lock_returns_none(self, tmp_path, monkeypatch, capsys):
        """Active lock (our own PID, definitely alive) → returns None with PID in error."""
        live_pid = os.getpid()
        lock_path = self._make_lock(tmp_path, pid=live_pid)

        real_flock = fcntl.flock

        def always_fail(fh, op):
            if op == (fcntl.LOCK_EX | fcntl.LOCK_NB):
                raise BlockingIOError("simulated: lock held by live process")
            return real_flock(fh, op)

        monkeypatch.setattr(fcntl, "flock", always_fail)

        fh = _acquire_run_lock(lock_path)
        assert fh is None, "Should return None when a live process holds the lock"

        captured = capsys.readouterr()
        assert str(live_pid) in captured.err, (
            f"Live PID {live_pid} must appear in stderr error; got: {captured.err!r}"
        )

    def test_active_lock_mentions_force(self, tmp_path, monkeypatch, capsys):
        """Error message for active lock must hint at --force."""
        lock_path = self._make_lock(tmp_path, pid=os.getpid())

        real_flock = fcntl.flock
        monkeypatch.setattr(
            fcntl, "flock",
            lambda fh, op: (_ for _ in ()).throw(BlockingIOError()) if op == (fcntl.LOCK_EX | fcntl.LOCK_NB) else real_flock(fh, op)
        )

        _acquire_run_lock(lock_path)
        captured = capsys.readouterr()
        assert "--force" in captured.err, "Error message must mention --force"


# ---------------------------------------------------------------------------
# --force flag via _cmd_run (no pipeline — _run_pipeline_cmd mocked)
# ---------------------------------------------------------------------------

class TestForceFlag:
    def _args(self, force: bool = False, **kw) -> argparse.Namespace:
        defaults = dict(as_of="2026-02-10", demo=True, out=None, config=None, lang="ja")
        defaults.update(kw)
        return argparse.Namespace(force=force, **defaults)

    def test_force_clears_live_lock_and_runs(self, tmp_path, monkeypatch, capsys):
        """--force removes a live-PID lock file and allows run to proceed."""
        # Create lock file with our own (live) PID
        lock_path = tmp_path / "logs" / "run.lock"
        lock_path.parent.mkdir()
        lock_path.write_text(json.dumps({"pid": os.getpid(), "started_at": "2026-01-01T00:00:00+09:00"}))

        # Work in tmp_path so "logs/run.lock" resolves there
        monkeypatch.chdir(tmp_path)
        # Short-circuit the pipeline
        monkeypatch.setattr("inga_quant.cli._run_pipeline_cmd", lambda args, lock_fh: 0)

        from inga_quant.cli import _cmd_run
        rc = _cmd_run(self._args(force=True))

        assert rc == 0, "Run should succeed after --force clears the lock"
        captured = capsys.readouterr()
        assert "force" in captured.err.lower() or "強制" in captured.err, (
            f"Expected --force acknowledgment in stderr; got: {captured.err!r}"
        )

    def test_force_with_no_existing_lock_still_runs(self, tmp_path, monkeypatch):
        """--force when no lock exists is harmless — run proceeds normally."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("inga_quant.cli._run_pipeline_cmd", lambda args, lock_fh: 0)

        from inga_quant.cli import _cmd_run
        rc = _cmd_run(self._args(force=True))
        assert rc == 0

    def test_no_force_active_lock_exits_1(self, tmp_path, monkeypatch):
        """Without --force, an active lock causes exit 1."""
        lock_path = tmp_path / "logs" / "run.lock"
        lock_path.parent.mkdir()
        lock_path.write_text(json.dumps({"pid": os.getpid(), "started_at": "2026-01-01T00:00:00+09:00"}))

        monkeypatch.chdir(tmp_path)
        # Make flock always fail (simulate another process holding it)
        real_flock = fcntl.flock
        monkeypatch.setattr(
            fcntl, "flock",
            lambda fh, op: (_ for _ in ()).throw(BlockingIOError()) if op == (fcntl.LOCK_EX | fcntl.LOCK_NB) else real_flock(fh, op)
        )
        monkeypatch.setattr("inga_quant.cli._run_pipeline_cmd", lambda args, lock_fh: 0)

        from inga_quant.cli import _cmd_run
        rc = _cmd_run(self._args(force=False))
        assert rc == 1, "Should exit 1 when active lock present and --force not given"
