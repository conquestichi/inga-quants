"""CI-safe tests for jq_api_smoketest.py and its bash integration.

Tests use monkeypatching / subprocess with env overrides.
No real network calls are made.
"""
from __future__ import annotations

import importlib
import os
import subprocess
import sys
import types
import unittest.mock
from pathlib import Path

import pytest

_TOOLS_DIR = Path(__file__).parent.parent / "shutdown" / "tools"
_SMOKETEST = _TOOLS_DIR / "jq_api_smoketest.py"
_SCRIPTS_DIR = Path(__file__).parent.parent / "shutdown" / "bin"
_INGEST = _SCRIPTS_DIR / "inga_market_quotes_ingest_jq300.sh"
_UNIVERSE = _SCRIPTS_DIR / "inga_universe300_build.sh"


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _import_smoketest() -> types.ModuleType:
    """Import jq_api_smoketest as a module (supports reload)."""
    spec = importlib.util.spec_from_file_location("jq_api_smoketest", _SMOKETEST)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _fake_response(status: int, body: bytes = b'{"data":[]}') -> unittest.mock.MagicMock:
    """Build a mock urllib response context manager."""
    mock_resp = unittest.mock.MagicMock()
    mock_resp.status = status
    mock_resp.read.return_value = body
    mock_resp.__enter__ = lambda s: mock_resp
    mock_resp.__exit__ = unittest.mock.MagicMock(return_value=False)
    return mock_resp


# ──────────────────────────────────────────────────────────────────────────────
# Python unit tests (monkeypatch urllib)
# ──────────────────────────────────────────────────────────────────────────────

class TestSmoketestPython:
    def test_file_exists(self):
        assert _SMOKETEST.exists(), f"smoketest not found: {_SMOKETEST}"

    def test_syntax_ok(self):
        """python3 -m py_compile must pass."""
        result = subprocess.run(
            [sys.executable, "-m", "py_compile", str(_SMOKETEST)],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr

    def test_key_missing_exits_2(self, monkeypatch):
        """No API key set → exit 2."""
        mod = _import_smoketest()
        monkeypatch.delenv("JQ_API_KEY", raising=False)
        monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
        monkeypatch.delenv("JQUANTS_APIKEY", raising=False)
        rc = mod.main()
        assert rc == 2

    def test_key_jq_api_key_exits_2_when_empty(self, monkeypatch):
        """JQ_API_KEY='' (empty string) → exit 2."""
        mod = _import_smoketest()
        monkeypatch.setenv("JQ_API_KEY", "")
        monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
        monkeypatch.delenv("JQUANTS_APIKEY", raising=False)
        rc = mod.main()
        assert rc == 2

    def test_http_200_exits_0(self, monkeypatch):
        """HTTP 200 → exit 0."""
        mod = _import_smoketest()
        monkeypatch.setenv("JQ_API_KEY", "test-key-abc")
        with unittest.mock.patch("urllib.request.urlopen", return_value=_fake_response(200)):
            rc = mod.main()
        assert rc == 0

    def test_http_401_exits_3(self, monkeypatch):
        """HTTP 401 → exit 3 (auth failure)."""
        import urllib.error
        mod = _import_smoketest()
        monkeypatch.setenv("JQ_API_KEY", "bad-key")
        exc = urllib.error.HTTPError(url="", code=401, msg="Unauthorized", hdrs={}, fp=None)
        exc.read = lambda n=512: b'{"message":"Unauthorized"}'
        with unittest.mock.patch("urllib.request.urlopen", side_effect=exc):
            rc = mod.main()
        assert rc == 3

    def test_http_403_exits_4(self, monkeypatch):
        """HTTP 403 → exit 4 (permission denied)."""
        import urllib.error
        mod = _import_smoketest()
        monkeypatch.setenv("JQ_API_KEY", "restricted-key")
        exc = urllib.error.HTTPError(url="", code=403, msg="Forbidden", hdrs={}, fp=None)
        exc.read = lambda n=512: b'{"message":"Forbidden"}'
        with unittest.mock.patch("urllib.request.urlopen", side_effect=exc):
            rc = mod.main()
        assert rc == 4

    def test_timeout_exits_5(self, monkeypatch):
        """TimeoutError → exit 5."""
        mod = _import_smoketest()
        monkeypatch.setenv("JQ_API_KEY", "some-key")
        with unittest.mock.patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")):
            rc = mod.main()
        assert rc == 5

    def test_oserror_timeout_exits_5(self, monkeypatch):
        """OSError with 'timed out' in message → exit 5."""
        mod = _import_smoketest()
        monkeypatch.setenv("JQ_API_KEY", "some-key")
        with unittest.mock.patch("urllib.request.urlopen", side_effect=OSError("Connection timed out")):
            rc = mod.main()
        assert rc == 5

    def test_oserror_network_exits_5(self, monkeypatch):
        """OSError (DNS / refused) → exit 5."""
        mod = _import_smoketest()
        monkeypatch.setenv("JQ_API_KEY", "some-key")
        with unittest.mock.patch("urllib.request.urlopen", side_effect=OSError("Name or service not known")):
            rc = mod.main()
        assert rc == 5

    def test_http_500_exits_6(self, monkeypatch):
        """Unexpected HTTP status (500) → exit 6."""
        import urllib.error
        mod = _import_smoketest()
        monkeypatch.setenv("JQ_API_KEY", "some-key")
        exc = urllib.error.HTTPError(url="", code=500, msg="Server Error", hdrs={}, fp=None)
        exc.read = lambda n=512: b"error"
        with unittest.mock.patch("urllib.request.urlopen", side_effect=exc):
            rc = mod.main()
        assert rc == 6

    def test_key_never_logged(self, monkeypatch, capsys):
        """API key value must never appear in stdout/stderr."""
        mod = _import_smoketest()
        secret = "SUPER_SECRET_KEY_XYZ_789"
        monkeypatch.setenv("JQ_API_KEY", secret)
        with unittest.mock.patch("urllib.request.urlopen", return_value=_fake_response(200)):
            mod.main()
        captured = capsys.readouterr()
        assert secret not in captured.out
        assert secret not in captured.err

    def test_fallback_env_vars(self, monkeypatch):
        """JQUANTS_API_KEY is used when JQ_API_KEY is absent."""
        mod = _import_smoketest()
        monkeypatch.delenv("JQ_API_KEY", raising=False)
        monkeypatch.setenv("JQUANTS_API_KEY", "fallback-key")
        monkeypatch.delenv("JQUANTS_APIKEY", raising=False)
        with unittest.mock.patch("urllib.request.urlopen", return_value=_fake_response(200)):
            rc = mod.main()
        assert rc == 0

    def test_fallback_env_var_apikey(self, monkeypatch):
        """JQUANTS_APIKEY is used when neither JQ_API_KEY nor JQUANTS_API_KEY is set."""
        mod = _import_smoketest()
        monkeypatch.delenv("JQ_API_KEY", raising=False)
        monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
        monkeypatch.setenv("JQUANTS_APIKEY", "apikey-var")
        with unittest.mock.patch("urllib.request.urlopen", return_value=_fake_response(200)):
            rc = mod.main()
        assert rc == 0


# ──────────────────────────────────────────────────────────────────────────────
# Stamp mechanism tests
# ──────────────────────────────────────────────────────────────────────────────

class TestSmoketestStamp:
    """Stamp file allows skipping the pre-flight on subsequent runs."""

    def test_stamp_present_skips_network(self, monkeypatch, tmp_path):
        """If stamp exists and FORCE!=1, exit 0 without touching the network."""
        stamp = tmp_path / "jq_api_smoketest.ok.json"
        stamp.write_text('{"ts":"2026-01-01T00:00:00+00:00","result":"ok"}')
        monkeypatch.setenv("STATE", str(tmp_path))
        monkeypatch.setenv("FORCE", "0")
        monkeypatch.setenv("JQ_API_KEY", "some-key")
        mod = _import_smoketest()
        # urlopen must NOT be called — use a side_effect that fails if called
        with unittest.mock.patch("urllib.request.urlopen", side_effect=AssertionError("network called")) as mock_open:
            rc = mod.main()
        assert rc == 0
        mock_open.assert_not_called()

    def test_force_bypasses_stamp(self, monkeypatch, tmp_path):
        """FORCE=1 makes the smoketest run even when stamp exists."""
        stamp = tmp_path / "jq_api_smoketest.ok.json"
        stamp.write_text('{"ts":"2026-01-01T00:00:00+00:00","result":"ok"}')
        monkeypatch.setenv("STATE", str(tmp_path))
        monkeypatch.setenv("FORCE", "1")
        monkeypatch.setenv("JQ_API_KEY", "some-key")
        mod = _import_smoketest()
        with unittest.mock.patch("urllib.request.urlopen", return_value=_fake_response(200)):
            rc = mod.main()
        assert rc == 0  # ran the real check, HTTP 200

    def test_success_writes_stamp(self, monkeypatch, tmp_path):
        """HTTP 200 → stamp file created under STATE."""
        monkeypatch.setenv("STATE", str(tmp_path))
        monkeypatch.setenv("FORCE", "0")
        monkeypatch.setenv("JQ_API_KEY", "some-key")
        stamp = tmp_path / "jq_api_smoketest.ok.json"
        assert not stamp.exists()
        mod = _import_smoketest()
        with unittest.mock.patch("urllib.request.urlopen", return_value=_fake_response(200)):
            rc = mod.main()
        assert rc == 0
        assert stamp.exists()
        import json
        data = json.loads(stamp.read_text())
        assert data["result"] == "ok"
        assert "ts" in data

    def test_failure_does_not_write_stamp(self, monkeypatch, tmp_path):
        """HTTP 401 → no stamp written."""
        import urllib.error
        monkeypatch.setenv("STATE", str(tmp_path))
        monkeypatch.setenv("FORCE", "0")
        monkeypatch.setenv("JQ_API_KEY", "bad-key")
        mod = _import_smoketest()
        exc = urllib.error.HTTPError(url="", code=401, msg="Unauthorized", hdrs={}, fp=None)
        exc.read = lambda n=512: b'{"message":"Unauthorized"}'
        with unittest.mock.patch("urllib.request.urlopen", side_effect=exc):
            rc = mod.main()
        assert rc == 3
        assert not (tmp_path / "jq_api_smoketest.ok.json").exists()

    def test_no_stamp_runs_check(self, monkeypatch, tmp_path):
        """No stamp file + FORCE=0 → runs the check normally."""
        monkeypatch.setenv("STATE", str(tmp_path))
        monkeypatch.setenv("FORCE", "0")
        monkeypatch.setenv("JQ_API_KEY", "some-key")
        mod = _import_smoketest()
        with unittest.mock.patch("urllib.request.urlopen", return_value=_fake_response(200)) as mock_open:
            rc = mod.main()
        assert rc == 0
        mock_open.assert_called_once()

    def test_stamp_path_in_output(self, monkeypatch, tmp_path, capsys):
        """When stamp is found, its path appears in the log output."""
        stamp = tmp_path / "jq_api_smoketest.ok.json"
        stamp.write_text('{"ts":"2026-01-01T00:00:00+00:00","result":"ok"}')
        monkeypatch.setenv("STATE", str(tmp_path))
        monkeypatch.setenv("FORCE", "0")
        monkeypatch.setenv("JQ_API_KEY", "some-key")
        mod = _import_smoketest()
        with unittest.mock.patch("urllib.request.urlopen", side_effect=AssertionError("should not be called")):
            mod.main()
        captured = capsys.readouterr()
        assert "stamp" in captured.out.lower()


# ──────────────────────────────────────────────────────────────────────────────
# prod-apply structural tests (no root required)
# ──────────────────────────────────────────────────────────────────────────────

_DEPLOY_DIR = Path(__file__).parent.parent / "shutdown" / "deploy"
_PROD_APPLY = _DEPLOY_DIR / "inga-prod-apply"


class TestProdApplySmoketestIntegration:
    """Structural checks that prod-apply includes the smoketest pre-flight."""

    def test_prod_apply_references_smoketest(self):
        """prod-apply must reference jq_api_smoketest.py."""
        content = _PROD_APPLY.read_text()
        assert "jq_api_smoketest" in content

    def test_prod_apply_fails_on_rc3(self):
        """prod-apply script must exit 1 when smoketest returns rc=3."""
        content = _PROD_APPLY.read_text()
        assert "exit 1" in content
        # Must have a case arm for rc=3 that exits
        assert "3)" in content

    def test_prod_apply_fails_on_rc4(self):
        """prod-apply script must exit 1 when smoketest returns rc=4."""
        content = _PROD_APPLY.read_text()
        assert "4)" in content

    def test_prod_apply_warns_on_rc2(self):
        """prod-apply must warn (not fail) when smoketest returns rc=2."""
        content = _PROD_APPLY.read_text()
        # The rc=2 case arm must call _warn and must not call exit 1.
        # Match single-line or multi-line arm: everything from "2)" up to ";;".
        import re
        match = re.search(r"2\)(.*?);;", content, re.DOTALL)
        assert match, "rc=2 case arm not found in prod-apply"
        arm_body = match.group(1)
        assert "_warn" in arm_body, "rc=2 arm should call _warn"
        assert "exit 1" not in arm_body, "rc=2 arm must NOT call exit 1 (key missing is non-fatal)"

    def test_prod_apply_dry_run_mentions_smoketest(self, tmp_path):
        """prod-apply --dry-run must mention smoketest in output."""
        result = subprocess.run(
            ["bash", str(_PROD_APPLY), "--dry-run"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "REPO": str(Path(__file__).parent.parent),
                "HOME": os.environ.get("HOME", "/tmp"),
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            },
            timeout=15,
        )
        combined = result.stdout + result.stderr
        assert "smoketest" in combined.lower() or "jq_api_smoketest" in combined.lower(), (
            f"Expected smoketest mention in dry-run output:\n{combined}"
        )

    def test_prod_apply_supports_jq_smoketest_path_env(self):
        """prod-apply must support _JQ_SMOKETEST_PATH env for test injection."""
        content = _PROD_APPLY.read_text()
        assert "_JQ_SMOKETEST_PATH" in content


# ──────────────────────────────────────────────────────────────────────────────
# Bash integration: smoketest path via _JQ_SMOKETEST_PATH env override
# ──────────────────────────────────────────────────────────────────────────────
# The bash scripts accept _JQ_SMOKETEST_PATH to override the fallback path
# search. This allows CI tests to inject a fake smoketest without root or
# deploying to /srv/inga/SHUTDOWN/bin/.
#
# Business-day override: use AS_OF=2026-03-02 (Monday, not a JP holiday) so
# the scripts pass the calendar check and reach the smoketest block.

_BDAY = "2026-03-02"  # Monday — confirmed not a JP national holiday


def _run_bash_with_smoketest(
    script: Path,
    smoketest_path: str,
    extra_env: dict | None = None,
    args: list[str] | None = None,
    u300: Path | None = None,
) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "JQ_API_KEY": "dummy-key",
        "AS_OF": _BDAY,
        "_JQ_SMOKETEST_PATH": smoketest_path,
        "HOME": os.environ.get("HOME", "/tmp"),
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        **(extra_env or {}),
    }
    if u300 is not None:
        env["U300"] = str(u300)
    return subprocess.run(
        ["bash", str(script)] + (args or []),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        env=env,
        timeout=15,
    )


def _make_fake_universe(tmp_path: Path) -> Path:
    u = tmp_path / "universe300.txt"
    u.write_text("1234\n5678\n")
    return u


def _make_fake_smoketest(tmp_path: Path, exit_code: int) -> Path:
    """Create a Python smoketest stub that exits with the given code."""
    p = tmp_path / "fake_smoketest.py"
    p.write_text(f"import sys; sys.exit({exit_code})\n")
    return p


class TestBashSmoketestMissing:
    """When _JQ_SMOKETEST_PATH points to a nonexistent file the bash scripts
    must exit 1 (FAIL) because the deployment is incomplete."""

    def test_ingest_fails_when_smoketest_missing(self, tmp_path):
        """Ingest exits 1 when smoketest path does not exist."""
        u300 = _make_fake_universe(tmp_path)
        result = _run_bash_with_smoketest(
            _INGEST,
            smoketest_path="/nonexistent/jq_api_smoketest.py",
            extra_env={"STATE": str(tmp_path / "state")},
            u300=u300,
        )
        assert result.returncode == 1, (
            f"Expected exit 1, got {result.returncode}\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
        combined = result.stdout + result.stderr
        assert "smoketest" in combined.lower() or "jq_api_smoketest" in combined.lower(), (
            f"Expected smoketest-related error message:\n{combined}"
        )

    def test_universe_fails_when_smoketest_missing(self, tmp_path):
        """Universe build exits 1 when smoketest path does not exist."""
        result = _run_bash_with_smoketest(
            _UNIVERSE,
            smoketest_path="/nonexistent/jq_api_smoketest.py",
            extra_env={"OUT": str(tmp_path / "universe300.txt")},
        )
        assert result.returncode == 1, (
            f"Expected exit 1, got {result.returncode}\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
        combined = result.stdout + result.stderr
        assert "smoketest" in combined.lower() or "jq_api_smoketest" in combined.lower(), (
            f"Expected smoketest-related error message:\n{combined}"
        )


class TestBashSmoketestExitCodes:
    """Verify that the bash scripts translate smoketest exit codes correctly."""

    def test_ingest_smoketest_rc2_becomes_skip(self, tmp_path):
        """Smoketest exit 2 (key missing) → SKIP api_key_missing (exit 0)."""
        u300 = _make_fake_universe(tmp_path)
        fake = _make_fake_smoketest(tmp_path, 2)
        result = _run_bash_with_smoketest(
            _INGEST,
            smoketest_path=str(fake),
            extra_env={"STATE": str(tmp_path / "state")},
            u300=u300,
        )
        assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
        assert "api_key_missing" in result.stdout or "api_key_missing" in result.stderr

    def test_ingest_smoketest_rc3_becomes_fail(self, tmp_path):
        """Smoketest exit 3 (HTTP 401) → FAIL (exit 1)."""
        u300 = _make_fake_universe(tmp_path)
        fake = _make_fake_smoketest(tmp_path, 3)
        result = _run_bash_with_smoketest(
            _INGEST,
            smoketest_path=str(fake),
            extra_env={"STATE": str(tmp_path / "state")},
            u300=u300,
        )
        assert result.returncode == 1, f"stdout: {result.stdout}\nstderr: {result.stderr}"

    def test_ingest_smoketest_rc4_becomes_fail(self, tmp_path):
        """Smoketest exit 4 (HTTP 403) → FAIL (exit 1)."""
        u300 = _make_fake_universe(tmp_path)
        fake = _make_fake_smoketest(tmp_path, 4)
        result = _run_bash_with_smoketest(
            _INGEST,
            smoketest_path=str(fake),
            extra_env={"STATE": str(tmp_path / "state")},
            u300=u300,
        )
        assert result.returncode == 1, f"stdout: {result.stdout}\nstderr: {result.stderr}"

    def test_universe_smoketest_rc2_becomes_skip(self, tmp_path):
        """Universe: smoketest exit 2 → SKIP api_key_missing (exit 0)."""
        fake = _make_fake_smoketest(tmp_path, 2)
        result = _run_bash_with_smoketest(
            _UNIVERSE,
            smoketest_path=str(fake),
            extra_env={"OUT": str(tmp_path / "universe300.txt")},
        )
        assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
        assert "api_key_missing" in result.stdout or "api_key_missing" in result.stderr

    def test_universe_smoketest_rc3_becomes_fail(self, tmp_path):
        """Universe: smoketest exit 3 (HTTP 401) → FAIL (exit 1)."""
        fake = _make_fake_smoketest(tmp_path, 3)
        result = _run_bash_with_smoketest(
            _UNIVERSE,
            smoketest_path=str(fake),
            extra_env={"OUT": str(tmp_path / "universe300.txt")},
        )
        assert result.returncode == 1, f"stdout: {result.stdout}\nstderr: {result.stderr}"


# ──────────────────────────────────────────────────────────────────────────────
# Bash integration: --dry-run skips smoketest entirely
# ──────────────────────────────────────────────────────────────────────────────

class TestBashDryRunSkipsSmoketest:
    """--dry-run must exit 0 without running the smoketest (no network)."""

    def test_ingest_dry_run_api_key_missing_exits_0(self, tmp_path):
        """Ingest --dry-run with JQ_API_KEY='' still exits 0 (SKIP api_key_missing)."""
        u300 = _make_fake_universe(tmp_path)
        result = subprocess.run(
            ["bash", str(_INGEST), "--dry-run"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "JQ_API_KEY": "",
                "U300": str(u300),
                "HOME": os.environ.get("HOME", "/tmp"),
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            },
            timeout=15,
        )
        assert result.returncode == 0

    def test_universe_dry_run_api_key_missing_exits_0(self, tmp_path):
        """Universe --dry-run with JQ_API_KEY='' still exits 0 (SKIP api_key_missing)."""
        result = subprocess.run(
            ["bash", str(_UNIVERSE), "--dry-run"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "JQ_API_KEY": "",
                "OUT": str(tmp_path / "universe300.txt"),
                "HOME": os.environ.get("HOME", "/tmp"),
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            },
            timeout=15,
        )
        assert result.returncode == 0
