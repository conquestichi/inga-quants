"""Tests for .env auto-load in cli.main().

Strategy: subprocess isolation so each test starts with a clean environment.
We write a small .env fixture, run `python -m inga_quant.cli --help` (no API
call), and verify the env var was set inside the subprocess.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

# Path to the venv python that has inga_quant installed
_PYTHON = sys.executable

# Helper that runs a one-liner to check os.environ after main() has loaded dotenv
_PROBE = textwrap.dedent("""\
    import os
    from inga_quant.cli import _load_dotenv_if_present
    _load_dotenv_if_present()
    print(os.environ.get("TEST_KEY_AUTOLOAD", "__MISSING__"))
""")


def _run_probe(env: dict[str, str] | None = None, cwd: str | None = None) -> str:
    """Run _PROBE in a subprocess, return stdout stripped."""
    result = subprocess.run(
        [_PYTHON, "-c", _PROBE],
        capture_output=True,
        text=True,
        env=env,
        cwd=cwd,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


# ---------------------------------------------------------------------------
# Happy path: .env in cwd is picked up
# ---------------------------------------------------------------------------

class TestDotEnvAutoLoad:
    def test_loads_key_from_cwd_dotenv(self, tmp_path):
        (tmp_path / ".env").write_text("TEST_KEY_AUTOLOAD=hello_from_dotenv\n")
        out = _run_probe(cwd=str(tmp_path), env={**os.environ, "TEST_KEY_AUTOLOAD": ""})
        # We set TEST_KEY_AUTOLOAD="" in env (empty string counts as set → setdefault
        # won't overwrite). Let's pass env WITHOUT the key.
        env_clean = {k: v for k, v in os.environ.items() if k != "TEST_KEY_AUTOLOAD"}
        out = _run_probe(cwd=str(tmp_path), env=env_clean)
        assert out == "hello_from_dotenv"

    def test_strips_double_quotes(self, tmp_path):
        (tmp_path / ".env").write_text('TEST_KEY_AUTOLOAD="quoted_value"\n')
        env_clean = {k: v for k, v in os.environ.items() if k != "TEST_KEY_AUTOLOAD"}
        out = _run_probe(cwd=str(tmp_path), env=env_clean)
        assert out == "quoted_value"

    def test_strips_single_quotes(self, tmp_path):
        (tmp_path / ".env").write_text("TEST_KEY_AUTOLOAD='single_quoted'\n")
        env_clean = {k: v for k, v in os.environ.items() if k != "TEST_KEY_AUTOLOAD"}
        out = _run_probe(cwd=str(tmp_path), env=env_clean)
        assert out == "single_quoted"

    def test_skips_comment_lines(self, tmp_path):
        (tmp_path / ".env").write_text(
            "# this is a comment\nTEST_KEY_AUTOLOAD=real_value\n"
        )
        env_clean = {k: v for k, v in os.environ.items() if k != "TEST_KEY_AUTOLOAD"}
        out = _run_probe(cwd=str(tmp_path), env=env_clean)
        assert out == "real_value"

    def test_skips_blank_lines(self, tmp_path):
        (tmp_path / ".env").write_text("\n\nTEST_KEY_AUTOLOAD=after_blanks\n\n")
        env_clean = {k: v for k, v in os.environ.items() if k != "TEST_KEY_AUTOLOAD"}
        out = _run_probe(cwd=str(tmp_path), env=env_clean)
        assert out == "after_blanks"

    def test_does_not_overwrite_existing_env(self, tmp_path):
        (tmp_path / ".env").write_text("TEST_KEY_AUTOLOAD=from_dotenv\n")
        env_with_existing = {**os.environ, "TEST_KEY_AUTOLOAD": "already_set"}
        out = _run_probe(cwd=str(tmp_path), env=env_with_existing)
        assert out == "already_set"

    def test_missing_dotenv_is_silent(self, tmp_path):
        """No .env file → no crash, key stays missing."""
        env_clean = {k: v for k, v in os.environ.items() if k != "TEST_KEY_AUTOLOAD"}
        out = _run_probe(cwd=str(tmp_path), env=env_clean)
        assert out == "__MISSING__"


# ---------------------------------------------------------------------------
# Integration: smoke-check with .env supplies the key (no "未設定" error)
# ---------------------------------------------------------------------------

class TestSmokeCheckWithDotEnv:
    def test_dotenv_supplies_key_to_smoke_check(self, tmp_path):
        """A .env with a dummy key reaches JQuantsLoader (auth error, not missing-key)."""
        (tmp_path / ".env").write_text("JQUANTS_API_KEY=dummy-test-key\n")
        env_clean = {
            k: v for k, v in os.environ.items()
            if k not in ("JQUANTS_API_KEY", "JQUANTS_APIKEY")
        }
        # smoke-check will try a real HTTP call with a fake key → 403 or network error.
        # Either way the key was picked up (no "未設定" error).
        result = subprocess.run(
            [_PYTHON, "-m", "inga_quant.cli", "smoke-check"],
            capture_output=True,
            text=True,
            env=env_clean,
            cwd=str(tmp_path),
            timeout=10,
        )
        combined = result.stdout + result.stderr
        assert "未設定" not in combined, "Key from .env was not picked up"
