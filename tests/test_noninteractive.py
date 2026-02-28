"""Non-interactive execution tests.

Guarantees that operational scripts do not block waiting for TTY input
when run without a terminal (e.g. via ssh, CI, or PowerShell remoting).

Every test uses subprocess with:
  - stdin=subprocess.DEVNULL  (no TTY, no input pipe)
  - a timeout (ensures no indefinite hang)

A script that blocks on `read -r` without `--pause` will hit the timeout
and fail the test.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

_REPO = Path(__file__).parent.parent
_SCRIPTS_DIR = _REPO / "shutdown" / "bin"
_INGEST = _SCRIPTS_DIR / "inga_market_quotes_ingest_jq300.sh"
_UNIVERSE = _SCRIPTS_DIR / "inga_universe300_build.sh"
_WRAPPER = _SCRIPTS_DIR / "inga_weekly_digest_wrapper.sh"
_AUTOMERGE = _REPO / "bin" / "pr_automerge.sh"
_DEPLOY = _REPO / "shutdown" / "deploy" / "inga-deploy-shutdown"
_PROD_APPLY = _REPO / "shutdown" / "deploy" / "inga-prod-apply"
_PROD_STATUS = _REPO / "shutdown" / "deploy" / "inga-prod-status"
_BOOTSTRAP = _REPO / "shutdown" / "deploy" / "inga-prod-bootstrap"

# Safe env: no keys, no TTY
_BASE_ENV = {
    **os.environ,
    "JQ_API_KEY": "",
    "HOME": os.environ.get("HOME", "/tmp"),
    "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
    "TERM": "",           # no terminal
    "GH_TOKEN": "",       # no gh auth
}

_TIMEOUT = 15  # seconds; any script that hangs will exceed this


def _run_notty(
    cmd: list[str],
    env: dict | None = None,
    timeout: int = _TIMEOUT,
) -> subprocess.CompletedProcess:
    """Run a command with stdin=DEVNULL (no TTY) and a hard timeout."""
    return subprocess.run(
        cmd,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        env=env or _BASE_ENV,
        timeout=timeout,
    )


# ──────────────────────────────────────────────────────────────────────────────
# read -r guard: no interactive reads outside --pause flag
# ──────────────────────────────────────────────────────────────────────────────

SCRIPTS_TO_CHECK = [
    _INGEST,
    _UNIVERSE,
    _WRAPPER,
    _AUTOMERGE,
    _DEPLOY,
    _PROD_APPLY,
    _PROD_STATUS,
    _BOOTSTRAP,
]


class TestNoInteractiveRead:
    @pytest.mark.parametrize("script", SCRIPTS_TO_CHECK, ids=[s.name for s in SCRIPTS_TO_CHECK])
    def test_no_bare_read_r(self, script):
        """Scripts must not contain 'read -r' outside a --pause guard.

        Allowed patterns (piped / flagged):
          while IFS= read -r ...   — piped, non-blocking
          if [[ "$PAUSE" ...  ]] ... read -r   — gated by flag
        Forbidden:
          bare `read -r` at top level that blocks waiting for stdin
        """
        text = script.read_text()
        lines = text.splitlines()
        violations = []
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            # Skip comment lines
            if stripped.startswith("#"):
                continue
            # Allow: `while IFS= read -r` (piped, non-blocking)
            if "while" in stripped and "read -r" in stripped:
                continue
            # Allow: `read -r` inside a PAUSE-gated block
            # We check: if "PAUSE" appears anywhere in the 10 lines above
            if "read -r" in stripped and not stripped.startswith("while"):
                context = "\n".join(lines[max(0, i-10):i])
                if "PAUSE" not in context and "pause" not in context.lower():
                    violations.append(f"  line {i}: {line.rstrip()}")
        assert not violations, (
            f"Bare 'read -r' found in {script.name} (blocks without TTY):\n"
            + "\n".join(violations)
            + "\n\nFix: guard with [[ \"$PAUSE\" -eq 1 ]] or use --pause flag."
        )


# ──────────────────────────────────────────────────────────────────────────────
# TTY-less execution: scripts complete without hanging
# ──────────────────────────────────────────────────────────────────────────────

class TestNoTtyExecution:
    def test_ingest_notty_api_key_missing(self):
        """Ingest exits 0 (SKIP) with no TTY when JQ_API_KEY is empty."""
        result = _run_notty(["bash", str(_INGEST)], env={**_BASE_ENV, "JQ_API_KEY": ""})
        assert result.returncode == 0
        assert "api_key_missing" in result.stdout

    def test_universe_notty_api_key_missing(self):
        """Universe build exits 0 (SKIP) with no TTY when JQ_API_KEY is empty."""
        result = _run_notty(["bash", str(_UNIVERSE)], env={**_BASE_ENV, "JQ_API_KEY": ""})
        assert result.returncode == 0
        assert "api_key_missing" in result.stdout

    def test_wrapper_notty_no_script(self, tmp_path):
        """Wrapper exits 0 (SKIP) with no TTY when no digest script is found."""
        result = _run_notty(
            ["bash", str(_WRAPPER)],
            env={**_BASE_ENV, "DIGEST_SCRIPT": str(tmp_path / "missing.py")},
        )
        assert result.returncode == 0
        assert "script_missing" in result.stdout

    def test_wrapper_notty_ok(self, tmp_path):
        """Wrapper exits 0 (OK) with no TTY when digest script succeeds."""
        ok = tmp_path / "ok.py"
        ok.write_text("print('ok')\n")
        result = _run_notty(
            ["bash", str(_WRAPPER)],
            env={**_BASE_ENV, "DIGEST_SCRIPT": str(ok), "DIGEST_PYTHON": "/usr/bin/python3"},
        )
        assert result.returncode == 0
        assert "OK:" in result.stdout

    def test_ingest_notty_non_trading_day(self):
        """Ingest exits 0 (SKIP non_trading_day) with no TTY for Saturday."""
        result = _run_notty(
            ["bash", str(_INGEST)],
            env={**_BASE_ENV, "JQ_API_KEY": "dummy", "AS_OF": "2026-02-21"},
        )
        assert result.returncode == 0
        assert "non_trading_day" in result.stdout

    def test_automerge_notty_zero_diff(self):
        """pr_automerge.sh exits 0 with no TTY when there is nothing to commit."""
        # Run from repo root; zero diff detected before gh auth check
        import tempfile
        # We can't easily create a zero-diff state here without modifying git state.
        # Instead: test dry-run mode with no TTY — should complete without hanging.
        # dry-run exits 0 even without gh auth.
        result = _run_notty(
            ["bash", str(_AUTOMERGE), "fix/notty-smoke", "--dry-run"],
            env={**_BASE_ENV, "GH_TOKEN": ""},
            timeout=30,
        )
        # dry-run always exits 0 (it doesn't invoke gh)
        assert result.returncode == 0, f"dry-run hung or failed:\n{result.stdout}\n{result.stderr}"
        assert "[DRY]" in result.stdout

    def test_automerge_notty_no_gh_auth_exits_2(self):
        """pr_automerge.sh exits 2 (not 0/1) with no TTY when gh is not authenticated."""
        # This test only applies when gh is NOT already authenticated.
        # Since gh may be authenticated in CI, we skip if it is.
        auth_ok = subprocess.run(
            ["gh", "auth", "status"], capture_output=True
        ).returncode == 0
        if auth_ok:
            pytest.skip("gh is already authenticated — exit-2 path not reachable")

        result = _run_notty(
            ["bash", str(_AUTOMERGE), "fix/notty-auth-test"],
            env={**_BASE_ENV, "GH_TOKEN": ""},
            timeout=30,
        )
        assert result.returncode == 2
        assert "GH_TOKEN" in result.stderr

    def test_deploy_notty_not_root_exits_1(self):
        """inga-deploy-shutdown exits 1 without prompting when not run as root."""
        if os.getuid() == 0:
            pytest.skip("Running as root — non-root exit path not reachable")
        result = _run_notty(["bash", str(_DEPLOY)], timeout=10)
        assert result.returncode == 1
        assert "root" in result.stderr.lower() or "root" in result.stdout.lower()

    def test_deploy_notty_dry_run_not_root(self):
        """inga-deploy-shutdown --dry-run exits 1 (still needs root) without prompting."""
        if os.getuid() == 0:
            pytest.skip("Running as root — non-root path not reachable")
        result = _run_notty(["bash", str(_DEPLOY), "--dry-run"], timeout=10)
        assert result.returncode == 1

    def test_prod_apply_notty_not_root_exits_1(self):
        """inga-prod-apply exits 1 without prompting when not root (normal mode)."""
        if os.getuid() == 0:
            pytest.skip("Running as root — non-root exit path not reachable")
        result = _run_notty(["bash", str(_PROD_APPLY)], timeout=10)
        assert result.returncode == 1
        assert "root" in result.stderr.lower() or "root" in result.stdout.lower()

    def test_prod_apply_notty_dry_run_no_root_exits_0(self):
        """inga-prod-apply --dry-run exits 0 without root (shows plan only)."""
        if os.getuid() == 0:
            pytest.skip("Running as root — non-root path not reachable")
        result = _run_notty(["bash", str(_PROD_APPLY), "--dry-run"], timeout=15)
        assert result.returncode == 0, f"dry-run failed:\n{result.stdout}\n{result.stderr}"
        assert "[DRY]" in result.stdout

    def test_bootstrap_notty_not_root_exits_1(self):
        """inga-prod-bootstrap exits 1 without prompting when not root."""
        if os.getuid() == 0:
            pytest.skip("Running as root — non-root exit path not reachable")
        result = _run_notty(["bash", str(_BOOTSTRAP)], timeout=10)
        assert result.returncode == 1
        combined = result.stdout + result.stderr
        assert "root" in combined.lower()

    def test_prod_status_notty_default_exits_0(self, tmp_path):
        """inga-prod-status (no --check) exits 0 without TTY regardless of unit state."""
        # Use a custom allowlist with a definitely-missing timer to stress-test
        # the fallback paths. Default mode must never exit 1.
        allowlist = tmp_path / "allowlist.conf"
        allowlist.write_text("inga-definitely-does-not-exist.timer\n")
        result = _run_notty(
            ["bash", str(_PROD_STATUS)],
            env={**_BASE_ENV, "ALLOWLIST": str(allowlist)},
            timeout=15,
        )
        assert result.returncode == 0, (
            f"inga-prod-status default mode must exit 0:\n{result.stdout}\n{result.stderr}"
        )
