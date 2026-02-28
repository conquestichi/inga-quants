"""CI lint tests for the production profile.

These tests verify structural integrity of the production allowlist/denylist
and the inga-prod-apply script — without requiring root or a running systemd.

Checks:
  - Config files exist and are parseable
  - No unit appears in both allowlist and denylist
  - All unit names look like valid systemd names
  - inga-prod-apply passes bash -n
  - inga-prod-apply --dry-run exits 0 without root
  - docs/PRODUCTION.md exists and references allowlisted units
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

_REPO = Path(__file__).parent.parent
_DEPLOY_DIR = _REPO / "shutdown" / "deploy"
_ALLOWLIST = _DEPLOY_DIR / "prod-allowlist.conf"
_DENYLIST = _DEPLOY_DIR / "prod-denylist.conf"
_PROD_APPLY = _DEPLOY_DIR / "inga-prod-apply"
_PROD_STATUS = _DEPLOY_DIR / "inga-prod-status"
_BOOTSTRAP = _DEPLOY_DIR / "inga-prod-bootstrap"
_SUDOERS_PROD = _DEPLOY_DIR / "inga-sudoers-prod"
_SUDOERS_DEPLOY = _DEPLOY_DIR / "inga-sudoers-deploy"
_PRODUCTION_MD = _REPO / "docs" / "PRODUCTION.md"

# Valid systemd unit name pattern (covers .service, .timer, .socket, .target)
_UNIT_PATTERN = re.compile(r'^[\w@.:\-]+\.(service|timer|socket|target|mount|path)$')


def _parse_conf(path: Path) -> list[str]:
    """Return non-comment, non-blank lines from a conf file."""
    lines = []
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            lines.append(stripped)
    return lines


# ──────────────────────────────────────────────────────────────────────────────
# Config file structure
# ──────────────────────────────────────────────────────────────────────────────

class TestConfigFiles:
    def test_allowlist_exists(self):
        assert _ALLOWLIST.exists(), f"Missing: {_ALLOWLIST}"

    def test_denylist_exists(self):
        assert _DENYLIST.exists(), f"Missing: {_DENYLIST}"

    def test_allowlist_has_entries(self):
        """Allowlist must have at least one production unit."""
        entries = _parse_conf(_ALLOWLIST)
        assert len(entries) >= 1, "prod-allowlist.conf has no entries — add at least one unit"

    def test_allowlist_valid_unit_names(self):
        """Every allowlist entry must look like a valid systemd unit name."""
        bad = [e for e in _parse_conf(_ALLOWLIST) if not _UNIT_PATTERN.match(e)]
        assert not bad, f"Invalid unit names in allowlist: {bad}"

    def test_denylist_valid_unit_names(self):
        """Every denylist entry must look like a valid systemd unit name."""
        bad = [e for e in _parse_conf(_DENYLIST) if not _UNIT_PATTERN.match(e)]
        assert not bad, f"Invalid unit names in denylist: {bad}"

    def test_no_overlap_allowlist_denylist(self):
        """A unit must not appear in both allowlist and denylist."""
        allow = set(_parse_conf(_ALLOWLIST))
        deny = set(_parse_conf(_DENYLIST))
        overlap = allow & deny
        assert not overlap, (
            f"Units in both allowlist and denylist (ambiguous): {overlap}"
        )

    def test_allowlist_has_timer_entries(self):
        """Allowlist must contain at least one .timer unit.
        Timers are the canonical way to schedule oneshot services.
        """
        timers = [e for e in _parse_conf(_ALLOWLIST) if e.endswith(".timer")]
        assert timers, (
            "prod-allowlist.conf has no .timer entries. "
            "Add the timer units (e.g. inga-market-quotes-ingest.timer) — not the .service units."
        )

    def test_allowlist_no_service_when_timer_present(self):
        """If foo.timer is in the allowlist, foo.service must NOT also be listed.
        The service is managed by the timer; listing both is redundant and confusing.
        """
        entries = set(_parse_conf(_ALLOWLIST))
        conflicts = []
        for e in entries:
            if e.endswith(".timer"):
                svc = e[: -len(".timer")] + ".service"
                if svc in entries:
                    conflicts.append(f"{e} and {svc}")
        assert not conflicts, (
            f"Both timer and service in allowlist (remove the .service entry): {conflicts}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# inga-prod-apply script
# ──────────────────────────────────────────────────────────────────────────────

class TestProdApplyScript:
    def test_script_exists(self):
        assert _PROD_APPLY.exists(), f"Missing: {_PROD_APPLY}"

    def test_bash_syntax(self):
        """bash -n must pass."""
        result = subprocess.run(["bash", "-n", str(_PROD_APPLY)], capture_output=True, text=True)
        assert result.returncode == 0, f"bash -n failed:\n{result.stderr}"

    def test_no_bare_read_r(self):
        """No bare 'read -r' outside PAUSE guard (would block without TTY)."""
        text = _PROD_APPLY.read_text()
        lines = text.splitlines()
        violations = []
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if "while" in stripped and "read -r" in stripped:
                continue
            if "read -r" in stripped:
                context = "\n".join(lines[max(0, i-10):i])
                if "PAUSE" not in context:
                    violations.append(f"  line {i}: {line.rstrip()}")
        assert not violations, (
            f"Bare 'read -r' in inga-prod-apply:\n" + "\n".join(violations)
        )

    def test_dry_run_exits_0_without_root(self):
        """--dry-run works without root (shows plan, no systemctl calls)."""
        import os
        if os.getuid() == 0:
            pytest.skip("Running as root — non-root path not reachable")
        result = subprocess.run(
            ["bash", str(_PROD_APPLY), "--dry-run"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert result.returncode == 0, f"--dry-run failed:\n{result.stdout}\n{result.stderr}"
        assert "[DRY]" in result.stdout

    def test_dry_run_mentions_allowlist_units(self):
        """--dry-run output references every allowlisted unit."""
        import os
        if os.getuid() == 0:
            pytest.skip("Running as root — non-root path not reachable")
        result = subprocess.run(
            ["bash", str(_PROD_APPLY), "--dry-run"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=15,
        )
        for unit in _parse_conf(_ALLOWLIST):
            assert unit in result.stdout, (
                f"Unit '{unit}' not mentioned in --dry-run output"
            )

    def test_not_root_normal_mode_exits_1(self):
        """Normal mode exits 1 immediately when not root (no hanging)."""
        import os
        if os.getuid() == 0:
            pytest.skip("Running as root — non-root exit path not reachable")
        result = subprocess.run(
            ["bash", str(_PROD_APPLY)],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 1
        combined = result.stdout + result.stderr
        assert "root" in combined.lower()

    def test_dry_run_shows_timer_verify(self):
        """--dry-run output includes is-enabled + is-active + is-failed checks for timers."""
        import os
        if os.getuid() == 0:
            pytest.skip("Running as root — non-root path not reachable")
        result = subprocess.run(
            ["bash", str(_PROD_APPLY), "--dry-run"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert result.returncode == 0
        out = result.stdout
        assert "is-enabled" in out, "--dry-run output missing is-enabled check"
        assert "is-active" in out, "--dry-run output missing is-active check"
        assert "is-failed" in out, "--dry-run output missing is-failed check"

    def test_prod_apply_checks_is_enabled(self):
        """inga-prod-apply must call systemctl is-enabled (timer must be enabled)."""
        text = _PROD_APPLY.read_text()
        assert "is-enabled" in text, "inga-prod-apply missing is-enabled check"

    def test_prod_apply_checks_is_active(self):
        """inga-prod-apply must call systemctl is-active (timer must be active/waiting)."""
        text = _PROD_APPLY.read_text()
        assert "is-active" in text, "inga-prod-apply missing is-active check"

    def test_prod_apply_unknown_is_error(self):
        """'unknown' from is-active must be treated as an error, not logged as OK.
        'unknown' means the unit file does not exist on this host.
        """
        text = _PROD_APPLY.read_text()
        lines = text.splitlines()
        violations = []
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            # Detect lines that log OK with 'unknown' in the same message
            if "_log" in stripped and "OK" in stripped and "unknown" in stripped:
                violations.append(f"  line {i}: {line.rstrip()}")
        assert not violations, (
            "Found log lines that accept 'unknown' state as OK — 'unknown' = unit missing = error:\n"
            + "\n".join(violations)
        )

    def test_prod_apply_derives_service_from_timer(self):
        """inga-prod-apply must derive the .service name from .timer for verification."""
        text = _PROD_APPLY.read_text()
        # The pattern that strips .timer and appends .service
        assert ".timer}.service" in text or '%.timer}.service"' in text or "%.timer" in text, (
            "inga-prod-apply does not appear to derive .service from .timer name"
        )


# ──────────────────────────────────────────────────────────────────────────────
# inga-prod-status script
# ──────────────────────────────────────────────────────────────────────────────

class TestProdStatusScript:
    def test_script_exists(self):
        assert _PROD_STATUS.exists(), f"Missing: {_PROD_STATUS}"

    def test_bash_syntax(self):
        """bash -n must pass."""
        result = subprocess.run(
            ["bash", "-n", str(_PROD_STATUS)], capture_output=True, text=True
        )
        assert result.returncode == 0, f"bash -n failed:\n{result.stderr}"

    def test_references_allowlist(self):
        """inga-prod-status must read the allowlist to enumerate units."""
        text = _PROD_STATUS.read_text()
        assert "prod-allowlist.conf" in text or "ALLOWLIST" in text


# ──────────────────────────────────────────────────────────────────────────────
# docs/PRODUCTION.md consistency
# ──────────────────────────────────────────────────────────────────────────────

class TestProductionMd:
    def test_production_md_exists(self):
        assert _PRODUCTION_MD.exists(), f"Missing: {_PRODUCTION_MD}"

    def test_production_md_mentions_allowlist_units(self):
        """PRODUCTION.md must mention every unit in the allowlist."""
        text = _PRODUCTION_MD.read_text()
        missing = [u for u in _parse_conf(_ALLOWLIST) if u not in text]
        assert not missing, (
            f"Units in prod-allowlist.conf not mentioned in PRODUCTION.md: {missing}\n"
            "Update docs/PRODUCTION.md to reference these units."
        )

    def test_production_md_has_key_sections(self):
        """PRODUCTION.md must contain the key section headings."""
        text = _PRODUCTION_MD.read_text()
        required = [
            "allowlist",
            "denylist",
            "SKIP",
            "sudo -n inga-prod-apply",
        ]
        missing = [r for r in required if r not in text]
        assert not missing, f"Missing content in PRODUCTION.md: {missing}"

    def test_production_md_has_bootstrap_powershell(self):
        """PRODUCTION.md must include PowerShell bootstrap and operational commands."""
        text = _PRODUCTION_MD.read_text()
        required = [
            "inga-prod-bootstrap",
            "sudo -n inga-deploy-shutdown",
            "sudo -n inga-prod-apply",
        ]
        missing = [r for r in required if r not in text]
        assert not missing, f"Missing PowerShell/bootstrap content in PRODUCTION.md: {missing}"


# ──────────────────────────────────────────────────────────────────────────────
# sudoers dual-path coverage
# ──────────────────────────────────────────────────────────────────────────────

class TestSudoersDualPath:
    def test_sudoers_prod_exists(self):
        assert _SUDOERS_PROD.exists(), f"Missing: {_SUDOERS_PROD}"

    def test_sudoers_deploy_exists(self):
        assert _SUDOERS_DEPLOY.exists(), f"Missing: {_SUDOERS_DEPLOY}"

    def test_sudoers_prod_covers_sbin(self):
        """/usr/local/sbin/inga-prod-apply must be in sudoers-prod."""
        text = _SUDOERS_PROD.read_text()
        assert "/usr/local/sbin/inga-prod-apply" in text, (
            "inga-sudoers-prod missing /usr/local/sbin/inga-prod-apply"
        )

    def test_sudoers_prod_covers_bin(self):
        """/usr/local/bin/inga-prod-apply must be in sudoers-prod (symlink-tolerance)."""
        text = _SUDOERS_PROD.read_text()
        assert "/usr/local/bin/inga-prod-apply" in text, (
            "inga-sudoers-prod missing /usr/local/bin/inga-prod-apply — "
            "add for symlink-tolerance when sudo PATH resolves bin before sbin"
        )

    def test_sudoers_deploy_covers_sbin(self):
        """/usr/local/sbin/inga-deploy-shutdown must be in sudoers-deploy."""
        text = _SUDOERS_DEPLOY.read_text()
        assert "/usr/local/sbin/inga-deploy-shutdown" in text, (
            "inga-sudoers-deploy missing /usr/local/sbin/inga-deploy-shutdown"
        )

    def test_sudoers_deploy_covers_bin(self):
        """/usr/local/bin/inga-deploy-shutdown must be in sudoers-deploy (symlink-tolerance)."""
        text = _SUDOERS_DEPLOY.read_text()
        assert "/usr/local/bin/inga-deploy-shutdown" in text, (
            "inga-sudoers-deploy missing /usr/local/bin/inga-deploy-shutdown — "
            "add for symlink-tolerance when sudo PATH resolves bin before sbin"
        )

    def test_sudoers_prod_nopasswd_both_paths(self):
        """Both NOPASSWD lines must be present and not commented out."""
        lines = [
            l.strip() for l in _SUDOERS_PROD.read_text().splitlines()
            if l.strip() and not l.strip().startswith("#")
        ]
        sbin = any("/usr/local/sbin/inga-prod-apply" in l and "NOPASSWD" in l for l in lines)
        binp = any("/usr/local/bin/inga-prod-apply" in l and "NOPASSWD" in l for l in lines)
        assert sbin, "inga-sudoers-prod: no active NOPASSWD line for /usr/local/sbin/inga-prod-apply"
        assert binp, "inga-sudoers-prod: no active NOPASSWD line for /usr/local/bin/inga-prod-apply"

    def test_sudoers_deploy_nopasswd_both_paths(self):
        """Both NOPASSWD lines must be present and not commented out."""
        lines = [
            l.strip() for l in _SUDOERS_DEPLOY.read_text().splitlines()
            if l.strip() and not l.strip().startswith("#")
        ]
        sbin = any("/usr/local/sbin/inga-deploy-shutdown" in l and "NOPASSWD" in l for l in lines)
        binp = any("/usr/local/bin/inga-deploy-shutdown" in l and "NOPASSWD" in l for l in lines)
        assert sbin, "inga-sudoers-deploy: no active NOPASSWD line for /usr/local/sbin/inga-deploy-shutdown"
        assert binp, "inga-sudoers-deploy: no active NOPASSWD line for /usr/local/bin/inga-deploy-shutdown"


# ──────────────────────────────────────────────────────────────────────────────
# inga-prod-bootstrap script
# ──────────────────────────────────────────────────────────────────────────────

class TestProdBootstrap:
    def test_bootstrap_exists(self):
        assert _BOOTSTRAP.exists(), f"Missing: {_BOOTSTRAP}"

    def test_bootstrap_bash_syntax(self):
        """bash -n must pass."""
        result = subprocess.run(
            ["bash", "-n", str(_BOOTSTRAP)], capture_output=True, text=True
        )
        assert result.returncode == 0, f"bash -n failed:\n{result.stderr}"

    def test_bootstrap_not_root_exits_1(self):
        """Bootstrap exits 1 immediately when not root."""
        import os
        if os.getuid() == 0:
            pytest.skip("Running as root — non-root exit path not reachable")
        result = subprocess.run(
            ["bash", str(_BOOTSTRAP)],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 1
        combined = result.stdout + result.stderr
        assert "root" in combined.lower()

    def test_bootstrap_installs_both_scripts(self):
        """Bootstrap script source must reference both deploy scripts."""
        text = _BOOTSTRAP.read_text()
        assert "inga-deploy-shutdown" in text, "Bootstrap doesn't reference inga-deploy-shutdown"
        assert "inga-prod-apply" in text, "Bootstrap doesn't reference inga-prod-apply"

    def test_bootstrap_installs_both_sudoers(self):
        """Bootstrap script must install both sudoers fragments."""
        text = _BOOTSTRAP.read_text()
        assert "inga-sudoers-deploy" in text, "Bootstrap doesn't install inga-sudoers-deploy"
        assert "inga-sudoers-prod" in text, "Bootstrap doesn't install inga-sudoers-prod"

    def test_bootstrap_references_visudo(self):
        """Bootstrap must validate sudoers with visudo."""
        text = _BOOTSTRAP.read_text()
        assert "visudo" in text, "Bootstrap doesn't run visudo validation"
