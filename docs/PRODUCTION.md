# Production Operations Guide

This document defines the production requirements for inga-quants and the
procedures to achieve and maintain them.

## Production Definition

A deployment is considered **production-ready** when all four conditions hold:

| # | Condition | How enforced |
|---|-----------|-------------|
| 1 | **main is the single source of truth** | Branch protection + PR required + CI required |
| 2 | **CI must pass before merge** | `pytest (Python 3.12)` status check required |
| 3 | **Deployment is non-interactive** | `sudo -n inga-deploy-shutdown` / `sudo -n inga-prod-apply` |
| 4 | **`systemctl --failed` is zero for managed units** | `inga-prod-apply` enforces this |

---

## GitHub Branch Protection (one-time UI setup)

In GitHub → Settings → Branches → `main`:

- [x] Require a pull request before merging
- [x] Require status checks: **`pytest (Python 3.12)`**
- [x] Require branches to be up to date before merging
- [x] Allow auto-merge (squash)
- [ ] Allow force push — **OFF**

After this, Claude Code PRs merge automatically once CI is green.
Direct pushes to `main` are blocked.

---

## systemd Unit Policy

### SKIP = exit 0 (units must NOT fall into `systemctl --failed` for operational reasons)

All managed oneshot/timer units must exit 0 for the following conditions:

| Condition | Log pattern | Implementation |
|-----------|-------------|----------------|
| API key not set | `[SKIP] reason=api_key_missing` | Check at script start, before I/O |
| Non-trading day (weekend / JP holiday) | `[SKIP] reason=non_trading_day` | `AS_OF`-aware jpholiday check |
| No data returned | `[SKIP] reason=no_data` | Probe result < threshold → SKIP |
| Digest script not found | `[SKIP] reason=script_missing` | Wrapper fallback search |
| Digest exits non-zero | `[SKIP] reason=notify_nonzero` | Wrapper converts to exit 0 |

### FAIL = exit 1 (only for genuine errors that need investigation)

| Condition | Effect |
|-----------|--------|
| BigQuery write error | exit 1 → systemd failed → alert |
| Universe file missing on business day | exit 1 |
| API persistently down on a business day | exit 1 |

### Allowlist — units to keep enabled

Defined in [`shutdown/deploy/prod-allowlist.conf`](../shutdown/deploy/prod-allowlist.conf).

Current managed units:

| Unit | Script | Trigger |
|------|--------|---------|
| `inga-market-quotes-ingest.service` | `inga_market_quotes_ingest_jq300.sh` | Daily timer (JST weekdays) |
| `inga-universe300-build.service` | `inga_universe300_build.sh` | Sunday 03:00 JST |
| `inga-weekly-digest.service` | `inga_weekly_digest_wrapper.sh` → `notify_digest.py` | Weekly timer |

To add a unit: append to `prod-allowlist.conf` and run `sudo -n inga-prod-apply`.

To discover all inga units on the VPS:
```bash
systemctl list-timers --all | grep inga
systemctl list-units 'inga-*' --all
```

### Denylist — units to disable + mask

Defined in [`shutdown/deploy/prod-denylist.conf`](../shutdown/deploy/prod-denylist.conf).

Units in the denylist are stopped, disabled, and masked (`systemctl mask`).
They cannot start accidentally and will not appear in `systemctl --failed`.

Policy: **if we don't run it, we mask it.**
Add retired units here instead of leaving them in a failed state.

---

## Deployment Procedure

### First time — one-time root bootstrap (password required once)

Run this single command from PowerShell (or any SSH client).
After this, **all subsequent operations are password-free**.

```powershell
# PowerShell — paste once, enter your sudo password when prompted
ssh -tt inga@YOUR_VPS_HOST "sudo bash /srv/inga-quants/shutdown/deploy/inga-prod-bootstrap"
```

What the bootstrap script does (idempotent, safe to re-run):
1. Installs `inga-deploy-shutdown` + `inga-prod-apply` to `/usr/local/sbin/` (root:root 755)
2. Creates `/usr/local/bin/` symlinks (symlink-tolerance for sudo PATH variations)
3. Installs sudoers fragments with NOPASSWD for the `inga` user (both sbin + bin paths)
4. Validates sudoers syntax with `visudo -cf`
5. Self-tests NOPASSWD grants via `sudo -l -U inga`

### Every deployment — password-free

```powershell
# PowerShell — no password prompt

# Step 1: Deploy scripts + systemd drop-ins from repo
ssh inga@YOUR_VPS_HOST "sudo -n inga-deploy-shutdown"

# Step 2: Apply production profile (enable allowlist, mask denylist, verify --failed is clean)
ssh inga@YOUR_VPS_HOST "sudo -n inga-prod-apply"

# Combined (stops on first failure):
ssh inga@YOUR_VPS_HOST "sudo -n inga-deploy-shutdown && sudo -n inga-prod-apply"
```

### Dry-run (preview without changes)

```powershell
# PowerShell — shows what would happen, exits 0, no root required
ssh inga@YOUR_VPS_HOST "bash /srv/inga-quants/shutdown/deploy/inga-deploy-shutdown --dry-run"
ssh inga@YOUR_VPS_HOST "bash /srv/inga-quants/shutdown/deploy/inga-prod-apply --dry-run"
```

### Verify production state

```powershell
# PowerShell
ssh inga@YOUR_VPS_HOST "systemctl --failed --no-pager"
ssh inga@YOUR_VPS_HOST "sudo -n inga-prod-apply && echo 'production OK'"
```

```bash
# On VPS directly
# Check for any failed units
systemctl --failed --no-pager

# Check managed units specifically
systemctl status inga-market-quotes-ingest.service inga-universe300-build.service inga-weekly-digest.service

# Test SKIP logic without network
JQ_API_KEY="" bash /srv/inga/SHUTDOWN/bin/inga_market_quotes_ingest_jq300.sh
# Expected: [SKIP] reason=api_key_missing   exit 0
```

### Troubleshooting: `sudo -n` fails with "password is required"

The sudoers fragment is not installed. Run the bootstrap:

```powershell
ssh -tt inga@YOUR_VPS_HOST "sudo bash /srv/inga-quants/shutdown/deploy/inga-prod-bootstrap"
```

Then verify:
```bash
# On VPS
sudo -l -U inga | grep -E 'inga-(deploy-shutdown|prod-apply)'
```
Expected output: two NOPASSWD lines, one for `/usr/local/sbin/`, one for `/usr/local/bin/`.

### Troubleshooting: `sudo -n inga-prod-apply: command not found`

The script is not installed to a directory in sudo's secure_path. The bootstrap
installs to `/usr/local/sbin/` and `/usr/local/bin/` — both are in the default
secure_path. Re-run the bootstrap to fix.

---

## Monitoring

- **`systemctl --failed` must be zero** for managed units after `inga-prod-apply`.
- If a unit enters failed state unexpectedly, `journalctl -u <unit> -n 50` for diagnosis.
- `inga-prod-apply` exits 1 and prints a status dump if any allowlisted unit is in failed state.
  This makes it suitable as a monitoring check: `sudo -n inga-prod-apply || alert`.

---

## Exit Code Reference

| Code | Meaning | systemd effect |
|------|---------|---------------|
| 0 | OK or operational SKIP | success — does not appear in `--failed` |
| 1 | FAIL — genuine error requiring investigation | failed — appears in `--failed` |
| 2 | Auth / infrastructure error (e.g. gh not authenticated) | — |

---

## CI Reference

GitHub Actions (`.github/workflows/ci.yml`) runs on every push and PR:
- Python 3.12, `pytest -q`
- Tests include `test_noninteractive.py` (TTY-less + `read -r` guard)
- Tests include `test_production_profile.py` (lint: allowlist/denylist, PRODUCTION.md)
