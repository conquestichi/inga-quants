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

### Allowlist — timer units to keep enabled

Defined in [`shutdown/deploy/prod-allowlist.conf`](../shutdown/deploy/prod-allowlist.conf).

**The allowlist holds `.timer` units, not `.service` units.**
Services are oneshot units that the timer launches. Enabling the timer is the
correct way to schedule them. `inga-prod-apply` verifies each timer is
`is-enabled=enabled` and `is-active=active`, then checks that the corresponding
`.service` is not in failed state. An `is-active=unknown` result means the unit
file does not exist on this host — this is always an error, not a skip.

Current managed timers:

| Timer | Service (launched by timer) | Script | Trigger |
|-------|-----------------------------|--------|---------|
| `inga-market-quotes-ingest.timer` | `inga-market-quotes-ingest.service` | `inga_market_quotes_ingest_jq300.sh` | Daily (JST weekdays) |
| `inga-universe300-build.timer` | `inga-universe300-build.service` | `inga_universe300_build.sh` | Sunday 03:00 JST |
| `inga-weekly-digest.timer` | `inga-weekly-digest.service` | `inga_weekly_digest_wrapper.sh` → `notify_digest.py` | Weekly |

To add a unit: append `foo.timer` to `prod-allowlist.conf` and run `sudo -n inga-prod-apply`.

To discover all inga timers on the VPS:
```bash
systemctl list-timers --all | grep inga
systemctl list-unit-files 'inga-*.timer'
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
# PowerShell — single paste, no password prompt
ssh -tt inga@YOUR_VPS_HOST "sudo -n inga-deploy-shutdown && sudo -n inga-prod-apply"
```

This command:
1. Deploys scripts + systemd drop-ins from the repo to `/srv/inga/SHUTDOWN/bin/`
2. Enables all allowlist timers, masks denylist units, verifies timer state is clean

### Check production state

```powershell
# PowerShell — timer schedule + per-unit state
ssh inga@YOUR_VPS_HOST "bash /srv/inga-quants/shutdown/deploy/inga-prod-status"

# Full verification (exits 1 if any timer is not enabled/active or service failed):
ssh inga@YOUR_VPS_HOST "sudo -n inga-prod-apply && echo 'production OK'"
```

### Dry-run (preview without changes, no root required)

```powershell
# PowerShell — shows what would happen, exits 0
ssh inga@YOUR_VPS_HOST "bash /srv/inga-quants/shutdown/deploy/inga-prod-apply --dry-run"
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

### Troubleshooting: `ERR is-active=unknown`

The timer unit file does not exist on this host. Either:
- The `.timer` file has not been deployed yet (`sudo -n inga-deploy-shutdown` installs scripts, but systemd unit files must be placed separately)
- Check with: `systemctl list-unit-files 'inga-*.timer'`
- If the unit file is missing, it must be created in `/etc/systemd/system/` and `systemctl daemon-reload` run.

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
