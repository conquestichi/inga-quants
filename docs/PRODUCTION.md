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

### First time (one-time root setup)

```bash
# 1. Install deploy scripts
sudo cp shutdown/deploy/inga-deploy-shutdown /usr/local/sbin/
sudo chown root:root /usr/local/sbin/inga-deploy-shutdown
sudo chmod 755 /usr/local/sbin/inga-deploy-shutdown

sudo cp shutdown/deploy/inga-prod-apply /usr/local/sbin/
sudo chown root:root /usr/local/sbin/inga-prod-apply
sudo chmod 755 /usr/local/sbin/inga-prod-apply

# 2. Install sudoers (NOPASSWD for inga user)
sudo cp shutdown/deploy/inga-sudoers-deploy /etc/sudoers.d/inga-deploy-shutdown
sudo chmod 440 /etc/sudoers.d/inga-deploy-shutdown

sudo cp shutdown/deploy/inga-sudoers-prod /etc/sudoers.d/inga-prod-apply
sudo chmod 440 /etc/sudoers.d/inga-prod-apply

sudo visudo -c   # validate; must print "parsed OK"
```

### Every deployment (non-interactive, password-free)

```bash
# Deploy scripts + systemd drop-ins from repo
sudo -n inga-deploy-shutdown

# Apply production profile (enable allowlist, mask denylist, verify --failed is clean)
sudo -n inga-prod-apply
```

### Verify production state

```bash
# Check for any failed units
systemctl --failed --no-pager

# Check managed units specifically
systemctl status inga-market-quotes-ingest.service inga-universe300-build.service inga-weekly-digest.service

# Test SKIP logic without network
JQ_API_KEY="" bash /srv/inga/SHUTDOWN/bin/inga_market_quotes_ingest_jq300.sh
# Expected: [SKIP] reason=api_key_missing   exit 0
```

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
