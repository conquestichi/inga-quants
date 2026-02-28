#!/usr/bin/env bash
# bin/pr_automerge.sh — non-interactive PR automation for inga-quants
#
# Usage:
#   bin/pr_automerge.sh <BRANCH> [OPTIONS]
#
# Arguments:
#   BRANCH               New branch name (e.g. fix/some-topic).
#                        Defaults to fix/<YYYYMMDD-HHMMSS> if omitted.
#
# Options:
#   --base <branch>      Base branch for PR (default: main)
#   --msg "<message>"    Commit message (default: "chore: <branch>")
#   --include-untracked  Stage ALL changes incl. untracked (git add -A)
#   --no-tests           Skip pytest
#   --keep-branch        Keep branch after merge (default: delete)
#   --wait-merge         Poll until PR is merged, then update local main
#   --pause              Stop and wait for Enter before committing
#                        (legacy interactive mode)
#   --dry-run            Show what would happen; no push/PR/merge
#   -h, --help           Show this help and exit
#
# Environment:
#   GH_TOKEN             Auto-authenticate gh if not already logged in
#   BASE                 Alternative to --base
#   MSG                  Alternative to --msg
#
# Non-interactive by default:
#   The script runs end-to-end without any prompts.  It stops only for:
#     (a) secrets / auth missing that cannot be automated
#     (b) genuinely destructive operations not covered by flags
#   Use --pause to restore the legacy "wait for edits" prompt.
#
# Zero-diff guard:
#   If there is nothing to stage/commit, the script exits 0 immediately.
#   This prevents "No commits between HEAD and main" PR errors.
set -euo pipefail

# ---------------------------------------------------------------------------
# Repo root
# ---------------------------------------------------------------------------
REPO_ROOT="$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel 2>/dev/null || true)"
[[ -n "$REPO_ROOT" ]] || { echo "[ERR] Not inside a git repository." >&2; exit 1; }
cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
BASE="${BASE:-main}"
BR=""
MSG_OVERRIDE=""
INCLUDE_UNTRACKED=0
RUN_TESTS=1
KEEP_BRANCH=0
WAIT_MERGE=0
PAUSE=0
DRY_RUN=0

# ---------------------------------------------------------------------------
# Arg parsing
# ---------------------------------------------------------------------------
_usage() {
  sed -n '2,/^set -euo/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)            _usage ;;
    --base)               BASE="$2"; shift 2 ;;
    --msg)                MSG_OVERRIDE="$2"; shift 2 ;;
    --include-untracked)  INCLUDE_UNTRACKED=1; shift ;;
    --no-tests)           RUN_TESTS=0; shift ;;
    --keep-branch)        KEEP_BRANCH=1; shift ;;
    --wait-merge)         WAIT_MERGE=1; shift ;;
    --pause)              PAUSE=1; shift ;;
    --dry-run)            DRY_RUN=1; shift ;;
    -*)                   echo "[ERR] Unknown option: $1" >&2; exit 1 ;;
    *)                    BR="$1"; shift ;;
  esac
done

BR="${BR:-fix/$(date +%Y%m%d-%H%M%S)}"
MSG="${MSG_OVERRIDE:-${MSG:-chore: ${BR}}}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_info()  { echo "[INFO] $*"; }
_warn()  { echo "[WARN] $*" >&2; }
_err()   { echo "[ERR]  $*" >&2; exit 1; }
_dry()   { echo "[DRY]  $*"; }

_run() {
  if [[ "$DRY_RUN" -eq 1 ]]; then _dry "$*"; else "$@"; fi
}

# ---------------------------------------------------------------------------
# Zero-diff pre-flight: abort early if there is nothing to commit
# ---------------------------------------------------------------------------
_has_changes() {
  if [[ "$INCLUDE_UNTRACKED" -eq 1 ]]; then
    [[ -n "$(git status --porcelain)" ]]
  else
    # tracked modifications + staged changes
    [[ -n "$(git status --porcelain | grep -v '^??' || true)" ]]
  fi
}

if [[ "$DRY_RUN" -eq 0 ]] && ! _has_changes; then
  _info "差分ゼロ — コミットするものがありません。exit 0."
  exit 0
fi

# ---------------------------------------------------------------------------
# Summary (printed once; no mid-execution prompts)
# ---------------------------------------------------------------------------
echo "┌─────────────────────────────────────────────────────"
echo "│ pr_automerge.sh"
echo "│  branch   : $BR  →  $BASE"
echo "│  message  : $MSG"
echo "│  tests    : $([ "$RUN_TESTS" -eq 1 ] && echo yes || echo skipped)"
echo "│  untracked: $([ "$INCLUDE_UNTRACKED" -eq 1 ] && echo included || echo excluded)"
echo "│  wait     : $([ "$WAIT_MERGE" -eq 1 ] && echo yes || echo no)"
echo "│  dry-run  : $([ "$DRY_RUN" -eq 1 ] && echo YES || echo no)"
echo "└─────────────────────────────────────────────────────"

# --pause: legacy interactive mode
if [[ "$PAUSE" -eq 1 && "$DRY_RUN" -eq 0 ]]; then
  echo ""
  _info "編集を確認したら Enter を押してください..."
  read -r
fi

# ---------------------------------------------------------------------------
# gh auth — auto-login with GH_TOKEN if available and not already logged in
# ---------------------------------------------------------------------------
if ! gh auth status >/dev/null 2>&1; then
  if [[ -n "${GH_TOKEN:-}" ]]; then
    _info "gh not authenticated; logging in with GH_TOKEN..."
    gh auth login --with-token <<< "$GH_TOKEN"
  else
    _info "gh not authenticated; starting device flow..."
    gh auth login -h github.com -p https --web
  fi
fi

# ---------------------------------------------------------------------------
# Switch to base and fast-forward
# ---------------------------------------------------------------------------
CURRENT_BR="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$CURRENT_BR" != "$BASE" ]]; then
  _info "Switching from '$CURRENT_BR' to '$BASE'..."
  _run git switch "$BASE"
fi
_run git pull --ff-only origin "$BASE"

# Verify working tree is clean on base (can't create branch otherwise)
if [[ "$DRY_RUN" -eq 0 ]]; then
  STATUS_ON_BASE="$(git status --porcelain)"
  if [[ -n "$STATUS_ON_BASE" ]]; then
    # If the changes are the ones we intend to commit, that's fine — we'll
    # branch immediately and stage them.  But if we just switched branches
    # and there are uncommitted changes that don't belong here, warn loudly.
    _warn "Working tree has unstaged/untracked changes on '$BASE' — proceeding to branch."
  fi
fi

# ---------------------------------------------------------------------------
# Create branch
# ---------------------------------------------------------------------------
_run git switch -c "$BR"

# ---------------------------------------------------------------------------
# Stage
# ---------------------------------------------------------------------------
if [[ "$INCLUDE_UNTRACKED" -eq 1 ]]; then
  _info "Staging ALL changes including untracked (git add -A)..."
  _run git add -A
else
  _info "Staging tracked-file changes only (git add -u)..."
  _run git add -u
  UNTRACKED="$(git ls-files --others --exclude-standard)"
  if [[ -n "$UNTRACKED" ]]; then
    _warn "Untracked files NOT staged (use --include-untracked to include):"
    while IFS= read -r f; do _warn "  $f"; done <<< "$UNTRACKED"
  fi
fi

# Re-check: anything staged?
if [[ "$DRY_RUN" -eq 0 ]] && [[ -z "$(git diff --cached --name-only)" ]]; then
  _info "ステージングされた変更がありません — exit 0."
  git switch "$BASE" 2>/dev/null || true
  git branch -d "$BR" 2>/dev/null || true
  exit 0
fi

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if [[ "$RUN_TESTS" -eq 1 ]]; then
  _info "Running pytest..."
  if [[ "$DRY_RUN" -eq 0 ]]; then
    if [[ -x ./.venv/bin/python ]]; then
      ./.venv/bin/python -m pytest -q
    else
      python3 -m pytest -q
    fi
  else
    _dry ".venv/bin/python -m pytest -q"
  fi
else
  _warn "Tests skipped (--no-tests)"
fi

# ---------------------------------------------------------------------------
# Commit
# ---------------------------------------------------------------------------
_run git commit -m "$MSG"

# ---------------------------------------------------------------------------
# Push
# ---------------------------------------------------------------------------
_run git push -u origin "$BR"

# ---------------------------------------------------------------------------
# Create PR
# ---------------------------------------------------------------------------
_info "Creating PR: $BR → $BASE ..."
if [[ "$DRY_RUN" -eq 0 ]]; then
  PR_URL="$(gh pr create --fill --base "$BASE" --head "$BR" 2>&1 | tail -1)"
  _info "PR: $PR_URL"
else
  _dry "gh pr create --fill --base $BASE --head $BR"
  PR_URL="https://github.com/OWNER/REPO/pull/DRY"
fi

# ---------------------------------------------------------------------------
# Auto-merge
# ---------------------------------------------------------------------------
if [[ "$KEEP_BRANCH" -eq 1 ]]; then
  _run gh pr merge --auto --squash "$PR_URL"
else
  _run gh pr merge --auto --squash --delete-branch "$PR_URL"
fi

# ---------------------------------------------------------------------------
# Optional: wait for merge
# ---------------------------------------------------------------------------
if [[ "$WAIT_MERGE" -eq 1 && "$DRY_RUN" -eq 0 ]]; then
  _info "Waiting for PR to merge..."
  PR_NUM="${PR_URL##*/}"
  for i in $(seq 1 60); do
    STATE="$(gh pr view "$PR_NUM" --json state -q '.state' 2>/dev/null || echo '')"
    if [[ "$STATE" == "MERGED" ]]; then
      _info "PR #${PR_NUM} merged."
      break
    fi
    [[ "$((i % 6))" -eq 0 ]] && _info "  still waiting... (${i}×5s)"
    sleep 5
  done
  # Update local base branch
  git switch "$BASE"
  git pull --ff-only origin "$BASE"
  [[ "$KEEP_BRANCH" -eq 0 ]] && git branch -d "$BR" 2>/dev/null || true
  _info "Local '$BASE' updated."
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
_info "Done. Auto-merge set → CI green になり次第自動 merge されます。"
if [[ "$WAIT_MERGE" -eq 0 ]]; then
  echo "  merge 後: git switch $BASE && git pull --ff-only"
  [[ "$KEEP_BRANCH" -eq 0 ]] && echo "             git branch -d $BR"
fi
echo ""
echo "PR: $PR_URL"
