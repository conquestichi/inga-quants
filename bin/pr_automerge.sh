#!/usr/bin/env bash
# bin/pr_automerge.sh — safe, reproducible PR automation for inga-quants
#
# Usage:
#   bin/pr_automerge.sh <BRANCH_PREFIX> [OPTIONS]
#
# Arguments:
#   BRANCH_PREFIX        New branch name (e.g. fix/some-topic)
#                        Defaults to fix/<YYYYMMDD-HHMMSS> if omitted.
#
# Options:
#   --base <branch>      Base branch to create PR against (default: main)
#   --msg "<message>"    Commit message (default: "chore: <branch>")
#   --include-untracked  Stage ALL changes incl. untracked (git add -A).
#                        Default: only tracked-file changes (git add -u).
#   --no-tests           Skip pytest run
#   --keep-branch        Keep branch after merge (default: delete it)
#   --dry-run            Show what would happen; no push/PR/merge
#   -h, --help           Show this help and exit
#
# Environment:
#   BASE                 Alternative to --base (env var; flag takes priority)
#   MSG                  Alternative to --msg (env var; flag takes priority)
set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
REPO_ROOT="$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$REPO_ROOT" ]]; then
  echo "[ERR] Not inside a git repository." >&2
  exit 1
fi
cd "$REPO_ROOT"

BASE="${BASE:-main}"
BR=""
MSG_OVERRIDE=""
INCLUDE_UNTRACKED=0
RUN_TESTS=1
KEEP_BRANCH=0
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
    -h|--help)          _usage ;;
    --base)             BASE="$2"; shift 2 ;;
    --msg)              MSG_OVERRIDE="$2"; shift 2 ;;
    --include-untracked) INCLUDE_UNTRACKED=1; shift ;;
    --no-tests)         RUN_TESTS=0; shift ;;
    --keep-branch)      KEEP_BRANCH=1; shift ;;
    --dry-run)          DRY_RUN=1; shift ;;
    -*)                 echo "[ERR] Unknown option: $1" >&2; exit 1 ;;
    *)                  BR="$1"; shift ;;
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
  if [[ "$DRY_RUN" -eq 1 ]]; then
    _dry "$*"
  else
    "$@"
  fi
}

# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------
_info "Branch:  $BR"
_info "Base:    $BASE"
_info "Message: $MSG"
[[ "$INCLUDE_UNTRACKED" -eq 1 ]] && _warn "Untracked files WILL be staged (--include-untracked)"
[[ "$DRY_RUN"           -eq 1 ]] && _info "DRY-RUN mode — no push/PR/merge will happen"

# gh auth check
if ! gh auth status >/dev/null 2>&1; then
  _info "gh not authenticated; attempting device login..."
  gh auth login -h github.com -p https --web
fi

# Switch to base and fast-forward pull
_info "Switching to '$BASE' and pulling..."
git switch "$BASE"
git pull --ff-only origin "$BASE"

# Working tree must be clean before creating new branch
STATUS="$(git status --porcelain)"
if [[ -n "$STATUS" ]]; then
  _err "Working tree is not clean on '$BASE'. Commit or stash first:\n$STATUS"
fi

# ---------------------------------------------------------------------------
# Create branch
# ---------------------------------------------------------------------------
_run git switch -c "$BR"

# ---------------------------------------------------------------------------
# Prompt: let Claude Code (or user) make edits
# ---------------------------------------------------------------------------
_info "Claude Code の編集をここで行ってください。"
_info "コミット可能な状態になったら Enter を押してください..."
if [[ "$DRY_RUN" -eq 0 ]]; then
  read -r
fi

# ---------------------------------------------------------------------------
# Check there is something to commit
# ---------------------------------------------------------------------------
if [[ -z "$(git status --porcelain)" ]]; then
  _err "変更がありません（差分ゼロ）。編集後に実行してください。"
fi

# ---------------------------------------------------------------------------
# Stage
# ---------------------------------------------------------------------------
if [[ "$INCLUDE_UNTRACKED" -eq 1 ]]; then
  _info "Staging ALL changes including untracked (git add -A)..."
  _run git add -A
else
  _info "Staging tracked-file changes only (git add -u)..."
  _run git add -u
  # Warn if there are untracked files that were NOT staged
  UNTRACKED="$(git ls-files --others --exclude-standard)"
  if [[ -n "$UNTRACKED" ]]; then
    _warn "Untracked files were NOT staged (use --include-untracked to include them):"
    while IFS= read -r f; do _warn "  $f"; done <<< "$UNTRACKED"
  fi
fi

# After staging, re-check there is something to commit
if [[ -z "$(git diff --cached --name-only)" ]]; then
  _err "ステージングされた変更がありません。--include-untracked を検討してください。"
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
if [[ -n "$(git status --porcelain)" ]]; then
  _run git commit -m "$MSG"
fi

# ---------------------------------------------------------------------------
# Push
# ---------------------------------------------------------------------------
_run git push -u origin "$BR"

# ---------------------------------------------------------------------------
# Create PR
# ---------------------------------------------------------------------------
_info "Creating PR: $BR → $BASE ..."
if [[ "$DRY_RUN" -eq 0 ]]; then
  PR_URL="$(gh pr create --fill --base "$BASE" --head "$BR" --json url -q '.url')"
  _info "PR: $PR_URL"
else
  _dry "gh pr create --fill --base $BASE --head $BR --json url -q '.url'"
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
# Done
# ---------------------------------------------------------------------------
echo ""
_info "Auto-merge をセットしました（必須チェックが通ると自動で merge されます）"
echo ""
echo "次のコマンド（merge完了後）:"
echo "  git switch $BASE"
echo "  git pull --ff-only"
if [[ "$KEEP_BRANCH" -eq 0 ]]; then
  echo "  git branch -d $BR  # ローカルブランチ削除"
fi
