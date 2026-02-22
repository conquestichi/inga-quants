#!/usr/bin/env bash
set -euo pipefail

cd /srv/inga-quants
BASE="${BASE:-main}"
BR="${1:-fix/$(date +%Y%m%d-%H%M%S)}"

# gh login（未ログインなら device code）
gh auth status >/dev/null 2>&1 || gh auth login -h github.com -p https --web

# main を最新へ
git fetch origin
git switch "$BASE"
git pull --ff-only

# 新ブランチ作成
git switch -c "$BR"

echo "[INFO] Claude Code で編集して、commit できる状態になったら Enter"
read -r

# 差分が無いなら中断（今回みたいに “No commits between ...” を防ぐ）
if [[ -z "$(git status --porcelain)" ]]; then
  echo "[ERR] 変更がありません（差分ゼロ）。Claude Code の編集 or 対象ブランチ確認。"
  exit 1
fi

# テスト
if [[ -x ./.venv/bin/python ]]; then
  ./.venv/bin/python -m pytest -q
else
  python3 -m pytest -q
fi

# commit（未コミットなら自動コミット）
git add -A
if [[ -n "$(git status --porcelain)" ]]; then
  MSG="${MSG:-fix: ${BR}}"
  git commit -m "$MSG"
fi

# push
git push -u origin "$BR"

# PR作成（URLを拾う）
PR_URL="$(gh pr create --fill --base "$BASE" --head "$BR" | grep -Eo 'https://github.com/[^ ]+' | tail -n1)"

echo "[INFO] PR: $PR_URL"

# Auto-merge（Squash + branch delete）
# ※必須チェック/レビューがある場合は「通るまで待機」になる
gh pr merge --auto --squash --delete-branch "$PR_URL"

echo "[OK] Auto-merge をセットしました（条件が揃うと自動で merge されます）"
