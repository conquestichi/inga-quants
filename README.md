# inga-quants

bootstrap ok

## PR 自動化 (`bin/pr_automerge.sh`)

Claude Code による編集セッションの後、新ブランチの作成・テスト・PR 作成・auto-merge を
ワンコマンドで実行します。

```bash
# 基本: tracked ファイルのみステージ → pytest → PR → auto-merge (ブランチ削除)
bin/pr_automerge.sh fix/my-topic

# ベースブランチ指定 & カスタムコミットメッセージ
bin/pr_automerge.sh feat/new-feature --base develop --msg "feat: add new signal"

# 未追跡ファイルも含める（data/, output/ など意図的に含めたい場合のみ）
bin/pr_automerge.sh fix/my-topic --include-untracked

# pytest をスキップ（緊急 hotfix など）
bin/pr_automerge.sh hotfix/urgent --no-tests

# ブランチを残す（merge 後も手元でブランチを保持）
bin/pr_automerge.sh fix/my-topic --keep-branch

# 何も実行せず、実行予定のコマンドだけ表示
bin/pr_automerge.sh fix/my-topic --dry-run
```

**デフォルト動作の安全策**

- `git add -u` で tracked ファイルのみステージ。`data/`・`output/` などの untracked ファイルを誤って commit しない。
- base ブランチが clean でない場合は即座にエラー停止。
- PR URL は `gh pr create --json url -q .url` で確実に取得（grep 依存なし）。

## CI (GitHub Actions)

`.github/workflows/ci.yml` が push / PR ごとに Python 3.12 + `pytest -q` を実行します。
GitHub リポジトリ設定で "Require status checks to pass" に `pytest (Python 3.12)` を追加すると、
CI グリーンを auto-merge の必須条件にできます。
