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

## SHUTDOWN スクリプト (`shutdown/bin/`)

`/srv/inga/SHUTDOWN/bin/` にデプロイされる bash スクリプト群です。
inga-quants リポジトリで管理し、`sudo cp` でデプロイします。

### exit code 規約

| code | 意味 | systemd への影響 |
|------|------|-----------------|
| 0    | OK または SKIP | success |
| 1    | FAIL (BQ エラー、ファイル欠損など) | failed (**要調査**) |

SKIP 条件 (exit 0):
- `api_key_missing` — `JQ_API_KEY` が未設定
- `non_trading_day` — 週末または JP 祝日 (jpholiday で判定)
- `no_data` — API から十分なデータが取得できなかった

### スクリプト一覧

| ファイル | 役割 | systemd unit |
|---------|------|-------------|
| `inga_market_quotes_ingest_jq300.sh` | J-Quants 日次バー → BigQuery | `inga-market-quotes-ingest.service` |
| `inga_universe300_build.sh` | 銘柄マスター更新 (日曜) | `inga-universe300-build.service` |
| `inga_weekly_digest_wrapper.sh` | notify_digest.py のラッパー (非ゼロ exit を SKIP に変換) | `inga-weekly-digest.service` |

### デプロイ手順

```bash
# 1. syntax check
bash -n shutdown/bin/inga_market_quotes_ingest_jq300.sh
bash -n shutdown/bin/inga_universe300_build.sh
bash -n shutdown/bin/inga_weekly_digest_wrapper.sh

# 2. SKIP テスト (ネット不要)
JQ_API_KEY="" bash shutdown/bin/inga_market_quotes_ingest_jq300.sh
# → [SKIP] reason=api_key_missing

JQ_API_KEY="" bash shutdown/bin/inga_universe300_build.sh
# → [SKIP] reason=api_key_missing

# 3. dry-run
JQ_API_KEY=xxx AS_OF=2026-02-10 bash shutdown/bin/inga_market_quotes_ingest_jq300.sh --dry-run

# 4. ファイルのデプロイ (root)
sudo cp shutdown/bin/inga_market_quotes_ingest_jq300.sh /srv/inga/SHUTDOWN/bin/
sudo cp shutdown/bin/inga_universe300_build.sh          /srv/inga/SHUTDOWN/bin/
sudo cp shutdown/bin/inga_weekly_digest_wrapper.sh      /srv/inga/SHUTDOWN/bin/
sudo chmod 750 /srv/inga/SHUTDOWN/bin/*.sh

# 5. weekly-digest のシステム override (root)
sudo mkdir -p /etc/systemd/system/inga-weekly-digest.service.d/
sudo cp shutdown/systemd/inga-weekly-digest.service.d/skip-wrapper.conf \
         /etc/systemd/system/inga-weekly-digest.service.d/
sudo systemctl daemon-reload
sudo systemctl reset-failed inga-weekly-digest.service

# 6. 動作確認
systemctl start inga-weekly-digest.service
systemctl --failed | grep inga
```

### テスト環境変数

スクリプトは以下の環境変数でパスを上書きできます (CI / root なし環境でのテスト用):

| 変数 | デフォルト | 用途 |
|------|-----------|------|
| `AS_OF` | 今日 (JST) | 営業日チェックの基準日 (YYYY-MM-DD) |
| `BASE` | `/srv/inga/SHUTDOWN` | スクリプトルートパス |
| `STATE` | `${BASE}/state` | ステータス TSV・エラーディレクトリの出力先 |
| `U300` | `${BASE}/conf/universe300.txt` | 銘柄ユニバースファイル |
| `DIGEST_SCRIPT` | `/root/inga-context-public/tools/notify_digest.py` | ラッパーが呼び出す Python スクリプト |
