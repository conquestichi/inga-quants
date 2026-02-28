# inga-quants

bootstrap ok

## PR 自動化 (`bin/pr_automerge.sh`)

Claude Code による編集セッションの後、**ノンインタラクティブ**に
branch 作成 → テスト → PR 作成 → auto-merge セットまで一気に実行します。
途中の Enter 待ちや確認プロンプトはありません。

```bash
# 基本: tracked ファイルのみステージ → pytest → PR → auto-merge
bin/pr_automerge.sh fix/my-topic

# ベースブランチ指定 & カスタムコミットメッセージ
bin/pr_automerge.sh feat/new-feature --base develop --msg "feat: add new signal"

# 未追跡ファイルも含める（shutdown/bin/ など新規ファイルがある場合）
bin/pr_automerge.sh fix/my-topic --include-untracked

# CI完了 & merge まで待って、ローカル main を更新して終了
bin/pr_automerge.sh fix/my-topic --wait-merge

# pytest をスキップ
bin/pr_automerge.sh hotfix/urgent --no-tests

# 旧挙動: Enter 待ちプロンプトを出す（--pause）
bin/pr_automerge.sh fix/my-topic --pause

# 差分表示のみ（push/PR/merge しない）
bin/pr_automerge.sh fix/my-topic --dry-run
```

**安全策 / 非インタラクティブ保証**

- 差分ゼロなら即 exit 0（"No commits between HEAD and main" エラーを防止）。
- `git add -u` で tracked ファイルのみステージ（`data/`・`output/` を誤 commit しない）。
- `GH_TOKEN` があれば `--with-token` で自動ログイン。無い&未認証なら **exit 2 + 手順表示**（device flowに入らない）。
- `--pause` を指定した場合のみ Enter 待ちプロンプトを表示（デフォルト: 止まらない）。
- TTY なし（`ssh "bash -lc '...'"` / CI / PowerShell）でも完走する。

## Claude 作業規約（止まる条件）

Claude が作業を止めて確認するのは以下のみ：

| 条件 | 理由 |
|------|------|
| secrets / credentials が不足して進めない | 補完不可能 |
| 大量削除・データ消去・force push など不可逆操作 | 取り戻せない |
| 仕様判断が2択以上で結果が大きく変わる | 方針確認が必要 |

それ以外は **確認なしで PLAN→実装→テスト→PR→auto-merge→デプロイ手順提示** まで完走する。

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

# 4. ファイルのデプロイ — NOPASSWD sudo (推奨)
sudo inga-deploy-shutdown   # ← 初回セットアップ後はパスワード不要

# または手動 (root)
sudo cp shutdown/bin/inga_market_quotes_ingest_jq300.sh /srv/inga/SHUTDOWN/bin/
sudo cp shutdown/bin/inga_universe300_build.sh          /srv/inga/SHUTDOWN/bin/
sudo cp shutdown/bin/inga_weekly_digest_wrapper.sh      /srv/inga/SHUTDOWN/bin/
sudo chmod 750 /srv/inga/SHUTDOWN/bin/*.sh

# notify_digest.py もデプロイ (weekly-digest が script_missing にならないために必要)
sudo cp shutdown/tools/notify_digest.py /srv/inga/SHUTDOWN/bin/notify_digest.py
sudo chmod 640 /srv/inga/SHUTDOWN/bin/notify_digest.py

# 5. weekly-digest のシステム override (root) — inga-deploy-shutdown が自動実施
sudo mkdir -p /etc/systemd/system/inga-weekly-digest.service.d/
sudo cp shutdown/systemd/inga-weekly-digest.service.d/skip-wrapper.conf \
         /etc/systemd/system/inga-weekly-digest.service.d/
sudo systemctl daemon-reload
sudo systemctl reset-failed inga-weekly-digest.service

# 6. 動作確認
sudo systemctl start inga-weekly-digest.service
sudo systemctl --no-pager -l status inga-weekly-digest.service
systemctl --failed | grep inga
```

### NOPASSWD sudo の初回セットアップ (root で一度だけ)

```bash
sudo cp shutdown/deploy/inga-deploy-shutdown /usr/local/sbin/
sudo chown root:root /usr/local/sbin/inga-deploy-shutdown
sudo chmod 755 /usr/local/sbin/inga-deploy-shutdown
sudo cp shutdown/deploy/inga-sudoers-deploy /etc/sudoers.d/inga-deploy-shutdown
sudo chmod 440 /etc/sudoers.d/inga-deploy-shutdown
sudo visudo -c   # 文法チェック
```

以降は `sudo inga-deploy-shutdown` がパスワードなしで実行できます。

### テスト環境変数

スクリプトは以下の環境変数でパスを上書きできます (CI / root なし環境でのテスト用):

| 変数 | デフォルト | 用途 |
|------|-----------|------|
| `AS_OF` | 今日 (JST) | 営業日チェックの基準日 (YYYY-MM-DD) |
| `BASE` | `/srv/inga/SHUTDOWN` | スクリプトルートパス |
| `STATE` | `${BASE}/state` | ステータス TSV・エラーディレクトリの出力先 |
| `U300` | `${BASE}/conf/universe300.txt` | 銘柄ユニバースファイル |
| `DIGEST_SCRIPT` | `/root/inga-context-public/tools/notify_digest.py` | ラッパーが呼び出す Python スクリプト |
