# spec_phase2.md — 因果quants "研究工場" v1（Phase 2）

Phase 1（Feature Store v1）を基盤として、
毎日18:00 JSTに **次営業日向け意思決定パッケージ** を自動生成・Slack投稿する。

---

## 1. 実行タイミング

| 実行タイミング | trade_date |
|---|---|
| 月〜木 18:00 JST | 翌営業日（祝日スキップ） |
| 金 18:00 JST | 翌週月曜（祝日スキップ） |
| 土日 18:00 JST | 翌週月曜（重複は上書き） |

- trade_date = `next_trade_date(as_of=today, calendar=JQuantsCalendar)`
- J-Quantsカレンダー（API）が取得できない場合は `jpholiday` + weekday フォールバック

---

## 2. データソース

| データ | 頻度 | 保存 |
|---|---|---|
| J-Quants 日足（Standard） | 日次 | `data/daily/` — Parquet、as_of別 |
| J-Quants 信用残（Standard） | 週次 | `data/margin/` — Parquet |
| 分足（watchlist_50限定） | 必要時取得 | `cache/minute_bars/<ticker>/<YYYYMMDD>.parquet` |

### 分足キャッシュ管理
- 保存期間：直近 `config.minute_cache_days`（default 20営業日）
- N日超のファイルは自動削除（`prune_minute_cache()`）
- 全銘柄×全期間の分足永続保存は**禁止**
- 分足から作成した日次要約特徴量は `data/minute_features/` に Parquet 永続保存

---

## 3. パイプライン（pipeline/runner.py）

```
ingest_daily(as_of)
  └─ J-QuantsLoader.fetch_daily(tickers, start, end)
  └─ save data/daily/<as_of>.parquet

build_features(bars, as_of)
  └─ Phase1 build_features (互換維持)
  └─ append forward_return_5d for training rows

build_watchlist(features, prev_watchlist, config)
  └─ score per ticker
  └─ rotation_limit (max_new=20, min_retained=30)

train_model(features_with_target, config)
  └─ Ridge or ElasticNet
  └─ target = forward_return_5d

run_quality_gates(model, features, predictions)
  └─ walk_forward
  └─ ticker_split_cv
  └─ cost_5bps / cost_15bps
  └─ param_stability
  └─ leak_detection
  └─ returns GateResult(all_passed, gate_details, rejection_reasons)

build_output(gate_result, watchlist, predictions, manifest)
  └─ decision_card_<trade_date>.json
  └─ watchlist_50_<trade_date>.csv
  └─ quality_report_<trade_date>.json
  └─ manifest_<run_id>.json
  └─ report_<trade_date>.md

notify(output_dir, trade_date)
  └─ POST to SLACK_WEBHOOK_URL
  └─ fallback: output/<trade_date>/slack_payload.json
```

---

## 4. CLI

```
# 本番実行
python -m inga_quant.cli run --as-of YYYY-MM-DD

# デモ（フィクスチャデータ使用、API不要）
python -m inga_quant.cli run --demo [--as-of YYYY-MM-DD] [--out <dir>]

# 分足キャッシュ削除
python -m inga_quant.cli prune-cache [--days N] [--cache-dir <dir>]
```

---

## 5. 出力（output/<trade_date>/）

### decision_card_<trade_date>.json
```json
{
  "schema_version": "2",
  "trade_date": "YYYY-MM-DD",
  "run_id": "YYYYMMDDTHHMMSS-<hash8>",
  "action": "TRADE" | "NO_TRADE",
  "no_trade_reasons": ["gate:walk_forward", ...],
  "top3": [
    {"rank": 1, "ticker": "7203", "score": 0.85, "reason_short": "strong momentum"},
    ...
  ],
  "key_metrics": {
    "confidence": 0.03,
    "wf_ic": 0.03,
    "n_eligible": 150,
    "missing_rate": 0.05
  }
}
```

### watchlist_50_<trade_date>.csv
列: `code, name, score, reason_short, is_new, turnover_penalty`

### quality_report_<trade_date>.json
```json
{
  "trade_date": "YYYY-MM-DD",
  "run_id": "...",
  "all_passed": true,
  "missing_rate": 0.05,
  "missing_rate_threshold": 0.20,
  "n_eligible": 150,
  "gates": {
    "walk_forward":     {"passed": true,  "ic": 0.032, "threshold": 0.01},
    "ticker_split_cv":  {"passed": true,  "ic": 0.025, "threshold": 0.00},
    "cost_5bps":        {"passed": true,  "net_return": 0.04},
    "cost_15bps":       {"passed": true,  "net_return": 0.01},
    "param_stability":  {"passed": true,  "cosine_sim": 0.88, "threshold": 0.70},
    "leak_detection":   {"passed": true,  "issues": []}
  },
  "rejection_reasons": []
}
```

### manifest_<run_id>.json
```json
{
  "run_id": "...",
  "code_hash": "f217755",
  "inputs_digest": "sha256:abc...",
  "data_asof": "YYYY-MM-DD",
  "trade_date": "YYYY-MM-DD",
  "params": {
    "model": "Ridge",
    "alpha": 1.0,
    "target": "forward_return_5d",
    "minute_cache_days": 20
  }
}
```

### report_<trade_date>.md
人間向け1枚レポ。action / top3 / gate summary / 品質サマリ。

### slack_payload.json（フォールバック）
Slack Webhook 未設定または送信失敗時に出力。

---

## 6. モデル

- アルゴリズム: Ridge（default）/ ElasticNet（config で切替）
- 目的変数: `forward_return_5d`（設定変更禁止）
- 特徴量: `config/signals_short.yaml` + `config/signals_mid.yaml` で列指定
- v1 で XGBoost 等は追加しない

---

## 7. 品質ゲート（NO_TRADE中核）

| Gate | 判定基準 | NG時 |
|---|---|---|
| walk_forward | rolling IC（Spearman） > 0.01 | NO_TRADE |
| ticker_split_cv | 銘柄外推IC > 0.00 | NO_TRADE |
| cost_5bps | net_return > 0（5bps コスト後） | NO_TRADE |
| cost_15bps | net_return > 0（15bps コスト後） | NO_TRADE |
| param_stability | coeff cosine_sim > 0.70 | NO_TRADE |
| leak_detection | 未来カラム/時系列分割違反なし | NO_TRADE |

追加 NO_TRADE 条件:
- 欠損率 > 20%
- 対象銘柄数 < 5
- 信頼度（WF IC）< 0.005

全 NO_TRADE 理由は `decision_card.no_trade_reasons` と `quality_report.rejection_reasons` に記録。

---

## 8. watchlist 回転制限

| パラメータ | default | config キー |
|---|---|---|
| max_new_entries | 20 | `watchlist.max_new` |
| min_retained | 30 | `watchlist.min_retained` |
| watchlist_size | 50 | `watchlist.size` |

前日 watchlist が存在しない場合（初回）: 制限なし。

---

## 9. 運用・堅牢性

- APIレート制限: 指数バックオフ（最大3回リトライ、base=1s, cap=30s）
- エラー分類: `connection_error` / `auth_error` / `rate_limit` / `data_error`
- ログ: `logs/run_<run_id>.log` + `logs/metrics_<run_id>.json`
- APIキーはログ出力禁止（.env のみ、git管理外）

---

## 10. テスト（受入条件）

| テスト | 確認内容 |
|---|---|
| test_trade_date.py | 平日/週末/祝日 の trade_date 計算 |
| test_prune_cache.py | N日超のキャッシュファイル自動削除 |
| test_watchlist.py | 回転制限（max_new/min_retained） |
| test_gates.py | 各ゲートの pass/fail ロジック |
| test_demo_e2e.py | demo E2E: 4点+md+slack_payload が生成される |

---

## 11. Makefile ターゲット

```
make setup                   # install deps
make ingest_daily            # J-Quants 日足取得
make ingest_margin           # J-Quants 信用取得
make build_features_daily    # Feature Store 実行
make build_watchlist         # watchlist 生成
make fetch_minute_cache      # 分足キャッシュ更新
make build_minute_features_daily  # 分足→日次要約
make train                   # モデル学習
make gate                    # 品質ゲート実行
make report                  # レポート生成
make slack                   # Slack 送信
make run                     # フルパイプライン（本番）
make demo                    # デモ実行（fixture使用）
make prune_cache             # 古いキャッシュ削除
make lint                    # ruff check
make test                    # pytest -q
```

---

## 12. 非機能要件（固定）

- 生分足の全銘柄永続保存: **禁止**
- 分足での全銘柄×全期間探索: **禁止**
- v1でモデル自動増加: **禁止**
- 欠損の黙示的 0 埋め: **禁止**（quality_flags に必ず反映）
- 受入未達での「動く」宣言: **禁止**
