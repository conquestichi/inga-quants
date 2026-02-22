"""Internationalisation: Japanese (default) / English string lookup.

Usage::

    from inga_quant.ui.i18n import get as t
    heading = t("key_metrics_hd", lang)      # "## 主要指標" or "## Key Metrics"
"""
from __future__ import annotations

_JA: dict[str, str] = {
    # report.md headings
    "report_title":        "# inga-quant 日次レポート — {td_str}",
    "no_trade_reasons_hd": "## NO_TRADE 理由",
    "key_metrics_hd":      "## 主要指標",
    "quality_gates_hd":    "## 品質ゲート",
    "watchlist_hd":        "## ウォッチリスト トップ10",
    # table column labels
    "col_metric":  "指標",
    "col_value":   "値",
    "col_gate":    "ゲート",
    "col_result":  "結果",
    "col_rank":    "順位",
    "col_ticker":  "コード",
    "col_name":    "銘柄名",
    "col_score":   "スコア",
    "col_new":     "新規",
    "col_reason":  "理由",
    # metric labels
    "lbl_wf_ic":    "WF IC",
    "lbl_eligible": "対象銘柄数",
    "lbl_missing":  "欠損率",
    # status
    "pass":      "✓ 通過",
    "fail":      "✗ 不合格",
    "no_entries": "（エントリーなし）",
    # Slack
    "slack_title":       "inga-quant 日次レポート — {date}",
    "slack_action":      "アクション: *{action}*",
    "slack_metrics":     "WF IC: {wf_ic:.4f}  |  対象: {n_eligible}",
    "slack_top3_hd":     "トップ3:",
    "slack_reasons_hd":  "NO_TRADE 理由:",
    "slack_none":        "  なし",
}

_EN: dict[str, str] = {
    # report.md headings
    "report_title":        "# inga-quant Daily Report — {td_str}",
    "no_trade_reasons_hd": "## NO_TRADE Reasons",
    "key_metrics_hd":      "## Key Metrics",
    "quality_gates_hd":    "## Quality Gates",
    "watchlist_hd":        "## Watchlist Top 10",
    # table column labels
    "col_metric":  "Metric",
    "col_value":   "Value",
    "col_gate":    "Gate",
    "col_result":  "Result",
    "col_rank":    "Rank",
    "col_ticker":  "Ticker",
    "col_name":    "Name",
    "col_score":   "Score",
    "col_new":     "New?",
    "col_reason":  "Reason",
    # metric labels
    "lbl_wf_ic":    "WF IC",
    "lbl_eligible": "Eligible stocks",
    "lbl_missing":  "Missing rate",
    # status
    "pass":       "✓ PASS",
    "fail":       "✗ FAIL",
    "no_entries": "_(no watchlist entries)_",
    # Slack
    "slack_title":       "inga-quant daily report — {date}",
    "slack_action":      "Action: *{action}*",
    "slack_metrics":     "WF IC: {wf_ic:.4f}  |  Eligible: {n_eligible}",
    "slack_top3_hd":     "Top 3:",
    "slack_reasons_hd":  "NO_TRADE reasons:",
    "slack_none":        "  (none)",
}


def get(key: str, lang: str = "ja") -> str:
    """Return localised string for *key* in *lang* ('ja' or 'en').

    Falls back to the key itself if not found.
    """
    d = _JA if lang == "ja" else _EN
    return d.get(key, key)
