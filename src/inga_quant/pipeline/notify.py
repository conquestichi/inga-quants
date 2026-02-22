"""Slack notification with fallback to slack_payload.json."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import requests

from inga_quant.ui.i18n import get as t

logger = logging.getLogger(__name__)


def build_slack_payload(
    trade_date: str,
    run_id: str,
    action: str,
    wf_ic: float,
    n_eligible: int,
    no_trade_reasons: list[str],
    top3: list[dict[str, Any]],
    lang: str = "ja",
) -> dict[str, Any]:
    """Build Slack message payload."""
    icon = ":white_check_mark:" if action == "TRADE" else ":no_entry:"

    top3_lines = []
    for e in top3:
        name_part = f" {e['name']}" if e.get("name") and e["name"] != e["ticker"] else ""
        top3_lines.append(
            f"  {e['rank']}. {e['ticker']}{name_part}  score={e['score']:.4f}  {e['reason_short']}"
        )
    top3_text = "\n".join(top3_lines) or t("slack_none", lang)

    reasons_text = (
        "\n".join(f"  • {r}" for r in no_trade_reasons) or t("slack_none", lang)
    )

    text = (
        f"{icon} *{t('slack_title', lang).format(date=trade_date)}*\n"
        f"{t('slack_action', lang).format(action=action)}\n"
        f"{t('slack_metrics', lang).format(wf_ic=wf_ic, n_eligible=n_eligible)}\n"
        f"{t('slack_top3_hd', lang)}\n{top3_text}\n"
        f"{t('slack_reasons_hd', lang)}\n{reasons_text}"
    )
    return {"text": text}


def send_slack(
    payload: dict[str, Any],
    webhook_url: str | None = None,
    fallback_path: Path | None = None,
) -> bool:
    """
    POST payload to Slack webhook.
    Returns True on success.
    On failure (or if webhook_url unset), writes to fallback_path.
    Never raises — always returns a bool.
    """
    webhook_url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL", "")

    if webhook_url:
        try:
            resp = requests.post(webhook_url, json=payload, timeout=10)
            resp.raise_for_status()
            logger.info("Slack notification sent successfully")
            return True
        except requests.RequestException as exc:
            logger.warning("Slack POST failed: %s — writing fallback", exc)

    # Fallback: write to file
    if fallback_path:
        fallback_path.parent.mkdir(parents=True, exist_ok=True)
        with open(fallback_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        logger.info("Slack payload written to fallback: %s", fallback_path)
    else:
        logger.warning("No fallback path provided and Slack send failed")

    return False
