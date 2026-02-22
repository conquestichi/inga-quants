"""Output generation: decision_card, watchlist_50, quality_report, manifest, report.md."""
from __future__ import annotations

import csv
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from inga_quant.pipeline.gates import AllGatesResult
from inga_quant.pipeline.watchlist import WatchlistEntry
from inga_quant.ui.i18n import get as t

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "2"


def write_outputs(
    out_dir: Path,
    trade_date: date,
    run_id: str,
    gate_result: AllGatesResult,
    watchlist: list[WatchlistEntry],
    manifest: dict[str, Any],
    wf_ic: float = 0.0,
    lang: str = "ja",
) -> dict[str, Path]:
    """Write all Phase 2 output files. Returns {name: path} dict."""
    out_dir.mkdir(parents=True, exist_ok=True)
    td_str = trade_date.strftime("%Y-%m-%d")
    paths: dict[str, Path] = {}

    paths["decision_card"] = _write_decision_card(out_dir, td_str, run_id, gate_result, watchlist, wf_ic)
    paths["watchlist_50"] = _write_watchlist_csv(out_dir, td_str, watchlist)
    paths["quality_report"] = _write_quality_report(out_dir, td_str, run_id, gate_result)
    paths["manifest"] = _write_manifest(out_dir, run_id, manifest)
    paths["report_md"] = _write_report_md(out_dir, td_str, run_id, gate_result, watchlist, wf_ic, lang)

    return paths


def _write_decision_card(
    out_dir: Path,
    td_str: str,
    run_id: str,
    gate_result: AllGatesResult,
    watchlist: list[WatchlistEntry],
    wf_ic: float,
) -> Path:
    action = "TRADE" if gate_result.all_passed else "NO_TRADE"
    top3 = [
        {
            "rank": i + 1,
            "ticker": e.ticker,
            "name": e.name,
            "score": round(e.score, 6),
            "reason_short": e.reason_short,
        }
        for i, e in enumerate(watchlist[:3])
    ]
    card = {
        "schema_version": SCHEMA_VERSION,
        "trade_date": td_str,
        "run_id": run_id,
        "action": action,
        "no_trade_reasons": gate_result.rejection_reasons,
        "top3": top3,
        "key_metrics": {
            "confidence": round(wf_ic, 6),
            "wf_ic": round(wf_ic, 6),
            "n_eligible": gate_result.n_eligible,
            "missing_rate": round(gate_result.missing_rate, 4),
        },
    }
    path = out_dir / f"decision_card_{td_str}.json"
    _write_json(path, card)
    logger.info("Written decision_card: %s (action=%s)", path, action)
    return path


def _write_watchlist_csv(
    out_dir: Path,
    td_str: str,
    watchlist: list[WatchlistEntry],
) -> Path:
    path = out_dir / f"watchlist_50_{td_str}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["code", "name", "score", "reason_short", "is_new", "turnover_penalty"],
        )
        writer.writeheader()
        for e in watchlist:
            writer.writerow({
                "code": e.ticker,
                "name": e.name,
                "score": round(e.score, 6),
                "reason_short": e.reason_short,
                "is_new": int(e.is_new),
                "turnover_penalty": round(e.turnover_penalty, 6),
            })
    logger.info("Written watchlist_50: %s (%d entries)", path, len(watchlist))
    return path


def _write_quality_report(
    out_dir: Path,
    td_str: str,
    run_id: str,
    gate_result: AllGatesResult,
) -> Path:
    report = {
        "trade_date": td_str,
        "run_id": run_id,
        "all_passed": gate_result.all_passed,
        "missing_rate": round(gate_result.missing_rate, 4),
        "n_eligible": gate_result.n_eligible,
        "gates": {
            name: {"passed": r.passed, "reason": r.reason, **r.details}
            for name, r in gate_result.gates.items()
        },
        "rejection_reasons": gate_result.rejection_reasons,
    }
    path = out_dir / f"quality_report_{td_str}.json"
    _write_json(path, report)
    logger.info("Written quality_report: %s (all_passed=%s)", path, gate_result.all_passed)
    return path


def _write_manifest(out_dir: Path, run_id: str, manifest: dict[str, Any]) -> Path:
    path = out_dir / f"manifest_{run_id}.json"
    _write_json(path, manifest)
    # Also write a stable manifest.json so output/latest/manifest.json always works
    _write_json(out_dir / "manifest.json", manifest)
    logger.info("Written manifest: %s", path)
    return path


def _write_report_md(
    out_dir: Path,
    td_str: str,
    run_id: str,
    gate_result: AllGatesResult,
    watchlist: list[WatchlistEntry],
    wf_ic: float,
    lang: str = "ja",
) -> Path:
    action = "TRADE" if gate_result.all_passed else "NO_TRADE"
    lines = [
        t("report_title", lang).format(td_str=td_str),
        "",
        f"**run_id**: `{run_id}`",
        f"**action**: **{action}**",
        "",
    ]
    if gate_result.rejection_reasons:
        lines += [t("no_trade_reasons_hd", lang), ""]
        for r in gate_result.rejection_reasons:
            lines.append(f"- {r}")
        lines.append("")

    col_m = t("col_metric", lang)
    col_v = t("col_value", lang)
    lines += [
        t("key_metrics_hd", lang),
        "",
        f"| {col_m} | {col_v} |",
        f"|{'—' * (len(col_m) + 2)}|{'—' * (len(col_v) + 2)}|",
        f"| {t('lbl_wf_ic', lang)} | {wf_ic:.4f} |",
        f"| {t('lbl_eligible', lang)} | {gate_result.n_eligible} |",
        f"| {t('lbl_missing', lang)} | {gate_result.missing_rate:.1%} |",
        "",
        t("quality_gates_hd", lang),
        "",
        f"| {t('col_gate', lang)} | {t('col_result', lang)} |",
        f"|{'—' * (len(t('col_gate', lang)) + 2)}|{'—' * (len(t('col_result', lang)) + 2)}|",
    ]
    for name, r in gate_result.gates.items():
        status = t("pass", lang) if r.passed else t("fail", lang)
        lines.append(f"| {name} | {status} |")

    lines += ["", t("watchlist_hd", lang), ""]
    if watchlist:
        r_rank = t("col_rank", lang)
        r_code = t("col_ticker", lang)
        r_name = t("col_name", lang)
        r_score = t("col_score", lang)
        r_new = t("col_new", lang)
        r_reason = t("col_reason", lang)
        lines.append(f"| {r_rank} | {r_code} | {r_name} | {r_score} | {r_new} | {r_reason} |")
        lines.append(f"|{'—'*4}|{'—'*6}|{'—'*8}|{'—'*7}|{'—'*4}|{'—'*6}|")
        for i, e in enumerate(watchlist[:10], 1):
            new_marker = "★" if e.is_new else ""
            lines.append(
                f"| {i} | {e.ticker} | {e.name} | {e.score:.4f} | {new_marker} | {e.reason_short} |"
            )
    else:
        lines.append(t("no_entries", lang))

    path = out_dir / f"report_{td_str}.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Written report: %s", path)
    return path


def _write_json(path: Path, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
