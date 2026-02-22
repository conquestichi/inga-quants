"""
End-to-end demo test: runs full Phase 2 pipeline with fixture data.
Verifies that output/<trade_date>/ contains all 4 required files + report.md + slack_payload.json.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

BARS_PATH = Path(__file__).parent / "fixtures" / "bars_small.parquet"
AS_OF = "2026-02-10"


@pytest.fixture(scope="module")
def demo_out(tmp_path_factory) -> Path:
    """Run demo pipeline once; return output directory."""
    out_base = tmp_path_factory.mktemp("demo_output")
    # Pre-set SLACK_WEBHOOK_URL to empty so dotenv auto-load won't overwrite it
    # and send_slack() falls through to write the fallback file.
    env = {**os.environ, "SLACK_WEBHOOK_URL": ""}
    result = subprocess.run(
        [
            sys.executable, "-m", "inga_quant.cli",
            "run",
            "--demo",
            "--as-of", AS_OF,
            "--out", str(out_base),
        ],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"Demo run failed (rc={result.returncode}):\n"
        f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )
    # Find the trade_date directory (exclude the latest symlink)
    subdirs = [d for d in out_base.iterdir() if d.is_dir() and not d.is_symlink()]
    assert len(subdirs) == 1, f"Expected exactly 1 output subdir, got: {[d.name for d in subdirs]}"
    return subdirs[0]


def test_output_dir_exists(demo_out):
    assert demo_out.exists()


def test_decision_card_exists(demo_out):
    cards = list(demo_out.glob("decision_card_*.json"))
    assert len(cards) == 1, f"Expected 1 decision_card, found {[f.name for f in cards]}"


def test_watchlist_csv_exists(demo_out):
    csvs = list(demo_out.glob("watchlist_50_*.csv"))
    assert len(csvs) == 1, f"Expected 1 watchlist_50 CSV, found {[f.name for f in csvs]}"


def test_quality_report_exists(demo_out):
    reports = list(demo_out.glob("quality_report_*.json"))
    assert len(reports) == 1


def test_manifest_exists(demo_out):
    manifests = list(demo_out.glob("manifest_*.json"))
    assert len(manifests) == 1


def test_report_md_exists(demo_out):
    mds = list(demo_out.glob("report_*.md"))
    assert len(mds) == 1


def test_slack_payload_exists(demo_out):
    """Slack URL not set in test env → fallback file must exist."""
    fallback = demo_out / "slack_payload.json"
    assert fallback.exists(), "slack_payload.json not found (fallback should have been written)"


def test_decision_card_schema(demo_out):
    card_path = next(demo_out.glob("decision_card_*.json"))
    card = json.loads(card_path.read_text())
    assert card["schema_version"] == "2"
    assert card["action"] in ("TRADE", "NO_TRADE")
    assert "trade_date" in card
    assert "run_id" in card
    assert "no_trade_reasons" in card
    assert isinstance(card["no_trade_reasons"], list)
    assert "top3" in card
    assert isinstance(card["top3"], list)
    assert "key_metrics" in card
    assert "wf_ic" in card["key_metrics"]


def test_quality_report_schema(demo_out):
    report_path = next(demo_out.glob("quality_report_*.json"))
    report = json.loads(report_path.read_text())
    assert "all_passed" in report
    assert "gates" in report
    required_gates = {"walk_forward", "ticker_split_cv", "param_stability", "leak_detection"}
    for gate in required_gates:
        assert gate in report["gates"], f"Missing gate in quality_report: {gate}"
        assert "passed" in report["gates"][gate]
    assert "rejection_reasons" in report
    assert isinstance(report["rejection_reasons"], list)


def test_manifest_schema(demo_out):
    manifest_path = next(demo_out.glob("manifest_*.json"))
    manifest = json.loads(manifest_path.read_text())
    for key in ("run_id", "code_hash", "inputs_digest", "data_asof", "trade_date", "params"):
        assert key in manifest, f"Missing key in manifest: {key}"
    assert manifest["params"]["target"] == "forward_return_5d"


def test_watchlist_csv_has_headers(demo_out):
    import csv
    csv_path = next(demo_out.glob("watchlist_50_*.csv"))
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames
    expected = {"code", "name", "score", "reason_short", "is_new", "turnover_penalty"}
    assert expected.issubset(set(fields or [])), f"Missing CSV columns: {expected - set(fields or [])}"


def test_report_md_contains_action(demo_out):
    md_path = next(demo_out.glob("report_*.md"))
    content = md_path.read_text()
    assert "TRADE" in content or "NO_TRADE" in content


def test_slack_payload_has_text(demo_out):
    fallback = demo_out / "slack_payload.json"
    payload = json.loads(fallback.read_text())
    assert "text" in payload
    assert len(payload["text"]) > 0


def test_dates_consistent(demo_out):
    """trade_date in decision_card == trade_date in quality_report."""
    card = json.loads(next(demo_out.glob("decision_card_*.json")).read_text())
    report = json.loads(next(demo_out.glob("quality_report_*.json")).read_text())
    assert card["trade_date"] == report["trade_date"]


def test_latest_symlink_exists(demo_out):
    """output/latest must be a symlink pointing to the trade_date directory."""
    latest = demo_out.parent / "latest"
    assert latest.is_symlink(), "output/latest symlink must be created after run"
    assert latest.resolve() == demo_out.resolve(), "latest must resolve to the current trade_date dir"


def test_manifest_json_exists(demo_out):
    assert (demo_out / "manifest.json").exists(), "stable manifest.json must be written"


def test_manifest_json_has_required_fields(demo_out):
    manifest = json.loads((demo_out / "manifest.json").read_text())
    for key in ("as_of", "trade_date", "generated_at_jst"):
        assert key in manifest, f"Missing required field in manifest.json: {key}"


def test_slack_payload_is_japanese(demo_out):
    payload = json.loads((demo_out / "slack_payload.json").read_text())
    assert "アクション" in payload["text"], "Slack message must contain Japanese 'アクション' label"


def test_report_md_japanese_headings(demo_out):
    md = next(demo_out.glob("report_*.md")).read_text()
    assert "日次レポート" in md or "主要指標" in md, "report.md must contain Japanese headings"


def test_decision_card_top3_has_name(demo_out):
    card = json.loads(next(demo_out.glob("decision_card_*.json")).read_text())
    for item in card["top3"]:
        assert "name" in item, f"decision_card top3 item missing 'name': {item}"


def test_phase1_cli_still_works(tmp_path):
    """Phase 1 build-features CLI must still work after Phase 2 changes."""
    result = subprocess.run(
        [
            sys.executable, "-m", "inga_quant.cli",
            "build-features",
            "--as-of", AS_OF,
            "--bars", str(BARS_PATH),
            "--out", str(tmp_path / "features"),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Phase1 CLI failed: {result.stderr}"
    assert (tmp_path / "features" / "features_daily.parquet").exists()
