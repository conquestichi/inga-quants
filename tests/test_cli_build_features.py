"""Integration test: CLI produces features_daily.parquet."""
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

BARS_PATH = Path(__file__).parent / "fixtures" / "bars_small.parquet"
AS_OF = "2026-02-10"


@pytest.fixture()
def out_dir(tmp_path):
    return tmp_path / "artifacts" / "latest"


def test_cli_creates_parquet(out_dir):
    result = subprocess.run(
        [
            sys.executable, "-m", "inga_quant.cli",
            "build-features",
            "--as-of", AS_OF,
            "--bars", str(BARS_PATH),
            "--out", str(out_dir),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"CLI failed (rc={result.returncode}):\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )
    out_file = out_dir / "features_daily.parquet"
    assert out_file.exists(), f"Output file not found: {out_file}"


def test_cli_output_has_rows(out_dir):
    subprocess.run(
        [
            sys.executable, "-m", "inga_quant.cli",
            "build-features",
            "--as-of", AS_OF,
            "--bars", str(BARS_PATH),
            "--out", str(out_dir),
        ],
        check=True,
        capture_output=True,
    )
    df = pd.read_parquet(out_dir / "features_daily.parquet")
    assert len(df) > 0


def test_cli_output_no_index_col(out_dir):
    """Parquet must not contain an '__index_level_0__' column (index=False)."""
    subprocess.run(
        [
            sys.executable, "-m", "inga_quant.cli",
            "build-features",
            "--as-of", AS_OF,
            "--bars", str(BARS_PATH),
            "--out", str(out_dir),
        ],
        check=True,
        capture_output=True,
    )
    df = pd.read_parquet(out_dir / "features_daily.parquet")
    assert "__index_level_0__" not in df.columns


def test_cli_quality_flags_valid_json(out_dir):
    subprocess.run(
        [
            sys.executable, "-m", "inga_quant.cli",
            "build-features",
            "--as-of", AS_OF,
            "--bars", str(BARS_PATH),
            "--out", str(out_dir),
        ],
        check=True,
        capture_output=True,
    )
    df = pd.read_parquet(out_dir / "features_daily.parquet")
    for val in df["quality_flags"]:
        flags = json.loads(val)
        assert isinstance(flags, list)


def test_cli_no_command_exits_nonzero():
    result = subprocess.run(
        [sys.executable, "-m", "inga_quant.cli"],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
