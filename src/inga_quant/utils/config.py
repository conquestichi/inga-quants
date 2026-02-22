"""Config loader: reads config/config.yaml (and signal YAMLs) into typed dicts."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


_DEFAULT_CONFIG = Path(__file__).parent.parent.parent.parent / "config" / "config.yaml"


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load main config YAML. Falls back to INGA_CONFIG env var, then default path."""
    if path is None:
        path = os.environ.get("INGA_CONFIG", str(_DEFAULT_CONFIG))
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with open(path) as f:
        cfg = yaml.safe_load(f)
    return cfg or {}


def load_signal_features(config_dir: str | Path | None = None) -> list[str]:
    """Return combined feature list from signals_short.yaml + signals_mid.yaml."""
    if config_dir is None:
        config_dir = Path(__file__).parent.parent.parent.parent / "config"
    config_dir = Path(config_dir)
    features: list[str] = []
    for fname in ("signals_short.yaml", "signals_mid.yaml"):
        p = config_dir / fname
        if p.exists():
            with open(p) as f:
                data = yaml.safe_load(f) or {}
            features.extend(data.get("features", []))
    return list(dict.fromkeys(features))  # deduplicate, preserve order
