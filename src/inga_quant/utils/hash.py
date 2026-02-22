"""Utilities for computing reproducibility hashes."""
from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path


def code_hash() -> str:
    """Return the current git HEAD short hash, or 'unknown' if not in a git repo."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def file_digest(path: str | Path, algorithm: str = "sha256") -> str:
    """Compute hex digest of a file."""
    h = hashlib.new(algorithm)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return f"{algorithm}:{h.hexdigest()}"


def inputs_digest(*paths: str | Path, algorithm: str = "sha256") -> str:
    """Compute a combined digest over multiple input files."""
    h = hashlib.new(algorithm)
    for path in sorted(str(p) for p in paths):
        p = Path(path)
        if p.exists():
            h.update(str(p).encode())
            h.update(file_digest(p, algorithm).encode())
    return f"{algorithm}:{h.hexdigest()}"
