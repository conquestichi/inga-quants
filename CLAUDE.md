# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`inga-quants` is a Python quantitative finance package (currently in bootstrap stage). It uses a `src/` layout with `setuptools` and targets Python >=3.11.

## Development Setup

```bash
# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

## Commands

```bash
# Run all tests
pytest

# Run a single test
pytest tests/test_smoke.py::test_main

# Lint
ruff check .

# Format
ruff format .
```

## Architecture

- `src/inga_quants/` — Main package
  - `cli.py` — CLI entry point (`main()` function)
  - `__init__.py` — Package exports
- `tests/` — pytest tests (uses `capsys` for output capture)

Pytest is configured to add `src/` to `PYTHONPATH` via `pyproject.toml`, so imports work without manual path manipulation.