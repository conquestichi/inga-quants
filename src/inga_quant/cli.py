"""CLI entry point for inga-quant."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _cmd_build_features(args: argparse.Namespace) -> int:
    from inga_quant.features.build_features import build_features
    from inga_quant.utils.io import load_bars, load_events, save_parquet

    bars = load_bars(args.bars, as_of=args.as_of)
    events = None
    if args.events:
        events = load_events(args.events)

    df = build_features(bars, events=events)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "features_daily.parquet"
    save_parquet(df, out_path)
    print(f"Written {len(df)} rows → {out_path}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    import logging
    from datetime import date, datetime

    from inga_quant.pipeline.ingest import DemoLoader, JQuantsLoader
    from inga_quant.pipeline.runner import run_pipeline

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.as_of:
        as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date()
    else:
        as_of = date.today()

    if args.demo:
        fixture = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "bars_small.parquet"
        if not fixture.exists():
            print(f"Demo fixture not found: {fixture}", file=sys.stderr)
            return 1
        loader = DemoLoader(bars_path=fixture)
        bars_path = fixture
    else:
        loader = JQuantsLoader()
        bars_path = None

    out_base = Path(args.out) if args.out else None
    out_dir = run_pipeline(
        as_of=as_of,
        loader=loader,
        bars_path=bars_path,
        out_base=out_base,
        config_path=Path(args.config) if args.config else None,
    )
    print(f"Output: {out_dir}")
    return 0


def _cmd_prune_cache(args: argparse.Namespace) -> int:
    from inga_quant.utils.cache import prune_minute_cache

    cache_dir = Path(args.cache_dir)
    deleted = prune_minute_cache(cache_dir, keep_days=args.days)
    print(f"Pruned {len(deleted)} file(s) from {cache_dir}")
    return 0


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="inga_quant.cli")
    sub = parser.add_subparsers(dest="command")

    # build-features (Phase 1 — preserved)
    p_bf = sub.add_parser("build-features", help="Build features_daily.parquet (Phase 1)")
    p_bf.add_argument("--as-of", required=True, help="Cutoff date YYYY-MM-DD")
    p_bf.add_argument("--bars", required=True, help="Path to bars_daily (CSV or Parquet)")
    p_bf.add_argument("--events", default=None, help="Optional path to events (CSV or Parquet)")
    p_bf.add_argument("--out", required=True, help="Output directory")

    # run (Phase 2)
    p_run = sub.add_parser("run", help="Run full Phase 2 pipeline (daily report)")
    p_run.add_argument("--as-of", default=None, help="as-of date YYYY-MM-DD (default: today)")
    p_run.add_argument("--demo", action="store_true", help="Use fixture data (no API calls)")
    p_run.add_argument("--out", default=None, help="Output base directory")
    p_run.add_argument("--config", default=None, help="Path to config YAML")

    # prune-cache (Phase 2)
    p_prune = sub.add_parser("prune-cache", help="Prune old minute-bar cache files")
    p_prune.add_argument("--days", type=int, default=20, help="Keep this many business days")
    p_prune.add_argument("--cache-dir", default="cache/minute_bars", help="Cache directory path")

    args = parser.parse_args(argv)

    if args.command == "build-features":
        sys.exit(_cmd_build_features(args))
    elif args.command == "run":
        sys.exit(_cmd_run(args))
    elif args.command == "prune-cache":
        sys.exit(_cmd_prune_cache(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
