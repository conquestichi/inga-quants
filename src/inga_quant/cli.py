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


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="inga_quant.cli")
    sub = parser.add_subparsers(dest="command")

    # build-features
    p_bf = sub.add_parser("build-features", help="Build features_daily.parquet")
    p_bf.add_argument("--as-of", required=True, help="Cutoff date YYYY-MM-DD")
    p_bf.add_argument("--bars", required=True, help="Path to bars_daily (CSV or Parquet)")
    p_bf.add_argument("--events", default=None, help="Optional path to events (CSV or Parquet)")
    p_bf.add_argument("--out", required=True, help="Output directory")

    args = parser.parse_args(argv)

    if args.command == "build-features":
        sys.exit(_cmd_build_features(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
