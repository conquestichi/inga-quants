"""CLI entry point for inga-quant."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def _load_dotenv_if_present() -> None:
    """
    Load .env file into os.environ (setdefault — never overwrites existing vars).

    Search order: ./.env → <repo-root>/.env (parent containing pyproject.toml).
    Parses KEY=VALUE lines; skips blank lines and # comments.
    Strips surrounding whitespace and quotes (" or ') from values.
    Logs one INFO line with key names only — never values.
    """
    candidates: list[Path] = [Path(".env")]

    # Walk up to find repo root (contains pyproject.toml)
    here = Path(__file__).resolve().parent
    for parent in [here, *here.parents]:
        if (parent / "pyproject.toml").exists():
            repo_env = parent / ".env"
            if repo_env not in candidates:
                candidates.append(repo_env)
            break

    loaded_from: Path | None = None
    loaded_keys: list[str] = []

    for candidate in candidates:
        try:
            text = candidate.read_text(encoding="utf-8")
        except FileNotFoundError:
            continue

        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            # Strip surrounding quotes
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            if key not in os.environ:
                os.environ[key] = value
                loaded_keys.append(key)

        loaded_from = candidate
        break

    if loaded_from and loaded_keys:
        logger.debug(".env loaded from %s: %s", loaded_from, ", ".join(loaded_keys))


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
    import fcntl
    import logging
    from datetime import date, datetime

    from inga_quant.pipeline.ingest import DemoLoader, JQuantsAuthError, JQuantsLoader
    from inga_quant.pipeline.runner import run_pipeline

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # --- single-instance guard (prevent concurrent cron overlap) ---
    lock_path = Path("logs/run.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_fh = open(lock_path, "w")  # noqa: SIM115
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock_fh.close()
        print("ERROR: 多重起動を検出しました。前の run が終了するまで待ってください。", file=sys.stderr)
        return 1

    try:
        return _run_pipeline_cmd(args, lock_fh)
    finally:
        fcntl.flock(lock_fh, fcntl.LOCK_UN)
        lock_fh.close()


def _run_pipeline_cmd(args: argparse.Namespace, _lock_fh: object) -> int:
    """Inner body of run command — called with run lock already held."""
    import logging
    from datetime import date, datetime

    from inga_quant.pipeline.ingest import DemoLoader, JQuantsAuthError, JQuantsLoader
    from inga_quant.pipeline.runner import run_pipeline

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
        from inga_quant.utils.config import load_config
        cfg = load_config(Path(args.config) if args.config else None)
        daily_dir = cfg.get("data", {}).get("daily_dir", "data/daily")
        cache_path = Path(daily_dir) / "bars_cache.parquet"
        try:
            loader = JQuantsLoader(cache_path=cache_path)
        except JQuantsAuthError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
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


def _cmd_smoke_check(args: argparse.Namespace) -> int:
    """Quick API connectivity smoke test (3 lines output max)."""
    from inga_quant.pipeline.ingest import JQuantsAuthError, JQuantsLoader

    try:
        loader = JQuantsLoader()
        ok = loader.check_connectivity()
        if ok:
            print("J-Quants API: OK")
            return 0
        else:
            print("J-Quants API: 接続失敗（ネットワークまたはサーバーエラー）")
            return 1
    except JQuantsAuthError as exc:
        print(f"J-Quants API: 認証エラー — {exc}", file=sys.stderr)
        return 1


def main(argv: list[str] | None = None) -> None:
    _load_dotenv_if_present()
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

    # smoke-check (V2 API connectivity)
    sub.add_parser("smoke-check", help="J-Quants V2 API key connectivity check (3 lines)")

    args = parser.parse_args(argv)

    if args.command == "build-features":
        sys.exit(_cmd_build_features(args))
    elif args.command == "run":
        sys.exit(_cmd_run(args))
    elif args.command == "prune-cache":
        sys.exit(_cmd_prune_cache(args))
    elif args.command == "smoke-check":
        sys.exit(_cmd_smoke_check(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
