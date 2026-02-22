"""Main pipeline orchestrator for Phase 2 daily run."""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from inga_quant.features.build_features import build_features
from inga_quant.pipeline.gates import AllGatesResult, run_all_gates
from inga_quant.pipeline.ingest import DataLoader, DemoLoader
from inga_quant.pipeline.model import ModelConfig, TrainResult, add_forward_return, predict, train_model
from inga_quant.pipeline.notify import build_slack_payload, send_slack
from inga_quant.pipeline.output import write_outputs
from inga_quant.pipeline.trade_date import next_trade_date
from inga_quant.pipeline.watchlist import WatchlistConfig, WatchlistEntry, build_watchlist
from inga_quant.utils.config import load_config, load_signal_features
from inga_quant.utils.hash import code_hash, inputs_digest
from inga_quant.utils.io import save_parquet

logger = logging.getLogger(__name__)


def _make_run_id(as_of: date) -> str:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    short = uuid.uuid4().hex[:8]
    return f"{ts}-{short}"


def run_pipeline(
    as_of: date,
    loader: DataLoader,
    bars_path: Path | None = None,
    out_base: Path | None = None,
    config_path: Path | None = None,
    prev_watchlist: list[str] | None = None,
    lang: str = "ja",
) -> Path:
    """
    Execute the full Phase 2 pipeline for a given as_of date.

    Returns the output directory path.
    """
    cfg = load_config(config_path)
    run_id = _make_run_id(as_of)
    trade_date = next_trade_date(as_of)
    td_str = trade_date.strftime("%Y-%m-%d")

    if out_base is None:
        out_base = Path(cfg.get("output", {}).get("base_dir", "output"))
    out_dir = out_base / td_str
    out_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Setup logging for this run
    # ------------------------------------------------------------------
    log_dir = Path(cfg.get("logging", {}).get("log_dir", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    _setup_run_logging(run_id, log_dir)

    logger.info("=== inga-quant run start: run_id=%s as_of=%s trade_date=%s ===", run_id, as_of, td_str)

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------
    model_cfg_dict = cfg.get("model", {})
    train_days = int(model_cfg_dict.get("train_days", 365))

    from datetime import timedelta
    start_date = date.fromordinal(as_of.toordinal() - train_days)
    bars = loader.fetch_daily(start_date=start_date, end_date=as_of)
    logger.info("Loaded %d rows of bars (%d tickers)", len(bars), bars["ticker"].nunique())

    # ------------------------------------------------------------------
    # Equities master (company names) — non-fatal if unavailable
    # ------------------------------------------------------------------
    master_dir = Path(cfg.get("data", {}).get("master_dir", "data/master"))
    master_df = loader.fetch_master(cache_path=master_dir / "equities_master.parquet")

    # ------------------------------------------------------------------
    # Feature building
    # ------------------------------------------------------------------
    price_col = "adj_close" if "adj_close" in bars.columns else "close"
    # Compute forward returns from raw bars (needs price column, not from features)
    bars_with_fwd = add_forward_return(bars, price_col=price_col, periods=5)
    features = build_features(bars)
    # Merge forward return into features for gate/model use
    fwd_series = bars_with_fwd.set_index(["as_of", "ticker"])["forward_return_5d"]
    features = features.copy()
    features["forward_return_5d"] = (
        features.set_index(["as_of", "ticker"]).index.map(fwd_series)
    )

    # Join company names (fallback to ticker code if master unavailable)
    if not master_df.empty and "ticker" in master_df.columns:
        name_map = master_df.set_index("ticker")["name"]
        features["name"] = features["ticker"].map(name_map).fillna(features["ticker"])
    else:
        features["name"] = features["ticker"]

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------
    signal_features = load_signal_features()
    # Filter to features actually present
    available = [f for f in signal_features if f in features.columns]
    if not available:
        available = [c for c in features.columns if c not in (
            "as_of", "ticker", "quality_flags", "market_regime",
            "forward_return_5d", "op_margin_yoy",
        )]

    model_cfg = ModelConfig(
        model_type=model_cfg_dict.get("type", "Ridge"),
        alpha=float(model_cfg_dict.get("alpha", 1.0)),
        l1_ratio=float(model_cfg_dict.get("l1_ratio", 0.5)),
    )

    # ------------------------------------------------------------------
    # Quality gates
    # ------------------------------------------------------------------
    gate_cfg_dict = cfg.get("gates", {})
    gate_result = run_all_gates(
        features=features,
        feature_names=available,
        as_of=as_of,
        cfg=model_cfg,
        gate_cfg=gate_cfg_dict,
    )
    logger.info("Gates: all_passed=%s rejections=%s", gate_result.all_passed, gate_result.rejection_reasons)

    # ------------------------------------------------------------------
    # Model training (for scoring, even if gates failed — for report)
    # ------------------------------------------------------------------
    model_result: TrainResult | None = None
    coef: dict[str, float] = {}
    try:
        model_result = train_model(features, available, model_cfg)
        coef = model_result.coef
    except Exception as exc:
        logger.warning("Model training failed: %s", exc)

    wf_ic = float(gate_result.gates.get("walk_forward", type("", (), {"details": {}})()).details.get("ic") or 0.0)

    # ------------------------------------------------------------------
    # Watchlist
    # ------------------------------------------------------------------
    wl_cfg_dict = cfg.get("watchlist", {})
    wl_config = WatchlistConfig(
        size=int(wl_cfg_dict.get("size", 50)),
        max_new=int(wl_cfg_dict.get("max_new", 20)),
        min_retained=int(wl_cfg_dict.get("min_retained", 30)),
    )
    watchlist: list[WatchlistEntry] = []
    if coef:
        watchlist = build_watchlist(
            features=features,
            as_of_date=as_of,
            model_coef=coef,
            signal_features=available,
            prev_watchlist=prev_watchlist,
            cfg=wl_config,
        )

    # ------------------------------------------------------------------
    # Manifest
    # ------------------------------------------------------------------
    digest = inputs_digest(bars_path) if bars_path and bars_path.exists() else "n/a"
    generated_at_jst = datetime.now(ZoneInfo("Asia/Tokyo")).isoformat(timespec="seconds")
    manifest = {
        "run_id": run_id,
        "code_hash": code_hash(),
        "inputs_digest": digest,
        "as_of": str(as_of),
        "data_asof": str(as_of),
        "trade_date": td_str,
        "generated_at_jst": generated_at_jst,
        "params": {
            "model": model_cfg.model_type,
            "alpha": model_cfg.alpha,
            "target": "forward_return_5d",
            "minute_cache_days": int(cfg.get("cache", {}).get("minute_cache_days", 20)),
        },
    }

    # ------------------------------------------------------------------
    # Write output files
    # ------------------------------------------------------------------
    write_outputs(
        out_dir=out_dir,
        trade_date=trade_date,
        run_id=run_id,
        gate_result=gate_result,
        watchlist=watchlist,
        manifest=manifest,
        wf_ic=wf_ic,
        lang=lang,
    )

    # ------------------------------------------------------------------
    # Slack / fallback
    # ------------------------------------------------------------------
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
    slack_payload = build_slack_payload(
        trade_date=td_str,
        run_id=run_id,
        action="TRADE" if gate_result.all_passed else "NO_TRADE",
        wf_ic=wf_ic,
        n_eligible=gate_result.n_eligible,
        no_trade_reasons=gate_result.rejection_reasons,
        top3=top3,
        lang=lang,
    )
    send_slack(
        payload=slack_payload,
        fallback_path=out_dir / "slack_payload.json",
    )

    logger.info("=== run complete: %s ===", out_dir)
    return out_dir


def _setup_run_logging(run_id: str, log_dir: Path) -> None:
    """Add a file handler for this run."""
    log_path = log_dir / f"run_{run_id}.log"
    fh = logging.FileHandler(log_path)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logging.getLogger("inga_quant").addHandler(fh)
