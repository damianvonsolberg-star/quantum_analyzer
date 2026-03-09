#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantum_analyzer.datasets.snapshots import build_snapshot, load_snapshot_manifest
from quantum_analyzer.experiments.leaderboard import write_leaderboard
from quantum_analyzer.experiments.registry import append_registry, write_failures, write_manifest
from quantum_analyzer.experiments.runner import run_experiments
from quantum_analyzer.experiments.search_space import make_search_space
from quantum_analyzer.experiments.specs import ExplorerRunManifest
from quantum_analyzer.features.feature_store import build_feature_snapshot, load_feature_snapshot
from quantum_analyzer.paths.archetypes import PathTemplate
from quantum_analyzer.paths.miner import MinerConfig, mine_path_templates


def _load_config(path: str | None) -> dict:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    text = p.read_text(encoding="utf-8")
    if p.suffix in {".json"}:
        return json.loads(text)
    try:
        import yaml  # type: ignore

        return yaml.safe_load(text) or {}
    except ModuleNotFoundError as e:
        raise RuntimeError(f"Failed to parse config {path}: YAML requires pyyaml. Use .json config or install pyyaml") from e
    except Exception as e:
        raise RuntimeError(f"Failed to parse config {path}: {e}") from e


def _synthetic_data(n: int = 24 * 120) -> tuple[pd.DataFrame, pd.Series, list[PathTemplate]]:
    start = datetime.now(timezone.utc) - timedelta(hours=n)
    idx = pd.DatetimeIndex([start + timedelta(hours=i) for i in range(n)], tz="UTC")
    rng = np.random.default_rng(7)
    close = pd.Series(80 + np.cumsum(rng.normal(0, 0.15, n)), index=idx)
    features = pd.DataFrame(
        {
            "micro_range_pos_24h": rng.normal(0.5, 0.2, n),
            "meso_range_pos_7d": rng.normal(0.5, 0.2, n),
            "realized_vol_24h": np.abs(rng.normal(0.03, 0.01, n)),
            "aggtrade_imbalance": rng.normal(0, 0.2, n),
            "orderbook_imbalance": rng.normal(0, 0.2, n),
            "basis_bps": rng.normal(5, 1, n),
            "oi_zscore": rng.normal(0, 1, n),
        },
        index=idx,
    )
    templates = [
        PathTemplate(
            template_id="tpl_fixture",
            cluster_id=0,
            sample_count=100,
            action="long",
            expectancy=0.01,
            pf_proxy=1.2,
            robustness=1.0,
            support=0.3,
            oos_stability=0.008,
            archetype_signature=[0.1, 0.0, -0.1],
            meta={"fixture": True},
        )
    ]
    return features, close, templates


def main() -> int:
    ap = argparse.ArgumentParser(description="Run multi-range explorer")
    ap.add_argument("--preset", default="fast", choices=["fast", "daily", "full"])
    ap.add_argument("--artifacts-root", default="artifacts/explorer")
    ap.add_argument("--config", default="config/research/solusdc_research.json")
    ap.add_argument("--snapshot", default="latest")
    ap.add_argument("--build-snapshot", action="store_true", help="Build a new snapshot before explorer run")
    ap.add_argument("--fixture-synthetic", action="store_true", help="Use synthetic fixture data (tests only)")
    args = ap.parse_args()

    out_root = Path(args.artifacts_root)
    out_root.mkdir(parents=True, exist_ok=True)

    cfg = _load_config(args.config)
    rcfg = cfg.get("research", {}) if isinstance(cfg, dict) else {}

    trading_symbol = "SOLUSDC"
    price_source_symbol = "SOLUSDT"
    timeframe = "1h"

    if args.fixture_synthetic:
        snapshot_id = "fixture-synthetic"
        features_full, close, templates = _synthetic_data()
    else:
        data_root = rcfg.get("data_root", "data/binance")
        timeframe = rcfg.get("explorer_timeframe", "1h")
        primary_symbol = rcfg.get("primary_symbol", "SOLUSDC")
        price_source_symbol = rcfg.get("price_source_symbol", "SOLUSDT")
        trading_symbol = primary_symbol
        ctx = rcfg.get("context_symbols", ["BTCUSDC", "BTCUSDT"])

        snapshot_dir = "artifacts/data_quality"
        if args.build_snapshot:
            snap = build_snapshot(
                data_root=data_root,
                out_dir=snapshot_dir,
                primary_symbol=primary_symbol,
                price_source_symbol=price_source_symbol,
                btc_context_candidates=ctx,
                timeframe=timeframe,
                min_coverage_ratio=float(rcfg.get("snapshot", {}).get("min_coverage_ratio", 0.0)),
                max_gap_ratio=float(rcfg.get("snapshot", {}).get("max_gap_ratio", 1.0)),
                optional_proxies=rcfg.get("optional_proxies", {}),
            )
        else:
            snap = load_snapshot_manifest(snapshot_dir, args.snapshot)
        fs = build_feature_snapshot(
            data_root=data_root,
            snapshot_manifest=snap.payload,
            out_root="artifacts/features",
            timeframe=timeframe,
        )
        snapshot_id = snap.snapshot_id

        f_all = load_feature_snapshot("artifacts/features", snapshot_id)
        features_full = f_all

        close_col = "close" if "close" in f_all.columns else None
        if close_col is None:
            raise RuntimeError("features snapshot missing close column")
        close = f_all["close"].astype(float)

        templates = mine_path_templates(f_all[[c for c in f_all.columns if c in {"close", "realized_vol_24h"}]].assign(close=close), MinerConfig())
        if not templates:
            templates = [
                PathTemplate(
                    template_id="fallback_hold",
                    cluster_id=0,
                    sample_count=max(len(f_all), 1),
                    action="hold",
                    expectancy=0.0,
                    pf_proxy=1.0,
                    robustness=1.0,
                    support=1.0,
                    oos_stability=0.0,
                    archetype_signature=[0.0],
                    meta={"reason": "no_templates_mined"},
                )
            ]

        # persist explorer-level pointers
        (out_root / "snapshot_manifest.json").write_text(json.dumps(snap.payload, indent=2), encoding="utf-8")
        (out_root / "feature_manifest.json").write_text(fs.manifest_path.read_text(encoding="utf-8"), encoding="utf-8")

    specs = make_search_space(args.preset)
    m = ExplorerRunManifest(preset=args.preset, snapshot_id=snapshot_id, total_specs=len(specs))

    rows, failures = run_experiments(
        specs=specs,
        snapshot_id=snapshot_id,
        features_full=features_full,
        close=close,
        templates=templates,
        out_root=out_root,
        trading_symbol=trading_symbol,
        price_source_symbol=price_source_symbol,
        timeframe=timeframe,
    )

    reg_path = append_registry(out_root, rows)
    write_leaderboard(reg_path, out_root)
    write_failures(out_root, failures)

    m.succeeded = len(rows)
    m.failed = len(failures)
    m.finished_at = datetime.now(timezone.utc).isoformat()
    write_manifest(out_root, m.to_dict())

    print(f"explorer done: succeeded={m.succeeded} failed={m.failed} total={m.total_specs}")
    print(f"registry={reg_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
