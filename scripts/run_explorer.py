#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantum_analyzer.experiments.leaderboard import write_leaderboard
from quantum_analyzer.experiments.registry import append_registry, write_failures, write_manifest
from quantum_analyzer.experiments.runner import run_experiments
from quantum_analyzer.experiments.search_space import make_search_space
from quantum_analyzer.experiments.specs import ExplorerRunManifest
from quantum_analyzer.paths.archetypes import PathTemplate


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
            template_id="tpl1",
            cluster_id=0,
            sample_count=100,
            action="long",
            expectancy=0.01,
            pf_proxy=1.2,
            robustness=1.0,
            support=0.3,
            oos_stability=0.008,
            archetype_signature=[0.1, 0.0, -0.1],
            meta={},
        )
    ]
    return features, close, templates


def main() -> int:
    ap = argparse.ArgumentParser(description="Run multi-range explorer")
    ap.add_argument("--preset", default="fast", choices=["fast", "daily", "full"])
    ap.add_argument("--artifacts-root", default="artifacts/explorer")
    ap.add_argument("--snapshot-id", default="synthetic-local")
    args = ap.parse_args()

    out_root = Path(args.artifacts_root)
    out_root.mkdir(parents=True, exist_ok=True)

    specs = make_search_space(args.preset)
    m = ExplorerRunManifest(preset=args.preset, snapshot_id=args.snapshot_id, total_specs=len(specs))

    features, close, templates = _synthetic_data()

    rows, failures = run_experiments(
        specs=specs,
        snapshot_id=args.snapshot_id,
        features=features,
        close=close,
        templates=templates,
        out_root=out_root,
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
