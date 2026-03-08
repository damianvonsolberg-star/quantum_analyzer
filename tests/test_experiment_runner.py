from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from quantum_analyzer.experiments.runner import run_experiments
from quantum_analyzer.experiments.specs import ExperimentSpec
from quantum_analyzer.paths.archetypes import PathTemplate


def _synthetic(n: int = 24 * 40):
    start = datetime.now(timezone.utc) - timedelta(hours=n)
    idx = pd.DatetimeIndex([start + timedelta(hours=i) for i in range(n)], tz="UTC")
    rng = np.random.default_rng(1)
    close = pd.Series(80 + np.cumsum(rng.normal(0, 0.1, n)), index=idx)
    feats = pd.DataFrame(
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
    tpls = [
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
    return feats, close, tpls


def test_experiment_runner_executes(tmp_path: Path):
    feats, close, tpls = _synthetic()
    specs = [ExperimentSpec(window_bars=24 * 30, test_bars=24 * 5, horizon=36, feature_subset="geom_core", regime_slice="all", policy_params={"turnover_cap": 0.1, "round_trip_cost_bps": 12.0})]
    rows, failures = run_experiments(specs=specs, snapshot_id="snap", features=feats, close=close, templates=tpls, out_root=tmp_path)
    assert len(failures) == 0
    assert len(rows) == 1
    assert Path(rows[0]["artifact_dir"]).exists()
