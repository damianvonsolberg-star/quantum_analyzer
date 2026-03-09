from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from quantum_analyzer.discovery.evaluate import evaluate_discovered_candidate


def test_discovery_evaluation_outputs_metrics():
    n = 120
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    idx = pd.DatetimeIndex([start + timedelta(hours=i) for i in range(n)], tz="UTC")
    rng = np.random.default_rng(5)
    feats = pd.DataFrame({
        "micro_range_pos_24h": rng.normal(0.5, 0.2, n),
        "meso_range_pos_7d": rng.normal(0.5, 0.2, n),
        "realized_vol_24h": np.abs(rng.normal(0.03, 0.01, n)),
    }, index=idx)
    close = pd.Series(80 + np.cumsum(rng.normal(0, 0.1, n)), index=idx)
    g = {"kind": "single_threshold", "feature": "micro_range_pos_24h", "threshold": 0.5, "rules": {"buy": 0.2, "reduce": -0.2}}
    out = evaluate_discovered_candidate(g, feats, close)
    assert "expectancy" in out
    assert "novelty" in out
    assert "complexity_penalty" in out
