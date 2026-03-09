from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from quantum_analyzer.backtest.engine import BacktestConfig
from quantum_analyzer.backtest.walkforward import WalkForwardConfig
from quantum_analyzer.experiments.evaluator import build_candidate, evaluate_candidate


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
            "btc_return_1h": rng.normal(0, 0.01, n),
        },
        index=idx,
    )
    return feats, close


def test_candidate_evaluator_outputs_family_and_candidate(tmp_path):
    f, c = _synthetic()
    wf = WalkForwardConfig(train_bars=24 * 30, test_bars=24 * 5, purge_bars=6, embargo_bars=6)
    bt = BacktestConfig()
    cand = build_candidate("cand-trend", "trend", {"buy_threshold": 0.1, "reduce_threshold": -0.1}, "geom_core", 36, "all")
    res = evaluate_candidate(features=f, close=c, candidate=cand, walkforward=wf, backtest=bt, out_dir=str(tmp_path))
    assert res.candidate_id == "cand-trend"
    assert res.family == "trend"
    assert "diagnostics" in {"diagnostics": res.diagnostics}
    actions = pd.read_csv(tmp_path / "actions.csv")
    assert "candidate_id" in actions.columns
    assert "candidate_family" in actions.columns
