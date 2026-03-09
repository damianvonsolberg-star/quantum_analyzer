from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from quantum_analyzer.strategies import (
    BreakoutContinuationStrategy,
    InterpretableMLBaselineStrategy,
    MeanReversionStrategy,
    RegimeSwitchStrategy,
    TrendFollowingStrategy,
)


def _feats(n: int = 100) -> pd.DataFrame:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    idx = pd.DatetimeIndex([start + timedelta(hours=i) for i in range(n)], tz="UTC")
    rng = np.random.default_rng(3)
    return pd.DataFrame(
        {
            "micro_range_pos_24h": rng.normal(0.5, 0.2, n),
            "meso_range_pos_7d": rng.normal(0.5, 0.2, n),
            "realized_vol_24h": np.abs(rng.normal(0.03, 0.01, n)),
            "aggtrade_imbalance": rng.normal(0, 0.2, n),
            "orderbook_imbalance": rng.normal(0, 0.2, n),
            "compression_state": rng.normal(0, 1, n),
            "range_width": np.abs(rng.normal(1, 0.3, n)),
        },
        index=idx,
    )


def test_strategy_families_generate_scores_and_actions():
    f = _feats()
    cands = [
        TrendFollowingStrategy("c1", "trend"),
        MeanReversionStrategy("c2", "mean_reversion"),
        BreakoutContinuationStrategy("c3", "breakout"),
        RegimeSwitchStrategy("c4", "regime_switch"),
        InterpretableMLBaselineStrategy("c5", "ml_baseline"),
    ]
    for c in cands:
        s = c.generate_scores(f)
        a = c.propose_actions(f)
        assert len(s) == len(f)
        assert len(a) == len(f)
        assert set(a.unique()).issubset({"BUY", "HOLD", "REDUCE", "WAIT"})
