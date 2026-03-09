from __future__ import annotations

import pandas as pd

from quantum_analyzer.backtest.diagnostics import action_quality_metrics


def test_action_quality_metrics_contains_buy_hold_reduce_wait():
    df = pd.DataFrame(
        {
            "action": ["BUY", "BUY", "HOLD", "REDUCE", "WAIT", "WAIT"],
            "pnl": [1.0, -0.5, 0.1, -0.2, 0.0, 0.2],
        }
    )
    q = action_quality_metrics(df)
    for k in ["BUY", "HOLD", "REDUCE", "WAIT"]:
        assert k in q
        assert "count" in q[k]
        assert "hit_rate" in q[k]
        assert "avg_pnl" in q[k]
