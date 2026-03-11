from __future__ import annotations

from datetime import date

import pandas as pd

from ui.charts import compute_drawdown, filter_actions, infer_kpis


def test_infer_kpis_degrades_gracefully():
    summary = {"return_pct": 0.12, "diagnostics": {"max_drawdown": -0.08}}
    actions = pd.DataFrame({"action": ["BUY", "HOLD"]})
    equity = pd.DataFrame({"equity": [100, 110, 108]})

    k = infer_kpis(summary, actions, equity)
    assert k["total_return"] == 0.12
    assert k["max_drawdown"] == -0.08
    assert k["profit_factor"] is None
    assert k["action_rate"] is not None


def test_compute_drawdown():
    eq = pd.DataFrame({"ts": ["2026-01-01", "2026-01-02", "2026-01-03"], "equity": [100, 120, 90]})
    dd = compute_drawdown(eq)
    assert "drawdown" in dd.columns
    assert dd["drawdown"].iloc[-1] < 0


def test_filter_actions_simple():
    df = pd.DataFrame(
        {
            "ts": ["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z"],
            "action": ["BUY", "HOLD", "REDUCE"],
            "horizon": ["h12", "h12", "h36"],
            "template_id": ["T1", "T2", "T1"],
        }
    )
    out = filter_actions(df, action_type="BUY", horizon="h12", template="T1")
    assert len(out) == 1
    assert out.iloc[0]["action"] == "BUY"


def test_filter_actions_date_end_includes_full_day():
    df = pd.DataFrame(
        {
            "ts": [
                "2026-01-02T00:00:00Z",
                "2026-01-02T12:00:00Z",
                "2026-01-02T23:59:59Z",
                "2026-01-03T00:00:00Z",
            ],
            "action": ["HOLD", "BUY", "REDUCE", "HOLD"],
        }
    )
    out = filter_actions(df, start=date(2026, 1, 2), end=date(2026, 1, 2))
    assert len(out) == 3
    assert set(out["action"]) == {"HOLD", "BUY", "REDUCE"}
