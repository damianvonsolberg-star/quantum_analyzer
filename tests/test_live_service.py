from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from quantum_analyzer.live.advisory_service import run_advisory
from quantum_analyzer.monitoring.governance import DriftThresholds
from quantum_analyzer.paths.archetypes import PathTemplate, save_templates_json


def _mk_market_data(n: int = 24 * 40):
    idx = pd.date_range("2026-01-01", periods=n, freq="h", tz="UTC")
    close = 80 + np.linspace(0, 2, n) + np.sin(np.arange(n) / 8) * 0.4
    sol_klines = pd.DataFrame(
        {
            "open_time_ms": (idx.view("int64") // 1_000_000).astype("int64"),
            "open": close,
            "high": close + 0.3,
            "low": close - 0.3,
            "close": close,
            "volume": np.full(n, 1000.0),
        }
    )
    btc_klines = pd.DataFrame(
        {
            "open_time_ms": (idx.view("int64") // 1_000_000).astype("int64"),
            "open": close * 1000,
            "high": close * 1000 + 10,
            "low": close * 1000 - 10,
            "close": close * 1000,
            "volume": np.full(n, 500.0),
        }
    )
    ts_ms = (idx.view("int64") // 1_000_000).astype("int64")
    agg = pd.DataFrame({"trade_time_ms": ts_ms, "qty": 5.0, "price": close, "is_buyer_maker": False})
    book = pd.DataFrame({"source_ts_ms": ts_ms, "bid_price": close - 0.01, "ask_price": close + 0.01, "bid_qty": 10.0, "ask_qty": 9.0})
    funding = pd.DataFrame({"source_ts_ms": ts_ms, "funding_rate": 0.0001})
    basis = pd.DataFrame({"source_ts_ms": ts_ms, "basis_bps": 5.0})
    oi = pd.DataFrame({"source_ts_ms": ts_ms, "open_interest": np.linspace(1000, 1200, n)})
    return {
        "sol_klines": sol_klines,
        "btc_klines": btc_klines,
        "agg_trades": agg,
        "book_ticker": book,
        "funding": funding,
        "basis": basis,
        "open_interest": oi,
    }


def _seed_artifacts(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    bundle = {
        "summary": {"ok": True},
        "config": {"x": 1},
        "template_count": 1,
    }
    (root / "artifact_bundle.json").write_text(json.dumps(bundle))

    templates = [
        PathTemplate(
            template_id="tpl1",
            cluster_id=0,
            sample_count=50,
            action="long",
            expectancy=0.01,
            pf_proxy=1.2,
            robustness=1.0,
            support=0.3,
            oos_stability=0.01,
            archetype_signature=[0.1, 0.2],
            meta={},
        )
    ]
    save_templates_json(templates, root / "templates.json")

    ref_actions = pd.DataFrame({"action": ["HOLD", "LONG"], "p_up": [0.5, 0.6], "realized_up": [0, 1], "expected_cost_bps": [10, 12], "state": ["trend_up", "trend_up"]})
    ref_actions.to_csv(root / "actions.csv", index=False)
    pd.DataFrame({"equity": [1_000_000, 1_001_000]}).to_csv(root / "equity_curve.csv", index=False)


def test_live_advisory_reproducible(tmp_path: Path):
    _seed_artifacts(tmp_path)

    def fetcher():
        return _mk_market_data()

    a1 = run_advisory(tmp_path, market_fetcher=fetcher)
    a2 = run_advisory(tmp_path, market_fetcher=fetcher)

    assert a1.proposal.action == a2.proposal.action
    assert abs(a1.proposal.target_position - a2.proposal.target_position) < 1e-9


def test_kill_switch_on_drift_breach(tmp_path: Path):
    _seed_artifacts(tmp_path)

    def fetcher():
        return _mk_market_data()

    th = DriftThresholds(max_calibration_drift=0.00001)
    out = run_advisory(tmp_path, market_fetcher=fetcher, thresholds=th)
    assert out.kill_switch is True
    assert out.kill_reason is not None
