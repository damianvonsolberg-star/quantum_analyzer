from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from ui.adapters import AdapterValidationError, ArtifactAdapter
from ui.contracts import ARTIFACT_SCHEMA_V2


FIXTURES = Path(__file__).resolve().parents[1] / "ui" / "fixtures"


def _write_common_tables(root: Path) -> None:
    pd.DataFrame({"ts": ["2026-03-08T00:00:00Z"], "equity": [1000000], "pnl": [0]}).to_csv(root / "equity_curve.csv", index=False)
    pd.DataFrame(
        {
            "ts": ["2026-03-08T00:00:00Z"],
            "action": ["HOLD"],
            "target_position": [0.2],
            "expected_edge_bps": [6.0],
            "expected_cost_bps": [5.0],
            "reason": ["test"],
        }
    ).to_csv(root / "actions.csv", index=False)
    (root / "summary.json").write_text(json.dumps({"bars": 10, "test_bars": 2, "ending_equity": 1.0, "return_pct": 0.0, "diagnostics": {"max_drawdown": -0.01}}))


def test_ui_adapter_loads_schema_v2_bundle(tmp_path: Path) -> None:
    _write_common_tables(tmp_path)
    bundle = {
        "schema_version": ARTIFACT_SCHEMA_V2,
        "artifact_meta": {"producer": "test", "produced_at": "2026-03-08T00:00:00Z"},
        "forecast": {
            "confidence": 0.71,
            "entropy": 0.33,
            "calibration_score": 0.88,
            "timestamps": {"as_of": "2026-03-08T00:00:00Z"},
            "distributions": {"h12": {}, "h36": {}, "h72": {}},
        },
        "proposal": {
            "timestamp": "2026-03-08T00:00:00Z",
            "action": "BUY",
            "target_position": 0.4,
            "expected_edge_bps": 11.0,
            "expected_cost_bps": 7.0,
            "reason": "edge",
            "reasons": ["edge", "trend"],
        },
        "drift": {
            "governance_status": "OK",
            "kill_switch": False,
            "kill_switch_reasons": [],
            "timestamps": {"as_of": "2026-03-08T00:00:00Z"},
        },
        "summary": {"bars": 100, "test_bars": 20, "ending_equity": 1010000.0, "return_pct": 0.01, "diagnostics": {"max_drawdown": -0.02}},
        "config": {"backtest": {}, "walkforward": {}},
    }
    (tmp_path / "artifact_bundle.json").write_text(json.dumps(bundle))

    ad = ArtifactAdapter(str(tmp_path))
    live = ad.to_live_advice()
    assert live.headline_action == "BUY"
    assert live.target_position == pytest.approx(0.4)

    fc = ad.to_forecast_view()
    assert fc.confidence == pytest.approx(0.71)
    assert fc.calibration_score == pytest.approx(0.88)


def test_ui_adapter_backward_compat_for_known_legacy_shape() -> None:
    ad = ArtifactAdapter(str(FIXTURES))
    live = ad.to_live_advice()
    assert live.headline_action in {"BUY", "HOLD", "REDUCE"}
    bt = ad.to_backtest_summary()
    assert bt.ending_equity is not None


def test_missing_required_bundle_sections_fail_cleanly(tmp_path: Path) -> None:
    _write_common_tables(tmp_path)
    bad = {
        "schema_version": ARTIFACT_SCHEMA_V2,
        "artifact_meta": {"producer": "x"},
        "forecast": {},
        # missing proposal/drift/summary/config sections
    }
    (tmp_path / "artifact_bundle.json").write_text(json.dumps(bad))
    ad = ArtifactAdapter(str(tmp_path))
    with pytest.raises(AdapterValidationError, match="missing required sections"):
        ad.to_live_advice()


def test_canonical_advisory_wait_flows_to_live_and_drift_consistently(tmp_path: Path) -> None:
    root = tmp_path / "proj"
    (root / "ui").mkdir(parents=True)
    (root / "quantum_analyzer").mkdir(parents=True)
    exp = root / "artifacts" / "explorer" / "experiments" / "exp1"
    exp.mkdir(parents=True)
    _write_common_tables(exp)

    bundle = {
        "schema_version": ARTIFACT_SCHEMA_V2,
        "artifact_meta": {"producer": "test", "produced_at": "2026-03-08T00:00:00Z"},
        "forecast": {"confidence": 0.6, "entropy": 0.4, "calibration_score": 0.8, "timestamps": {"as_of": "2026-03-08T00:00:00Z"}, "distributions": {"h12": {}, "h36": {}, "h72": {}}},
        "proposal": {"timestamp": "2026-03-08T00:00:00Z", "action": "BUY", "target_position": 0.4, "expected_edge_bps": 10.0, "expected_cost_bps": 6.0},
        "drift": {"governance_status": "OK", "kill_switch": False, "kill_switch_reasons": [], "timestamps": {"as_of": "2026-03-08T00:00:00Z"}},
        "summary": {"bars": 10, "test_bars": 2, "ending_equity": 1.0, "return_pct": 0.0},
        "config": {"backtest": {}, "walkforward": {}},
    }
    (exp / "artifact_bundle.json").write_text(json.dumps(bundle))

    promoted = root / "artifacts" / "promoted"
    promoted.mkdir(parents=True)
    advisory = {
        "status": "no_edge",
        "timestamp": "2026-03-09T23:00:00Z",
        "action": "WAIT",
        "action_spot": "WAIT",
        "target_position": None,
        "expected_edge_bps": None,
        "expected_cost_bps": None,
        "reason": "release_gates_failed",
        "governance": {"overall_status": "WATCH", "kill_switch_active": True, "kill_switch_reasons": ["no_candidate"]},
        "freshness": {"state": "fresh", "reason": "fresh"},
        "release_state": "NO_EDGE",
        "symbol": "SOLUSDC",
        "timeframe": "1h",
    }
    (promoted / "advisory_latest.json").write_text(json.dumps(advisory))

    ad = ArtifactAdapter(str(exp))
    live = ad.to_live_advice()
    drift = ad.to_drift_status()
    assert live.headline_action == "WAIT"
    assert live.target_position is None
    assert drift.governance_status == "WATCH"


def test_governance_payload_adapter_roundtrip(tmp_path: Path) -> None:
    _write_common_tables(tmp_path)
    gov = {
        "overall_status": "WATCH",
        "kill_switch_active": False,
        "kill_switch_reasons": ["artifact_stale"],
        "artifact_staleness": "stale",
        "data_staleness": "fresh",
        "feature_drift": 0.1,
        "calibration_drift": 0.02,
        "state_occupancy_drift": 0.01,
        "action_rate_drift": 0.03,
        "cost_drift_bps": 1.2,
    }
    bundle = {
        "schema_version": ARTIFACT_SCHEMA_V2,
        "artifact_meta": {"producer": "test", "produced_at": "2026-03-08T00:00:00Z"},
        "forecast": {"confidence": 0.6, "entropy": 0.4, "calibration_score": 0.8, "timestamps": {"as_of": "2026-03-08T00:00:00Z"}, "distributions": {"h12": {}, "h36": {}, "h72": {}}},
        "proposal": {"timestamp": "2026-03-08T00:00:00Z", "action": "HOLD", "target_position": 0.0, "expected_edge_bps": 1.0, "expected_cost_bps": 2.0},
        "drift": {"governance": gov},
        "summary": {"bars": 10, "test_bars": 2, "ending_equity": 1.0, "return_pct": 0.0},
        "config": {"backtest": {}, "walkforward": {}},
    }
    (tmp_path / "artifact_bundle.json").write_text(json.dumps(bundle))

    ad = ArtifactAdapter(str(tmp_path))
    d = ad.to_drift_status()
    assert d.governance_status == "WATCH"
    assert d.governance_payload["cost_drift_bps"] == pytest.approx(1.2)
    assert d.hard_failures == ["artifact_stale"]
