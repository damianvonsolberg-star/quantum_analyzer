from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from quantum_analyzer.contracts import ARTIFACT_SCHEMA_V2
from quantum_analyzer.live.advisory_service import run_advisory
from quantum_analyzer.live.artifact_loader import ArtifactLoader
from ui.adapters import ArtifactAdapter
from ui.drift_view import build_drift_view
from quantum_analyzer.monitoring.governance import DriftThresholds
from quantum_analyzer.paths.archetypes import PathTemplate, save_templates_json
from quantum_analyzer.state.latent_model import GaussianHMMBaseline


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


def _seed_artifacts(root: Path, *, with_model: bool = True, malformed: bool = False):
    root.mkdir(parents=True, exist_ok=True)

    if malformed:
        (root / "artifact_bundle.json").write_text("{}")
    else:
        bundle = {
            "schema_version": ARTIFACT_SCHEMA_V2,
            "artifact_meta": {"producer": "test", "produced_at": "2026-03-01T00:00:00Z", "latest_timestamp": "2026-03-01T00:00:00Z"},
            "forecast": {
                "confidence": 0.6,
                "entropy": 0.4,
                "calibration_score": 0.7,
                "timestamps": {"as_of": "2026-03-01T00:00:00Z"},
                "distributions": {"h12": {}, "h36": {}, "h72": {}},
            },
            "proposal": {
                "timestamp": "2026-03-01T00:00:00Z",
                "action": "HOLD",
                "target_position": 0.0,
                "expected_edge_bps": 1.0,
                "expected_cost_bps": 2.0,
            },
            "drift": {
                "governance_status": "OK",
                "kill_switch": False,
                "kill_switch_reasons": [],
                "timestamps": {"as_of": "2026-03-01T00:00:00Z"},
            },
            "summary": {"ok": True},
            "config": {"policy": {"estimated_round_trip_cost_bps": 15.0, "current_position": 0.0}},
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

    if with_model:
        X = pd.DataFrame(
            {
                "micro_range_pos_24h": np.random.normal(0, 1, 200),
                "meso_range_pos_7d": np.random.normal(0, 1, 200),
                "realized_vol_24h": np.abs(np.random.normal(0.1, 0.02, 200)),
                "aggtrade_imbalance": np.random.normal(0, 1, 200),
                "orderbook_imbalance": np.random.normal(0, 1, 200),
                "basis_bps": np.random.normal(5, 1, 200),
                "oi_zscore": np.random.normal(0, 1, 200),
                "coverage_agg_trades": np.ones(200),
                "coverage_book_ticker": np.ones(200),
                "coverage_open_interest": np.ones(200),
                "coverage_basis": np.ones(200),
                "historical_liquidity_ok": np.ones(200),
                "historical_derivatives_ok": np.ones(200),
            }
        )
        model = GaussianHMMBaseline(n_states=10, random_state=7).fit(X)
        model.save(root / "latent_model.pkl")


def test_run_advisory_requires_model_artifact(tmp_path: Path):
    _seed_artifacts(tmp_path, with_model=False)

    def fetcher():
        return _mk_market_data()

    out = run_advisory(tmp_path, market_fetcher=fetcher)
    assert out.kill_switch is True
    assert out.proposal.action == "HOLD"
    assert "artifact_validation_failed" in (out.kill_reason or "")


def test_missing_latent_model_causes_hold_or_halt(tmp_path: Path):
    _seed_artifacts(tmp_path, with_model=False)

    def fetcher():
        return _mk_market_data()

    out = run_advisory(tmp_path, market_fetcher=fetcher)
    assert out.proposal.action == "HOLD"
    assert out.kill_switch is True


def test_malformed_bundle_causes_hold_or_halt(tmp_path: Path):
    _seed_artifacts(tmp_path, with_model=False, malformed=True)

    def fetcher():
        return _mk_market_data()

    out = run_advisory(tmp_path, market_fetcher=fetcher)
    assert out.proposal.action == "HOLD"
    assert out.kill_switch is True


def test_dev_fallback_only_when_explicitly_enabled(tmp_path: Path):
    _seed_artifacts(tmp_path, with_model=False)

    def fetcher():
        return _mk_market_data()

    strict = run_advisory(tmp_path, market_fetcher=fetcher)
    assert strict.proposal.action == "HOLD"
    assert strict.kill_switch is True

    dev = run_advisory(tmp_path, market_fetcher=fetcher, allow_dev_fallback=True)
    assert dev.proposal.action in {"HOLD", "LONG", "SHORT", "REDUCE"}


def test_live_advisory_reproducible_with_model(tmp_path: Path):
    _seed_artifacts(tmp_path, with_model=True)

    def fetcher():
        return _mk_market_data()

    a1 = run_advisory(tmp_path, market_fetcher=fetcher)
    a2 = run_advisory(tmp_path, market_fetcher=fetcher)

    assert a1.proposal.action == a2.proposal.action
    assert abs(a1.proposal.target_position - a2.proposal.target_position) < 1e-9


def test_kill_switch_on_drift_breach(tmp_path: Path):
    _seed_artifacts(tmp_path, with_model=True)

    def fetcher():
        return _mk_market_data()

    th = DriftThresholds(max_calibration_drift=0.00001)
    out = run_advisory(tmp_path, market_fetcher=fetcher, thresholds=th)
    assert out.kill_switch is True
    assert out.kill_reason is not None


def test_artifact_loader_reports_clear_failure_reason(tmp_path: Path):
    _seed_artifacts(tmp_path, with_model=False)
    loader = ArtifactLoader(tmp_path)
    bundle = loader.load_bundle(tmp_path / "artifact_bundle.json")
    result = loader.validate_production_artifacts(bundle, tmp_path)
    assert result.ok is False
    assert result.reason in {"missing_latent_model", "missing_templates", "missing_policy_config"} or result.reason.startswith("bundle_schema_invalid")


def test_live_and_drift_status_consistency(tmp_path: Path):
    _seed_artifacts(tmp_path, with_model=True)

    def fetcher():
        return _mk_market_data()

    out = run_advisory(tmp_path, market_fetcher=fetcher)
    gov = out.drift.get("governance", {})

    b = json.loads((tmp_path / "artifact_bundle.json").read_text())
    b["drift"] = gov
    (tmp_path / "artifact_bundle.json").write_text(json.dumps(b))

    ad = ArtifactAdapter(str(tmp_path))
    ui_drift = ad.to_drift_status()
    assert ui_drift.governance_status == gov.get("overall_status")


def test_stale_artifact_status_consistency(tmp_path: Path):
    _seed_artifacts(tmp_path, with_model=True)
    b = json.loads((tmp_path / "artifact_bundle.json").read_text())
    b["drift"] = {
        "overall_status": "WATCH",
        "kill_switch_active": False,
        "kill_switch_reasons": [],
        "artifact_staleness": "stale",
        "data_staleness": "fresh",
        "feature_drift": 0.0,
        "calibration_drift": 0.0,
        "state_occupancy_drift": 0.0,
        "action_rate_drift": 0.0,
        "cost_drift_bps": 0.0,
    }
    (tmp_path / "artifact_bundle.json").write_text(json.dumps(b))

    ad = ArtifactAdapter(str(tmp_path))
    ui_drift = ad.to_drift_status()
    vm = build_drift_view({"ok": True, "latest_timestamp": "2026-03-01T00:00:00Z"}, {"governance": ui_drift.governance_payload})
    assert ui_drift.governance_status == "WATCH"
    assert vm.overall_status == "WATCH"


def test_cost_drift_field_is_propagated_end_to_end(tmp_path: Path):
    _seed_artifacts(tmp_path, with_model=True)
    b = json.loads((tmp_path / "artifact_bundle.json").read_text())
    b["drift"] = {
        "overall_status": "WATCH",
        "kill_switch_active": False,
        "kill_switch_reasons": [],
        "artifact_staleness": "fresh",
        "data_staleness": "fresh",
        "feature_drift": 0.0,
        "calibration_drift": 0.0,
        "state_occupancy_drift": 0.0,
        "action_rate_drift": 0.0,
        "cost_drift_bps": 12.34,
    }
    (tmp_path / "artifact_bundle.json").write_text(json.dumps(b))

    ad = ArtifactAdapter(str(tmp_path))
    ui_drift = ad.to_drift_status()
    assert ui_drift.governance_payload["cost_drift_bps"] == 12.34


def test_kill_switch_reasons_match_between_pages(tmp_path: Path):
    _seed_artifacts(tmp_path, with_model=True)
    b = json.loads((tmp_path / "artifact_bundle.json").read_text())
    reasons = ["calibration_drift_breach"]
    b["drift"] = {
        "overall_status": "HALT",
        "kill_switch_active": True,
        "kill_switch_reasons": reasons,
        "artifact_staleness": "fresh",
        "data_staleness": "fresh",
        "feature_drift": 0.0,
        "calibration_drift": 0.3,
        "state_occupancy_drift": 0.0,
        "action_rate_drift": 0.0,
        "cost_drift_bps": 0.0,
    }
    (tmp_path / "artifact_bundle.json").write_text(json.dumps(b))

    ad = ArtifactAdapter(str(tmp_path))
    ui_drift = ad.to_drift_status()
    vm = build_drift_view({"ok": True, "latest_timestamp": "2026-03-01T00:00:00Z"}, {"governance": ui_drift.governance_payload})
    assert ui_drift.hard_failures == reasons
    assert vm.kill_switch_reasons == reasons
