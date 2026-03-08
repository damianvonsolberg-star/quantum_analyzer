from datetime import datetime, timezone

from quantum_analyzer import (
    ARTIFACT_SCHEMA_V2,
    ActionProposal,
    ArtifactBundleV2,
    FeatureSnapshot,
    ForecastBundle,
    HorizonDistribution,
    StateBelief,
)


def test_package_imports_cleanly() -> None:
    assert FeatureSnapshot is not None
    assert StateBelief is not None
    assert HorizonDistribution is not None
    assert ForecastBundle is not None
    assert ActionProposal is not None


def test_contracts_serialize_to_dict_and_json() -> None:
    now = datetime.now(timezone.utc)

    fs = FeatureSnapshot(ts=now, symbol="SOLUSDC", feature_vector={"mom": 0.1})
    sb = StateBelief(ts=now, symbol="SOLUSDC", regime_probabilities={"trend": 0.7})
    hd = HorizonDistribution(horizon_hours=12, mean_return=0.01, std_return=0.04)
    fb = ForecastBundle(ts=now, symbol="SOLUSDC", distributions={"h12": hd})
    ap = ActionProposal(ts=now, symbol="SOLUSDC", action="HOLD", score=0.6, size_fraction=0.0)

    for obj in (fs, sb, hd, fb, ap):
        d = obj.to_dict()
        j = obj.to_json()
        assert isinstance(d, dict)
        assert isinstance(j, str)
        assert len(j) > 2

    assert "h12" in fb.to_dict()["distributions"]


def test_artifact_bundle_schema_v2_roundtrip() -> None:
    payload = {
        "schema_version": ARTIFACT_SCHEMA_V2,
        "artifact_meta": {"producer": "test", "produced_at": datetime.now(timezone.utc).isoformat()},
        "forecast": {"confidence": 0.6, "entropy": 0.4, "calibration_score": 0.8, "timestamps": {"as_of": datetime.now(timezone.utc).isoformat()}},
        "proposal": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": "HOLD",
            "target_position": 0.0,
            "expected_edge_bps": 1.0,
            "expected_cost_bps": 2.0,
        },
        "drift": {"governance_status": "OK", "kill_switch": False, "kill_switch_reasons": []},
        "summary": {"bars": 10, "test_bars": 2, "ending_equity": 1.0, "return_pct": 0.0},
        "config": {"backtest": {}, "walkforward": {}},
    }
    b = ArtifactBundleV2.from_dict(payload)
    out = b.to_dict()
    assert out["schema_version"] == ARTIFACT_SCHEMA_V2
    assert "forecast" in out and "proposal" in out and "drift" in out
