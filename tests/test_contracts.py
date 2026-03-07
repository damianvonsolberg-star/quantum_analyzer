from datetime import datetime, timezone

from quantum_analyzer import (
    ActionProposal,
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
