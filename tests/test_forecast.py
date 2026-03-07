from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from quantum_analyzer.contracts import StateBelief
from quantum_analyzer.forecast.calibrate import ProbCalibrator
from quantum_analyzer.forecast.mixture import build_forecast_bundle
from quantum_analyzer.forecast.outputs import forecast_to_json, save_forecast_json
from quantum_analyzer.paths.archetypes import PathTemplate


def _sample_belief() -> StateBelief:
    return StateBelief(
        ts=datetime.now(timezone.utc),
        symbol="SOLUSDT",
        regime_probabilities={
            "trend_up": 0.25,
            "trend_down": 0.10,
            "breakout_up": 0.20,
            "breakdown_down": 0.05,
            "range_mid_drift": 0.20,
            "stabilization": 0.20,
        },
        entropy=0.45,
        confidence=0.25,
    )


def _sample_templates() -> list[PathTemplate]:
    return [
        PathTemplate(
            template_id="t1",
            cluster_id=1,
            sample_count=120,
            action="long",
            expectancy=0.012,
            pf_proxy=1.3,
            robustness=1.0,
            support=0.24,
            oos_stability=0.011,
            archetype_signature=[0.1, 0.2, -0.1],
            meta={},
        ),
        PathTemplate(
            template_id="t2",
            cluster_id=2,
            sample_count=95,
            action="fade",
            expectancy=0.004,
            pf_proxy=1.1,
            robustness=1.0,
            support=0.18,
            oos_stability=0.002,
            archetype_signature=[-0.2, 0.1, 0.05],
            meta={},
        ),
    ]


def test_forecast_bundle_generation_contracts(tmp_path: Path) -> None:
    bundle = build_forecast_bundle("SOLUSDT", _sample_belief(), _sample_templates(), calibration_score=0.77)

    assert bundle.symbol == "SOLUSDT"
    assert "h12" in bundle.distributions and "h36" in bundle.distributions and "h72" in bundle.distributions

    d = bundle.to_dict()
    q = d["distributions"]["h12"]["quantiles"]
    for k in ["q05", "q25", "q50", "q75", "q95", "p_up", "p_down", "p_break_up", "p_break_down"]:
        assert k in q

    js = forecast_to_json(bundle)
    assert isinstance(js, str) and len(js) > 2

    out = tmp_path / "forecast.json"
    save_forecast_json(bundle, out)
    assert out.exists()


def test_calibration_isotonic_and_beta() -> None:
    y = np.array([0, 0, 0, 1, 1, 1, 1, 0, 1, 0], dtype=int)
    p = np.array([0.1, 0.2, 0.3, 0.8, 0.7, 0.9, 0.85, 0.4, 0.65, 0.35])

    iso = ProbCalibrator(method="isotonic").fit(p, y)
    p_iso = iso.predict(p)
    assert p_iso.shape == p.shape
    assert np.all((p_iso >= 0) & (p_iso <= 1))
    assert 0.0 <= iso.calibration_score(p, y) <= 1.0

    beta = ProbCalibrator(method="beta").fit(p, y)
    p_beta = beta.predict(p)
    assert p_beta.shape == p.shape
    assert np.all((p_beta >= 0) & (p_beta <= 1))
    assert 0.0 <= beta.calibration_score(p, y) <= 1.0
