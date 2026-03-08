from __future__ import annotations

from pathlib import Path

import pytest

from ui.adapters import AdapterValidationError, ArtifactAdapter


FIXTURES = Path(__file__).resolve().parents[1] / "ui" / "fixtures"


def test_adapter_loads_fixture_models() -> None:
    ad = ArtifactAdapter(str(FIXTURES))

    live = ad.to_live_advice()
    assert live.headline_action == "BUY"
    assert live.target_position == pytest.approx(0.35)
    assert live.expected_edge_bps == pytest.approx(15.0)
    assert live.expected_cost_bps == pytest.approx(8.5)
    assert live.traffic_light in {"green", "yellow", "red"}
    assert live.reasons

    fc = ad.to_forecast_view()
    assert fc.horizons == ["h12", "h36", "h72"]
    assert fc.confidence == pytest.approx(0.67)

    bt = ad.to_backtest_summary()
    assert bt.ending_equity == pytest.approx(1023400.5)

    templates = ad.to_templates()
    assert len(templates) == 2
    assert templates[0].template_id == "T1"

    drift = ad.to_drift_status()
    assert drift.ok is True


def test_schema_versions_detected() -> None:
    ad = ArtifactAdapter(str(FIXTURES))
    versions = ad.schema_versions()
    assert "1.1.0" in versions


def test_missing_required_fields_fail_clearly(tmp_path: Path) -> None:
    (tmp_path / "artifact_bundle.json").write_text('{"forecast": {}}', encoding="utf-8")
    (tmp_path / "summary.json").write_text("{}", encoding="utf-8")
    (tmp_path / "equity_curve.csv").write_text("ts,equity\n2026-03-08T00:00:00Z,1\n", encoding="utf-8")
    # missing expected_edge_bps/expected_cost_bps
    (tmp_path / "actions.csv").write_text(
        "ts,action,target_position\n2026-03-08T00:00:00Z,HOLD,0\n", encoding="utf-8"
    )

    ad = ArtifactAdapter(str(tmp_path))
    with pytest.raises(AdapterValidationError, match="missing required fields"):
        ad.to_live_advice()
