from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from quantum_analyzer.monitoring.governance import DriftThresholds, evaluate_governance


@dataclass
class DriftGovernanceView:
    overall_status: str  # OK|WATCH|HALT
    artifact_freshness: str
    data_freshness: str
    feature_drift: str
    calibration_drift: str
    state_occupancy_drift: str
    action_rate_drift: str
    cost_drift: str
    kill_switch_reasons: list[str] = field(default_factory=list)
    recommended_response: str = "continue advisory"
    latest_artifact_ts: str | None = None
    latest_live_ts: str | None = None


def _metric_label(v: float | None, watch: float, halt: float) -> str:
    if v is None:
        return "not yet implemented"
    if abs(v) >= halt:
        return "HALT"
    if abs(v) >= watch:
        return "WATCH"
    return "OK"


def _response(status: str) -> str:
    if status == "OK":
        return "continue advisory"
    if status == "WATCH":
        return "reduce trust"
    return "halt usage"


def build_drift_view(doctor: dict[str, Any] | None, drift_payload: dict[str, Any] | None = None) -> DriftGovernanceView:
    d = doctor or {}
    drift = drift_payload or {}

    # canonical payload preferred
    gov = drift.get("governance") if isinstance(drift.get("governance"), dict) else None
    if gov is None and all(k in drift for k in ["overall_status", "kill_switch_active", "kill_switch_reasons"]):
        gov = drift

    if gov is None:
        m = drift.get("metrics") if isinstance(drift.get("metrics"), dict) else drift
        g = evaluate_governance(
            bad_data=not bool(d.get("ok", True)),
            feature_psi_max=float(m.get("feature_drift", m.get("feature_psi", 0.0)) or 0.0),
            state_drift=float(m.get("state_occupancy_drift", 0.0) or 0.0),
            action_rate_drift=float(m.get("action_rate_drift", 0.0) or 0.0),
            cost_drift_bps=float(m.get("cost_drift_bps", m.get("cost_drift", 0.0)) or 0.0),
            calibration_drift=float(m.get("calibration_drift", 0.0) or 0.0),
            artifact_timestamp=d.get("latest_timestamp"),
            data_timestamp=drift.get("latest_live_timestamp"),
            th=DriftThresholds(),
        )
        gov = g.to_dict()

    status = str(gov.get("overall_status", "OK")).upper()
    artifact_staleness = str(gov.get("artifact_staleness", "unknown"))
    data_staleness = str(gov.get("data_staleness", "unknown"))
    fd = float(gov.get("feature_drift", 0.0) or 0.0)
    cd = float(gov.get("calibration_drift", 0.0) or 0.0)
    sd = float(gov.get("state_occupancy_drift", 0.0) or 0.0)
    ad = float(gov.get("action_rate_drift", 0.0) or 0.0)
    cost = float(gov.get("cost_drift_bps", 0.0) or 0.0)

    return DriftGovernanceView(
        overall_status=status,
        artifact_freshness=artifact_staleness,
        data_freshness=data_staleness,
        feature_drift=_metric_label(fd, watch=0.2, halt=0.35),
        calibration_drift=_metric_label(cd, watch=0.1, halt=0.2),
        state_occupancy_drift=_metric_label(sd, watch=0.15, halt=0.3),
        action_rate_drift=_metric_label(ad, watch=0.15, halt=0.3),
        cost_drift=_metric_label(cost, watch=10.0, halt=25.0),
        kill_switch_reasons=(gov.get("kill_switch_reasons", []) if isinstance(gov.get("kill_switch_reasons"), list) else []),
        recommended_response=_response(status),
        latest_artifact_ts=d.get("latest_timestamp"),
        latest_live_ts=drift.get("latest_live_timestamp"),
    )
