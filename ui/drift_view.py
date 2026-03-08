from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


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


def _parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        t = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _freshness_label(ts: str | None, max_age_minutes: int = 180) -> str:
    dt = _parse_ts(ts)
    if dt is None:
        return "not yet implemented"
    age = datetime.now(timezone.utc) - dt
    if age <= timedelta(minutes=max_age_minutes):
        return "fresh"
    if age <= timedelta(minutes=max_age_minutes * 2):
        return "stale"
    return "very stale"


def _metric_label(v: float | None, watch: float, halt: float) -> str:
    if v is None:
        return "not yet implemented"
    if abs(v) >= halt:
        return "HALT"
    if abs(v) >= watch:
        return "WATCH"
    return "OK"


def build_drift_view(doctor: dict[str, Any] | None, drift_payload: dict[str, Any] | None = None) -> DriftGovernanceView:
    d = doctor or {}
    drift = drift_payload or {}

    latest_artifact_ts = d.get("latest_timestamp")
    latest_live_ts = drift.get("latest_live_timestamp") if isinstance(drift, dict) else None

    artifact_freshness = _freshness_label(latest_artifact_ts)
    data_freshness = _freshness_label(latest_live_ts)

    # pull metrics from drift payload first, fallback to doctor hints
    feature_psi = drift.get("feature_psi") if isinstance(drift, dict) else None
    calibration = drift.get("calibration_drift") if isinstance(drift, dict) else None
    state_occ = drift.get("state_occupancy_drift") if isinstance(drift, dict) else None
    action_rate = drift.get("action_rate_drift") if isinstance(drift, dict) else None
    cost = drift.get("cost_drift") if isinstance(drift, dict) else None

    feature_label = _metric_label(feature_psi, watch=0.2, halt=0.35)
    calibration_label = _metric_label(calibration, watch=0.1, halt=0.2)
    state_label = _metric_label(state_occ, watch=0.15, halt=0.3)
    action_label = _metric_label(action_rate, watch=0.15, halt=0.3)
    cost_label = _metric_label(cost, watch=10.0, halt=25.0)

    reasons = list(d.get("hard_failures", []) or [])
    if d.get("ok") is False and not reasons:
        reasons.append("doctor status not ok")

    statuses = [feature_label, calibration_label, state_label, action_label, cost_label]
    if "HALT" in statuses or d.get("ok") is False or artifact_freshness == "very stale":
        overall = "HALT"
        response = "halt usage"
        if artifact_freshness == "very stale":
            reasons.append("artifact freshness breach")
    elif "WATCH" in statuses or artifact_freshness == "stale" or data_freshness in {"stale", "very stale"}:
        overall = "WATCH"
        response = "reduce trust"
    else:
        overall = "OK"
        response = "continue advisory"

    return DriftGovernanceView(
        overall_status=overall,
        artifact_freshness=artifact_freshness,
        data_freshness=data_freshness,
        feature_drift=feature_label,
        calibration_drift=calibration_label,
        state_occupancy_drift=state_label,
        action_rate_drift=action_label,
        cost_drift=cost_label,
        kill_switch_reasons=reasons,
        recommended_response=response,
        latest_artifact_ts=latest_artifact_ts,
        latest_live_ts=latest_live_ts,
    )
