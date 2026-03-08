from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass
class DriftThresholds:
    max_feature_psi: float = 0.35
    max_state_drift: float = 0.30
    max_action_rate_drift: float = 0.25
    max_cost_drift_bps: float = 10.0
    max_calibration_drift: float = 0.08


@dataclass
class GovernancePayload:
    overall_status: str  # OK|WATCH|HALT
    kill_switch_active: bool
    kill_switch_reasons: list[str] = field(default_factory=list)
    artifact_staleness: str = "unknown"  # fresh|stale|very_stale|unknown
    data_staleness: str = "unknown"
    feature_drift: float = 0.0
    calibration_drift: float = 0.0
    state_occupancy_drift: float = 0.0
    action_rate_drift: float = 0.0
    cost_drift_bps: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_status": self.overall_status,
            "kill_switch_active": self.kill_switch_active,
            "kill_switch_reasons": list(self.kill_switch_reasons),
            "artifact_staleness": self.artifact_staleness,
            "data_staleness": self.data_staleness,
            "feature_drift": float(self.feature_drift),
            "calibration_drift": float(self.calibration_drift),
            "state_occupancy_drift": float(self.state_occupancy_drift),
            "action_rate_drift": float(self.action_rate_drift),
            "cost_drift_bps": float(self.cost_drift_bps),
        }


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


def _staleness_label(ts: str | None, max_age_minutes: int = 180) -> str:
    dt = _parse_ts(ts)
    if dt is None:
        return "unknown"
    age = datetime.now(timezone.utc) - dt
    if age <= timedelta(minutes=max_age_minutes):
        return "fresh"
    if age <= timedelta(minutes=max_age_minutes * 2):
        return "stale"
    return "very_stale"


def evaluate_governance(
    *,
    bad_data: bool,
    feature_psi_max: float,
    state_drift: float,
    action_rate_drift: float,
    cost_drift_bps: float,
    calibration_drift: float,
    artifact_timestamp: str | None,
    data_timestamp: str | None,
    th: DriftThresholds,
) -> GovernancePayload:
    artifact_staleness = _staleness_label(artifact_timestamp)
    data_staleness = _staleness_label(data_timestamp)

    reasons: list[str] = []
    if bad_data:
        reasons.append("bad_data")
    if feature_psi_max > th.max_feature_psi:
        reasons.append("feature_psi_breach")
    if state_drift > th.max_state_drift:
        reasons.append("state_occupancy_breach")
    if action_rate_drift > th.max_action_rate_drift:
        reasons.append("action_rate_breach")
    if cost_drift_bps > th.max_cost_drift_bps:
        reasons.append("cost_drift_breach")
    if calibration_drift > th.max_calibration_drift:
        reasons.append("calibration_drift_breach")

    kill_switch = len(reasons) > 0

    if kill_switch:
        overall = "HALT"
    elif artifact_staleness == "very_stale":
        overall = "HALT"
        reasons.append("artifact_freshness_breach")
        kill_switch = True
    elif artifact_staleness == "stale" or data_staleness in {"stale", "very_stale"}:
        overall = "WATCH"
    else:
        overall = "OK"

    return GovernancePayload(
        overall_status=overall,
        kill_switch_active=kill_switch,
        kill_switch_reasons=reasons,
        artifact_staleness=artifact_staleness,
        data_staleness=data_staleness,
        feature_drift=feature_psi_max,
        calibration_drift=calibration_drift,
        state_occupancy_drift=state_drift,
        action_rate_drift=action_rate_drift,
        cost_drift_bps=cost_drift_bps,
    )


def kill_switch_reason(
    bad_data: bool,
    feature_psi_max: float,
    state_drift: float,
    action_rate_drift: float,
    cost_drift_bps: float,
    calibration_drift: float,
    th: DriftThresholds,
) -> str | None:
    g = evaluate_governance(
        bad_data=bad_data,
        feature_psi_max=feature_psi_max,
        state_drift=state_drift,
        action_rate_drift=action_rate_drift,
        cost_drift_bps=cost_drift_bps,
        calibration_drift=calibration_drift,
        artifact_timestamp=None,
        data_timestamp=None,
        th=th,
    )
    return g.kill_switch_reasons[0] if g.kill_switch_reasons else None
