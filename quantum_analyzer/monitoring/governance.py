from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DriftThresholds:
    max_feature_psi: float = 0.35
    max_state_drift: float = 0.30
    max_action_rate_drift: float = 0.25
    max_cost_drift_bps: float = 10.0
    max_calibration_drift: float = 0.08


def kill_switch_reason(
    bad_data: bool,
    feature_psi_max: float,
    state_drift: float,
    action_rate_drift: float,
    cost_drift_bps: float,
    calibration_drift: float,
    th: DriftThresholds,
) -> str | None:
    if bad_data:
        return "bad_data"
    if feature_psi_max > th.max_feature_psi:
        return "feature_psi_breach"
    if state_drift > th.max_state_drift:
        return "state_occupancy_breach"
    if action_rate_drift > th.max_action_rate_drift:
        return "action_rate_breach"
    if cost_drift_bps > th.max_cost_drift_bps:
        return "cost_drift_breach"
    if calibration_drift > th.max_calibration_drift:
        return "calibration_drift_breach"
    return None
