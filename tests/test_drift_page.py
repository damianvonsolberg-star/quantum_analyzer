from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ui.drift_view import build_drift_view


def test_ok_status_with_fresh_and_low_drift():
    ts = datetime.now(timezone.utc).isoformat()
    doctor = {"ok": True, "latest_timestamp": ts, "hard_failures": []}
    drift = {
        "latest_live_timestamp": ts,
        "feature_psi": 0.05,
        "calibration_drift": 0.02,
        "state_occupancy_drift": 0.04,
        "action_rate_drift": 0.03,
        "cost_drift": 3.0,
    }
    vm = build_drift_view(doctor, drift)
    assert vm.overall_status == "OK"
    assert vm.recommended_response == "continue advisory"


def test_watch_status_on_medium_drift():
    ts = datetime.now(timezone.utc).isoformat()
    doctor = {"ok": True, "latest_timestamp": ts}
    drift = {"latest_live_timestamp": ts, "feature_psi": 0.25}
    vm = build_drift_view(doctor, drift)
    assert vm.overall_status == "WATCH"
    assert vm.recommended_response == "reduce trust"


def test_halt_status_on_hard_failure_or_stale():
    stale = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    doctor = {"ok": False, "latest_timestamp": stale, "hard_failures": ["feature_psi_breach"]}
    vm = build_drift_view(doctor, {})
    assert vm.overall_status == "HALT"
    assert vm.recommended_response == "halt usage"
    assert vm.kill_switch_reasons


def test_placeholder_on_missing_drift():
    vm = build_drift_view({}, None)
    assert vm.feature_drift == "not yet implemented"
    assert vm.calibration_drift == "not yet implemented"
