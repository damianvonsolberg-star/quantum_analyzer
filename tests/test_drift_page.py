from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ui.drift_view import build_drift_view


def test_ok_status_with_fresh_and_low_drift():
    ts = datetime.now(timezone.utc).isoformat()
    doctor = {"ok": True, "latest_timestamp": ts, "hard_failures": []}
    drift = {
        "governance": {
            "overall_status": "OK",
            "kill_switch_active": False,
            "kill_switch_reasons": [],
            "artifact_staleness": "fresh",
            "data_staleness": "fresh",
            "feature_drift": 0.05,
            "calibration_drift": 0.02,
            "state_occupancy_drift": 0.04,
            "action_rate_drift": 0.03,
            "cost_drift_bps": 3.0,
        },
        "latest_live_timestamp": ts,
    }
    vm = build_drift_view(doctor, drift)
    assert vm.overall_status == "OK"
    assert vm.recommended_response == "continue advisory"


def test_watch_status_on_medium_drift():
    ts = datetime.now(timezone.utc).isoformat()
    doctor = {"ok": True, "latest_timestamp": ts}
    drift = {
        "governance": {
            "overall_status": "WATCH",
            "kill_switch_active": False,
            "kill_switch_reasons": [],
            "artifact_staleness": "stale",
            "data_staleness": "fresh",
            "feature_drift": 0.25,
            "calibration_drift": 0.0,
            "state_occupancy_drift": 0.0,
            "action_rate_drift": 0.0,
            "cost_drift_bps": 0.0,
        },
        "latest_live_timestamp": ts,
    }
    vm = build_drift_view(doctor, drift)
    assert vm.overall_status == "WATCH"
    assert vm.recommended_response == "reduce trust"


def test_halt_status_on_hard_failure_or_stale():
    stale = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    doctor = {"ok": False, "latest_timestamp": stale, "hard_failures": ["feature_psi_breach"]}
    drift = {
        "governance": {
            "overall_status": "HALT",
            "kill_switch_active": True,
            "kill_switch_reasons": ["feature_psi_breach"],
            "artifact_staleness": "very_stale",
            "data_staleness": "unknown",
            "feature_drift": 0.4,
            "calibration_drift": 0.0,
            "state_occupancy_drift": 0.0,
            "action_rate_drift": 0.0,
            "cost_drift_bps": 0.0,
        }
    }
    vm = build_drift_view(doctor, drift)
    assert vm.overall_status == "HALT"
    assert vm.recommended_response == "halt usage"
    assert vm.kill_switch_reasons


def test_placeholder_on_missing_drift():
    vm = build_drift_view({}, None)
    # missing diagnostics should not crash; conservative defaults still render
    assert vm.overall_status in {"OK", "WATCH", "HALT"}
    assert vm.feature_drift in {"OK", "WATCH", "HALT", "not yet implemented"}
