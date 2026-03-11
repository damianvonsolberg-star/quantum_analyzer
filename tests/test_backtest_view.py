from __future__ import annotations

import pandas as pd
from pandas.testing import assert_frame_equal

from ui.backtest_view import apply_promoted_advisory_overlay, normalize_synthetic_ts


def test_overlay_replaces_latest_same_hour_action() -> None:
    actions = pd.DataFrame(
        {
            "ts": ["2026-03-11T13:00:00Z", "2026-03-11T14:10:00Z"],
            "action": ["BUY", "BUY"],
            "target_position": [1.0, 1.0],
            "expected_edge_bps": [12.0, 11.0],
            "expected_cost_bps": [10.0, 10.0],
        }
    )
    advisory = {
        "timestamp": "2026-03-11T14:45:00Z",
        "action": "REDUCE SPOT",
        "target_position": 0.0,
        "expected_edge_bps": 4.0,
        "expected_cost_bps": 2.0,
        "reason": "promoted_cluster_consensus",
    }

    out, applied, mode = apply_promoted_advisory_overlay(actions, advisory)
    assert applied is True
    assert mode == "replaced_same_hour"
    assert out.loc[1, "action"] == "REDUCE"
    assert out.loc[1, "ts"] == "2026-03-11T14:45:00+00:00"
    assert float(out.loc[1, "target_position"]) == 0.0
    assert out.loc[1, "reason"] == "decision_engine_promoted_consensus_override"
    assert bool(out.loc[1, "decision_overlay"]) is True


def test_overlay_keeps_hourly_timestamp_when_action_unchanged() -> None:
    actions = pd.DataFrame(
        {
            "ts": ["2026-03-11T14:00:00Z"],
            "action": ["REDUCE"],
        }
    )
    advisory = {
        "timestamp": "2026-03-11T14:45:00Z",
        "action": "REDUCE",
    }

    out, applied, mode = apply_promoted_advisory_overlay(actions, advisory)
    assert applied is True
    assert mode == "replaced_same_hour"
    assert out.loc[0, "ts"] == "2026-03-11T14:00:00Z"


def test_overlay_appends_when_no_same_hour_but_recent() -> None:
    actions = pd.DataFrame(
        {
            "ts": ["2026-03-11T12:00:00Z", "2026-03-11T13:00:00Z"],
            "action": ["BUY", "BUY"],
        }
    )
    advisory = {
        "timestamp": "2026-03-11T14:05:00Z",
        "action": "REDUCE",
        "target_position": 0.0,
    }

    out, applied, mode = apply_promoted_advisory_overlay(actions, advisory, max_hour_gap=2.0)
    assert applied is True
    assert mode == "appended_latest"
    assert len(out) == 3
    assert out.iloc[-1]["action"] == "REDUCE"
    assert out.iloc[-1]["reason"] == "decision_engine_promoted_consensus"


def test_overlay_noop_when_advisory_too_stale_vs_timeline() -> None:
    actions = pd.DataFrame(
        {
            "ts": ["2026-03-11T08:00:00Z", "2026-03-11T10:00:00Z"],
            "action": ["BUY", "BUY"],
        }
    )
    advisory = {
        "timestamp": "2026-03-11T14:00:00Z",
        "action": "REDUCE",
    }

    out, applied, mode = apply_promoted_advisory_overlay(actions, advisory, max_hour_gap=2.0)
    assert applied is False
    assert mode is None
    assert_frame_equal(out, actions)


def test_normalize_synthetic_ts_aligns_to_exact_hour() -> None:
    df = pd.DataFrame({"ts": [722, 723, 724], "action": ["HOLD", "BUY", "REDUCE"]})
    out = normalize_synthetic_ts(df, ts_col="ts", artifact_ts="2026-03-11T13:09:26.950596+00:00")
    assert out["ts"].astype(str).tolist() == [
        "2026-03-11 11:00:00+00:00",
        "2026-03-11 12:00:00+00:00",
        "2026-03-11 13:00:00+00:00",
    ]
