from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import subprocess
import sys


def _run_emit(tmp_path: Path, signal_bundle: dict, release_gate: dict) -> dict:
    artifacts = tmp_path / "artifacts"
    promoted = artifacts / "promoted"
    promoted.mkdir(parents=True, exist_ok=True)
    promoted.joinpath("current_signal_bundle.json").write_text(json.dumps(signal_bundle), encoding="utf-8")
    promoted.joinpath("release_gate_report.json").write_text(json.dumps(release_gate), encoding="utf-8")

    artifacts.joinpath("research_cycle_status.json").write_text(
        json.dumps(
            {
                "state": "idle",
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }
        ),
        encoding="utf-8",
    )

    script = Path(__file__).resolve().parents[1] / "scripts" / "emit_advisory.py"
    subprocess.run(
        [sys.executable, str(script), "--artifacts-root", str(promoted)],
        check=True,
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    return json.loads((promoted / "advisory_latest.json").read_text(encoding="utf-8"))


def test_emit_advisory_normalizes_promoted_action(tmp_path: Path):
    out = _run_emit(
        tmp_path,
        signal_bundle={
            "status": "approved",
            "action": "BUY SPOT",
            "target_position": 0.6,
            "confidence": 0.72,
            "reason": "cluster_consensus",
            "supporting_metrics": {
                "expectancy": 0.0012,
                "supporting_metrics": {"candidate_id": "cand_a"},
            },
            "source": {"trading_symbol": "SOLUSDC", "timeframe": "1h", "promotion_cluster": "family_regime"},
        },
        release_gate={
            "passed": True,
            "overall_state": "EDGE",
            "failures": [],
            "evaluated_subject": {"candidate_id": "cand_a", "promotion_cluster": "family_regime"},
        },
    )
    assert out["action_raw"] == "BUY SPOT"
    assert out["action_spot"] == "BUY"
    assert out["action"] == "BUY"
    assert out["status"] == "approved"
    assert out["target_position"] == 0.6
    assert out["governance"]["overall_status"] == "OK"
    assert out["governance"]["kill_switch_active"] is False


def test_emit_advisory_keeps_signal_when_release_gate_fails(tmp_path: Path):
    out = _run_emit(
        tmp_path,
        signal_bundle={
            "status": "approved",
            "action": "BUY SPOT",
            "target_position": 0.55,
            "confidence": 0.68,
            "reason": "cluster_consensus",
            "supporting_metrics": {
                "expectancy": 0.0010,
                "supporting_metrics": {"candidate_id": "cand_b"},
            },
            "source": {"trading_symbol": "SOLUSDC", "timeframe": "1h", "promotion_cluster": "family_regime"},
        },
        release_gate={
            "passed": False,
            "overall_state": "NO_EDGE",
            "failures": ["no_benchmark_outperformance"],
            "evaluated_subject": {"candidate_id": "cand_b", "promotion_cluster": "family_regime"},
        },
    )
    assert out["status"] == "no_edge"
    assert out["action"] == "BUY"
    assert out["target_position"] == 0.55
    assert out["governance"]["overall_status"] == "WATCH"
    assert out["governance"]["kill_switch_active"] is True


def test_emit_advisory_rejects_release_gate_subject_mismatch(tmp_path: Path):
    out = _run_emit(
        tmp_path,
        signal_bundle={
            "status": "approved",
            "action": "BUY SPOT",
            "target_position": 0.55,
            "confidence": 0.68,
            "reason": "cluster_consensus",
            "supporting_metrics": {
                "expectancy": 0.0010,
                "supporting_metrics": {"candidate_id": "cand_expected"},
            },
            "source": {"trading_symbol": "SOLUSDC", "timeframe": "1h", "promotion_cluster": "family_regime"},
        },
        release_gate={
            "passed": True,
            "overall_state": "EDGE",
            "failures": [],
            "evaluated_subject": {"candidate_id": "cand_other", "promotion_cluster": "family_regime"},
        },
    )
    assert out["status"] == "insufficient_evidence"
    assert out["reason"] == "release_gate_subject_mismatch"
    assert out["release_state"] == "NO_EDGE"
    assert out["governance"]["kill_switch_active"] is True
