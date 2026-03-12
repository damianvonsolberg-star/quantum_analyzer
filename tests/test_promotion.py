from pathlib import Path

import json
import pandas as pd

from quantum_analyzer.experiments.promotion import promote_from_leaderboard


def test_promotion_hold_when_no_approved(tmp_path: Path):
    lb = pd.DataFrame(
        [
            {"hard_gate_pass": False, "score": 0.9, "return_pct": 0.1},
        ]
    )
    lb.to_parquet(tmp_path / "leaderboard.parquet", index=False)
    out = promote_from_leaderboard(tmp_path, tmp_path / "promoted", min_score=0.25)
    assert out["action"] == "HOLD"


def test_promotion_writes_signal_bundle(tmp_path: Path):
    lb = pd.DataFrame(
        [
            {"hard_gate_pass": True, "score": 0.8, "return_pct": 0.2},
            {"hard_gate_pass": True, "score": 0.6, "return_pct": 0.1},
        ]
    )
    lb.to_parquet(tmp_path / "leaderboard.parquet", index=False)
    out = promote_from_leaderboard(tmp_path, tmp_path / "promoted", min_score=0.25, governance_status="OK")
    assert out["action"] in {"BUY SPOT", "HOLD", "REDUCE SPOT", "GO FLAT"}
    assert (tmp_path / "promoted" / "current_signal_bundle.json").exists()


def test_promotion_shadow_only_when_governance_not_ok(tmp_path: Path):
    lb = pd.DataFrame(
        [
            {"hard_gate_pass": True, "score": 0.8, "return_pct": 0.2},
        ]
    )
    lb.to_parquet(tmp_path / "leaderboard.parquet", index=False)
    out = promote_from_leaderboard(tmp_path, tmp_path / "promoted", min_score=0.25, governance_status="WATCH")
    assert out["status"] == "shadow_only"
    assert out["action"] == "HOLD"


def test_promotion_uses_candidate_artifact_proposal_for_action_target(tmp_path: Path):
    exp = tmp_path / "experiments" / "exp1"
    exp.mkdir(parents=True, exist_ok=True)
    (exp / "artifact_bundle.json").write_text(
        json.dumps(
            {
                "proposal": {
                    "action": "BUY",
                    "target_position": 1.0,
                }
            }
        ),
        encoding="utf-8",
    )

    lb = pd.DataFrame(
        [
            {
                "hard_gate_pass": True,
                "score": 0.8,
                "return_pct": 0.2,
                "candidate_id": "trend:geom_core:12:all",
                "candidate_family": "trend",
                "feature_subset": "geom_core",
                "regime_slice": "all",
                "horizon": 12,
                "artifact_dir": str(exp),
            }
        ]
    )
    lb.to_parquet(tmp_path / "leaderboard.parquet", index=False)
    out = promote_from_leaderboard(tmp_path, tmp_path / "promoted", min_score=0.25, governance_status="OK")
    assert out["action"] == "BUY SPOT"
    assert float(out["target_position"]) > 0.0
