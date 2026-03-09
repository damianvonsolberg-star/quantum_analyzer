from pathlib import Path

import pandas as pd

from quantum_analyzer.experiments.promotion import promote_from_leaderboard


def test_promotion_outputs_ranked_and_alternatives(tmp_path: Path):
    lb = pd.DataFrame(
        [
            {"hard_gate_pass": True, "robust_score": 0.8, "score": 0.8, "return_pct": 0.2, "candidate_family": "trend", "regime_slice": "all", "expectancy": 0.01},
            {"hard_gate_pass": True, "robust_score": 0.7, "score": 0.7, "return_pct": 0.15, "candidate_family": "mr", "regime_slice": "high_vol", "expectancy": 0.008},
        ]
    )
    lb.to_parquet(tmp_path / "leaderboard.parquet", index=False)
    out = promote_from_leaderboard(tmp_path, tmp_path / "promoted", min_score=0.25, governance_status="OK")
    assert "top_alternatives" in out
    assert (tmp_path / "promoted" / "ranked_candidates.json").exists()
    assert (tmp_path / "promoted" / "signals_latest.json").exists()
