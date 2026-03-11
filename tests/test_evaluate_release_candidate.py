from __future__ import annotations

from pathlib import Path
import json
import subprocess
import sys

import pandas as pd


def test_evaluate_release_candidate_uses_promoted_subject(tmp_path: Path) -> None:
    explorer = tmp_path / "artifacts" / "explorer"
    promoted = tmp_path / "artifacts" / "promoted"
    explorer.mkdir(parents=True, exist_ok=True)
    promoted.mkdir(parents=True, exist_ok=True)

    lb = pd.DataFrame(
        [
            {
                "leaderboard_rank": 1,
                "candidate_id": "c_top",
                "candidate_family": "trend",
                "feature_subset": "geom_core",
                "regime_slice": "all",
                "horizon": 12,
                "expectancy": 0.10,
                "max_drawdown": -0.10,
                "s_confidence_reliability": 0.8,
                "action_quality": 0.8,
                "turnover": 0.2,
                "s_regime_robustness": 0.8,
                "start_date_stability": 0.8,
                "neighbor_stability": 0.8,
                "benchmark_lift": 0.2,
                "return_pct": 0.20,
                "baseline_wait_return_pct": 0.01,
                "baseline_always_long_return_pct": 0.02,
                "baseline_btc_follow_return_pct": 0.03,
                "baseline_random_action_return_pct": 0.00,
                "baseline_momentum_simple_return_pct": 0.01,
                "baseline_mean_reversion_simple_return_pct": 0.01,
            },
            {
                "leaderboard_rank": 2,
                "candidate_id": "c_promoted",
                "candidate_family": "trend",
                "feature_subset": "geom_core",
                "regime_slice": "all",
                "horizon": 12,
                "expectancy": -0.15,
                "max_drawdown": -0.10,
                "s_confidence_reliability": 0.8,
                "action_quality": 0.8,
                "turnover": 0.2,
                "s_regime_robustness": 0.8,
                "start_date_stability": 0.8,
                "neighbor_stability": 0.8,
                "benchmark_lift": 0.2,
                "return_pct": -0.05,
                "baseline_wait_return_pct": 0.01,
                "baseline_always_long_return_pct": 0.02,
                "baseline_btc_follow_return_pct": 0.03,
                "baseline_random_action_return_pct": 0.00,
                "baseline_momentum_simple_return_pct": 0.01,
                "baseline_mean_reversion_simple_return_pct": 0.01,
            },
        ]
    )
    lb.to_parquet(explorer / "leaderboard.parquet", index=False)
    pd.DataFrame(lb).to_parquet(explorer / "registry.parquet", index=False)

    promoted_bundle = {
        "status": "approved",
        "action": "BUY SPOT",
        "target_position": 1.0,
        "source": {"promotion_cluster": "family_regime"},
        "supporting_metrics": {
            "supporting_metrics": {
                "candidate_id": "c_promoted",
                "candidate_family": "trend",
                "feature_subset": "geom_core",
                "regime_slice": "all",
                "horizon": 12,
            },
            "selected_cluster": {"candidate_id": "c_promoted"},
        },
    }
    (promoted / "current_signal_bundle.json").write_text(json.dumps(promoted_bundle), encoding="utf-8")

    script = Path(__file__).resolve().parents[1] / "scripts" / "evaluate_release_candidate.py"
    subprocess.run(
        [
            sys.executable,
            str(script),
            "--explorer-root",
            str(explorer),
            "--promoted-root",
            str(promoted),
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )

    gate = json.loads((promoted / "release_gate_report.json").read_text(encoding="utf-8"))
    assert gate["evaluated_subject"]["candidate_id"] == "c_promoted"
    assert str(gate["evaluated_subject"]["source_alignment"]).startswith("aligned")
    # Ensures release gate metrics are computed from promoted subject, not top leaderboard row.
    assert gate["metrics"]["post_cost_expectancy"] == -0.15
