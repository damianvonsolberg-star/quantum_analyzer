from pathlib import Path

from quantum_analyzer.discovery.meta_research import write_feature_importance_drift, write_signal_decay_monitor
from quantum_analyzer.discovery.report import write_discovery_report


def test_meta_research_outputs(tmp_path: Path):
    rows = [
        {
            "candidate_id": "c1",
            "parent_features": ["f1", "f2"],
            "robustness_score": 0.8,
            "survival_status": "survived",
            "rejection_reason": None,
            "interpretability_score": 0.7,
        },
        {
            "candidate_id": "c2",
            "parent_features": ["f2"],
            "robustness_score": 0.2,
            "survival_status": "rejected",
            "rejection_reason": "low_oos_usefulness",
            "interpretability_score": 0.5,
        },
    ]
    p1 = write_feature_importance_drift(rows, tmp_path)
    p2 = write_signal_decay_monitor(rows, tmp_path)
    p3 = write_discovery_report(rows, tmp_path)
    assert p1.exists()
    assert p2.exists()
    assert p3.exists()
