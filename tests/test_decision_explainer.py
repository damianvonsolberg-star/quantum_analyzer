from quantum_analyzer.decision.consensus import decide_action
from quantum_analyzer.decision.explainer import explain_decision


def test_decision_explainer_includes_alternatives():
    ranked = [
        {"candidate_id": "c1", "candidate_family": "trend", "robust_score": 0.8, "confidence": 0.8, "expectancy": 0.02, "action": "BUY SPOT", "regime_slice": "all"},
        {"candidate_id": "c2", "candidate_family": "mr", "robust_score": 0.6, "confidence": 0.6, "expectancy": 0.01, "action": "HOLD", "regime_slice": "high_vol"},
    ]
    d = decide_action(ranked)
    e = explain_decision(d, ranked)
    assert e["final_action"] in {"BUY SPOT", "WAIT"}
    assert "alternatives" in e
