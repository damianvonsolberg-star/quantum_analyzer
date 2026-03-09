from quantum_analyzer.monitoring.release_gates import evaluate_release_gates


def test_release_gates_pass_with_strong_candidate():
    c = {
        "expectancy": 0.01,
        "max_drawdown": -0.12,
        "confidence_reliability": 0.7,
        "action_quality": 0.6,
        "turnover": 0.8,
        "regime_robustness": 0.7,
        "start_date_stability": 0.7,
        "neighbor_stability": 0.7,
        "benchmark_lift": 0.03,
    }
    r = evaluate_release_gates(c)
    assert r.passed is True
    assert r.overall_state == "EDGE"


def test_release_gates_fail_to_no_edge():
    c = {
        "expectancy": -0.01,
        "max_drawdown": -0.5,
        "confidence_reliability": 0.2,
        "action_quality": 0.1,
        "turnover": 3.0,
        "regime_robustness": 0.2,
        "start_date_stability": 0.1,
        "neighbor_stability": 0.1,
        "benchmark_lift": -0.01,
    }
    r = evaluate_release_gates(c)
    assert r.passed is False
    assert r.overall_state == "NO_EDGE"
    assert len(r.failures) >= 3
