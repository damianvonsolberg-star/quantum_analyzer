from quantum_analyzer.experiments.scoring import score_result


def test_score_engine_hard_gate():
    s = score_result({"return_pct": 0.1}, {"max_drawdown": -0.5, "expectancy": -0.01, "profit_factor": 0.8, "calibration_proxy": 0.5, "turnover": 0.2})
    assert s["hard_gate_pass"] is False


def test_score_engine_positive_candidate():
    s = score_result({"return_pct": 0.2}, {"max_drawdown": -0.1, "expectancy": 0.01, "profit_factor": 1.5, "calibration_proxy": 0.8, "turnover": 0.3})
    assert s["hard_gate_pass"] is True
    assert 0 <= s["score"] <= 1
