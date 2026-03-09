from quantum_analyzer.experiments.scoring import score_result


def test_robust_scoring_prefers_better_profile():
    s1 = score_result(
        {"return_pct": 0.12, "test_bars": 120},
        {
            "max_drawdown": -0.10,
            "profit_factor": 1.4,
            "expectancy": 0.01,
            "calibration_proxy": 0.7,
            "turnover": 0.4,
            "action_quality": {"BUY": {"hit_rate": 0.6}},
            "performance_by_vol_bucket": {"low": 0.01, "high": 0.005},
        },
    )
    s2 = score_result(
        {"return_pct": 0.04, "test_bars": 120},
        {
            "max_drawdown": -0.25,
            "profit_factor": 1.0,
            "expectancy": 0.001,
            "calibration_proxy": 0.5,
            "turnover": 1.8,
            "action_quality": {"BUY": {"hit_rate": 0.3}},
            "performance_by_vol_bucket": {"low": 0.0, "high": -0.02},
        },
    )
    assert s1["score"] > s2["score"]
