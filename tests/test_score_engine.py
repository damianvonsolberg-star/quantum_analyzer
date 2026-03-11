from quantum_analyzer.experiments.scoring import score_result


def test_score_engine_hard_gate():
    s = score_result({"return_pct": 0.1}, {"max_drawdown": -0.5, "expectancy": -0.01, "profit_factor": 0.8, "calibration_proxy": 0.5, "turnover": 0.2})
    assert s["hard_gate_pass"] is False


def test_score_engine_positive_candidate():
    s = score_result({"return_pct": 0.2}, {"max_drawdown": -0.1, "expectancy": 0.01, "profit_factor": 1.5, "calibration_proxy": 0.8, "turnover": 0.3})
    assert s["hard_gate_pass"] is True
    assert 0 <= s["score"] <= 1


def test_score_engine_strict_mode_with_proxy_metrics():
    s = score_result(
        {"return_pct": 0.12, "test_bars": 120, "strict_robustness": True, "hold_ratio": 0.45},
        {
            "max_drawdown": -0.08,
            "expectancy_by_template": {"t1": 0.004},
            "calibration_proxy": 0.72,
            "turnover": 0.7,
            "action_consistency": 0.63,
            "performance_by_vol_bucket": {"low": 0.0012, "mid": 0.0008, "high": 0.0004},
            "performance_by_btc_regime": {"btc_up": 0.0011, "btc_flat": 0.0005, "btc_down": 0.0002},
            "action_quality": {
                "BUY": {"count": 60.0, "hit_rate": 0.58, "avg_pnl": 0.003},
                "HOLD": {"count": 20.0, "hit_rate": 0.50, "avg_pnl": 0.0},
                "REDUCE": {"count": 40.0, "hit_rate": 0.55, "avg_pnl": 0.001},
                "WAIT": {"count": 0.0, "hit_rate": 0.0, "avg_pnl": 0.0},
            },
        },
    )
    assert s["hard_gate_pass"] is True
    assert 0 < s["score"] <= 1
