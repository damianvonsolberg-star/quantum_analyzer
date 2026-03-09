from quantum_analyzer.discovery.survival import attach_survival_fields


def test_signal_survival_rejection_reason_explicit():
    row = {
        "oos_usefulness": 0.2,
        "neighbor_consistency": 0.5,
        "cross_window_repeatability": 0.5,
        "regime_specialization": 0.95,
        "redundancy": 0.9,
        "complexity_penalty": 0.8,
        "cost_adjusted_value": 0.1,
    }
    out = attach_survival_fields(row)
    assert out["survival_status"] == "rejected"
    assert isinstance(out["rejection_reason"], str)
