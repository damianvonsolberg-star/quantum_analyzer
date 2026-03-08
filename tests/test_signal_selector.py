from quantum_analyzer.signals.selector import select_final_signal


def test_selector_holds_on_weak_margin():
    out = select_final_signal(
        [
            {"action": "BUY", "target_position": 0.4, "vote_weight": 0.51},
            {"action": "HOLD", "target_position": 0.0, "vote_weight": 0.49},
        ],
        min_mass=0.3,
        min_margin=0.1,
    )
    assert out["action"] == "HOLD"


def test_selector_picks_consensus_action():
    out = select_final_signal(
        [
            {"action": "BUY", "target_position": 0.3, "vote_weight": 0.6},
            {"action": "BUY", "target_position": 0.5, "vote_weight": 0.3},
            {"action": "HOLD", "target_position": 0.0, "vote_weight": 0.1},
        ]
    )
    assert out["action"] == "BUY SPOT"
    assert out["target_position"] > 0
