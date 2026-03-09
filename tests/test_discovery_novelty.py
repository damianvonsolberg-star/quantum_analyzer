from quantum_analyzer.discovery.novelty import novelty_distance, novelty_score


def test_discovery_novelty_behaviour():
    a = {"kind": "single_threshold", "feature": "x", "threshold": 0.5}
    b = {"kind": "single_threshold", "feature": "x", "threshold": 0.5}
    c = {"kind": "interaction", "a": "x", "b": "y", "weight": 1.0}
    assert novelty_distance(a, b) == 0.0
    assert novelty_distance(a, c) > 0.0
    assert novelty_score(c, [a, b]) > 0.0
