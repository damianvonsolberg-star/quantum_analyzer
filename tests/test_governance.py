from quantum_analyzer.monitoring.governance import release_state_from_gates


def test_governance_release_state_mapping():
    assert release_state_from_gates(True, []) == "EDGE"
    assert release_state_from_gates(False, ["x"]) == "LOW_EDGE"
    assert release_state_from_gates(False, ["a", "b", "c"]) == "NO_EDGE"
