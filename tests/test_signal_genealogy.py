from quantum_analyzer.discovery.genealogy import build_genealogy_entry


def test_signal_genealogy_contains_required_fields():
    g = build_genealogy_entry(
        candidate_id="c1",
        genome={"kind": "single_threshold"},
        parent_features=["micro_range_pos_24h"],
        method="random",
        transforms=["none"],
        params={"threshold": 0.5},
        timeframe="1h",
        validation={"expectancy": 0.01},
        robustness_score=0.7,
        interpretability_score=0.8,
        survival_status="survived",
        rejection_reason=None,
    )
    for k in [
        "parent_features",
        "generation_method",
        "transforms_applied",
        "parameters",
        "timeframes",
        "validation_results",
        "robustness_score",
        "interpretability_score",
        "survival_status",
    ]:
        assert k in g
