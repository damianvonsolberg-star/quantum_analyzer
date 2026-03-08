from quantum_analyzer.experiments.specs import ExperimentSpec


def test_experiment_id_deterministic():
    s = ExperimentSpec(window_bars=100, test_bars=20, horizon=36, feature_subset="geom_core", regime_slice="all", policy_params={"turnover_cap": 0.1}, seed=7)
    a = s.experiment_id("snap1")
    b = s.experiment_id("snap1")
    assert a == b
