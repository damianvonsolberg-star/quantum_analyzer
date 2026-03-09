from quantum_analyzer.discovery.generator import generate_bruteforce, generate_random, mutate_genome


def test_discovery_generator_produces_candidates():
    feats = ["a", "b", "c", "d"]
    b = generate_bruteforce(feats)
    r = generate_random(feats, n=10, seed=7)
    assert len(b) > 0
    assert len(r) == 10
    m = mutate_genome(r[0], feats, seed=9)
    assert isinstance(m, dict)
