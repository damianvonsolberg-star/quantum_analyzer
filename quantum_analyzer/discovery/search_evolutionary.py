from __future__ import annotations

from .generator import mutate_genome


def run_evolutionary(seed_population: list[dict], feature_names: list[str], generations: int = 2) -> list[dict]:
    pop = list(seed_population)
    for g in range(generations):
        next_pop = []
        for i, cand in enumerate(pop):
            next_pop.append(mutate_genome(cand, feature_names, seed=7 + g * 100 + i))
        pop.extend(next_pop)
    return pop
