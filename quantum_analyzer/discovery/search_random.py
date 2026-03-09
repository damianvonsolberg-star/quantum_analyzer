from __future__ import annotations

from .generator import generate_random


def run_random(feature_names: list[str], n: int = 50, seed: int = 7) -> list[dict]:
    return generate_random(feature_names, n=n, seed=seed)
