from __future__ import annotations

from .generator import generate_bruteforce


def run_bruteforce(feature_names: list[str], max_terms: int = 3) -> list[dict]:
    return generate_bruteforce(feature_names, max_terms=max_terms)
