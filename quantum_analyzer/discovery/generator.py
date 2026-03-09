from __future__ import annotations

import itertools
import random
from typing import Any


def generate_bruteforce(features: list[str], max_terms: int = 3) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for f in features:
        for thr in [0.3, 0.5, 0.7]:
            out.append({"kind": "single_threshold", "feature": f, "threshold": thr, "rules": {"buy": 0.2, "reduce": -0.2}})
    for a, b in itertools.combinations(features[:6], 2):
        out.append({"kind": "interaction", "a": a, "b": b, "weight": 1.0, "rules": {"buy": 0.15, "reduce": -0.15}})
    for n in range(2, max_terms + 1):
        for comb in itertools.combinations(features[:8], n):
            out.append({"kind": "composite", "terms": [{"feature": c, "weight": 1.0 / n} for c in comb], "rules": {"buy": 0.2, "reduce": -0.2}})
    return out


def generate_random(features: list[str], n: int = 50, seed: int = 7) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    out: list[dict[str, Any]] = []
    for _ in range(n):
        k = rng.choice(["single_threshold", "interaction", "lag_relation", "composite"])
        if k == "single_threshold":
            out.append({"kind": k, "feature": rng.choice(features), "threshold": rng.uniform(-1, 1), "rules": {"buy": 0.2, "reduce": -0.2}})
        elif k == "interaction":
            a, b = rng.sample(features, 2)
            out.append({"kind": k, "a": a, "b": b, "weight": rng.uniform(-2, 2), "rules": {"buy": 0.2, "reduce": -0.2}})
        elif k == "lag_relation":
            out.append({"kind": k, "feature": rng.choice(features), "lag": rng.choice([1, 3, 6, 12]), "rules": {"buy": 0.1, "reduce": -0.1}})
        else:
            n_terms = rng.choice([2, 3])
            terms = [{"feature": f, "weight": rng.uniform(-1, 1)} for f in rng.sample(features, n_terms)]
            out.append({"kind": k, "terms": terms, "rules": {"buy": 0.2, "reduce": -0.2}})
    return out


def mutate_genome(g: dict[str, Any], features: list[str], seed: int = 7) -> dict[str, Any]:
    rng = random.Random(seed)
    x = dict(g)
    if x.get("kind") == "single_threshold":
        x["threshold"] = float(x.get("threshold", 0.0)) + rng.uniform(-0.1, 0.1)
    elif x.get("kind") == "interaction":
        x["weight"] = float(x.get("weight", 1.0)) + rng.uniform(-0.3, 0.3)
    elif x.get("kind") == "lag_relation":
        x["lag"] = int(max(1, int(x.get("lag", 1)) + rng.choice([-1, 1])))
    elif x.get("kind") == "composite":
        terms = list(x.get("terms", []))
        if terms:
            i = rng.randrange(len(terms))
            terms[i]["weight"] = float(terms[i].get("weight", 0.0)) + rng.uniform(-0.2, 0.2)
        x["terms"] = terms
    if rng.random() < 0.2:
        x["kind"] = "single_threshold"
        x["feature"] = rng.choice(features)
        x["threshold"] = rng.uniform(-1, 1)
    return x
