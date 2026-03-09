from __future__ import annotations

from typing import Any


def complexity_penalty(genome: dict[str, Any]) -> float:
    kind = genome.get("kind", "single_threshold")
    base = {
        "single_threshold": 0.05,
        "interaction": 0.10,
        "lag_relation": 0.12,
        "composite": 0.15,
        "regime_conditional": 0.18,
    }.get(kind, 0.2)
    terms = genome.get("terms", []) if isinstance(genome.get("terms"), list) else []
    return float(base + 0.02 * max(0, len(terms) - 2))
