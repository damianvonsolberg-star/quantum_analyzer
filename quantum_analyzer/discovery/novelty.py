from __future__ import annotations

from typing import Any


def novelty_distance(a: dict[str, Any], b: dict[str, Any]) -> float:
    sa = str(sorted(a.items()))
    sb = str(sorted(b.items()))
    if sa == sb:
        return 0.0
    overlap = len(set(sa).intersection(set(sb)))
    union = max(len(set(sa).union(set(sb))), 1)
    return float(1.0 - overlap / union)


def novelty_score(genome: dict[str, Any], prior: list[dict[str, Any]]) -> float:
    if not prior:
        return 1.0
    d = [novelty_distance(genome, p) for p in prior]
    return float(max(d))
