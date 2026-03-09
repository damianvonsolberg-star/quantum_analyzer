from __future__ import annotations

from typing import Any


def build_genealogy_entry(
    *,
    candidate_id: str,
    genome: dict[str, Any],
    parent_features: list[str],
    method: str,
    transforms: list[str],
    params: dict[str, Any],
    timeframe: str,
    validation: dict[str, Any],
    robustness_score: float,
    interpretability_score: float,
    survival_status: str,
    rejection_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "genome": genome,
        "parent_features": parent_features,
        "generation_method": method,
        "transforms_applied": transforms,
        "parameters": params,
        "timeframes": [timeframe],
        "validation_results": validation,
        "robustness_score": robustness_score,
        "interpretability_score": interpretability_score,
        "survival_status": survival_status,
        "rejection_reason": rejection_reason,
    }
