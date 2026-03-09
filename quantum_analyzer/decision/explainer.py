from __future__ import annotations

from typing import Any


def explain_decision(decision: dict[str, Any], ranked: list[dict[str, Any]]) -> dict[str, Any]:
    alts = decision.get("top_alternatives", [])
    why_lost = []
    top_score = float(ranked[0].get("robust_score", 0.0)) if ranked else 0.0
    for a in alts:
        s = float(a.get("robust_score", 0.0) or 0.0)
        why_lost.append(
            {
                "candidate_id": a.get("candidate_id"),
                "family": a.get("candidate_family"),
                "why_lost": f"score_gap={top_score - s:.4f}",
            }
        )
    return {
        "final_action": decision.get("action", "WAIT"),
        "confidence": decision.get("confidence", 0.0),
        "expectancy": decision.get("expectancy", 0.0),
        "regime_explanation": ranked[0].get("regime_slice", "all") if ranked else "unknown",
        "supporting_metrics": ranked[0] if ranked else {},
        "alternatives": why_lost,
    }
