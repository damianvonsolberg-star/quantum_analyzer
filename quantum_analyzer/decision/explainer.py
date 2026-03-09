from __future__ import annotations

from typing import Any


def explain_decision(decision: dict[str, Any], ranked: list[dict[str, Any]]) -> dict[str, Any]:
    alts = decision.get("top_alternatives", [])
    why_lost = []
    top = ranked[0] if ranked else {}
    top_score = float(top.get("cluster_score", top.get("robust_score", 0.0)) or 0.0)
    top_cost = float(top.get("expected_cost_bps", 0.0) or 0.0)
    top_ctx = float(top.get("context_match", 0.5) or 0.5)

    for a in alts:
        s = float(a.get("cluster_score", a.get("robust_score", 0.0)) or 0.0)
        a_cost = float(a.get("expected_cost_bps", 0.0) or 0.0)
        a_ctx = float(a.get("context_match", 0.5) or 0.5)
        reasons = [f"score_gap={top_score - s:.4f}"]
        if a_cost > top_cost:
            reasons.append("higher_cost_fragility")
        if a_ctx < top_ctx:
            reasons.append("weaker_context_match")
        if float(a.get("sample_support", 0.0) or 0.0) < float(top.get("sample_support", 0.0) or 0.0):
            reasons.append("lower_support")
        why_lost.append(
            {
                "candidate_id": a.get("candidate_id"),
                "family": a.get("candidate_family"),
                "score": s,
                "why_lost": ", ".join(reasons),
            }
        )

    return {
        "final_action": decision.get("action", "WAIT"),
        "confidence": decision.get("confidence", 0.0),
        "expectancy": decision.get("expectancy", 0.0),
        "regime_explanation": top.get("regime_slice", "all") if ranked else "unknown",
        "selected_cluster": {
            "candidate_id": top.get("candidate_id"),
            "cluster_score": top.get("cluster_score", top.get("robust_score", 0.0)),
            "support": top.get("sample_support", 0.0),
            "agreement": top.get("agreement", 0.0),
        },
        "supporting_metrics": top if ranked else {},
        "alternatives": why_lost,
    }
