from __future__ import annotations

from typing import Any


def decide_action(ranked: list[dict[str, Any]], *, min_confidence: float = 0.4) -> dict[str, Any]:
    if not ranked:
        return {
            "action": "WAIT",
            "confidence": 0.0,
            "expectancy": 0.0,
            "reason": "no_ranked_candidates",
            "top_alternatives": [],
        }
    top = ranked[0]
    conf = float(top.get("confidence", top.get("robust_score", 0.0)) or 0.0)
    action = str(top.get("action", "WAIT"))
    if conf < min_confidence:
        action = "WAIT"
    return {
        "action": action,
        "confidence": conf,
        "expectancy": float(top.get("expectancy", 0.0) or 0.0),
        "reason": str(top.get("reason", "robust_consensus")),
        "top_alternatives": ranked[1:4],
    }
