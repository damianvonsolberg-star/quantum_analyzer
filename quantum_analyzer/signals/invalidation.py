from __future__ import annotations

from typing import Any


def build_invalidation_reasons(*, entropy: float, governance_status: str, edge_bps: float, cost_bps: float) -> list[str]:
    reasons: list[str] = []
    if entropy > 0.8:
        reasons.append("entropy_too_high")
    if governance_status.upper() in {"WATCH", "HALT"}:
        reasons.append("governance_not_ok")
    if edge_bps <= cost_bps:
        reasons.append("edge_below_cost")
    return reasons
