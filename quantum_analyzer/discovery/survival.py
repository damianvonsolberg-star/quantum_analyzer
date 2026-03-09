from __future__ import annotations

from typing import Any


def survival_decision(row: dict[str, Any]) -> tuple[bool, str | None]:
    if float(row.get("oos_usefulness", row.get("action_usefulness", 0.0)) or 0.0) < 0.45:
        return False, "low_oos_usefulness"
    if float(row.get("neighbor_consistency", 0.0) or 0.0) < 0.60:
        return False, "low_neighbor_consistency"
    if float(row.get("cross_window_repeatability", 0.0) or 0.0) < 0.60:
        return False, "low_cross_window_repeatability"
    if float(row.get("regime_specialization", 0.0) or 0.0) > 0.90:
        return False, "too_regime_narrow"
    if float(row.get("redundancy", 1.0) or 1.0) > 0.85:
        return False, "high_redundancy"
    c = float(row.get("complexity_penalty", 0.0) or 0.0)
    b = float(row.get("cost_adjusted_value", 0.0) or 0.0)
    if c > (abs(b) + 0.2):
        return False, "poor_complexity_benefit_tradeoff"
    return True, None


def attach_survival_fields(row: dict[str, Any]) -> dict[str, Any]:
    ok, reason = survival_decision(row)
    x = dict(row)
    x["survival_status"] = "survived" if ok else "rejected"
    x["rejection_reason"] = None if ok else reason
    return x
