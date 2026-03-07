from __future__ import annotations


def apply_turnover_cap(current_position: float, target_position: float, turnover_cap: float) -> float:
    """Limit single-step absolute position delta."""
    delta = target_position - current_position
    if abs(delta) <= turnover_cap:
        return target_position
    return current_position + (turnover_cap if delta > 0 else -turnover_cap)
