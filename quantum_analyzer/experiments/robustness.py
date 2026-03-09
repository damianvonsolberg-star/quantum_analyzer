from __future__ import annotations

import math
from typing import Any


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def robustness_penalty(row: dict[str, Any]) -> float:
    p = 0.0
    p += max(0.0, 0.4 - float(row.get("start_date_stability", 0.0) or 0.0)) * 0.6
    p += max(0.0, 0.4 - float(row.get("neighbor_stability", 0.0) or 0.0)) * 0.6
    p += max(0.0, float(row.get("regime_concentration", 0.0) or 0.0) - 0.7) * 0.4
    p += max(0.0, float(row.get("complexity", 0.0) or 0.0) - 0.6) * 0.2
    return float(max(0.0, p))


def robust_composite_score(metrics: dict[str, Any]) -> float:
    """Robustness-first weighted geometric score (multiplicative discipline)."""
    w = {
        "s_return": 0.10,
        "s_drawdown": 0.12,
        "s_profit_factor": 0.08,
        "s_expectancy": 0.10,
        "s_calibration": 0.07,
        "s_drift_stability": 0.08,
        "s_action_consistency": 0.08,
        "s_turnover": 0.06,
        "s_cross_window": 0.08,
        "s_regime_robustness": 0.08,
        "s_btc_quality": 0.05,
        "s_vol_regime": 0.05,
        "s_confidence_reliability": 0.07,
        "s_entropy_quality": 0.06,
    }

    eps = 1e-6
    log_sum = 0.0
    weight_sum = 0.0
    for k, wt in w.items():
        v = _clip01(float(metrics.get(k, 0.0) or 0.0))
        log_sum += wt * math.log(max(v, eps))
        weight_sum += wt

    if weight_sum <= 0.0:
        base = 0.0
    else:
        base = float(math.exp(log_sum / weight_sum))

    score = base - robustness_penalty(metrics)
    return float(_clip01(score))
