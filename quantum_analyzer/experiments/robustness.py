from __future__ import annotations

from typing import Any


def robustness_penalty(row: dict[str, Any]) -> float:
    p = 0.0
    p += max(0.0, 0.3 - float(row.get("start_date_stability", 0.3) or 0.0)) * 0.5
    p += max(0.0, 0.3 - float(row.get("neighbor_stability", 0.3) or 0.0)) * 0.5
    p += max(0.0, float(row.get("regime_concentration", 0.0) or 0.0) - 0.7) * 0.3
    p += float(row.get("complexity", 0.0) or 0.0) * 0.1
    return p


def robust_composite_score(metrics: dict[str, Any]) -> float:
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
    score = 0.0
    for k, wt in w.items():
        score += float(metrics.get(k, 0.0) or 0.0) * wt
    score -= robustness_penalty(metrics)
    return float(max(0.0, min(1.0, score)))
