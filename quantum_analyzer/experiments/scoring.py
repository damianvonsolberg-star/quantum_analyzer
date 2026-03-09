from __future__ import annotations

from typing import Any

from .robustness import robust_composite_score


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


def score_result(summary: dict[str, Any], diagnostics: dict[str, Any]) -> dict[str, float | bool]:
    ret = float(summary.get("return_pct", 0.0) or 0.0)
    mdd = abs(float(diagnostics.get("max_drawdown", 0.0) or 0.0))
    pf = float(diagnostics.get("profit_factor", 1.0) or 1.0)
    exp = float(diagnostics.get("expectancy", 0.0) or 0.0)
    cal = float(diagnostics.get("calibration_proxy", 0.5) or 0.5)
    turnover = float(diagnostics.get("turnover", 0.0) or 0.0)

    action_quality = float(((diagnostics.get("action_quality") or {}).get("BUY", {}) or {}).get("hit_rate", 0.5) or 0.5)
    sample_support = float(summary.get("test_bars", 0.0) or 0.0)
    regime_worst = float(min((diagnostics.get("performance_by_vol_bucket") or {"x": 0.0}).values()) if diagnostics.get("performance_by_vol_bucket") else 0.0)

    # hard gates
    hard_ok = True
    if mdd > 0.35:
        hard_ok = False
    if exp <= 0.0:
        hard_ok = False
    if action_quality < 0.2:
        hard_ok = False
    if sample_support < 20:
        hard_ok = False
    if regime_worst < -0.05:
        hard_ok = False

    s_ret = _clip01((ret + 0.2) / 0.6)
    s_dd = _clip01(1.0 - (mdd / 0.35))
    s_pf = _clip01((pf - 1.0) / 1.5)
    s_exp = _clip01(exp / 0.01)
    s_cal = _clip01(cal)
    s_turn = _clip01(1.0 - min(turnover, 2.0) / 2.0)

    s_drift = _clip01(1.0 - float(diagnostics.get("drift_stability", 0.0) or 0.0))
    s_cons = _clip01(float(diagnostics.get("action_consistency", 0.5) or 0.5))
    s_cross = _clip01(float(diagnostics.get("cross_window_robustness", 0.5) or 0.5))
    s_reg = _clip01(float(diagnostics.get("regime_robustness", 0.5) or 0.5))
    s_btc = _clip01(float(diagnostics.get("btc_context_quality", 0.5) or 0.5))
    s_vol = _clip01(float(diagnostics.get("volatility_regime_performance", 0.5) or 0.5))
    s_conf = _clip01(float(diagnostics.get("confidence_reliability", cal) or cal))
    s_ent = _clip01(1.0 - float(diagnostics.get("entropy_quality", 0.5) or 0.5))

    metrics = {
        "s_return": s_ret,
        "s_drawdown": s_dd,
        "s_profit_factor": s_pf,
        "s_expectancy": s_exp,
        "s_calibration": s_cal,
        "s_turnover": s_turn,
        "s_drift_stability": s_drift,
        "s_action_consistency": s_cons,
        "s_cross_window": s_cross,
        "s_regime_robustness": s_reg,
        "s_btc_quality": s_btc,
        "s_vol_regime": s_vol,
        "s_confidence_reliability": s_conf,
        "s_entropy_quality": s_ent,
        "start_date_stability": float(diagnostics.get("start_date_stability", 0.5) or 0.5),
        "neighbor_stability": float(diagnostics.get("neighbor_stability", 0.5) or 0.5),
        "regime_concentration": float(diagnostics.get("regime_concentration", 0.5) or 0.5),
        "complexity": float(diagnostics.get("complexity", 0.0) or 0.0),
    }

    robust_score = robust_composite_score(metrics)

    return {
        "hard_gate_pass": hard_ok,
        "score": float(robust_score),
        "robust_score": float(robust_score),
        "action_quality": action_quality,
        "sample_support": sample_support,
        "regime_worst": regime_worst,
        **metrics,
    }
