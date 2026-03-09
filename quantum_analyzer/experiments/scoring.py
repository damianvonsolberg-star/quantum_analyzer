from __future__ import annotations

from typing import Any

from .robustness import robust_composite_score


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def score_result(summary: dict[str, Any], diagnostics: dict[str, Any]) -> dict[str, Any]:
    ret = float(summary.get("return_pct", 0.0) or 0.0)
    mdd = abs(float(diagnostics.get("max_drawdown", 0.0) or 0.0))
    cal = float(diagnostics.get("calibration_proxy", 0.0) or 0.0)
    turnover = float(diagnostics.get("turnover", 0.0) or 0.0)

    aq_raw = diagnostics.get("action_quality")
    aq = (aq_raw or {}).get("BUY", {}) or {}
    action_quality_available = isinstance(aq_raw, dict) and bool(aq_raw)
    action_quality = float(aq.get("hit_rate", 0.5 if not action_quality_available else 0.0) or 0.0)
    exp = float(aq.get("avg_pnl", diagnostics.get("expectancy", 0.0)) or 0.0)
    sample_support_available = "test_bars" in summary and summary.get("test_bars") is not None
    sample_support = float(summary.get("test_bars", 0.0) or 0.0)

    vol_perf = diagnostics.get("performance_by_vol_bucket") or {}
    regime_worst = float(min(vol_perf.values())) if vol_perf else 0.0

    # explicit hard gates + reasons
    hard_gate_failures: list[str] = []
    if mdd > 0.35:
        hard_gate_failures.append("max_drawdown_ceiling")
    if exp <= 0.0:
        hard_gate_failures.append("non_positive_expectancy_after_cost")
    if sample_support_available and sample_support < 20:
        hard_gate_failures.append("insufficient_sample_support")
    if regime_worst < -0.05:
        hard_gate_failures.append("catastrophic_regime_failure")
    if action_quality_available and action_quality < 0.35:
        hard_gate_failures.append("confidence_reliability_failure")
    if turnover > 2.0:
        hard_gate_failures.append("excessive_turnover_cost_fragility")

    # benchmark-relative components (if unavailable, explicit zero contribution)
    baseline_cash = float(summary.get("baseline_wait_return_pct", 0.0) or 0.0)
    baseline_long = float(summary.get("baseline_always_long_return_pct", 0.0) or 0.0)
    baseline_btc = float(summary.get("baseline_btc_follow_return_pct", 0.0) or 0.0)
    benchmark_lift = min(ret - baseline_cash, ret - baseline_long, ret - baseline_btc)

    s_ret = _clip01((ret + 0.2) / 0.6)
    s_dd = _clip01(1.0 - (mdd / 0.35))
    s_pf = _clip01(float(diagnostics.get("profit_factor", 1.0) or 1.0) / 2.0)
    s_exp = _clip01(exp / 0.01)
    s_cal = _clip01(cal)
    s_turn = _clip01(1.0 - min(turnover, 2.0) / 2.0)
    unavailable_metrics: list[str] = []

    def _metric(name: str, neutral: float = 0.5) -> float:
        if name not in diagnostics or diagnostics.get(name) is None:
            unavailable_metrics.append(name)
            return float(neutral)
        return float(diagnostics.get(name) or neutral)

    s_drift = _clip01(1.0 - _metric("drift_stability", 0.5))
    s_cons = _clip01(_metric("action_consistency", 0.5))
    s_cross = _clip01(_metric("cross_window_robustness", 0.5))
    s_reg = _clip01(_metric("regime_robustness", 0.5))
    s_btc = _clip01(_metric("btc_context_quality", 0.5))
    s_vol = _clip01(_metric("volatility_regime_performance", 0.5))
    s_conf = _clip01(_metric("confidence_reliability", cal))
    s_ent = _clip01(1.0 - _metric("entropy_quality", 0.5))

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
        "start_date_stability": float(diagnostics.get("start_date_stability", 0.0) or 0.0),
        "neighbor_stability": float(diagnostics.get("neighbor_stability", 0.0) or 0.0),
        "regime_concentration": float(diagnostics.get("regime_concentration", 0.0) or 0.0),
        "complexity": float(diagnostics.get("complexity", 0.0) or 0.0),
        "benchmark_lift": benchmark_lift,
    }

    robust_score = robust_composite_score(metrics)
    hard_ok = len(hard_gate_failures) == 0
    final_score = float(robust_score if hard_ok else 0.0)

    return {
        "hard_gate_pass": hard_ok,
        "hard_gate_failures": hard_gate_failures,
        "score": final_score,
        "robust_score": float(robust_score),
        "promoted_score": final_score,
        "action_quality": action_quality,
        "sample_support": sample_support,
        "regime_worst": regime_worst,
        "benchmark_lift": benchmark_lift,
        "unavailable_metrics": unavailable_metrics,
        **metrics,
    }
