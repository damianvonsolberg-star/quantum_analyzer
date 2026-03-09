from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ReleaseGateResult:
    passed: bool
    overall_state: str  # EDGE | LOW_EDGE | NO_EDGE
    failures: list[str]
    metrics: dict[str, float]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "overall_state": self.overall_state,
            "failures": list(self.failures),
            "metrics": dict(self.metrics),
        }


def evaluate_release_gates(candidate: dict[str, Any]) -> ReleaseGateResult:
    m = {
        "post_cost_expectancy": float(candidate.get("expectancy", 0.0) or 0.0),
        "max_drawdown": abs(float(candidate.get("max_drawdown", 0.0) or 0.0)),
        "calibration_reliability": float(candidate.get("confidence_reliability", candidate.get("s_confidence_reliability", 0.0)) or 0.0),
        "action_quality": float(candidate.get("action_quality", 0.0) or 0.0),
        "slippage_sensitivity": float(candidate.get("turnover", 0.0) or 0.0),
        "regime_robustness": float(candidate.get("regime_robustness", candidate.get("s_regime_robustness", 0.0)) or 0.0),
        "start_stability": float(candidate.get("start_date_stability", 0.0) or 0.0),
        "neighbor_stability": float(candidate.get("neighbor_stability", 0.0) or 0.0),
        "benchmark_lift": float(candidate.get("benchmark_lift", 0.0) or 0.0),
    }

    failures: list[str] = []
    if m["post_cost_expectancy"] <= 0.0:
        failures.append("non_positive_post_cost_expectancy")
    if m["max_drawdown"] > 0.35:
        failures.append("max_drawdown_too_high")
    if m["calibration_reliability"] < 0.45:
        failures.append("calibration_reliability_low")
    if m["action_quality"] < 0.35:
        failures.append("action_quality_low")
    if m["slippage_sensitivity"] > 2.0:
        failures.append("slippage_fragility")
    if m["regime_robustness"] < 0.45:
        failures.append("regime_robustness_low")
    if m["start_stability"] < 0.40:
        failures.append("start_date_instability")
    if m["neighbor_stability"] < 0.40:
        failures.append("neighbor_instability")
    if m["benchmark_lift"] <= 0.0:
        failures.append("no_benchmark_outperformance")

    passed = len(failures) == 0
    overall_state = "EDGE" if passed else ("LOW_EDGE" if len(failures) <= 2 else "NO_EDGE")
    return ReleaseGateResult(passed=passed, overall_state=overall_state, failures=failures, metrics=m)
