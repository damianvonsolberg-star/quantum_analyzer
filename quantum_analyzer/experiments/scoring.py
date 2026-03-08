from __future__ import annotations

from typing import Any


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


def score_result(summary: dict[str, Any], diagnostics: dict[str, Any]) -> dict[str, float | bool]:
    ret = float(summary.get("return_pct", 0.0) or 0.0)
    mdd = abs(float(diagnostics.get("max_drawdown", 0.0) or 0.0))
    pf = float(diagnostics.get("profit_factor", 1.0) or 1.0)
    exp = float(diagnostics.get("expectancy", 0.0) or 0.0)
    cal = float(diagnostics.get("calibration_proxy", 0.5) or 0.5)
    turnover = float(diagnostics.get("turnover", 0.0) or 0.0)

    # hard gates
    hard_ok = True
    if mdd > 0.35:
        hard_ok = False
    if exp <= 0.0:
        hard_ok = False

    s_ret = _clip01((ret + 0.2) / 0.6)
    s_dd = _clip01(1.0 - (mdd / 0.35))
    s_pf = _clip01((pf - 1.0) / 1.5)
    s_exp = _clip01(exp / 0.01)
    s_cal = _clip01(cal)
    s_turn = _clip01(1.0 - min(turnover, 2.0) / 2.0)

    # weighted geometric-style blend
    eps = 1e-6
    score = (
        (s_ret + eps) ** 0.24
        * (s_dd + eps) ** 0.24
        * (s_pf + eps) ** 0.16
        * (s_exp + eps) ** 0.16
        * (s_cal + eps) ** 0.12
        * (s_turn + eps) ** 0.08
    )

    return {
        "hard_gate_pass": hard_ok,
        "score": float(score),
        "s_return": s_ret,
        "s_drawdown": s_dd,
        "s_profit_factor": s_pf,
        "s_expectancy": s_exp,
        "s_calibration": s_cal,
        "s_turnover": s_turn,
    }
