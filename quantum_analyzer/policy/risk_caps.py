from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DrawdownState:
    drawdown_pct: float  # negative in drawdown, e.g. -0.06


@dataclass
class RegimeCaps:
    default_max_abs: float = 0.35
    trend_max_abs: float = 0.50
    range_max_abs: float = 0.25
    crisis_max_abs: float = 0.10


def drawdown_tier_multiplier(drawdown_pct: float) -> float:
    if drawdown_pct <= -0.12:
        return 0.0
    if drawdown_pct <= -0.08:
        return 0.25
    if drawdown_pct <= -0.05:
        return 0.5
    if drawdown_pct <= -0.03:
        return 0.75
    return 1.0


def regime_cap(regime: str, caps: RegimeCaps) -> float:
    r = (regime or "").lower()
    if "trend" in r:
        return caps.trend_max_abs
    if "range" in r:
        return caps.range_max_abs
    if "break" in r or "capitulation" in r:
        return caps.crisis_max_abs
    return caps.default_max_abs


def effective_abs_cap(regime: str, drawdown_pct: float, caps: RegimeCaps) -> float:
    return regime_cap(regime, caps) * drawdown_tier_multiplier(drawdown_pct)
