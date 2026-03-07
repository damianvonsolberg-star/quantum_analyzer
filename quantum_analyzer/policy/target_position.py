from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from quantum_analyzer.contracts import ActionProposal, ForecastBundle
from .execution import apply_turnover_cap
from .risk_caps import DrawdownState, RegimeCaps, effective_abs_cap
from .utility import PolicyConfig, edge_bps, in_no_trade_region


@dataclass
class PolicyInputs:
    forecast: ForecastBundle
    estimated_round_trip_cost_bps: float
    current_position: float
    regime: str
    drawdown_state: DrawdownState
    regime_caps: RegimeCaps
    turnover_cap: float = 0.15
    entropy_threshold: float = 0.75
    calibration_threshold: float = 0.55


def _h36_metrics(forecast: ForecastBundle) -> tuple[float, float, float, float]:
    d = forecast.distributions.get("h36")
    if d is None:
        return 0.0, 0.5, 0.5, 0.5
    q = d.quantiles
    mean_ret = d.mean_return
    p_up = float(q.get("p_up", d.probability_up))
    p_down = float(q.get("p_down", 1 - p_up))
    p_break_down = float(q.get("p_break_down", 0.0))
    return mean_ret, p_up, p_down, p_break_down


def propose_action(inp: PolicyInputs) -> ActionProposal:
    f = inp.forecast
    entropy = float(f.diagnostics.get("entropy", 1.0))
    calib = float(f.diagnostics.get("calibration_score", 0.0))

    mean_ret, p_up, p_down, p_break_down = _h36_metrics(f)
    exp_edge_bps = edge_bps(mean_ret)
    exp_cost_bps = float(inp.estimated_round_trip_cost_bps)

    # hard no-trade gates
    if entropy > inp.entropy_threshold:
        return ActionProposal(
            ts=datetime.now(timezone.utc),
            symbol=f.symbol,
            action="HOLD",
            score=0.0,
            size_fraction=0.0,
            target_position=inp.current_position,
            expected_edge_bps=exp_edge_bps,
            expected_cost_bps=exp_cost_bps,
            reason="No-trade: entropy above threshold",
        )
    if calib < inp.calibration_threshold:
        return ActionProposal(
            ts=datetime.now(timezone.utc),
            symbol=f.symbol,
            action="HOLD",
            score=0.0,
            size_fraction=0.0,
            target_position=inp.current_position,
            expected_edge_bps=exp_edge_bps,
            expected_cost_bps=exp_cost_bps,
            reason="No-trade: calibration below threshold",
        )

    if in_no_trade_region(exp_edge_bps, exp_cost_bps, PolicyConfig().no_trade_band_bps):
        return ActionProposal(
            ts=datetime.now(timezone.utc),
            symbol=f.symbol,
            action="HOLD",
            score=0.0,
            size_fraction=0.0,
            target_position=inp.current_position,
            expected_edge_bps=exp_edge_bps,
            expected_cost_bps=exp_cost_bps,
            reason="No-trade: net edge inside explicit no-trade region",
        )

    # separate short logic (not flipped long)
    abs_cap = effective_abs_cap(inp.regime, inp.drawdown_state.drawdown_pct, inp.regime_caps)
    long_score = max(0.0, p_up - 0.5)
    short_score = max(0.0, p_down - 0.5 + 0.5 * p_break_down)

    if long_score > short_score and exp_edge_bps > exp_cost_bps:
        desired = min(abs_cap, long_score * 2 * abs_cap)
        action = "LONG"
        score = long_score
        reason = "Long policy: directional edge after costs"
    elif short_score > long_score and (-exp_edge_bps) > exp_cost_bps:
        desired = -min(abs_cap, short_score * 2 * abs_cap)
        action = "SHORT"
        score = short_score
        reason = "Short policy: downside + breakdown risk after costs"
    else:
        desired = inp.current_position
        action = "HOLD"
        score = 0.0
        reason = "No-trade: no side clears edge-cost constraints"

    capped_target = apply_turnover_cap(inp.current_position, desired, inp.turnover_cap)
    size_fraction = abs(capped_target - inp.current_position)

    return ActionProposal(
        ts=datetime.now(timezone.utc),
        symbol=f.symbol,
        action=action,
        score=float(score),
        size_fraction=float(size_fraction),
        target_position=float(capped_target),
        expected_edge_bps=float(exp_edge_bps),
        expected_cost_bps=float(exp_cost_bps),
        reason=reason,
        controls={
            "entropy": entropy,
            "calibration_score": calib,
            "turnover_cap": inp.turnover_cap,
            "abs_cap": abs_cap,
            "current_position": inp.current_position,
        },
    )
