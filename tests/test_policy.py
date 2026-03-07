from __future__ import annotations

from datetime import datetime, timezone

from quantum_analyzer.contracts import ForecastBundle, HorizonDistribution
from quantum_analyzer.policy.risk_caps import DrawdownState, RegimeCaps
from quantum_analyzer.policy.target_position import PolicyInputs, propose_action


def _bundle(mean: float, p_up: float, p_break_down: float, entropy: float = 0.3, calib: float = 0.8) -> ForecastBundle:
    h36 = HorizonDistribution(
        horizon_hours=36,
        mean_return=mean,
        std_return=0.03,
        quantiles={
            "q05": mean - 0.05,
            "q25": mean - 0.02,
            "q50": mean,
            "q75": mean + 0.02,
            "q95": mean + 0.05,
            "p_up": p_up,
            "p_down": 1 - p_up,
            "p_break_up": 0.1,
            "p_break_down": p_break_down,
        },
        probability_up=p_up,
    )
    return ForecastBundle(
        ts=datetime.now(timezone.utc),
        symbol="SOLUSDT",
        distributions={"h36": h36},
        diagnostics={"entropy": entropy, "calibration_score": calib},
    )


def test_no_trade_case_entropy_gate() -> None:
    inp = PolicyInputs(
        forecast=_bundle(mean=0.002, p_up=0.65, p_break_down=0.05, entropy=0.95),
        estimated_round_trip_cost_bps=10,
        current_position=0.1,
        regime="trend_up",
        drawdown_state=DrawdownState(drawdown_pct=-0.01),
        regime_caps=RegimeCaps(),
    )
    ap = propose_action(inp)
    assert ap.action == "HOLD"
    assert "entropy" in ap.reason.lower()


def test_long_case() -> None:
    inp = PolicyInputs(
        forecast=_bundle(mean=0.01, p_up=0.75, p_break_down=0.03),
        estimated_round_trip_cost_bps=5,
        current_position=0.0,
        regime="trend_up",
        drawdown_state=DrawdownState(drawdown_pct=-0.01),
        regime_caps=RegimeCaps(),
        turnover_cap=0.2,
    )
    ap = propose_action(inp)
    assert ap.action in {"LONG", "HOLD"}
    assert -1.0 <= ap.target_position <= 1.0


def test_short_case() -> None:
    inp = PolicyInputs(
        forecast=_bundle(mean=-0.015, p_up=0.25, p_break_down=0.4),
        estimated_round_trip_cost_bps=5,
        current_position=0.0,
        regime="breakdown_down",
        drawdown_state=DrawdownState(drawdown_pct=-0.01),
        regime_caps=RegimeCaps(),
        turnover_cap=0.2,
    )
    ap = propose_action(inp)
    assert ap.action in {"SHORT", "HOLD"}
    assert ap.target_position <= 0.0


def test_turnover_cap_respected() -> None:
    inp = PolicyInputs(
        forecast=_bundle(mean=0.03, p_up=0.9, p_break_down=0.0),
        estimated_round_trip_cost_bps=2,
        current_position=0.0,
        regime="trend_up",
        drawdown_state=DrawdownState(drawdown_pct=-0.01),
        regime_caps=RegimeCaps(),
        turnover_cap=0.05,
    )
    ap = propose_action(inp)
    assert abs(ap.target_position - inp.current_position) <= inp.turnover_cap + 1e-9
