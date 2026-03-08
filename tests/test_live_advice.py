from __future__ import annotations

from datetime import datetime, timezone, timedelta

from ui.recommendation import decide_recommendation
from ui.view_models import UiDriftStatus, UiLiveAdvice, UiPortfolioSnapshot


def _base_advice(**kwargs):
    x = UiLiveAdvice(
        timestamp=datetime.now(timezone.utc).isoformat(),
        headline_action="BUY",
        traffic_light="green",
        target_position=0.6,
        expected_edge_bps=12.0,
        expected_cost_bps=5.0,
        confidence=0.72,
        entropy=0.35,
        risk_note="normal",
        reasons=["edge positive", "trend"],
    )
    for k, v in kwargs.items():
        setattr(x, k, v)
    return x


def _base_portfolio(weight=0.2):
    return UiPortfolioSnapshot(
        wallet="abc",
        sol=10.0,
        usdc=500.0,
        ok=True,
        current_sol_weight=weight,
        total_nav_usd=1500.0,
        sol_price_usd=100.0,
    )


def _base_drift(ok=True, hard_failures=None, governance_status=None):
    return UiDriftStatus(ok=ok, hard_failures=hard_failures or [], warnings=[], governance_status=governance_status)


def test_halt_on_drift_failure():
    rec = decide_recommendation(_base_advice(), _base_portfolio(), _base_drift(ok=False, hard_failures=["feature_psi_breach"]))
    assert rec.light == "HALT"


def test_stale_artifact_status_consistency():
    old_ts = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    rec = decide_recommendation(_base_advice(timestamp=old_ts), _base_portfolio(), _base_drift(governance_status="WATCH"))
    assert rec.light == "WATCH"


def test_live_and_drift_status_consistency():
    rec = decide_recommendation(_base_advice(target_position=0.7, confidence=0.8, entropy=0.3), _base_portfolio(weight=0.2), _base_drift(governance_status="WATCH"))
    assert rec.light == "WATCH"


def test_green_when_target_above_current_and_quality_good():
    rec = decide_recommendation(_base_advice(target_position=0.7, confidence=0.8, entropy=0.3), _base_portfolio(weight=0.2), _base_drift(governance_status="OK"))
    assert rec.light == "GREEN"
    assert rec.action_text == "BUY"


def test_red_when_target_below_current():
    rec = decide_recommendation(_base_advice(target_position=0.1), _base_portfolio(weight=0.4), _base_drift())
    assert rec.light == "RED"


def test_yellow_when_uncertain_or_small_delta():
    rec = decide_recommendation(_base_advice(target_position=0.22, confidence=0.52, entropy=0.8), _base_portfolio(weight=0.2), _base_drift())
    assert rec.light == "YELLOW"


def test_live_advice_labels_wallet_vs_sleeve_semantics():
    advice = _base_advice()
    assert advice.advisory_mode == "spot_only"
    assert advice.target_scope == "advisory_sleeve"
