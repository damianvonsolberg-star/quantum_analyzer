from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from ui.view_models import UiDriftStatus, UiLiveAdvice, UiPortfolioSnapshot


@dataclass
class RecommendationView:
    light: str  # HALT|GREEN|YELLOW|RED
    action_text: str
    tail_risk_note: str
    top_reasons: list[str] = field(default_factory=list)
    top_risks: list[str] = field(default_factory=list)
    what_changes_light: str = ""
    stale: bool = False


def _parse_ts(ts: str) -> datetime | None:
    if not ts:
        return None
    t = ts.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def is_stale(ts: str, now: datetime | None = None, max_age_minutes: int = 180) -> bool:
    dt = _parse_ts(ts)
    if dt is None:
        return True
    now = now or datetime.now(timezone.utc)
    return now - dt > timedelta(minutes=max_age_minutes)


def decide_recommendation(
    advice: UiLiveAdvice,
    portfolio: UiPortfolioSnapshot | None,
    drift: UiDriftStatus,
    now: datetime | None = None,
) -> RecommendationView:
    stale = is_stale(advice.timestamp, now=now)

    required_invalid = (
        advice.headline_action == ""
        or advice.confidence is None
        or advice.entropy is None
    )

    status = (drift.governance_status or ("OK" if drift.ok else "HALT")).upper()

    if status == "HALT" or required_invalid:
        risks = list(drift.hard_failures[:3])
        if required_invalid:
            risks.append("Missing required advice fields")
        change_hint = "Fix kill-switch reason(s): " + (", ".join(risks[:2]) if risks else "validate artifacts and drift payload")
        return RecommendationView(
            light="HALT",
            action_text="HALT",
            tail_risk_note=advice.risk_note or "Risk controls active.",
            top_reasons=advice.reasons[:3],
            top_risks=risks[:3],
            what_changes_light=change_hint,
            stale=stale,
        )

    if status == "WATCH":
        watch_risks = (drift.hard_failures[:3] if drift.hard_failures else ["Use reduced trust", "Await fresher diagnostics", "Avoid aggressive sizing"])
        if stale:
            change_hint = "Load fresh artifacts and refresh wallet/price to move from WATCH to OK."
        else:
            change_hint = "Reduce drift metrics below watch thresholds and keep data/artifacts fresh."
        return RecommendationView(
            light="WATCH",
            action_text="HOLD / WAIT",
            tail_risk_note=advice.risk_note or "Governance is in WATCH mode.",
            top_reasons=advice.reasons[:3] or ["Governance watch"],
            top_risks=watch_risks,
            what_changes_light=change_hint,
            stale=stale,
        )

    current_w = float(portfolio.current_sol_weight) if portfolio and portfolio.current_sol_weight is not None else 0.0
    delta = float(advice.target_position) - current_w
    conf_ok = (advice.confidence or 0.0) >= 0.55
    ent_ok = (advice.entropy or 1.0) <= 0.75

    if advice.target_position < 0 or "HEDGE" in advice.headline_action.upper() or "REDUCE" in advice.headline_action.upper():
        return RecommendationView(
            light="RED",
            action_text="REDUCE / HEDGE",
            tail_risk_note=advice.risk_note or "Downside risk elevated.",
            top_reasons=advice.reasons[:3],
            top_risks=["Risk reduction mode", "Potential downside momentum", "Preserve capital"],
            what_changes_light="Need stronger edge, improved confidence, and lower uncertainty.",
            stale=False,
        )

    if delta > 0.05 and conf_ok and ent_ok:
        return RecommendationView(
            light="GREEN",
            action_text="BUY",
            tail_risk_note=advice.risk_note or "Risk acceptable for adding exposure.",
            top_reasons=advice.reasons[:3],
            top_risks=["Execution slippage", "Sudden volatility spike", "Model drift"],
            what_changes_light="Turns yellow/red if confidence drops or entropy rises.",
            stale=False,
        )

    if delta < -0.05:
        return RecommendationView(
            light="RED",
            action_text="REDUCE",
            tail_risk_note=advice.risk_note or "Trim risk exposure.",
            top_reasons=advice.reasons[:3],
            top_risks=["Overexposed vs target", "Tail risk", "Adverse trend"],
            what_changes_light="Turns yellow/green if target allocation rises and risk eases.",
            stale=False,
        )

    return RecommendationView(
        light="YELLOW",
        action_text="HOLD / WAIT",
        tail_risk_note=advice.risk_note or "No strong edge right now.",
        top_reasons=advice.reasons[:3] or ["Weak edge"],
        top_risks=["High uncertainty", "Chop/no-trade region", "Cost may dominate edge"],
        what_changes_light="Need clearer edge and lower uncertainty for GREEN.",
        stale=False,
    )
