from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class UiForecastView:
    horizons: list[str] = field(default_factory=list)
    entropy: float | None = None
    confidence: float | None = None
    calibration_score: float | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class UiLiveAdvice:
    timestamp: str
    headline_action: str
    traffic_light: str
    target_position: float | None
    expected_edge_bps: float | None
    expected_cost_bps: float | None
    confidence: float | None
    entropy: float | None
    risk_note: str
    reasons: list[str] = field(default_factory=list)
    advisory_mode: str = "spot_only"
    target_scope: str = "advisory_sleeve"
    top_alternatives: list[dict[str, Any]] = field(default_factory=list)
    invalidation_notes: list[str] = field(default_factory=list)
    status: str | None = None
    release_state: str | None = None
    governance_status: str | None = None
    freshness_state: str | None = None
    symbol: str | None = None
    timeframe: str | None = None
    source_ids: dict[str, Any] = field(default_factory=dict)


@dataclass
class UiBacktestSummary:
    bars: int | None = None
    test_bars: int | None = None
    ending_equity: float | None = None
    return_pct: float | None = None
    max_drawdown: float | None = None
    schema_version: str | None = None
    discovery_survivors: int | None = None
    discovery_rejected: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class UiPathTemplate:
    template_id: str
    label: str | None = None
    expectancy: float | None = None
    support: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class UiDriftStatus:
    ok: bool
    warnings: list[str] = field(default_factory=list)
    hard_failures: list[str] = field(default_factory=list)
    latest_timestamp: str | None = None
    schema_versions: list[str] = field(default_factory=list)
    governance_status: str | None = None
    governance_payload: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class UiPortfolioSnapshot:
    wallet: str
    sol: float | None
    usdc: float | None
    ok: bool
    message: str = ""
    sol_price_usd: float | None = None
    sol_mtm_usd: float | None = None
    total_nav_usd: float | None = None
    current_sol_weight: float | None = None
    dry_powder_usd: float | None = None


# backward-compatible aliases used in current pages
LiveAdviceVM = UiLiveAdvice
BacktestVM = UiBacktestSummary
TemplatesVM = list[UiPathTemplate]
DriftVM = UiDriftStatus
