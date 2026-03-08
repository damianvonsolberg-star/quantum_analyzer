from __future__ import annotations

from dataclasses import dataclass, field

from ui.wallet import WalletBalances


@dataclass
class PortfolioSnapshot:
    wallet: str
    sol: float
    usdc: float
    sol_price_usd: float
    sol_mtm_usd: float
    total_nav_usd: float
    current_sol_weight: float
    dry_powder_usd: float


@dataclass
class PortfolioAdvice:
    model_target_position: float
    target_scope: str
    advisory_mode: str
    spot_implementable_target_weight: float
    target_sol_usd: float
    recommended_delta_usd: float
    recommended_delta_sol: float
    post_trade_target_sol_weight: float
    implementable_in_spot: bool
    unsupported_in_spot: bool
    action_label: str
    warnings: list[str] = field(default_factory=list)
    note: str = ""


def build_portfolio_snapshot(bal: WalletBalances, sol_price_usd: float) -> PortfolioSnapshot:
    sol_mtm = float(bal.sol) * float(sol_price_usd)
    nav = sol_mtm + float(bal.usdc)
    w = (sol_mtm / nav) if nav > 0 else 0.0
    return PortfolioSnapshot(
        wallet=bal.wallet,
        sol=float(bal.sol),
        usdc=float(bal.usdc),
        sol_price_usd=float(sol_price_usd),
        sol_mtm_usd=sol_mtm,
        total_nav_usd=nav,
        current_sol_weight=w,
        dry_powder_usd=float(bal.usdc),
    )


def _spot_mapping(model_target_position: float) -> tuple[float, bool, bool, list[str], str]:
    p = float(model_target_position)
    warnings: list[str] = []
    if p < 0:
        warnings.append("Model target is negative (short/hedge). Not implementable in spot-only workflow.")
        warnings.append("Spot-only actionable mapping is GO FLAT (target 0).")
        return 0.0, False, True, warnings, "HEDGE/SHORT NOT SUPPORTED IN SPOT MODE"
    if p > 1:
        warnings.append("Model target is above 1.0. Clipped to 1.0 for spot-only allocation semantics.")
        return 1.0, True, False, warnings, "BUY SPOT"
    if p == 0.0:
        return 0.0, True, False, warnings, "GO FLAT"
    return p, True, False, warnings, "BUY SPOT"


def advice_from_target(
    snapshot: PortfolioSnapshot,
    target_position: float,
    *,
    advisory_mode: str = "spot_only",
    target_scope: str = "advisory_sleeve",
) -> PortfolioAdvice:
    if advisory_mode == "spot_only":
        spot_target_weight, implementable, unsupported, warnings, action_label = _spot_mapping(target_position)
    else:
        # derivatives-capable mode retains generic model target semantics
        spot_target_weight = float(target_position)
        implementable, unsupported, warnings = True, False, []
        action_label = "BUY SPOT" if target_position > 0 else ("GO FLAT" if target_position == 0 else "REDUCE SPOT")

    target_sol_usd = snapshot.total_nav_usd * float(spot_target_weight)
    delta_usd = target_sol_usd - snapshot.sol_mtm_usd
    delta_sol = (delta_usd / snapshot.sol_price_usd) if snapshot.sol_price_usd > 0 else 0.0

    if advisory_mode == "spot_only" and unsupported:
        # explicit operator-facing spot action for short request
        if snapshot.current_sol_weight > 0:
            action_label = "REDUCE SPOT"
        else:
            action_label = "GO FLAT"

    note = "Model target and spot actionable target are explicitly separated."

    return PortfolioAdvice(
        model_target_position=float(target_position),
        target_scope=target_scope,
        advisory_mode=advisory_mode,
        spot_implementable_target_weight=float(spot_target_weight),
        target_sol_usd=target_sol_usd,
        recommended_delta_usd=delta_usd,
        recommended_delta_sol=delta_sol,
        post_trade_target_sol_weight=float(spot_target_weight),
        implementable_in_spot=implementable,
        unsupported_in_spot=unsupported,
        action_label=action_label,
        warnings=warnings,
        note=note,
    )
