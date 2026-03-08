from __future__ import annotations

from dataclasses import dataclass

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
    target_sol_weight: float
    target_sol_usd: float
    recommended_delta_usd: float
    recommended_delta_sol: float
    post_trade_target_sol_weight: float
    note: str


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


def target_weight_from_position(target_position: float) -> tuple[float, str]:
    p = float(target_position)
    if p < 0:
        return 0.0, "Negative target_position implies short; mapped to 0 for spot advisory mode"
    if p > 1:
        return 1.0, "target_position > 1 clipped to 1.0"
    return p, "ok"


def advice_from_target(snapshot: PortfolioSnapshot, target_position: float) -> PortfolioAdvice:
    target_weight, note = target_weight_from_position(target_position)
    target_sol_usd = snapshot.total_nav_usd * target_weight
    delta_usd = target_sol_usd - snapshot.sol_mtm_usd
    delta_sol = (delta_usd / snapshot.sol_price_usd) if snapshot.sol_price_usd > 0 else 0.0
    return PortfolioAdvice(
        target_sol_weight=target_weight,
        target_sol_usd=target_sol_usd,
        recommended_delta_usd=delta_usd,
        recommended_delta_sol=delta_sol,
        post_trade_target_sol_weight=target_weight,
        note=note,
    )
