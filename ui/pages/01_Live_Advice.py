from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.adapters import AdapterValidationError, ArtifactAdapter
from ui.components import artifact_banner, render_headline_card, sidebar_controls
from ui.portfolio import advice_from_target, build_portfolio_snapshot
from ui.recommendation import decide_recommendation
from ui.state import init_state
from ui.view_models import UiPortfolioSnapshot
from ui.wallet import WalletFetchError, fetch_solusdt_price, fetch_wallet_balances, resolve_rpc_url, resolve_wallet


st.set_page_config(page_title="Live Advice", layout="wide")
init_state()
sidebar_controls()
st.title("Live Advice")
artifact_banner()

adapter = ArtifactAdapter(st.session_state["artifact_dir"])

try:
    ui_advice = adapter.to_live_advice()
    drift = adapter.to_drift_status()
    forecast = adapter.to_forecast_view()
except AdapterValidationError as e:
    st.error(f"Invalid/missing artifact data: {e}")
    st.stop()

def _refresh_wallet_snapshot() -> None:
    rpc_url = resolve_rpc_url(st.session_state.get("rpc_url"))
    wallet = resolve_wallet(st.session_state.get("wallet_address"))
    if not wallet:
        st.session_state["wallet_snapshot"] = UiPortfolioSnapshot(
            wallet="",
            sol=None,
            usdc=None,
            ok=False,
            message="Set wallet address in sidebar (or BENCHMARK_WALLET in .env)",
        )
        return

    bal = fetch_wallet_balances(rpc_url=rpc_url, wallet=wallet)
    px = fetch_solusdt_price()
    snap = build_portfolio_snapshot(bal, px)
    st.session_state["wallet_snapshot"] = UiPortfolioSnapshot(
        wallet=snap.wallet,
        sol=snap.sol,
        usdc=snap.usdc,
        ok=True,
        message="ok",
        sol_price_usd=snap.sol_price_usd,
        sol_mtm_usd=snap.sol_mtm_usd,
        total_nav_usd=snap.total_nav_usd,
        current_sol_weight=snap.current_sol_weight,
        dry_powder_usd=snap.dry_powder_usd,
    )
    st.session_state["last_live_refresh_ts"] = datetime.now(timezone.utc).isoformat()


# refresh wallet+price (manual + automatic)
manual_refresh = st.button("Refresh live wallet + price")
if manual_refresh:
    try:
        _refresh_wallet_snapshot()
    except WalletFetchError as e:
        st.session_state["wallet_snapshot"] = UiPortfolioSnapshot(wallet="", sol=None, usdc=None, ok=False, message=str(e))

if "wallet_snapshot" not in st.session_state:
    try:
        _refresh_wallet_snapshot()
    except WalletFetchError as e:
        st.session_state["wallet_snapshot"] = UiPortfolioSnapshot(wallet="", sol=None, usdc=None, ok=False, message=str(e))

snap = st.session_state.get("wallet_snapshot")
if not isinstance(snap, UiPortfolioSnapshot):
    snap = UiPortfolioSnapshot(wallet="", sol=0.0, usdc=0.0, ok=False, message="Wallet not refreshed yet", sol_price_usd=None, total_nav_usd=None, current_sol_weight=0.0, dry_powder_usd=0.0)

rec = decide_recommendation(ui_advice, snap, drift)

portfolio_advice = None
if snap.ok and snap.total_nav_usd is not None and snap.sol_price_usd is not None:
    from ui.portfolio import PortfolioSnapshot

    ps = PortfolioSnapshot(
        wallet=snap.wallet,
        sol=snap.sol or 0.0,
        usdc=snap.usdc or 0.0,
        sol_price_usd=snap.sol_price_usd,
        sol_mtm_usd=snap.sol_mtm_usd or 0.0,
        total_nav_usd=snap.total_nav_usd,
        current_sol_weight=snap.current_sol_weight or 0.0,
        dry_powder_usd=snap.dry_powder_usd or 0.0,
    )
    portfolio_advice = advice_from_target(ps, ui_advice.target_position)

render_headline_card(rec.light, rec.action_text, "Simple advisory view (read-only)")

m1, m2, m3, m4 = st.columns(4)
m1.metric("Wallet SOL", f"{(snap.sol or 0.0):.4f}")
m2.metric("Wallet USDC", f"{(snap.usdc or 0.0):.2f}")
m3.metric("Current NAV", f"${(snap.total_nav_usd or 0.0):,.2f}")
m4.metric("Current SOL Allocation", f"{(snap.current_sol_weight or 0.0)*100:.2f}%")

n1, n2, n3 = st.columns(3)
if portfolio_advice is not None:
    n1.metric("Recommended Delta USD", f"{portfolio_advice.recommended_delta_usd:+,.2f}")
    n2.metric("Recommended Delta SOL", f"{portfolio_advice.recommended_delta_sol:+.4f}")
    n3.metric("Target Allocation", f"{portfolio_advice.post_trade_target_sol_weight*100:.2f}%")
else:
    n1.metric("Recommended Delta USD", "n/a")
    n2.metric("Recommended Delta SOL", "n/a")
    n3.metric("Target Allocation", f"{ui_advice.target_position*100:.2f}%")

s1, s2, s3 = st.columns(3)
s1.metric("Confidence", f"{(ui_advice.confidence or 0.0):.2f}")
s2.metric("Entropy", f"{(ui_advice.entropy or 0.0):.2f}")
s3.metric("Edge vs Cost (bps)", f"{ui_advice.expected_edge_bps:.2f} / {ui_advice.expected_cost_bps:.2f}")

st.markdown(f"**Tail risk note:** {rec.tail_risk_note}")
st.markdown("**Top 3 reasons:** " + (", ".join(rec.top_reasons[:3]) if rec.top_reasons else "n/a"))
st.markdown("**Top 3 risks:** " + (", ".join(rec.top_risks[:3]) if rec.top_risks else "n/a"))
st.markdown(f"**What would change the light:** {rec.what_changes_light}")

l1, l2 = st.columns(2)
l1.caption(f"Last artifact timestamp: {ui_advice.timestamp}")
l2.caption(f"Last live refresh timestamp: {st.session_state.get('last_live_refresh_ts', 'not refreshed')}")

with st.expander("Advanced stats"):
    st.json(
        {
            "live_advice": ui_advice.__dict__,
            "forecast": forecast.__dict__,
            "drift": drift.__dict__,
            "wallet": snap.__dict__,
            "portfolio_advice": (portfolio_advice.__dict__ if portfolio_advice is not None else None),
        }
    )

st.info("Advisory-only. No signing, no private keys, no order execution.")
