from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.adapters import AdapterValidationError, ArtifactAdapter
from ui.components import artifact_banner, render_headline_card, render_soft_card, sidebar_controls
from ui.portfolio import advice_from_target, build_portfolio_snapshot
from ui.recommendation import decide_recommendation
from ui.state import init_state
from ui.view_models import UiPortfolioSnapshot
from ui.wallet import WalletFetchError, fetch_solusdt_price, fetch_wallet_balances, resolve_rpc_url, resolve_wallet


st.set_page_config(page_title="Live Advice", layout="wide")
init_state()
sidebar_controls()
st.title("Live Advice · Decision Desk")
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
    portfolio_advice = advice_from_target(
        ps,
        ui_advice.target_position,
        advisory_mode="spot_only",
        target_scope="advisory_sleeve",
    )

display_action = portfolio_advice.action_label if portfolio_advice is not None else rec.action_text
render_headline_card(rec.light, display_action, "Simple advisory view (read-only)")

st.markdown(f"**What to do now:** {display_action}. {'Proceed cautiously.' if rec.light in {'WATCH','YELLOW'} else ('Do not act until resolved.' if rec.light=='HALT' else 'Normal advisory confidence.')}")

m1, m2, m3, m4 = st.columns(4)
with m1:
    render_soft_card("Wallet SOL", f"{(snap.sol or 0.0):,.4f}")
with m2:
    render_soft_card("Wallet USDC", f"{(snap.usdc or 0.0):,.2f}")
with m3:
    render_soft_card("Current NAV", f"${(snap.total_nav_usd or 0.0):,.2f}")
with m4:
    render_soft_card("Current SOL Allocation", f"{(snap.current_sol_weight or 0.0)*100:.2f}%")

n1, n2, n3 = st.columns(3)
if portfolio_advice is not None:
    with n1:
        render_soft_card("Do this now (USD)", f"{portfolio_advice.recommended_delta_usd:+,.2f}", "Spot actionable delta")
    with n2:
        render_soft_card("Do this now (SOL)", f"{portfolio_advice.recommended_delta_sol:+.4f}", "Spot actionable delta")
    with n3:
        render_soft_card("Target spot allocation", f"{portfolio_advice.post_trade_target_sol_weight*100:.2f}%", "For selected advisory scope")
else:
    with n1:
        render_soft_card("Do this now (USD)", "n/a")
    with n2:
        render_soft_card("Do this now (SOL)", "n/a")
    with n3:
        render_soft_card("Target spot allocation", f"{ui_advice.target_position*100:.2f}%")

s1, s2, s3 = st.columns(3)
s1.metric("Confidence", f"{(ui_advice.confidence or 0.0):.2f}")
s2.metric("Entropy", f"{(ui_advice.entropy or 0.0):.2f}")
s3.metric("Edge vs Cost (bps)", f"{ui_advice.expected_edge_bps:.2f} / {ui_advice.expected_cost_bps:.2f}")

gov_status = (drift.governance_status or ("OK" if drift.ok else "HALT")).upper()
gov_reasons = drift.hard_failures[:3]

st.markdown(f"**Governance status:** {gov_status}")
if gov_reasons:
    st.markdown("**Governance reasons:** " + ", ".join(gov_reasons))

if portfolio_advice is not None:
    st.markdown(f"**Main spot recommendation:** {portfolio_advice.action_label}")
    st.markdown(f"**Model raw action field:** {ui_advice.headline_action}")
    st.markdown(
        f"**Target semantics:** model target_position={portfolio_advice.model_target_position:+.3f} (generic model target), "
        f"spot actionable target={portfolio_advice.spot_implementable_target_weight:+.3f} (spot only), "
        f"scope={portfolio_advice.target_scope} (whole wallet vs advisory sleeve), mode={portfolio_advice.advisory_mode}."
    )
    if portfolio_advice.warnings:
        for w in portfolio_advice.warnings[:3]:
            st.warning(w)

# explicit reduced-trust warning when drift diagnostics are incomplete
if gov_status in {"WATCH", "HALT"} and not gov_reasons:
    st.warning("Reduced-trust mode: drift/doctor details are incomplete. Treat advisory conservatively until diagnostics are fully available.")

st.markdown(f"**Tail risk note:** {rec.tail_risk_note}")
st.markdown("**Top 3 reasons:** " + (", ".join(rec.top_reasons[:3]) if rec.top_reasons else "n/a"))
st.markdown("**Top 3 risks:** " + (", ".join(rec.top_risks[:3]) if rec.top_risks else "n/a"))
st.markdown(f"**What would change the light:** {rec.what_changes_light}")

def _freshness_badge(ts_str: str | None, label: str) -> str:
    if not ts_str:
        return f"🔴 {label}: missing"
    try:
        t = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        age_min = (now - t).total_seconds() / 60.0
        if age_min <= 180:
            return f"🟢 {label}: fresh ({age_min:.0f}m)"
        if age_min <= 360:
            return f"🟡 {label}: stale ({age_min:.0f}m)"
        return f"🔴 {label}: very stale ({age_min:.0f}m)"
    except Exception:
        return f"🔴 {label}: invalid timestamp"


l1, l2 = st.columns(2)
artifact_ts = ui_advice.timestamp
live_ts = st.session_state.get('last_live_refresh_ts', None)
l1.caption(_freshness_badge(artifact_ts, "Artifact timestamp"))
l2.caption(_freshness_badge(live_ts, "Live wallet/price refresh"))

with st.expander("Advanced stats"):
    st.json(
        {
            "display_action": display_action,
            "live_advice": ui_advice.__dict__,
            "forecast": forecast.__dict__,
            "drift": drift.__dict__,
            "wallet": snap.__dict__,
            "portfolio_advice": (portfolio_advice.__dict__ if portfolio_advice is not None else None),
        }
    )

st.info("Advisory-only. No signing, no private keys, no order execution.")
