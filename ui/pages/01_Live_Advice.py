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
from ui.state import init_state, load_json
from ui.view_models import UiPortfolioSnapshot
from ui.wallet import (
    WalletFetchError,
    fetch_sol_price_helius,
    fetch_solusdt_24h_change_pct,
    fetch_solusdt_price,
    fetch_wallet_balances,
    resolve_rpc_url,
    resolve_wallet,
)


st.set_page_config(page_title="Live Advice", layout="wide")
init_state()
sidebar_controls()
st.title("Live Advice · Decision Desk")
artifact_banner()

cycle = load_json(Path("artifacts/research_cycle_status.json"))
if isinstance(cycle, dict):
    state = str(cycle.get("state", "unknown")).lower()
    step = cycle.get("current_cmd")
    if isinstance(step, list) and step:
        step_txt = " ".join(str(x) for x in step[:4])
    else:
        step_txt = "idle"
    if state == "running":
        st.info(f"Research cycle: RUNNING · {step_txt}")
    elif state == "failed":
        err = str(cycle.get("error", "unknown_error"))
        if len(err) > 220:
            err = err[:220] + " …"
        st.warning(f"Research cycle: FAILED · {err}")
        with st.expander("Research cycle failure details"):
            st.json(cycle)
    else:
        st.caption(f"Research cycle: {state.upper()} · last finish: {cycle.get('finished_at', 'n/a')}")

adapter = ArtifactAdapter(st.session_state["artifact_dir"])

try:
    ui_advice = adapter.to_live_advice()
    drift = adapter.to_drift_status()
    forecast = adapter.to_forecast_view()
except AdapterValidationError as e:
    st.error(f"Invalid/missing artifact data: {e}")
    st.stop()

def _fetch_live_price_helius(rpc_url: str) -> tuple[float, float | None]:
    try:
        px, chg24 = fetch_sol_price_helius(rpc_url=rpc_url)
        if chg24 is None:
            try:
                chg24 = fetch_solusdt_24h_change_pct()
            except WalletFetchError:
                chg24 = None
        return float(px), (float(chg24) if chg24 is not None else None)
    except WalletFetchError:
        # fallback only when Helius price endpoint is unavailable
        px = float(fetch_solusdt_price())
        try:
            chg24 = float(fetch_solusdt_24h_change_pct())
        except WalletFetchError:
            chg24 = None
        return px, chg24


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
    px, chg24 = _fetch_live_price_helius(rpc_url)
    st.session_state["sol_24h_change_pct"] = chg24
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

# lightweight live ticker loop (price every few seconds)
auto_secs = st.session_state.get("live_ticker_seconds", 5)
if hasattr(st, "autorefresh"):
    st.autorefresh(interval=int(auto_secs * 1000), key="live_ticker_autorefresh")

try:
    rpc_url_live = resolve_rpc_url(st.session_state.get("rpc_url"))
    px_live, chg24_live = _fetch_live_price_helius(rpc_url_live)
    st.session_state["sol_24h_change_pct"] = chg24_live
    ws = st.session_state.get("wallet_snapshot")
    if isinstance(ws, UiPortfolioSnapshot):
        ws.sol_price_usd = float(px_live)
        if ws.sol is not None:
            ws.sol_mtm_usd = float(ws.sol) * float(px_live)
        if ws.usdc is not None and ws.sol_mtm_usd is not None:
            ws.total_nav_usd = float(ws.usdc) + float(ws.sol_mtm_usd)
            if ws.total_nav_usd > 0:
                ws.current_sol_weight = float(ws.sol_mtm_usd) / float(ws.total_nav_usd)
        st.session_state["wallet_snapshot"] = ws
        st.session_state["last_live_refresh_ts"] = datetime.now(timezone.utc).isoformat()
except Exception:
    pass

snap = st.session_state.get("wallet_snapshot")
if not isinstance(snap, UiPortfolioSnapshot):
    snap = UiPortfolioSnapshot(wallet="", sol=0.0, usdc=0.0, ok=False, message="Wallet not refreshed yet", sol_price_usd=None, total_nav_usd=None, current_sol_weight=0.0, dry_powder_usd=0.0)

# Lightweight live ticker context
curr_px = float(snap.sol_price_usd) if snap.sol_price_usd is not None else None
prev_px = st.session_state.get("_prev_sol_price_usd")
if curr_px is not None:
    st.session_state["_prev_sol_price_usd"] = curr_px

px_delta = None
px_delta_pct = None
if curr_px is not None and isinstance(prev_px, (int, float)) and prev_px > 0:
    px_delta = curr_px - float(prev_px)
    px_delta_pct = (px_delta / float(prev_px)) * 100.0

rec = decide_recommendation(ui_advice, snap, drift)
if ui_advice.status:
    # Canonical advisory-driven rendering path (no heuristic recomputation for trust headline).
    canonical_light = "HALT" if (ui_advice.governance_status or "").upper() == "HALT" else ("RED" if (ui_advice.release_state or "").upper() in {"NO_EDGE", "LOW_EDGE"} or (ui_advice.status or "").lower() in {"no_edge", "stale_cycle", "insufficient_evidence", "missing_signal_bundle"} else ui_advice.traffic_light.upper())
    rec.light = canonical_light
    rec.action_text = ui_advice.headline_action
    if ui_advice.reasons:
        rec.top_reasons = list(ui_advice.reasons)
    if ui_advice.invalidation_notes:
        rec.top_risks = list(ui_advice.invalidation_notes)

portfolio_advice = None
action_eligible = (str(ui_advice.status or "").lower() == "approved") and (ui_advice.headline_action in {"BUY", "REDUCE", "HOLD"})
if action_eligible and snap.ok and snap.total_nav_usd is not None and snap.sol_price_usd is not None and ui_advice.target_position is not None:
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

# Canonical operator action comes from final advisory artifact.
display_action = ui_advice.headline_action

h1, h2 = st.columns([3.2, 1.8])
with h1:
    render_headline_card(rec.light, display_action, "Simple advisory view (read-only)")
with h2:
    chg24 = st.session_state.get("sol_24h_change_pct")
    if curr_px is None:
        render_soft_card("SOL Live Ticker", "n/a", "Price unavailable")
    else:
        up = (chg24 is not None and float(chg24) >= 0)
        arrow = "▲" if up else "▼"
        col = "#66bb6a" if up else "#ef5350"
        sub = "24h change unavailable"
        if chg24 is not None:
            sub = f"{arrow} {float(chg24):+.2f}% (24h)"
        st.markdown(
            f"""
            <div class='qa-card' style='padding:12px 14px;'>
              <div class='qa-title'>SOL LIVE TICKER</div>
              <div class='qa-value'>${curr_px:,.2f}</div>
              <div class='qa-sub' style='color:{col};font-weight:700'>{sub}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

status_l = str(ui_advice.status or "").lower()
release_l = str(ui_advice.release_state or "").upper()
gov_l = str(ui_advice.governance_status or "").upper()
if status_l in {"no_edge", "stale_cycle", "insufficient_evidence", "missing_signal_bundle"} or release_l in {"NO_EDGE", "LOW_EDGE"}:
    helper_txt = "Do not act: no reliable edge / reduced trust."
elif gov_l == "WATCH":
    helper_txt = "Use reduced trust; advisory is not action-ready yet."
elif rec.light == "HALT":
    helper_txt = "Do not act until resolved."
elif rec.light in {"WATCH", "YELLOW"}:
    helper_txt = "Proceed cautiously."
else:
    helper_txt = "Normal advisory confidence."

st.markdown(f"**What to do now:** {display_action}. {helper_txt}  ")
st.caption("Ticker refreshes automatically every few seconds via Helius pricing.")

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
        render_soft_card("Target spot allocation", (f"{ui_advice.target_position*100:.2f}%" if ui_advice.target_position is not None else "n/a"))

s1, s2, s3 = st.columns(3)
s1.metric("Confidence", f"{(ui_advice.confidence or 0.0):.2f}")
s2.metric("Entropy", f"{(ui_advice.entropy or 0.0):.2f}")
edge_txt = f"{ui_advice.expected_edge_bps:.2f}" if ui_advice.expected_edge_bps is not None else "n/a"
cost_txt = f"{ui_advice.expected_cost_bps:.2f}" if ui_advice.expected_cost_bps is not None else "n/a"
s3.metric("Edge vs Cost (bps)", f"{edge_txt} / {cost_txt}")

gov_status = (drift.governance_status or ("OK" if drift.ok else "HALT")).upper()
gov_reasons = drift.hard_failures[:3]

st.markdown(f"**Governance status:** {gov_status}")
if gov_reasons:
    st.markdown("**Governance reasons:** " + ", ".join(gov_reasons))

if portfolio_advice is not None:
    st.markdown(f"**Main spot recommendation:** {display_action}")
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

st.markdown("**Top alternatives (why they lost):**")
if ui_advice.top_alternatives:
    for a in ui_advice.top_alternatives[:3]:
        st.markdown(f"- {a}")
else:
    st.markdown("- n/a")

st.markdown("**Invalidation notes:** " + (", ".join(ui_advice.invalidation_notes[:3]) if ui_advice.invalidation_notes else "n/a"))

def _freshness_badge(ts_str: str | None, label: str) -> str:
    if not ts_str:
        return f"🔴 {label}: missing"
    try:
        t = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        age_min = (now - t).total_seconds() / 60.0

        # freshness should follow configured research cadence, not fixed legacy windows
        cycle_min = int(st.session_state.get("research_cycle_minutes", 15) or 15)
        fresh_cut = max(5, cycle_min * 2)
        stale_cut = max(fresh_cut + 1, cycle_min * 4)

        if age_min <= fresh_cut:
            return f"🟢 {label}: fresh ({age_min:.0f}m)"
        if age_min <= stale_cut:
            return f"🟡 {label}: stale ({age_min:.0f}m)"
        return f"🔴 {label}: very stale ({age_min:.0f}m)"
    except Exception:
        return f"🔴 {label}: invalid timestamp"


l1, l2 = st.columns(2)
# Freshness should track the freshest trustworthy pipeline timestamp.
artifact_ts = drift.latest_timestamp or ui_advice.timestamp
cycle_ts = None
if isinstance(cycle, dict):
    cycle_ts = cycle.get("finished_at") or cycle.get("started_at")

def _max_ts(a: str | None, b: str | None) -> str | None:
    try:
        ta = datetime.fromisoformat(str(a).replace("Z", "+00:00")) if a else None
    except Exception:
        ta = None
    try:
        tb = datetime.fromisoformat(str(b).replace("Z", "+00:00")) if b else None
    except Exception:
        tb = None
    if ta and tb:
        return a if ta >= tb else b
    return a or b

artifact_ts = _max_ts(artifact_ts, cycle_ts)
live_ts = st.session_state.get('last_live_refresh_ts', None)
l1.caption(_freshness_badge(artifact_ts, "Artifact timestamp"))
l2.caption(_freshness_badge(live_ts, "Live wallet/price refresh"))

st.caption(f"Artifact dir in use: {st.session_state.get('artifact_dir')}")

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
