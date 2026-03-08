from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.adapters import AdapterValidationError, ArtifactAdapter
from ui.components import artifact_banner, render_soft_card, sidebar_controls
from ui.journal import compute_position, load_exec_log, load_fills, mark_executed_advice, record_fill, reconcile_wallet, resolve_data_dir
from ui.state import init_state
from ui.wallet import fetch_solusdt_price


st.set_page_config(page_title="Journal", layout="wide")
init_state()
sidebar_controls()
st.title("Manual Execution Journal · Audit Trail")
st.caption("Advisory-only tracker. No signing, no order execution.")
artifact_banner()

# configurable persistent directory
default_data_dir = str(resolve_data_dir(None))
data_dir = st.text_input("Journal data directory", value=st.session_state.get("journal_data_dir", default_data_dir))
st.session_state["journal_data_dir"] = data_dir

adapter = ArtifactAdapter(st.session_state["artifact_dir"])

advice_ref = ""
latest_target = 0.0
try:
    live = adapter.to_live_advice()
    latest_target = live.target_position
    advice_ref = live.timestamp
except AdapterValidationError:
    pass

raw = adapter.load_raw()
actions = raw.get("actions")
if hasattr(actions, "empty") and not actions.empty:
    row = actions.iloc[-1]
    pid = row.get("proposal_id") if "proposal_id" in actions.columns else None
    ts = row.get("ts") if "ts" in actions.columns else None
    if pid:
        advice_ref = str(pid)
    elif ts:
        advice_ref = str(ts)

st.subheader("Record fill")
c1, c2, c3, c4 = st.columns(4)
with c1:
    ts = st.text_input("Timestamp (ISO)", value=datetime.now(timezone.utc).isoformat())
with c2:
    side = st.selectbox("Side", ["BUY", "SELL"])
with c3:
    symbol = st.text_input("Symbol", value="SOLUSDT")
with c4:
    qty = st.number_input("Quantity", min_value=0.0, value=0.0, step=0.01)

c5, c6, c7 = st.columns(3)
with c5:
    price = st.number_input("Price", min_value=0.0, value=0.0, step=0.01)
with c6:
    fees = st.number_input("Fees", min_value=0.0, value=0.0, step=0.0001)
with c7:
    note = st.text_input("Note", value="")

if st.button("Save fill"):
    try:
        record_fill(
            data_dir=data_dir,
            ts=ts,
            side=side,
            symbol=symbol,
            quantity=float(qty),
            price=float(price),
            fees=float(fees),
            note=note,
            advice_ref=advice_ref,
        )
        st.success("Fill recorded")
    except Exception as e:  # noqa: BLE001
        st.error(str(e))

if st.button("Mark executed current advice"):
    try:
        mark_executed_advice(data_dir, advice_ref=advice_ref or "unknown", note=f"target_position={latest_target}")
        st.success(f"Marked executed advice: {advice_ref or 'unknown'}")
    except Exception as e:  # noqa: BLE001
        st.error(str(e))

fills = load_fills(data_dir, symbol=None)
px = None
try:
    px = fetch_solusdt_price()
except Exception:
    px = None
position = compute_position(fills, current_price=px)

st.subheader("Tactical lot snapshot")
m1, m2, m3, m4, m5 = st.columns(5)
with m1:
    render_soft_card("Net qty", f"{position.net_qty:.4f}")
with m2:
    render_soft_card("Avg entry", f"{position.avg_entry:.4f}")
with m3:
    render_soft_card("Realized PnL", f"{position.realized_pnl:+.2f}")
with m4:
    render_soft_card("Unrealized PnL", f"{position.unrealized_pnl:+.2f}")
with m5:
    render_soft_card("Market value", f"{position.market_value:+.2f}")

wallet_sol = None
ws = st.session_state.get("wallet_snapshot")
if ws is not None and hasattr(ws, "sol"):
    wallet_sol = ws.sol

rec = reconcile_wallet(position, wallet_sol)
if rec["diverged"]:
    st.warning(f"{rec['message']}: journal={rec['journal_sol']:.4f} vs wallet={rec['wallet_sol']:.4f} (diff={rec['diff_sol']:+.4f})")
else:
    st.success(rec["message"])

st.subheader("Recent fills")
if fills.empty:
    st.info("No fills recorded yet")
else:
    st.dataframe(fills.tail(100), use_container_width=True)

st.subheader("Advice execution log")
elog = load_exec_log(data_dir)
if elog.empty:
    st.info("No execution marks yet")
else:
    st.dataframe(elog.head(100), use_container_width=True)
