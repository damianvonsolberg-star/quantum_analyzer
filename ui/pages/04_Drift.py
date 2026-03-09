from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.adapters import ArtifactAdapter
from ui.components import artifact_banner, render_headline_card, render_soft_card, sidebar_controls
from ui.state import init_state


st.set_page_config(page_title="Drift & Governance", layout="wide")
init_state()
sidebar_controls()
st.title("Drift & Governance · Trust Monitor")
st.caption("Governance controls advisory trust level (OK/WATCH/HALT).")
st.caption("Should we trust the model right now?")
artifact_banner()

adapter = ArtifactAdapter(st.session_state["artifact_dir"])
vm = adapter.to_drift_status()
gov = vm.raw.get("governance", {}) if isinstance(vm.raw, dict) else {}
status = (vm.governance_status or ("OK" if vm.ok else "HALT")).upper()

response = "continue advisory" if status == "OK" else ("reduce trust" if status == "WATCH" else "halt usage")
render_headline_card(status, "Governance Status", response)
st.markdown(f"**What to do now:** {response.upper()}.")

c1, c2, c3 = st.columns(3)
with c1:
    render_soft_card("Artifact Staleness", str(gov.get("artifact_staleness", "unknown")))
with c2:
    render_soft_card("Data Staleness", str(gov.get("data_staleness", "unknown")))
with c3:
    render_soft_card("Operator Response", response.upper())

d1, d2, d3 = st.columns(3)
with d1:
    render_soft_card("Feature Drift", f"{float(gov.get('feature_drift', 0.0)):.4f}")
with d2:
    render_soft_card("Calibration Drift", f"{float(gov.get('calibration_drift', 0.0)):.4f}")
with d3:
    render_soft_card("State Occupancy Drift", f"{float(gov.get('state_occupancy_drift', 0.0)):.4f}")

x1, x2 = st.columns(2)
with x1:
    render_soft_card("Action-Rate Drift", f"{float(gov.get('action_rate_drift', 0.0)):.4f}")
with x2:
    render_soft_card("Cost Drift (bps)", f"{float(gov.get('cost_drift_bps', 0.0)):.4f}")

reasons = gov.get("kill_switch_reasons", []) if isinstance(gov.get("kill_switch_reasons"), list) else vm.hard_failures
st.subheader("Kill-switch reasons")
if reasons:
    for r in reasons:
        st.error(f"- {r}")
else:
    st.info("No explicit kill-switch reason reported.")

st.subheader("Recommended operator response")
if response == "continue advisory":
    st.success("Continue advisory mode. Monitor normally.")
elif response == "reduce trust":
    st.warning("Reduce trust: keep advisory only, lower sizing confidence, wait for cleaner diagnostics.")
else:
    st.error("HALT usage until diagnostics/freshness issues are resolved.")

st.caption(f"Latest artifact timestamp: {vm.latest_timestamp or 'not available'}")

with st.expander("Raw governance payload"):
    st.json(gov if gov else vm.raw)
