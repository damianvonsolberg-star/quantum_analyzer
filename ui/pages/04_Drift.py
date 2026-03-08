from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.adapters import ArtifactAdapter
from ui.components import artifact_banner, render_headline_card, sidebar_controls
from ui.drift_view import build_drift_view
from ui.state import init_state


st.set_page_config(page_title="Drift & Governance", layout="wide")
init_state()
sidebar_controls()
st.title("Drift & Governance")
st.caption("Should we trust the model right now?")
artifact_banner()

adapter = ArtifactAdapter(st.session_state["artifact_dir"])
raw = adapter.load_raw()
doctor = raw.get("doctor") if isinstance(raw.get("doctor"), dict) else {}
bundle = raw.get("bundle") if isinstance(raw.get("bundle"), dict) else {}

# optional drift payload locations
optional_drift = {}
if isinstance(bundle.get("drift"), dict):
    optional_drift = bundle.get("drift", {})
elif isinstance(bundle.get("monitoring"), dict) and isinstance(bundle.get("monitoring", {}).get("drift"), dict):
    optional_drift = bundle.get("monitoring", {}).get("drift", {})

vm = build_drift_view(doctor, optional_drift)

render_headline_card(vm.overall_status, "Governance Status", vm.recommended_response)

c1, c2, c3 = st.columns(3)
c1.metric("Artifact Freshness", vm.artifact_freshness)
c2.metric("Data Freshness", vm.data_freshness)
c3.metric("Operator Response", vm.recommended_response.upper())

d1, d2, d3 = st.columns(3)
d1.metric("Feature Drift", vm.feature_drift)
d2.metric("Calibration Drift", vm.calibration_drift)
d3.metric("State Occupancy Drift", vm.state_occupancy_drift)

x1, x2 = st.columns(2)
x1.metric("Action-Rate Drift", vm.action_rate_drift)
x2.metric("Cost/Slippage Drift", vm.cost_drift)

st.subheader("Kill-switch reasons")
if vm.kill_switch_reasons:
    for r in vm.kill_switch_reasons:
        st.error(f"- {r}")
else:
    st.info("No explicit kill-switch reason reported.")

st.subheader("Recommended operator response")
if vm.recommended_response == "continue advisory":
    st.success("Continue advisory mode. Monitor normally.")
elif vm.recommended_response == "reduce trust":
    st.warning("Reduce trust: keep advisory only, lower sizing confidence, wait for cleaner diagnostics.")
else:
    st.error("HALT usage until diagnostics/freshness issues are resolved.")

st.caption(f"Latest artifact timestamp: {vm.latest_artifact_ts or 'not yet implemented'}")
st.caption(f"Latest live timestamp: {vm.latest_live_ts or 'not yet implemented'}")

with st.expander("Raw diagnostics"):
    st.json({"doctor": doctor, "drift": optional_drift})
