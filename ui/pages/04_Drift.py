from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.adapters import ArtifactAdapter
from ui.components import artifact_banner, render_headline_card, sidebar_controls
from ui.state import init_state


st.set_page_config(page_title="Drift & Governance", layout="wide")
init_state()
sidebar_controls()
st.title("Drift & Governance")
st.caption("Should we trust the model right now?")
artifact_banner()

adapter = ArtifactAdapter(st.session_state["artifact_dir"])
vm = adapter.to_drift_status()
gov = vm.raw.get("governance", {}) if isinstance(vm.raw, dict) else {}
status = (vm.governance_status or ("OK" if vm.ok else "HALT")).upper()

response = "continue advisory" if status == "OK" else ("reduce trust" if status == "WATCH" else "halt usage")
render_headline_card(status, "Governance Status", response)

c1, c2, c3 = st.columns(3)
c1.metric("Artifact Staleness", str(gov.get("artifact_staleness", "unknown")))
c2.metric("Data Staleness", str(gov.get("data_staleness", "unknown")))
c3.metric("Operator Response", response.upper())

d1, d2, d3 = st.columns(3)
d1.metric("Feature Drift", f"{float(gov.get('feature_drift', 0.0)):.4f}")
d2.metric("Calibration Drift", f"{float(gov.get('calibration_drift', 0.0)):.4f}")
d3.metric("State Occupancy Drift", f"{float(gov.get('state_occupancy_drift', 0.0)):.4f}")

x1, x2 = st.columns(2)
x1.metric("Action-Rate Drift", f"{float(gov.get('action_rate_drift', 0.0)):.4f}")
x2.metric("Cost Drift (bps)", f"{float(gov.get('cost_drift_bps', 0.0)):.4f}")

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
