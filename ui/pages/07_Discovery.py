from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.components import artifact_banner, render_soft_card, sidebar_controls
from ui.discovery import discovery_summary
from ui.state import init_state

st.set_page_config(page_title="Discovery · Signal Lab", layout="wide")
init_state()
sidebar_controls()
artifact_banner()

st.title("Discovery · Signal Lab")
s = discovery_summary("artifacts/discovery")

c1, c2, c3, c4 = st.columns(4)
with c1:
    render_soft_card("Discovered", str(s["discovered"]))
with c2:
    render_soft_card("Surviving", str(s["surviving"]))
with c3:
    render_soft_card("Rejected", str(s["rejected"]))
with c4:
    render_soft_card("Advisory source", "DISCOVERED" if s["uses_discovered_signal"] else "BASELINE")

st.subheader("Surviving signals")
st.dataframe(pd.DataFrame(s["survivors"]))

st.subheader("Rejected signals")
st.dataframe(pd.DataFrame(s["rejections"]))

with st.expander("Discovery memory / report"):
    st.write("See artifacts/discovery/discovery_report.md, signal_genealogy.json, novelty_scores.csv")
