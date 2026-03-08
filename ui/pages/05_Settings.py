from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.components import artifact_banner, sidebar_controls
from ui.state import init_state


st.set_page_config(page_title="Settings", layout="wide")
init_state()
sidebar_controls()
st.title("Settings")
artifact_banner()

st.write("Current runtime settings")
st.json(
    {
        "artifact_dir": st.session_state.get("artifact_dir"),
        "wallet_address": st.session_state.get("wallet_address"),
        "rpc_url": st.session_state.get("rpc_url"),
        "refresh_seconds": st.session_state.get("refresh_seconds"),
    }
)

st.caption("All inputs are advisory/runtime only. No model training is triggered here.")
