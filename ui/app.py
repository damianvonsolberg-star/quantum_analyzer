from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.components import apply_theme, artifact_banner, sidebar_controls
from ui.state import init_state


st.set_page_config(page_title="Quantum Analyzer", page_icon="⚛️", layout="wide")
apply_theme()
init_state()
sidebar_controls()

st.title("⚛️ Quantum Analyzer UI")
st.caption("Advisory-only shell. No training/retraining is triggered by UI.")
artifact_banner()

st.markdown(
    """
Use the pages in the left sidebar:
- Live Advice
- Backtest
- Templates
- Drift
- Settings
"""
)
