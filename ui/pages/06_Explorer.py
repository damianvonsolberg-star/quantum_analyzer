from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.components import artifact_banner, sidebar_controls
from ui.explorer import explorer_paths, load_json, run_explorer_preset, run_promotion
from ui.state import init_state


st.set_page_config(page_title="Explorer", layout="wide")
init_state()
sidebar_controls()
st.title("Explorer")
st.caption("Run preset scan, inspect leaderboard, promote current signal.")
artifact_banner()

explorer_root = st.text_input("Explorer artifacts root", value=str(Path("artifacts/explorer").resolve()))
paths = explorer_paths(explorer_root)

c1, c2, c3 = st.columns(3)
if c1.button("Run FAST preset"):
    ok, msg = run_explorer_preset("fast", paths["root"])
    (st.success if ok else st.error)(f"explorer fast: {msg}")
if c2.button("Run DAILY preset"):
    ok, msg = run_explorer_preset("daily", paths["root"])
    (st.success if ok else st.error)(f"explorer daily: {msg}")
if c3.button("Promote signal (governance OK)"):
    ok, msg = run_promotion(paths["root"], governance_status="OK")
    (st.success if ok else st.error)(f"promotion: {msg}")

if paths["lock"].exists():
    st.warning("Explorer run is currently in progress (lock active).")

manifest = load_json(paths["manifest"])
if isinstance(manifest, dict):
    st.subheader("Latest run manifest")
    st.json(manifest)
else:
    st.info("No run_manifest.json yet. Run a preset scan.")

st.subheader("Leaderboard")
if paths["leaderboard_parquet"].exists():
    try:
        lb = pd.read_parquet(paths["leaderboard_parquet"])
        st.dataframe(lb.head(50), use_container_width=True)
    except Exception as e:  # noqa: BLE001
        st.error(f"Failed to load leaderboard.parquet: {e}")
elif paths["leaderboard_json"].exists():
    lbj = load_json(paths["leaderboard_json"])
    st.json(lbj)
else:
    st.info("No leaderboard output yet.")

promoted = load_json(paths["promoted_bundle"])
st.subheader("Current promoted signal")
if isinstance(promoted, dict):
    st.json(promoted)
else:
    st.info("No promoted signal bundle yet. Run promotion after explorer.")
