from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.components import artifact_banner, render_soft_card, sidebar_controls
from ui.explorer import explorer_paths, load_json, run_advise_now, run_explorer_preset, run_promotion
from ui.state import init_state


st.set_page_config(page_title="Explorer", layout="wide")
init_state()
sidebar_controls()
st.title("Explorer · Strategy Scan")
st.caption("Run preset scan, inspect leaderboard, promote current signal.")
artifact_banner()

explorer_root = st.text_input("Explorer artifacts root", value=str(Path("artifacts/explorer").resolve()))
paths = explorer_paths(explorer_root)

c1, c2, c3, c4 = st.columns(4)
if c1.button("Run FAST preset"):
    ok, msg = run_explorer_preset("fast", paths["root"])
    (st.success if ok else st.error)(f"explorer fast: {msg}")
if c2.button("Run DAILY preset"):
    ok, msg = run_explorer_preset("daily", paths["root"])
    (st.success if ok else st.error)(f"explorer daily: {msg}")
if c3.button("Promote signal (governance OK)"):
    ok, msg = run_promotion(paths["root"], governance_status="OK")
    (st.success if ok else st.error)(f"promotion: {msg}")
if c4.button("Advise NOW (fast + promote + doctor)"):
    ok, status = run_advise_now(paths["root"], st.session_state["artifact_dir"], governance_status="OK")
    if isinstance(status, dict) and status.get("doctor_artifacts"):
        st.session_state["artifact_dir"] = str(status["doctor_artifacts"])
    (st.success if ok else st.error)(f"advise-now: {status}")
    st.rerun()

if paths["lock"].exists():
    st.warning("Explorer run is currently in progress (lock active).")

status = load_json(paths["root"] / "advise_now_status.json")
if isinstance(status, dict):
    st.subheader("Latest Advise NOW status")
    st.caption(f"Last run: {status.get('last_run_at', 'n/a')}")
    s1, s2, s3 = st.columns(3)
    with s1:
        render_soft_card("Step", str(status.get("step", "n/a")).upper())
    with s2:
        render_soft_card("Doctor", "PASS" if status.get("ok") else "FAIL")
    with s3:
        render_soft_card("Message", str(status.get("message", "n/a")))
    st.markdown("**What to do now:** If doctor is PASS, use Live Advice immediately; otherwise re-run Advise NOW and inspect doctor report.")
    with st.expander("Advise NOW raw status"):
        st.json(status)

manifest = load_json(paths["manifest"])
if isinstance(manifest, dict):
    st.subheader("Latest run manifest")
    m1, m2, m3 = st.columns(3)
    with m1:
        render_soft_card("Preset", str(manifest.get("preset", "n/a")).upper())
    with m2:
        render_soft_card("Succeeded", str(manifest.get("succeeded", "n/a")))
    with m3:
        render_soft_card("Failed", str(manifest.get("failed", "n/a")))
    with st.expander("Run manifest details"):
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
