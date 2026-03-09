from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.components import artifact_banner, render_soft_card, sidebar_controls
from ui.discovery import discovery_summary
from ui.explorer import explorer_paths, load_json, run_advise_now, run_explorer_preset, run_promotion
from ui.research_ops import run_research_cycle
from ui.state import init_state


st.set_page_config(page_title="Explorer", layout="wide")
init_state()
sidebar_controls()
st.title("Explorer · Strategy Scan")
st.caption("Run preset scan, inspect leaderboard, promote current signal.")
artifact_banner()

explorer_root = st.text_input("Explorer artifacts root", value=str(Path("artifacts/explorer").resolve()))
paths = explorer_paths(explorer_root)

c1, c2, c3, c4, c5 = st.columns(5)
st.session_state.setdefault("explorer_last_status", None)

if c1.button("Run FAST preset"):
    ok, msg = run_explorer_preset("fast", paths["root"])
    st.session_state["explorer_last_status"] = {"op": "explorer_fast", "ok": ok, "payload": msg}
if c2.button("Run DAILY preset"):
    ok, msg = run_explorer_preset("daily", paths["root"])
    st.session_state["explorer_last_status"] = {"op": "explorer_daily", "ok": ok, "payload": msg}
if c3.button("Promote signal (governance OK)"):
    ok, msg = run_promotion(paths["root"], governance_status="OK")
    st.session_state["explorer_last_status"] = {"op": "promotion", "ok": ok, "payload": msg}
if c4.button("Advise NOW (fast + promote + doctor)"):
    ok, status = run_advise_now(paths["root"], st.session_state["artifact_dir"], governance_status="OK")
    if isinstance(status, dict) and status.get("doctor_artifacts"):
        st.session_state["artifact_dir"] = str(status["doctor_artifacts"])
    st.session_state["explorer_last_status"] = {"op": "advise_now", "ok": ok, "payload": status}
if c5.button("Run full Research Cycle"):
    ok, status = run_research_cycle()
    lock_busy = isinstance(status, dict) and status.get("step") == "lock" and status.get("error") == "run_in_progress"
    if lock_busy:
        ok = True
        status = {
            "ok": True,
            "message": "research_cycle_already_running_in_background",
            "note": "Scheduler is active; wait for next cycle completion.",
            "last_status": load_json(Path("artifacts/research_cycle_status.json")),
        }
    st.session_state["explorer_last_status"] = {"op": "research_cycle", "ok": ok, "payload": status}

last_status = st.session_state.get("explorer_last_status")
if isinstance(last_status, dict):
    op = str(last_status.get("op", "operation"))
    ok = bool(last_status.get("ok", False))
    payload = last_status.get("payload")
    (st.success if ok else st.error)(f"{op}: {'ok' if ok else 'failed'}")
    with st.expander("Last operation details", expanded=not ok):
        st.json(payload)

if paths["lock"].exists():
    st.warning("Explorer run is currently in progress (lock active).")
    if st.button("Clear stale Explorer lock"):
        paths["lock"].unlink(missing_ok=True)
        st.success("Explorer lock cleared.")
        st.rerun()

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

st.subheader("Discovery summary")
ds = discovery_summary("artifacts/discovery")
d1, d2, d3 = st.columns(3)
with d1:
    render_soft_card("Discovered", str(ds["discovered"]))
with d2:
    render_soft_card("Surviving", str(ds["surviving"]))
with d3:
    render_soft_card("Rejected", str(ds["rejected"]))

cycle = load_json(Path("artifacts/research_cycle_status.json"))
st.subheader("Research cycle freshness")
if isinstance(cycle, dict):
    st.caption(f"Last run: {cycle.get('finished_at', cycle.get('started_at', 'n/a'))}")
    st.json(cycle)
else:
    st.info("No research cycle status yet.")
