from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.adapters import ArtifactAdapter
from ui.components import artifact_banner, render_soft_card, sidebar_controls
from ui.state import init_state
from ui.templates_view import apply_template_filters, build_why_now, templates_to_view_df


st.set_page_config(page_title="Templates", layout="wide")
init_state()
sidebar_controls()
st.title("Templates · Regime Lens")
st.caption("Template diagnostics support advisory reasoning; no execution path.")
st.caption("Which historical path archetypes does the market currently resemble?")
artifact_banner()

adapter = ArtifactAdapter(st.session_state["artifact_dir"])
raw = adapter.load_raw()
templates_raw = raw.get("templates") if isinstance(raw.get("templates"), pd.DataFrame) else pd.DataFrame()
view_df = templates_to_view_df(templates_raw)

if view_df.empty:
    st.warning("No templates found (templates.json/parquet missing or empty).")
    st.stop()

f1, f2, f3, f4 = st.columns(4)
with f1:
    action_opts = ["ALL"] + sorted(view_df["preferred_action"].dropna().astype(str).unique().tolist())
    action_filter = st.selectbox("Action", action_opts)
with f2:
    horizon_opts = ["ALL"] + sorted(view_df["preferred_horizon"].dropna().astype(str).unique().tolist())
    horizon_filter = st.selectbox("Horizon", horizon_opts)
with f3:
    robustness_threshold = st.slider("Robustness threshold", min_value=0.0, max_value=1.0, value=0.0, step=0.05)
with f4:
    min_sample_count = st.number_input("Minimum sample count", min_value=0, max_value=100000, value=0, step=1)

filtered = apply_template_filters(
    view_df,
    action=action_filter,
    horizon=horizon_filter,
    robustness_threshold=robustness_threshold,
    min_sample_count=int(min_sample_count),
)

st.subheader("Top matched templates")
if filtered.empty:
    st.info("No templates match current filters.")
else:
    top = filtered.head(10)
    st.dataframe(
        top[
            [
                "template_id",
                "label",
                "description",
                "sample_count",
                "expectancy",
                "pf_proxy",
                "robustness",
                "preferred_action",
                "preferred_horizon",
                "similarity",
            ]
        ],
        use_container_width=True,
    )

    if len(top) > 0:
        best = top.iloc[0]
        tone = "bullish" if str(best["preferred_action"]).upper() in {"BUY", "LONG"} else ("bearish" if str(best["preferred_action"]).upper() in {"SELL", "SHORT", "REDUCE", "HEDGE"} else "neutral")
        st.success(
            f"Model tone now: **{tone.upper()}** — Dominant template is **{best['label']}** (similarity {best['similarity']:.2f}, robustness {best['robustness']:.2f})."
        )
        st.markdown(f"**What to do now:** Follow **{str(best['preferred_action']).upper()}** bias with horizon **{best['preferred_horizon']}**, while keeping risk controls active.")

bundle = raw.get("bundle") if isinstance(raw.get("bundle"), dict) else {}
context_features = {}
if isinstance(bundle.get("current_context"), dict):
    context_features = bundle.get("current_context", {})
elif isinstance(bundle.get("features"), dict):
    context_features = bundle.get("features", {})

why = build_why_now(context_features, filtered if not filtered.empty else view_df)
st.subheader("Why now?")
w1, w2, w3 = st.columns(3)
with w1:
    render_soft_card("Micro range position", str(why["micro_range_position"]))
with w2:
    render_soft_card("Meso range position", str(why["meso_range_position"]))
with w3:
    render_soft_card("Macro range position", str(why["macro_range_position"]))

x1, x2 = st.columns(2)
with x1:
    render_soft_card("Current volatility state", str(why["current_volatility_state"]))
with x2:
    render_soft_card("Dominant template family", str(why["dominant_template_family"]))
st.caption(f"Cross-asset context: {why['cross_asset_context']}")

with st.expander("Advanced template details"):
    st.dataframe(filtered if not filtered.empty else view_df, use_container_width=True)
