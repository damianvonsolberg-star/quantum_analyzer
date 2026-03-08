from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.adapters import ArtifactAdapter


CSS = """
<style>
.block-container {padding-top: 1rem;}
.stAlert {border-radius: 10px;}
</style>
"""


def apply_theme() -> None:
    st.markdown(CSS, unsafe_allow_html=True)


def sidebar_controls() -> None:
    st.sidebar.header("Quantum Analyzer")
    st.session_state["artifact_dir"] = st.sidebar.text_input("Artifact directory", st.session_state["artifact_dir"])
    st.session_state["wallet_address"] = st.sidebar.text_input("Wallet address (BENCHMARK_WALLET)", st.session_state["wallet_address"])
    st.session_state["rpc_url"] = st.sidebar.text_input("Solana RPC URL (SOL_RPC_URL)", st.session_state["rpc_url"])
    st.session_state["refresh_seconds"] = st.sidebar.number_input(
        "Auto-refresh interval (sec)", min_value=5, max_value=3600, value=int(st.session_state["refresh_seconds"])
    )
    if st.sidebar.button("Refresh now"):
        st.rerun()


def artifact_banner() -> None:
    adapter = ArtifactAdapter(st.session_state["artifact_dir"])
    missing = adapter.required_missing()
    if missing:
        st.error(f"Missing required artifacts: {', '.join(missing)}")
    else:
        st.success("Core artifacts found.")


def load_artifacts() -> dict[str, object]:
    adapter = ArtifactAdapter(st.session_state["artifact_dir"])
    return adapter.load_raw()


def maybe_templates_df(artifacts: dict[str, object]) -> pd.DataFrame:
    raw = artifacts.get("templates")
    if isinstance(raw, pd.DataFrame):
        return raw
    if isinstance(raw, list):
        return pd.DataFrame(raw)
    return pd.DataFrame()


def render_headline_card(light: str, action_text: str, subtitle: str = "") -> None:
    colors = {
        "HALT": "#9e9e9e",
        "RED": "#e53935",
        "YELLOW": "#fdd835",
        "GREEN": "#43a047",
    }
    c = colors.get(light.upper(), "#607d8b")
    st.markdown(
        f"""
        <div style='padding:16px;border-radius:12px;border:1px solid #333;background:{c}22;'>
          <div style='font-size:42px;font-weight:800;color:{c};line-height:1.0'>{light.upper()}</div>
          <div style='font-size:24px;font-weight:700'>{action_text}</div>
          <div style='opacity:0.8'>{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
