from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.adapters import ArtifactAdapter
from ui.state import latest_operator_artifact_dir, persist_artifact_dir


CSS = """
<style>
.block-container {
  padding-top: 0.9rem;
  max-width: 1320px;
  padding-bottom: 2rem;
}

/* Typography polish */
h1, h2, h3 {
  letter-spacing: -0.25px;
  line-height: 1.15;
}
p, li, .stCaption { line-height: 1.35; }

/* Alerts + expanders */
.stAlert {
  border-radius: 14px;
  border: 1px solid rgba(255,255,255,0.10);
}
.streamlit-expanderHeader {
  border-radius: 12px;
  font-weight: 600;
}

/* Apple-like soft cards */
.qa-card {
  border: 1px solid rgba(255,255,255,0.10);
  background: linear-gradient(180deg, rgba(255,255,255,0.065), rgba(255,255,255,0.03));
  border-radius: 16px;
  padding: 14px 16px;
  backdrop-filter: blur(8px);
  -webkit-backdrop-filter: blur(8px);
  box-shadow: 0 8px 24px rgba(0,0,0,0.14);
}
.qa-title {
  font-size: 12px;
  opacity: 0.72;
  margin-bottom: 6px;
  text-transform: uppercase;
  letter-spacing: 0.45px;
}
.qa-value {
  font-size: 24px;
  font-weight: 720;
  line-height: 1.08;
}
.qa-sub {
  font-size: 12px;
  opacity: 0.74;
  margin-top: 7px;
}

/* Inputs / controls */
.stTextInput input, .stNumberInput input {
  border-radius: 11px !important;
}
.stSelectbox > div > div {
  border-radius: 11px;
}

/* Buttons */
.stButton > button {
  border-radius: 12px;
  font-weight: 650;
  border: 1px solid rgba(255,255,255,0.16);
  box-shadow: 0 2px 8px rgba(0,0,0,0.18);
}

/* Dataframes look cleaner */
[data-testid="stDataFrame"] {
  border-radius: 12px;
  overflow: hidden;
  border: 1px solid rgba(255,255,255,0.10);
}

/* Sidebar */
[data-testid="stSidebar"] [data-testid="stMarkdown"] h2,
[data-testid="stSidebar"] [data-testid="stMarkdown"] h3 {
  letter-spacing: -0.1px;
}

/* Section rhythm */
hr {
  border: none;
  border-top: 1px solid rgba(255,255,255,0.10);
  margin: 0.8rem 0 1rem 0;
}

/* Small screens */
@media (max-width: 1200px) {
  .block-container { max-width: 100%; padding-top: 0.7rem; }
  .qa-value { font-size: 21px; }
}
@media (max-width: 900px) {
  .qa-card { padding: 12px 13px; border-radius: 14px; }
  .qa-title { font-size: 11px; }
  .qa-value { font-size: 19px; }
  .qa-sub { font-size: 11px; }
}
</style>
"""


def apply_theme() -> None:
    st.markdown(CSS, unsafe_allow_html=True)


def sidebar_controls() -> None:
    st.sidebar.header("Quantum Analyzer")

    # Always follow newest valid operator artifact dir to avoid stale pinned sessions.
    latest = latest_operator_artifact_dir()
    if latest and st.session_state.get("artifact_dir") != latest:
        st.session_state["artifact_dir"] = latest
        persist_artifact_dir(latest)

    st.session_state.setdefault("artifact_dir_input", st.session_state["artifact_dir"])
    if st.session_state["artifact_dir_input"] != st.session_state["artifact_dir"]:
        st.session_state["artifact_dir_input"] = st.session_state["artifact_dir"]

    # Ensure fallback is set BEFORE widget instantiation (Streamlit constraint).
    prefill = str(st.session_state.get("artifact_dir_input", st.session_state["artifact_dir"]) or "").strip()
    if not prefill:
        prefill = latest or st.session_state.get("artifact_dir", "")
        st.session_state["artifact_dir_input"] = prefill

    st.sidebar.text_input("Artifact directory", key="artifact_dir_input")
    candidate_dir = str(st.session_state.get("artifact_dir_input", st.session_state["artifact_dir"]) or "").strip()
    if not candidate_dir:
        candidate_dir = latest or st.session_state.get("artifact_dir", "")
    st.session_state["artifact_dir"] = candidate_dir
    persist_artifact_dir(st.session_state["artifact_dir"])
    st.session_state["wallet_address"] = st.sidebar.text_input("Wallet address (BENCHMARK_WALLET)", st.session_state["wallet_address"])
    st.session_state["rpc_url"] = st.sidebar.text_input("Solana RPC URL (SOL_RPC_URL)", st.session_state["rpc_url"])
    st.session_state["live_ticker_seconds"] = st.sidebar.number_input("Ticker refresh (sec)", min_value=2, max_value=60, value=int(st.session_state.get("live_ticker_seconds", 5)))
    st.session_state["research_cycle_minutes"] = st.sidebar.number_input("Research cycle cadence (min)", min_value=2, max_value=240, value=int(st.session_state.get("research_cycle_minutes", 15)))
    st.session_state["discovery_cycle_minutes"] = st.sidebar.number_input("Discovery cadence (min)", min_value=5, max_value=720, value=int(st.session_state.get("discovery_cycle_minutes", 60)))
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
        "WATCH": "#fb8c00",
        "RED": "#e53935",
        "YELLOW": "#fdd835",
        "GREEN": "#43a047",
    }
    c = colors.get(light.upper(), "#607d8b")
    st.markdown(
        f"""
        <div class='qa-card' style='background:{c}18;border-color:{c}66;'>
          <div style='font-size:40px;font-weight:800;color:{c};line-height:1.0'>{light.upper()}</div>
          <div style='font-size:26px;font-weight:700;margin-top:4px'>{action_text}</div>
          <div style='opacity:0.82;margin-top:6px'>{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_soft_card(title: str, value: str, subtitle: str = "") -> None:
    st.markdown(
        f"""
        <div class='qa-card'>
          <div class='qa-title'>{title}</div>
          <div class='qa-value'>{value}</div>
          <div class='qa-sub'>{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
