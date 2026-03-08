from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.adapters import AdapterValidationError, ArtifactAdapter
from ui.charts import action_hist_chart, compute_drawdown, drawdown_chart, equity_chart, filter_actions, infer_kpis
from ui.components import artifact_banner, sidebar_controls
from ui.state import init_state


st.set_page_config(page_title="Backtest", layout="wide")
init_state()
sidebar_controls()
st.title("Backtest")
artifact_banner()

adapter = ArtifactAdapter(st.session_state["artifact_dir"])
raw = adapter.load_raw()
summary = raw.get("summary") if isinstance(raw.get("summary"), dict) else {}
equity = raw.get("equity") if isinstance(raw.get("equity"), pd.DataFrame) else pd.DataFrame()
actions = raw.get("actions") if isinstance(raw.get("actions"), pd.DataFrame) else pd.DataFrame()
templates = raw.get("templates") if isinstance(raw.get("templates"), pd.DataFrame) else pd.DataFrame()

# filters
f1, f2, f3, f4 = st.columns(4)
min_d = max_d = None
if not equity.empty and "ts" in equity.columns:
    ts = pd.to_datetime(equity["ts"], errors="coerce", utc=True).dropna()
    if not ts.empty:
        min_d, max_d = ts.min().date(), ts.max().date()

with f1:
    date_range = st.date_input("Date range", value=(min_d, max_d) if min_d and max_d else ())
with f2:
    action_opts = ["ALL"] + (sorted(actions["action"].dropna().astype(str).unique().tolist()) if "action" in actions.columns else [])
    action_filter = st.selectbox("Action type", action_opts)
with f3:
    horizon_opts = ["ALL"] + (sorted(actions["horizon"].dropna().astype(str).unique().tolist()) if "horizon" in actions.columns else [])
    horizon_filter = st.selectbox("Horizon", horizon_opts)
with f4:
    tcol = "template_id" if "template_id" in actions.columns else ("archetype" if "archetype" in actions.columns else None)
    tmp_opts = ["ALL"] + (sorted(actions[tcol].dropna().astype(str).unique().tolist()) if tcol else [])
    template_filter = st.selectbox("Template/Archetype", tmp_opts)

start = end = None
if isinstance(date_range, tuple) and len(date_range) == 2 and all(isinstance(d, date) for d in date_range):
    start, end = date_range

actions_f = filter_actions(actions, start=start, end=end, action_type=action_filter, horizon=horizon_filter, template=template_filter)

# KPI cards
k = infer_kpis(summary, actions_f, equity)
k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
k1.metric("Total Return", f"{(k['total_return'] or 0.0)*100:.2f}%" if k["total_return"] is not None else "n/a")
k2.metric("Max Drawdown", f"{(k['max_drawdown'] or 0.0)*100:.2f}%" if k["max_drawdown"] is not None else "n/a")
k3.metric("Profit Factor", f"{k['profit_factor']:.2f}" if k["profit_factor"] is not None else "n/a")
k4.metric("Expectancy", f"{k['expectancy']:.4f}" if k["expectancy"] is not None else "n/a")
k5.metric("Action Rate", f"{k['action_rate']:.3f}" if k["action_rate"] is not None else "n/a")
k6.metric("Turnover", f"{k['turnover']:.3f}" if k["turnover"] is not None else "n/a")
k7.metric("Calibration", f"{k['calibration_proxy']:.4f}" if k["calibration_proxy"] is not None else "n/a")

# charts
st.subheader("Equity Curve")
eq_chart = equity_chart(equity)
if eq_chart is not None:
    st.altair_chart(eq_chart, use_container_width=True)
else:
    st.info("No equity curve available")

st.subheader("Drawdown")
dd = compute_drawdown(equity)
dd_chart = drawdown_chart(dd)
if dd_chart is not None:
    st.altair_chart(dd_chart, use_container_width=True)
else:
    st.info("No drawdown data available")

st.subheader("Action Timeline / Histogram")
ah = action_hist_chart(actions_f)
if ah is not None:
    st.altair_chart(ah, use_container_width=True)
else:
    st.info("No action data available")

st.subheader("Recent Actions")
if not actions_f.empty:
    st.dataframe(actions_f.tail(100), use_container_width=True)
else:
    st.info("No actions after filters")

# optional rolling metrics
st.subheader("Rolling Metrics")
rolling_candidates = [
    adapter.paths()["equity"].parent / "rolling_metrics.csv",
    adapter.paths()["equity"].parent / "diagnostics_rolling.csv",
]
rolling_df = pd.DataFrame()
for rp in rolling_candidates:
    if rp.exists():
        try:
            rolling_df = pd.read_csv(rp)
            break
        except Exception:
            pass
if rolling_df.empty:
    st.info("No rolling metrics file found")
else:
    st.dataframe(rolling_df.tail(200), use_container_width=True)

# downloads
st.subheader("Downloads")
for name in ["summary", "equity", "actions", "bundle", "doctor", "templates_json", "templates_parquet"]:
    p = adapter.paths().get(name)
    if p and p.exists():
        mime = "application/octet-stream"
        if p.suffix == ".json":
            mime = "application/json"
        elif p.suffix == ".csv":
            mime = "text/csv"
        st.download_button(f"Download {p.name}", data=p.read_bytes(), file_name=p.name, mime=mime)

with st.expander("Raw summary"):
    st.json(summary)
