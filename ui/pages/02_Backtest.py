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
from ui.charts import (
    action_hist_chart,
    compute_drawdown,
    drawdown_chart,
    equity_chart,
    fetch_solusdc_price_series,
    filter_actions,
    infer_kpis,
    signal_price_overlay_chart,
)
from ui.components import artifact_banner, render_soft_card, sidebar_controls
from ui.state import init_state


st.set_page_config(page_title="Backtest", layout="wide")
init_state()
sidebar_controls()
st.title("Backtest · Performance Lab")
st.caption("Advanced diagnostics are below; headline advisory remains on Live Advice.")
artifact_banner()

adapter = ArtifactAdapter(st.session_state["artifact_dir"])
raw = adapter.load_raw()
summary = raw.get("summary") if isinstance(raw.get("summary"), dict) else {}
bundle = raw.get("bundle") if isinstance(raw.get("bundle"), dict) else {}
equity = raw.get("equity") if isinstance(raw.get("equity"), pd.DataFrame) else pd.DataFrame()
actions = raw.get("actions") if isinstance(raw.get("actions"), pd.DataFrame) else pd.DataFrame()
templates = raw.get("templates") if isinstance(raw.get("templates"), pd.DataFrame) else pd.DataFrame()

artifact_ts = None
if isinstance(bundle.get("artifact_meta"), dict):
    artifact_ts = bundle.get("artifact_meta", {}).get("produced_at") or bundle.get("artifact_meta", {}).get("latest_timestamp")
if artifact_ts:
    st.caption(f"🕒 Artifact timestamp: {artifact_ts}")
else:
    st.caption("🕒 Artifact timestamp: not available")


def _normalize_ts(df: pd.DataFrame, ts_col: str = "ts") -> pd.DataFrame:
    if df is None or df.empty or ts_col not in df.columns:
        return df
    out = df.copy()
    s = out[ts_col]
    # If ts is numeric bar-index style (small integers), create synthetic hourly timeline ending at artifact time.
    if pd.api.types.is_numeric_dtype(s):
        s_num = pd.to_numeric(s, errors="coerce")
        if s_num.notna().any() and float(s_num.max()) < 10_000_000_000:  # not epoch ms/us/ns
            try:
                end_ts = pd.to_datetime(artifact_ts, utc=True, errors="coerce") if artifact_ts else pd.Timestamp.utcnow().tz_localize("UTC")
            except Exception:
                end_ts = pd.Timestamp.utcnow().tz_localize("UTC")
            n = len(out)
            out[ts_col] = pd.date_range(end=end_ts, periods=n, freq="h", tz="UTC")
            return out
    return out


equity = _normalize_ts(equity, "ts")
actions = _normalize_ts(actions, "ts")

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
st.markdown("**Backtest summary:** higher return/profit factor and lower drawdown are better.")
k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
with k1:
    render_soft_card("Total Return", f"{(k['total_return'] or 0.0)*100:.2f}%" if k["total_return"] is not None else "n/a")
with k2:
    render_soft_card("Max Drawdown", f"{(k['max_drawdown'] or 0.0)*100:.2f}%" if k["max_drawdown"] is not None else "n/a")
with k3:
    render_soft_card("Profit Factor", f"{k['profit_factor']:.2f}" if k["profit_factor"] is not None else "n/a")
with k4:
    render_soft_card("Expectancy", f"{k['expectancy']:.4f}" if k["expectancy"] is not None else "n/a")
with k5:
    render_soft_card("Action Rate", f"{k['action_rate']:.3f}" if k["action_rate"] is not None else "n/a")
with k6:
    render_soft_card("Turnover", f"{k['turnover']:.3f}" if k["turnover"] is not None else "n/a")
with k7:
    render_soft_card("Calibration", f"{k['calibration_proxy']:.4f}" if k["calibration_proxy"] is not None else "n/a")

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

st.subheader("Signal Overlay on SOLUSDC Price")
price_df = pd.DataFrame()
if not actions_f.empty and ("ts" in actions_f.columns or "timestamp" in actions_f.columns):
    ts_col = "ts" if "ts" in actions_f.columns else "timestamp"
    ts_vals = pd.to_datetime(actions_f[ts_col], errors="coerce", utc=True).dropna()
    if not ts_vals.empty:
        price_df = fetch_solusdc_price_series(ts_vals.min().isoformat(), ts_vals.max().isoformat(), interval="1h")

ov = signal_price_overlay_chart(price_df, actions_f)
if ov is not None:
    st.altair_chart(ov, use_container_width=True)
else:
    st.info("Price overlay unavailable (missing actions/timestamps or Binance fetch unavailable)")

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
