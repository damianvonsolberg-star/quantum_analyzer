from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import json

import pandas as pd
import streamlit as st
import tempfile

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.adapters import AdapterValidationError, ArtifactAdapter
from quantum_analyzer.backtest.engine import BacktestConfig
from quantum_analyzer.backtest.walkforward import WalkForwardConfig
from quantum_analyzer.experiments.evaluator import build_candidate, evaluate_candidate
from quantum_analyzer.features.feature_store import load_feature_snapshot
from quantum_analyzer.features.subsets import resolve_feature_subset
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
from ui.state import init_state, persist_artifact_dir


st.set_page_config(page_title="Backtest", layout="wide")
init_state()
sidebar_controls()
st.title("Backtest · Performance Lab")
st.caption("Advanced diagnostics are below; headline advisory remains on Live Advice.")
artifact_banner()

selected_artifact_dir = st.session_state["artifact_dir"]
adapter = ArtifactAdapter(selected_artifact_dir)
raw = adapter.load_raw()
chart_source_run = Path(selected_artifact_dir).name if selected_artifact_dir else "unknown"
split_reason = "none"
summary = raw.get("summary") if isinstance(raw.get("summary"), dict) else {}
bundle = raw.get("bundle") if isinstance(raw.get("bundle"), dict) else {}
equity = raw.get("equity") if isinstance(raw.get("equity"), pd.DataFrame) else pd.DataFrame()
actions = raw.get("actions") if isinstance(raw.get("actions"), pd.DataFrame) else pd.DataFrame()
templates = raw.get("templates") if isinstance(raw.get("templates"), pd.DataFrame) else pd.DataFrame()

# Backtest page should prefer a run with chartable outputs.
if equity.empty and actions.empty:
    exp_root = ROOT / "artifacts" / "explorer" / "experiments"
    if exp_root.exists():
        candidates = [d for d in exp_root.iterdir() if d.is_dir() and (d / "artifact_bundle.json").exists() and (d / "equity_curve.csv").exists() and (d / "actions.csv").exists()]
        if candidates:
            latest_rich = sorted(candidates, key=lambda d: d.stat().st_mtime, reverse=True)[0]
            st.session_state["artifact_dir"] = str(latest_rich)
            persist_artifact_dir(str(latest_rich))
            adapter = ArtifactAdapter(str(latest_rich))
            raw = adapter.load_raw()
            summary = raw.get("summary") if isinstance(raw.get("summary"), dict) else {}
            bundle = raw.get("bundle") if isinstance(raw.get("bundle"), dict) else {}
            equity = raw.get("equity") if isinstance(raw.get("equity"), pd.DataFrame) else pd.DataFrame()
            actions = raw.get("actions") if isinstance(raw.get("actions"), pd.DataFrame) else pd.DataFrame()
            templates = raw.get("templates") if isinstance(raw.get("templates"), pd.DataFrame) else pd.DataFrame()
            chart_source_run = latest_rich.name
            split_reason = "latest_advisory_run_not_chartable_no_actions_or_equity"
            st.info(f"Backtest page auto-switched to latest chartable run: {latest_rich.name}")

artifact_ts = None
if isinstance(bundle.get("artifact_meta"), dict):
    artifact_ts = bundle.get("artifact_meta", {}).get("produced_at") or bundle.get("artifact_meta", {}).get("latest_timestamp")
if artifact_ts:
    st.caption(f"🕒 Artifact timestamp: {artifact_ts}")
else:
    st.caption("🕒 Artifact timestamp: not available")

adv_ts = "n/a"
adv_src = "n/a"
adv_p = ROOT / "artifacts" / "promoted" / "advisory_latest.json"
if adv_p.exists():
    try:
        aj = json.loads(adv_p.read_text(encoding="utf-8"))
        adv_ts = str(aj.get("updated_at") or aj.get("timestamp") or "n/a")
        if isinstance(aj.get("source_ids"), dict):
            adv_src = str(aj["source_ids"].get("leaderboard") or "n/a")
    except Exception:
        pass
st.caption(f"Advisory source timestamp: {adv_ts} · Chart source run: {chart_source_run}")
st.caption(f"Advisory source id/path: {adv_src}")
st.caption(f"Chart source timestamp: {artifact_ts or 'n/a'} · Split reason: {split_reason}")


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


def _try_recompute_replay(run_id: str) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    try:
        reg_p = ROOT / "artifacts" / "explorer" / "registry.parquet"
        if not reg_p.exists():
            return pd.DataFrame(), pd.DataFrame(), "registry_missing"
        reg = pd.read_parquet(reg_p)
        row = reg.loc[reg["experiment_id"] == run_id]
        if row.empty:
            return pd.DataFrame(), pd.DataFrame(), "run_not_in_registry"
        r = row.iloc[0]
        snapshot_id = str(r.get("snapshot_id", ""))
        feature_subset = str(r.get("feature_subset", "full_stack"))
        regime_slice = str(r.get("regime_slice", "all"))
        horizon = int(r.get("horizon", 12) or 12)
        window_bars = int(r.get("window_bars", 720) or 720)
        test_bars = int(r.get("test_bars", 120) or 120)
        policy = r.get("policy_params", {}) or {}

        feats_full = load_feature_snapshot(ROOT / "artifacts" / "features", snapshot_id)
        close = feats_full["close"].astype(float)
        f_all = feats_full.tail(window_bars).copy()
        c_all = close.reindex(f_all.index).copy()

        # regime slice on full frame first
        if regime_slice == "all":
            mask = pd.Series(True, index=f_all.index)
        else:
            if "realized_vol_24h" not in f_all.columns:
                return pd.DataFrame(), pd.DataFrame(), "missing_realized_vol_for_regime_slice"
            vol = f_all["realized_vol_24h"].astype(float)
            q1 = float(vol.quantile(0.33))
            q2 = float(vol.quantile(0.66))
            if regime_slice == "low_vol":
                mask = vol <= q1
            elif regime_slice == "mid_vol":
                mask = (vol > q1) & (vol <= q2)
            elif regime_slice == "high_vol":
                mask = vol > q2
            else:
                return pd.DataFrame(), pd.DataFrame(), f"unknown_regime_slice:{regime_slice}"
        f_reg = f_all.loc[mask].copy()
        c_reg = c_all.reindex(f_reg.index).copy()
        if f_reg.empty:
            return pd.DataFrame(), pd.DataFrame(), "empty_after_regime_slice"

        cols = resolve_feature_subset(feature_subset)
        missing = [c for c in cols if c not in f_reg.columns]
        if missing:
            return pd.DataFrame(), pd.DataFrame(), f"missing_subset_cols:{','.join(missing[:5])}"
        f = f_reg[cols].copy()
        c = c_reg.copy()

        fam = str((policy.get("candidate_family") or r.get("candidate_family") or "trend"))
        params = dict(policy.get("candidate_params", {}) or {})
        candidate_id = str(r.get("candidate_id") or f"replay:{run_id}")
        candidate = build_candidate(candidate_id, fam, params, feature_subset, horizon, regime_slice)

        wf = WalkForwardConfig(
            train_bars=max(test_bars * 5, int(test_bars * 1.5)),
            test_bars=test_bars,
            purge_bars=6,
            embargo_bars=6,
        )
        bt = BacktestConfig(
            turnover_cap=float((policy.get("turnover_cap") if isinstance(policy, dict) else None) or 0.1),
            round_trip_cost_bps=float((policy.get("round_trip_cost_bps") if isinstance(policy, dict) else None) or 20.0),
            initial_equity=1_000_000.0,
            symbol=str(r.get("price_source_symbol") or "SOLUSDT"),
        )

        with tempfile.TemporaryDirectory(prefix="qa_replay_") as td:
            _ = evaluate_candidate(features=f, close=c, candidate=candidate, walkforward=wf, backtest=bt, out_dir=td)
            eq_p = Path(td) / "equity_curve.csv"
            ac_p = Path(td) / "actions.csv"
            if not (eq_p.exists() and ac_p.exists()):
                return pd.DataFrame(), pd.DataFrame(), "replay_artifacts_missing"
            eq = pd.read_csv(eq_p)
            ac = pd.read_csv(ac_p)
            return eq, ac, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), f"replay_error:{e}"


if equity.empty and actions.empty:
    eq2, ac2, err = _try_recompute_replay(chart_source_run)
    if not eq2.empty or not ac2.empty:
        equity, actions = _normalize_ts(eq2, "ts"), _normalize_ts(ac2, "ts")
        st.info("Replay mode: showing recomputed timeline from fresh features + current logic (not emitted actions.csv).")
    else:
        # Last-resort visibility mode: show recent timeline as explicit HOLD/no-action bars
        # so operators can still see that no actionable signals were generated.
        try:
            reg = pd.read_parquet(ROOT / "artifacts" / "explorer" / "registry.parquet")
            rr = reg.loc[reg["experiment_id"] == chart_source_run]
            if not rr.empty:
                snapshot_id = str(rr.iloc[0].get("snapshot_id", ""))
                if snapshot_id:
                    feats = load_feature_snapshot(ROOT / "artifacts" / "features", snapshot_id)
                    c = feats[["close"]].copy().tail(24)
                    c = c.reset_index().rename(columns={"index": "ts"}) if "ts" not in c.columns else c
                    if "ts" not in c.columns:
                        c["ts"] = feats.index[-len(c):]
                    equity = pd.DataFrame({"ts": c["ts"], "equity": [1_000_000.0] * len(c)})
                    actions = pd.DataFrame({
                        "ts": c["ts"],
                        "action": ["HOLD"] * len(c),
                        "target_position": [0.0] * len(c),
                        "expected_edge_bps": [0.0] * len(c),
                        "expected_cost_bps": [0.0] * len(c),
                        "reason": ["no_action_generated_in_latest_run"] * len(c),
                    })
                    equity, actions = _normalize_ts(equity, "ts"), _normalize_ts(actions, "ts")
                    st.warning("No actionable signals were generated in the latest run. Showing HOLD/no-action timeline for visibility.")
        except Exception:
            pass
        if equity.empty and actions.empty and err:
            st.warning(f"Replay mode unavailable: {err}")

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
