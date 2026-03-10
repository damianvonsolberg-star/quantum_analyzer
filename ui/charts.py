from __future__ import annotations

from typing import Any

import altair as alt
import pandas as pd
import requests


def compute_drawdown(equity_df: pd.DataFrame) -> pd.DataFrame:
    if equity_df is None or equity_df.empty or "equity" not in equity_df.columns:
        return pd.DataFrame()
    out = equity_df.copy()
    if "ts" not in out.columns:
        out = out.reset_index(drop=True)
        out["ts"] = out.index.astype(str)
    out["peak"] = out["equity"].cummax()
    out["drawdown"] = (out["equity"] / out["peak"]) - 1.0
    return out


def _pick(d: dict[str, Any], keys: list[str], default: float | None = None) -> float | None:
    for k in keys:
        if k in d and d[k] is not None:
            try:
                return float(d[k])
            except Exception:
                continue
    return default


def infer_kpis(summary: dict[str, Any], actions: pd.DataFrame, equity: pd.DataFrame) -> dict[str, float | None]:
    s = summary or {}
    diag = s.get("diagnostics", {}) if isinstance(s.get("diagnostics"), dict) else {}

    total_return = _pick(s, ["return_pct", "total_return", "total_return_pct"])
    max_drawdown = _pick(diag, ["max_drawdown", "max_drawdown_pct"], _pick(s, ["max_drawdown", "max_drawdown_pct"]))
    profit_factor = _pick(diag, ["profit_factor"], _pick(s, ["profit_factor"]))
    expectancy = _pick(diag, ["expectancy"], _pick(s, ["expectancy"]))
    action_rate = _pick(diag, ["action_rate"], _pick(s, ["action_rate"]))
    turnover = _pick(diag, ["turnover"], _pick(s, ["turnover"]))
    calibration = _pick(diag, ["calibration_proxy", "calibration_error"], _pick(s, ["calibration_proxy", "calibration_error"]))

    if action_rate is None and actions is not None and not actions.empty and equity is not None and not equity.empty:
        action_rate = float(len(actions)) / max(float(len(equity)), 1.0)

    return {
        "total_return": total_return,
        "max_drawdown": max_drawdown,
        "profit_factor": profit_factor,
        "expectancy": expectancy,
        "action_rate": action_rate,
        "turnover": turnover,
        "calibration_proxy": calibration,
    }


def filter_actions(
    actions: pd.DataFrame,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
    action_type: str = "ALL",
    horizon: str = "ALL",
    template: str = "ALL",
) -> pd.DataFrame:
    if actions is None or actions.empty:
        return pd.DataFrame()
    out = actions.copy()
    ts_col = "ts" if "ts" in out.columns else ("timestamp" if "timestamp" in out.columns else None)
    if ts_col:
        out["_ts"] = pd.to_datetime(out[ts_col], errors="coerce", utc=True)
        if start is not None:
            out = out[out["_ts"] >= pd.Timestamp(start, tz="UTC")]
        if end is not None:
            out = out[out["_ts"] <= pd.Timestamp(end, tz="UTC")]
    if action_type != "ALL" and "action" in out.columns:
        out = out[out["action"] == action_type]
    if horizon != "ALL" and "horizon" in out.columns:
        out = out[out["horizon"].astype(str) == horizon]
    if template != "ALL":
        tcol = "template_id" if "template_id" in out.columns else ("archetype" if "archetype" in out.columns else None)
        if tcol:
            out = out[out[tcol].astype(str) == template]
    return out.drop(columns=[c for c in ["_ts"] if c in out.columns])


def _chart_style(c):
    return c.properties(height=280).configure_axis(labelFontSize=11, titleFontSize=12, gridOpacity=0.2).configure_view(strokeOpacity=0.15)


def equity_chart(equity_df: pd.DataFrame):
    if equity_df is None or equity_df.empty or "equity" not in equity_df.columns:
        return None
    d = equity_df.copy()
    x = "ts" if "ts" in d.columns else d.columns[0]
    d[x] = pd.to_datetime(d[x], errors="coerce", utc=True)
    d = d.dropna(subset=[x])
    c = alt.Chart(d).mark_line(strokeWidth=2.4, color="#7dd3fc").encode(
        x=alt.X(f"{x}:T", title="Time", axis=alt.Axis(format="%b %d %H:%M", labelAngle=0)),
        y=alt.Y("equity", title="Equity"),
        tooltip=[alt.Tooltip(f"{x}:T", title="Time"), alt.Tooltip("equity:Q", format=",.2f")],
    ).interactive()
    return _chart_style(c)


def drawdown_chart(drawdown_df: pd.DataFrame):
    if drawdown_df is None or drawdown_df.empty or "drawdown" not in drawdown_df.columns:
        return None
    d = drawdown_df.copy()
    x = "ts" if "ts" in d.columns else d.columns[0]
    d[x] = pd.to_datetime(d[x], errors="coerce", utc=True)
    d = d.dropna(subset=[x])
    c = alt.Chart(d).mark_area(color="#f97316", opacity=0.35).encode(
        x=alt.X(f"{x}:T", title="Time", axis=alt.Axis(format="%b %d %H:%M", labelAngle=0)),
        y=alt.Y("drawdown", title="Drawdown"),
        tooltip=[alt.Tooltip(f"{x}:T", title="Time"), alt.Tooltip("drawdown:Q", format=".4f")],
    ).interactive()
    return _chart_style(c)


def action_hist_chart(actions_df: pd.DataFrame):
    if actions_df is None or actions_df.empty or "action" not in actions_df.columns:
        return None
    d = actions_df.copy()
    counts = d.groupby("action", as_index=False).size()
    c = alt.Chart(counts).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
        x=alt.X("action", title="Action"),
        y=alt.Y("size", title="Count"),
        color=alt.Color("action:N", legend=None),
    )
    return _chart_style(c)


def fetch_solusdc_price_series(start_ts: str, end_ts: str, interval: str = "1h") -> pd.DataFrame:
    """Fetch SOLUSDC klines from Binance for overlay charts.

    Returns empty DataFrame on network/API failures (UI should degrade gracefully).
    """
    try:
        s_ms = int(pd.Timestamp(start_ts).tz_convert("UTC").timestamp() * 1000)
        e_ms = int(pd.Timestamp(end_ts).tz_convert("UTC").timestamp() * 1000)
    except Exception:
        return pd.DataFrame()

    url = "https://api.binance.com/api/v3/klines"
    rows: list[list[Any]] = []
    cursor = s_ms
    try:
        while cursor < e_ms:
            r = requests.get(
                url,
                params={
                    "symbol": "SOLUSDC",
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": e_ms,
                    "limit": 1000,
                },
                timeout=10,
            )
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            rows.extend(batch)
            last_open = int(batch[-1][0])
            if len(batch) < 1000 or last_open <= cursor:
                break
            cursor = last_open + 1
    except Exception:
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows, columns=[
        "open_time_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time_ms",
        "quote_asset_volume",
        "n_trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ])
    out["ts"] = pd.to_datetime(out["open_time_ms"].astype("int64"), unit="ms", utc=True)
    out["close"] = out["close"].astype(float)
    return out[["ts", "close"]]


def signal_price_overlay_chart(price_df: pd.DataFrame, actions_df: pd.DataFrame):
    if price_df is None or price_df.empty:
        return None
    p0 = price_df.copy()
    p0["ts"] = pd.to_datetime(p0["ts"], errors="coerce", utc=True)
    p0 = p0.dropna(subset=["ts"]).sort_values("ts")
    p0["ts_label"] = p0["ts"].dt.strftime("%Y-%m-%d %H:%M UTC")

    base = alt.Chart(p0).mark_line(color="#4fc3f7", strokeWidth=2.6).encode(
        x=alt.X("ts:T", title="Time", axis=alt.Axis(format="%b %d %H:%M", labelAngle=0)),
        y=alt.Y("close:Q", title="SOLUSDC price"),
        tooltip=[alt.Tooltip("ts_label:N", title="Time"), alt.Tooltip("close:Q", title="Price", format=",.2f")],
    )

    if actions_df is None or actions_df.empty or "action" not in actions_df.columns:
        return _chart_style(base.interactive())

    d = actions_df.copy()
    ts_col = "ts" if "ts" in d.columns else ("timestamp" if "timestamp" in d.columns else None)
    if ts_col is None:
        return _chart_style(base.interactive())
    d["ts"] = pd.to_datetime(d[ts_col], errors="coerce", utc=True)
    d = d.dropna(subset=["ts"]).copy()
    d["ts_label"] = d["ts"].dt.strftime("%Y-%m-%d %H:%M UTC")
    if d.empty:
        return _chart_style(base.interactive())

    # align nearest price for markers
    p = p0.set_index("ts").sort_index()
    d = d.sort_values("ts")
    d["close"] = p["close"].reindex(d["ts"], method="nearest").values
    d["size"] = d["action"].astype(str).str.upper().map({"SHORT": 260, "SELL": 260, "REDUCE": 220, "LONG": 160, "BUY": 160, "HOLD": 130, "GO FLAT": 220}).fillna(150)

    color = alt.Color("action:N", scale=alt.Scale(domain=["BUY", "LONG", "HOLD", "REDUCE", "SELL", "SHORT", "GO FLAT"], range=["#66bb6a", "#7cb342", "#fdd835", "#ff7043", "#ef5350", "#e53935", "#ab47bc"]))
    shape = alt.Shape("action:N", scale=alt.Scale(domain=["BUY", "LONG", "HOLD", "REDUCE", "SELL", "SHORT", "GO FLAT"], range=["circle", "triangle-up", "circle", "triangle-down", "diamond", "cross", "square"]))

    pts = alt.Chart(d).mark_point(filled=True, opacity=0.95).encode(
        x="ts:T",
        y="close:Q",
        color=color,
        shape=shape,
        size=alt.Size("size:Q", legend=None),
        tooltip=[
            alt.Tooltip("ts_label:N", title="Signal time"),
            alt.Tooltip("action:N", title="Action"),
            alt.Tooltip("close:Q", title="Price", format=",.2f"),
        ],
    )

    return _chart_style((base + pts).interactive())
