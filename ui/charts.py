from __future__ import annotations

from typing import Any

import altair as alt
import pandas as pd


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


def equity_chart(equity_df: pd.DataFrame):
    if equity_df is None or equity_df.empty or "equity" not in equity_df.columns:
        return None
    d = equity_df.copy()
    x = "ts" if "ts" in d.columns else d.columns[0]
    return alt.Chart(d).mark_line().encode(x=x, y="equity")


def drawdown_chart(drawdown_df: pd.DataFrame):
    if drawdown_df is None or drawdown_df.empty or "drawdown" not in drawdown_df.columns:
        return None
    x = "ts" if "ts" in drawdown_df.columns else drawdown_df.columns[0]
    return alt.Chart(drawdown_df).mark_area().encode(x=x, y="drawdown")


def action_hist_chart(actions_df: pd.DataFrame):
    if actions_df is None or actions_df.empty or "action" not in actions_df.columns:
        return None
    d = actions_df.copy()
    counts = d.groupby("action", as_index=False).size()
    return alt.Chart(counts).mark_bar().encode(x="action", y="size")
