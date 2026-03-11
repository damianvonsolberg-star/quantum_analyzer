from __future__ import annotations

from typing import Any

import pandas as pd


def _normalize_action(action: Any) -> str:
    x = str(action or "").strip().upper()
    if x in {"BUY", "LONG", "BUY SPOT"}:
        return "BUY"
    if x in {"REDUCE", "SELL", "SHORT", "REDUCE SPOT", "GO FLAT", "FLAT"}:
        return "REDUCE"
    if x in {"WAIT", "HOLD"}:
        return x
    return "HOLD"


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _advisory_row(advisory: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(advisory, dict):
        return None

    ts_raw = advisory.get("timestamp") or advisory.get("updated_at")
    ts = pd.to_datetime(ts_raw, utc=True, errors="coerce")
    if ts is pd.NaT or ts is None or pd.isna(ts):
        return None

    return {
        "timestamp": ts,
        "action": _normalize_action(advisory.get("action") or advisory.get("action_spot") or advisory.get("action_raw")),
        "target_position": _as_float(advisory.get("target_position_spot") if advisory.get("target_position_spot") is not None else advisory.get("target_position")),
        "expected_edge_bps": _as_float(advisory.get("expected_edge_bps")),
        "expected_cost_bps": _as_float(advisory.get("expected_cost_bps")),
        "reason": str(advisory.get("reason") or "promoted_cluster_consensus"),
    }


def _ts_column(df: pd.DataFrame) -> str | None:
    if "ts" in df.columns:
        return "ts"
    if "timestamp" in df.columns:
        return "timestamp"
    return None


def apply_promoted_advisory_overlay(
    actions: pd.DataFrame,
    advisory: dict[str, Any] | None,
    *,
    max_hour_gap: float = 2.0,
) -> tuple[pd.DataFrame, bool, str | None]:
    out = actions.copy() if isinstance(actions, pd.DataFrame) else pd.DataFrame()
    adv = _advisory_row(advisory)
    if adv is None:
        return out, False, None

    ts_col = _ts_column(out)
    if ts_col is None:
        ts_col = "ts"
        out[ts_col] = pd.Series(dtype="object")
    ts = pd.to_datetime(out[ts_col], utc=True, errors="coerce") if not out.empty else pd.Series(dtype="datetime64[ns, UTC]")

    adv_hour = adv["timestamp"].floor("h")
    if not ts.empty:
        same_hour = ts.dt.floor("h") == adv_hour
        if bool(same_hour.any()):
            idx = ts[same_hour].index[-1]
            out.loc[idx, "action"] = adv["action"]
            if adv["target_position"] is not None or "target_position" in out.columns:
                out.loc[idx, "target_position"] = adv["target_position"]
            if adv["expected_edge_bps"] is not None or "expected_edge_bps" in out.columns:
                out.loc[idx, "expected_edge_bps"] = adv["expected_edge_bps"]
            if adv["expected_cost_bps"] is not None or "expected_cost_bps" in out.columns:
                out.loc[idx, "expected_cost_bps"] = adv["expected_cost_bps"]
            out.loc[idx, "reason"] = "decision_engine_promoted_consensus_override"
            out.loc[idx, "decision_source"] = "promoted_advisory_latest"
            out.loc[idx, "decision_overlay"] = True
            return out, True, "replaced_same_hour"

        ts_valid = ts.dropna()
        if not ts_valid.empty:
            lag_h = abs((adv["timestamp"] - ts_valid.max()).total_seconds()) / 3600.0
            if lag_h > max_hour_gap:
                return out, False, None

    row = {c: None for c in out.columns}
    row[ts_col] = adv["timestamp"].isoformat()
    row["action"] = adv["action"]
    row["target_position"] = adv["target_position"]
    row["expected_edge_bps"] = adv["expected_edge_bps"]
    row["expected_cost_bps"] = adv["expected_cost_bps"]
    row["reason"] = "decision_engine_promoted_consensus"
    row["decision_source"] = "promoted_advisory_latest"
    row["decision_overlay"] = True
    out = pd.concat([out, pd.DataFrame([row])], ignore_index=True)

    ts2 = pd.to_datetime(out[ts_col], utc=True, errors="coerce")
    if ts2.notna().any():
        out = out.assign(_ts_sort=ts2).sort_values("_ts_sort").drop(columns=["_ts_sort"]).reset_index(drop=True)

    return out, True, "appended_latest"
