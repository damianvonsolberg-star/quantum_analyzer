from __future__ import annotations

from dataclasses import dataclass, asdict
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class BacktestDiagnostics:
    calibration_proxy: float
    hit_rate_by_state: dict[str, float]
    expectancy_by_template: dict[str, float]
    action_rate: float
    turnover: float
    max_drawdown: float
    action_quality: dict[str, dict[str, float]] | None = None
    performance_by_vol_bucket: dict[str, float] | None = None
    performance_by_btc_regime: dict[str, float] | None = None
    performance_by_family: dict[str, float] | None = None
    rolling_performance: dict[str, float] | None = None
    action_consistency: float | None = None
    turnover_cost_sensitivity: dict[str, float] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    peak = equity.cummax()
    dd = equity / peak - 1.0
    return float(dd.min())


def calibration_proxy(p_up: pd.Series, realized_up: pd.Series) -> float:
    # 1 - Brier score
    p = p_up.fillna(0.5).clip(0, 1).values
    y = realized_up.fillna(0).astype(float).values
    brier = float(np.mean((p - y) ** 2)) if len(p) else 1.0
    return float(max(0.0, 1.0 - brier))


def hit_rate_by_state(states: pd.Series, pnl: pd.Series) -> dict[str, float]:
    out: dict[str, float] = {}
    if states.empty:
        return out
    tmp = pd.DataFrame({"state": states, "pnl": pnl}).dropna()
    for st, g in tmp.groupby("state"):
        out[str(st)] = float((g["pnl"] > 0).mean()) if len(g) else 0.0
    return out


def expectancy_by_template(template_ids: pd.Series, pnl: pd.Series) -> dict[str, float]:
    out: dict[str, float] = {}
    tmp = pd.DataFrame({"template": template_ids, "pnl": pnl}).dropna()
    for tid, g in tmp.groupby("template"):
        out[str(tid)] = float(g["pnl"].mean()) if len(g) else 0.0
    return out


def action_quality_metrics(actions: pd.DataFrame) -> dict[str, dict[str, float]]:
    if actions is None or actions.empty:
        return {}
    out: dict[str, dict[str, float]] = {}
    d = actions.copy()
    for a in ["BUY", "HOLD", "REDUCE", "WAIT"]:
        g = d[d["action"].astype(str).str.upper() == a]
        if g.empty:
            out[a] = {"count": 0.0, "hit_rate": 0.0, "avg_pnl": 0.0}
        else:
            out[a] = {
                "count": float(len(g)),
                "hit_rate": float((g.get("pnl", pd.Series(dtype=float)) > 0).mean()),
                "avg_pnl": float(g.get("pnl", pd.Series(dtype=float)).mean()),
            }
    return out


def performance_by_bucket(actions: pd.DataFrame, bucket_col: str) -> dict[str, float]:
    if actions is None or actions.empty or bucket_col not in actions.columns:
        return {}
    d = actions[[bucket_col, "pnl"]].dropna()
    if d.empty:
        return {}
    return {str(k): float(v["pnl"].mean()) for k, v in d.groupby(bucket_col)}


def rolling_performance(equity: pd.Series, window: int = 24) -> dict[str, float]:
    if equity is None or equity.empty:
        return {}
    r = equity.pct_change().fillna(0.0)
    rr = r.rolling(window).mean()
    return {
        "rolling_mean_return": float(rr.iloc[-1]) if len(rr) else 0.0,
        "rolling_std_return": float(r.rolling(window).std().iloc[-1]) if len(r) else 0.0,
    }


def action_consistency(actions: pd.DataFrame) -> float:
    if actions is None or actions.empty or "action" not in actions.columns:
        return 0.0
    a = actions["action"].astype(str).tolist()
    if len(a) < 2:
        return 1.0
    switches = sum(1 for i in range(1, len(a)) if a[i] != a[i - 1])
    return float(1.0 - (switches / (len(a) - 1)))


def turnover_cost_sensitivity(actions: pd.DataFrame) -> dict[str, float]:
    if actions is None or actions.empty:
        return {"turnover": 0.0, "cost_proxy": 0.0}
    t = float(actions.get("turnover_abs", pd.Series(dtype=float)).sum())
    return {"turnover": t, "cost_proxy": t * 0.5}


def export_diagnostics_bundle(
    out_dir: str | Path,
    summary: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> None:
    p = Path(out_dir)
    p.mkdir(parents=True, exist_ok=True)
    with (p / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    for name, df in tables.items():
        if df.empty:
            continue
        df.to_csv(p / f"{name}.csv", index=True)
