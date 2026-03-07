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
