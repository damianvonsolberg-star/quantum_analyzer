from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

ACTIONS = ["flat", "long", "short", "breakout_follow", "fade"]


@dataclass
class PayoffStats:
    action: str
    expectancy: float
    pf_proxy: float
    support: int
    ci_low: float
    ci_high: float


def _bootstrap_ci(x: np.ndarray, n_boot: int = 200, alpha: float = 0.05) -> tuple[float, float]:
    if len(x) == 0:
        return (0.0, 0.0)
    rng = np.random.default_rng(42)
    means = np.empty(n_boot)
    for i in range(n_boot):
        sample = rng.choice(x, size=len(x), replace=True)
        means[i] = float(np.mean(sample))
    lo = float(np.quantile(means, alpha / 2))
    hi = float(np.quantile(means, 1 - alpha / 2))
    return (lo, hi)


def future_returns(close: pd.Series, horizons: tuple[int, int, int] = (12, 36, 72)) -> pd.DataFrame:
    out = pd.DataFrame(index=close.index)
    for h in horizons:
        out[f"ret_{h}h"] = close.shift(-h) / close - 1.0
    return out


def action_payoff(ret_36h: pd.Series, action: str) -> pd.Series:
    r = ret_36h.fillna(0.0)
    if action == "flat":
        return pd.Series(np.zeros(len(r)), index=r.index)
    if action == "long":
        return r
    if action == "short":
        return -r
    if action == "breakout_follow":
        return r.where(r > 0, 0.0)
    if action == "fade":
        return (-r).where(r > 0, r.abs())
    raise ValueError(action)


def summarize_action_surface(ret_36h: pd.Series) -> dict[str, PayoffStats]:
    out: dict[str, PayoffStats] = {}
    for a in ACTIONS:
        p = action_payoff(ret_36h, a).values.astype(float)
        exp = float(np.mean(p)) if len(p) else 0.0
        wins = p[p > 0].sum()
        losses = abs(p[p < 0].sum())
        pf = float(wins / losses) if losses > 0 else (999.0 if wins > 0 else 0.0)
        lo, hi = _bootstrap_ci(p)
        out[a] = PayoffStats(action=a, expectancy=exp, pf_proxy=pf, support=len(p), ci_low=lo, ci_high=hi)
    return out
