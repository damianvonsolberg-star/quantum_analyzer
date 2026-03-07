from __future__ import annotations

import numpy as np
import pandas as pd


def _psi_numeric(expected: pd.Series, actual: pd.Series, bins: int = 10) -> float:
    e = expected.dropna().values
    a = actual.dropna().values
    if len(e) == 0 or len(a) == 0:
        return 0.0
    qs = np.quantile(e, np.linspace(0, 1, bins + 1))
    qs[0] = -np.inf
    qs[-1] = np.inf
    e_hist, _ = np.histogram(e, bins=qs)
    a_hist, _ = np.histogram(a, bins=qs)
    e_pct = np.clip(e_hist / max(e_hist.sum(), 1), 1e-6, 1)
    a_pct = np.clip(a_hist / max(a_hist.sum(), 1), 1e-6, 1)
    return float(np.sum((a_pct - e_pct) * np.log(a_pct / e_pct)))


def feature_psi(reference: pd.DataFrame, current: pd.DataFrame) -> dict[str, float]:
    cols = [c for c in reference.columns if c in current.columns]
    return {c: _psi_numeric(reference[c], current[c]) for c in cols}


def state_occupancy_drift(ref_probs: pd.DataFrame, cur_probs: pd.DataFrame) -> float:
    cols = [c for c in ref_probs.columns if c in cur_probs.columns]
    if not cols:
        return 0.0
    r = ref_probs[cols].mean().values
    c = cur_probs[cols].mean().values
    return float(np.abs(r - c).sum())


def action_rate_drift(ref_actions: pd.Series, cur_actions: pd.Series) -> float:
    rr = (ref_actions != "HOLD").mean() if len(ref_actions) else 0.0
    cr = (cur_actions != "HOLD").mean() if len(cur_actions) else 0.0
    return float(abs(cr - rr))


def cost_drift(ref_cost_bps: pd.Series, cur_cost_bps: pd.Series) -> float:
    if len(ref_cost_bps) == 0 or len(cur_cost_bps) == 0:
        return 0.0
    return float(abs(cur_cost_bps.mean() - ref_cost_bps.mean()))
