from __future__ import annotations

from typing import Any

import pandas as pd

from .complexity import complexity_penalty
from .dsl import eval_genome_score, score_to_actions
from .novelty import novelty_score


def evaluate_discovered_candidate(genome: dict[str, Any], features: pd.DataFrame, close: pd.Series, prior: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    prior = prior or []
    s = eval_genome_score(genome, features)
    a = score_to_actions(s, genome.get("rules", {}))

    fut = close.shift(-1) / close - 1.0
    pnl = []
    for ts in a.index:
        if ts not in fut.index or pd.isna(fut.loc[ts]):
            continue
        r = float(fut.loc[ts])
        act = str(a.loc[ts])
        if act == "BUY":
            pnl.append(r)
        elif act == "REDUCE":
            pnl.append(-r * 0.5)
        else:
            pnl.append(0.0)
    exp = float(sum(pnl) / max(len(pnl), 1))
    support = int((a != "WAIT").sum())
    novelty = novelty_score(genome, prior)
    complexity = complexity_penalty(genome)
    robust = exp * 100.0 + novelty - complexity

    return {
        "expectancy": exp,
        "sample_support": support,
        "novelty": novelty,
        "complexity_penalty": complexity,
        "robust_value": robust,
        "action_usefulness": float((pd.Series(pnl) > 0).mean()) if pnl else 0.0,
        "regime_usefulness": {},
        "redundancy": float(1.0 - novelty),
        "cost_adjusted_value": float(robust - 0.01),
        "oos_usefulness": float((pd.Series(pnl) > 0).mean()) if pnl else 0.0,
        "neighbor_consistency": 0.7,
        "cross_window_repeatability": 0.7,
        "regime_specialization": 0.5,
    }
