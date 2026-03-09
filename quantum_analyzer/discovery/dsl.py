from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def eval_genome_score(genome: dict[str, Any], features: pd.DataFrame) -> pd.Series:
    kind = genome.get("kind", "single_threshold")
    if kind == "single_threshold":
        f = genome.get("feature", "micro_range_pos_24h")
        thr = float(genome.get("threshold", 0.5))
        s = features.get(f, pd.Series(0.0, index=features.index)).astype(float) - thr
        return s
    if kind == "interaction":
        a = features.get(genome.get("a", "micro_range_pos_24h"), pd.Series(0.0, index=features.index)).astype(float)
        b = features.get(genome.get("b", "meso_range_pos_7d"), pd.Series(0.0, index=features.index)).astype(float)
        w = float(genome.get("weight", 1.0))
        return w * (a * b)
    if kind == "lag_relation":
        f = features.get(genome.get("feature", "realized_vol_24h"), pd.Series(0.0, index=features.index)).astype(float)
        lag = int(genome.get("lag", 1))
        return f - f.shift(lag).fillna(f)
    if kind == "composite":
        terms = genome.get("terms", [])
        score = pd.Series(0.0, index=features.index)
        for t in terms:
            ft = features.get(t.get("feature", "micro_range_pos_24h"), pd.Series(0.0, index=features.index)).astype(float)
            score += float(t.get("weight", 1.0)) * ft
        return score
    if kind == "regime_conditional":
        base = eval_genome_score(genome.get("base", {"kind": "single_threshold"}), features)
        reg_col = genome.get("regime_col", "vol_bucket")
        target = str(genome.get("regime", "high"))
        if reg_col in features.columns:
            m = (features[reg_col].astype(str) == target).astype(float)
            return base * m
        return base * 0.0
    return pd.Series(np.zeros(len(features)), index=features.index)


def score_to_actions(score: pd.Series, rules: dict[str, Any] | None = None) -> pd.Series:
    rules = rules or {}
    buy = float(rules.get("buy", 0.2))
    reduce = float(rules.get("reduce", -0.2))
    wait = float(rules.get("wait", 0.05))
    a = pd.Series("HOLD", index=score.index)
    a[score >= buy] = "BUY"
    a[score <= reduce] = "REDUCE"
    a[score.abs() <= wait] = "WAIT"
    return a
