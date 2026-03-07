from __future__ import annotations

import numpy as np
import pandas as pd

from quantum_analyzer.contracts import StateBelief


def normalized_entropy(probabilities: np.ndarray) -> float:
    p = np.asarray(probabilities, dtype=float)
    p = np.clip(p, 1e-12, 1.0)
    p = p / p.sum()
    k = len(p)
    return float(-(p * np.log(p)).sum() / np.log(max(k, 2)))


def probs_to_state_beliefs(
    probs: pd.DataFrame,
    symbol: str,
) -> list[StateBelief]:
    beliefs: list[StateBelief] = []
    for ts, row in probs.iterrows():
        p = row.values.astype(float)
        beliefs.append(
            StateBelief(
                ts=ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                symbol=symbol,
                regime_probabilities={k: float(v) for k, v in row.items()},
                entropy=normalized_entropy(p),
                confidence=float(np.max(p)),
            )
        )
    return beliefs
