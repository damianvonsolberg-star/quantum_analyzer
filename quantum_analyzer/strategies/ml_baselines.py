from __future__ import annotations

import numpy as np
import pandas as pd

from .base import CandidateStrategy


class InterpretableMLBaselineStrategy(CandidateStrategy):
    """Deterministic linear-proxy baseline (interpretable coefficients)."""

    def generate_scores(self, features: pd.DataFrame) -> pd.Series:
        cols = [
            "micro_range_pos_24h",
            "meso_range_pos_7d",
            "realized_vol_24h",
            "aggtrade_imbalance",
            "orderbook_imbalance",
        ]
        X = pd.DataFrame({c: features.get(c, pd.Series(0.0, index=features.index)).astype(float) for c in cols}, index=features.index)
        w = np.array([0.6, 0.8, -0.4, 0.5, 0.5], dtype=float)
        z = (X.values @ w) / max(np.linalg.norm(w), 1e-9)
        return pd.Series(z, index=features.index)
