from __future__ import annotations

import pandas as pd

from quantum_analyzer.strategies.base import CandidateStrategy


class BreakoutContinuationStrategy(CandidateStrategy):
    """Simple breakout continuation scorer.

    Uses compression + range-width style features when available.
    If required inputs are unavailable, returns explicit no-signal zeros.
    """

    def generate_scores(self, features: pd.DataFrame) -> pd.Series:
        comp_col = "compression_ratio" if "compression_ratio" in features.columns else (
            "compression_state" if "compression_state" in features.columns else None
        )
        width_col = "range_width" if "range_width" in features.columns else None

        if comp_col is None or width_col is None:
            return pd.Series(0.0, index=features.index)

        comp = features[comp_col].astype(float)
        width = features[width_col].astype(float)
        s = comp * (1.0 - width)
        return s.fillna(0.0)
