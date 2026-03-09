from __future__ import annotations

import pandas as pd

from .base import CandidateStrategy


class BreakoutContinuationStrategy(CandidateStrategy):
    def generate_scores(self, features: pd.DataFrame) -> pd.Series:
        comp = features.get("compression_state", pd.Series(0.0, index=features.index)).astype(float)
        rng = features.get("range_width", pd.Series(0.0, index=features.index)).astype(float)
        # more compression + expanding range = breakout continuation bias
        return (comp * -1.0) + (rng * 0.1)
