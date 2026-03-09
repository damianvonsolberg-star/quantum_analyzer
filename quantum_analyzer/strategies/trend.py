from __future__ import annotations

import pandas as pd

from .base import CandidateStrategy


class TrendFollowingStrategy(CandidateStrategy):
    def generate_scores(self, features: pd.DataFrame) -> pd.Series:
        a = features.get("micro_range_pos_24h", pd.Series(0.0, index=features.index)).astype(float)
        b = features.get("meso_range_pos_7d", pd.Series(0.0, index=features.index)).astype(float)
        return (a + b) - 1.0
