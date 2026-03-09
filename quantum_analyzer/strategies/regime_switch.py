from __future__ import annotations

import pandas as pd

from .base import CandidateStrategy


class RegimeSwitchStrategy(CandidateStrategy):
    def generate_scores(self, features: pd.DataFrame) -> pd.Series:
        vol = features.get("realized_vol_24h", pd.Series(0.02, index=features.index)).astype(float)
        trend = features.get("meso_range_pos_7d", pd.Series(0.5, index=features.index)).astype(float) - 0.5
        # lower vol -> follow trend, high vol -> fade trend
        return trend.where(vol < vol.median(), -trend)
