from __future__ import annotations

import pandas as pd

from .base import CandidateStrategy


class MeanReversionStrategy(CandidateStrategy):
    def generate_scores(self, features: pd.DataFrame) -> pd.Series:
        mr = features.get("micro_range_pos_24h", pd.Series(0.5, index=features.index)).astype(float)
        vol = features.get("realized_vol_24h", pd.Series(0.02, index=features.index)).astype(float)
        return (0.5 - mr) * (1.0 / (1.0 + vol))
