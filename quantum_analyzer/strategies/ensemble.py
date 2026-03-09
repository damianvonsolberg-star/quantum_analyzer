from __future__ import annotations

import pandas as pd

from .base import CandidateStrategy


class EnsembleStrategy(CandidateStrategy):
    def __init__(self, *args, members: list[CandidateStrategy] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.members = members or []

    def generate_scores(self, features: pd.DataFrame) -> pd.Series:
        if not self.members:
            return pd.Series(0.0, index=features.index)
        mats = [m.generate_scores(features).reindex(features.index).fillna(0.0) for m in self.members]
        return pd.concat(mats, axis=1).mean(axis=1)
