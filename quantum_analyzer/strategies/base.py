from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class CandidateStrategy:
    candidate_id: str
    family: str
    params: dict[str, Any] = field(default_factory=dict)
    feature_subset: str = "full_stack"
    horizon: int = 36
    regime_filter: str = "all"

    def generate_scores(self, features: pd.DataFrame) -> pd.Series:
        raise NotImplementedError

    def propose_actions(self, features: pd.DataFrame) -> pd.Series:
        s = self.generate_scores(features)
        buy = float(self.params.get("buy_threshold", 0.2))
        reduce = float(self.params.get("reduce_threshold", -0.2))
        out = pd.Series("HOLD", index=s.index)
        out[s >= buy] = "BUY"
        out[s <= reduce] = "REDUCE"
        wait_band = float(self.params.get("wait_band", 0.05))
        out[s.abs() <= wait_band] = "WAIT"
        return out
