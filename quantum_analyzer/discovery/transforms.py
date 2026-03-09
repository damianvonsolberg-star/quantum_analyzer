from __future__ import annotations

import pandas as pd


def enrich_time_structure(features: pd.DataFrame) -> pd.DataFrame:
    f = features.copy()
    idx = f.index
    if hasattr(idx, "hour"):
        f["hour_sin"] = (idx.hour / 24.0)
        f["dow"] = idx.dayofweek
    return f
