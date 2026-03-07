from __future__ import annotations

import numpy as np
import pandas as pd


def calibration_drift(ref_p: pd.Series, ref_y: pd.Series, cur_p: pd.Series, cur_y: pd.Series) -> float:
    def brier(p: pd.Series, y: pd.Series) -> float:
        if len(p) == 0 or len(y) == 0:
            return 0.0
        pp = p.astype(float).clip(0, 1).values
        yy = y.astype(float).values
        return float(np.mean((pp - yy) ** 2))

    return float(abs(brier(cur_p, cur_y) - brier(ref_p, ref_y)))
