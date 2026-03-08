from __future__ import annotations

import numpy as np
import pandas as pd


def _zscore(s: pd.Series, window: int) -> pd.Series:
    m = s.rolling(window, min_periods=window).mean()
    sd = s.rolling(window, min_periods=window).std()
    return (s - m) / sd.replace(0.0, np.nan)


def compute_structural_break_features(
    close: pd.Series,
    funding_rate: pd.Series,
    oi: pd.Series,
) -> pd.DataFrame:
    out = pd.DataFrame(index=close.index)

    ret = np.log(close / close.shift(1))
    out["break_ret_zscore_24h"] = _zscore(ret, 24)
    out["break_ret_zscore_7d"] = _zscore(ret, 24 * 7)

    if funding_rate is None or funding_rate.empty:
        out["funding_rate"] = np.nan
    else:
        fr = funding_rate.astype(float).reindex(close.index).ffill()
        out["funding_rate"] = fr

    if oi is None or oi.empty:
        out["oi_zscore"] = np.nan
    else:
        oi_s = oi.astype(float).reindex(close.index).ffill()
        out["oi_zscore"] = _zscore(oi_s, 24 * 7)

    return out
