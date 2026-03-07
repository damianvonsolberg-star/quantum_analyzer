from __future__ import annotations

import numpy as np
import pandas as pd


def compute_volatility_features(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    log_ret = np.log(close / close.shift(1))
    rv_24h = log_ret.rolling(24, min_periods=24).std() * np.sqrt(24)
    rv_7d = log_ret.rolling(24 * 7, min_periods=24 * 7).std() * np.sqrt(24)

    out["realized_vol_24h"] = rv_24h
    out["realized_vol_7d"] = rv_7d
    out["vol_of_vol_24h"] = rv_24h.rolling(24, min_periods=24).std()

    tr = pd.concat(
        [
            (high - low).abs(),
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr_24 = tr.rolling(24, min_periods=24).mean()
    out["atr_pct_24h"] = atr_24 / close.replace(0.0, np.nan)

    return out
